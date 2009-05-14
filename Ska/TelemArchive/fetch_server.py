import sys
import os
import re
import cPickle
import optparse
import socket
import signal
import stat
import time
import pprint
import shutil
import logging
import Ska.TelemArchive.fetch

SKA = os.getenv('SKA') or '/proj/sot/ska'
SKA_DATA = os.path.join(SKA, 'data', 'telem_archive')

# Unlikely sequence of characters to terminate conversation
TERMINATOR = "\_$|)<~};!)}]+/)()]\;}&|&*\\%_$^^;;-+=:_;\\<|\'-_/]*?`-"

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError

class FetchJobs(object):
    def __init__(self):
        self.jobs = []

        # Find existing jobs and clean expired directories
        jobids = self.clean_jobs()

        logging.debug('Initial jobids = %s' % str(jobids))

        # Read existing job information from jobfile.  
        for jobid in jobids:
            outdir = os.path.join(opt.outroot, jobid)
            try:
                job = cPickle.load(open(os.path.join(opt.outroot, jobid, opt.jobfile)))
            except IOError:
                logging.info('Jobfile for %s failed to load during init %.1f' % outdir)
                continue
            logging.info('Adding job %s' % jobid)
            self.add(job)

    def clean_jobs(self):
        """
        Clean job directories and job structures that have expired or are incomplete.
        :rtype: Cleaned list of jobids
        """
        jobids = []
        for jobid in self.get_jobids():
            outdir = os.path.join(opt.outroot, jobid)
            if time.time() - os.stat(outdir)[stat.ST_MTIME] > opt.max_age * 86400:
                logging.info('Removing directory %s because age exceeds %.1f' % (outdir, opt.max_age))
                shutil.rmtree(outdir)
            elif not os.path.exists(os.path.join(outdir, opt.jobfile)):
                logging.info('Removing directory %s because jobfile not foudn' % outdir)
                shutil.rmtree(outdir)
            else:
                jobids.append(jobid)

        return jobids
                
    def get_jobids(self):
        jobids = (int(x) for x in os.listdir(opt.outroot) if re.match('\d+$', x))
        return [str(x) for x in sorted(jobids)]

    def create_job(self):
        """Create a new job"""
        jobids = self.get_jobids()
        jobid = max([1] + [int(x) for x in jobids]) + 1
        jobid = str(jobid)
        outdir = os.path.join(opt.outroot, jobid)

        job = dict(jobid=jobid,
                   systime_start=time.time(),
                   outdir=outdir,
                   status='starting',
                   url=os.path.join(opt.urlroot, jobid, opt.outfile),
                   jobfile=os.path.join(opt.outroot, jobid, opt.jobfile),
                   statusfile=os.path.join(outdir, opt.statusfile),
                   outfile=os.path.join(outdir, opt.outfile),
                   )
        self.add(job)

        os.makedirs(outdir)
        cPickle.dump(job, open(job['jobfile'], 'w'))
        cPickle.dump(job, open(job['statusfile'], 'w'))

        return job

    def add(self, job):
        self.jobs.insert(0, job)

    def _n_active(self):
        return len([x for x in self.jobs if x['status'] == 'active'])

    n_active = property(_n_active)

    def update_jobs(self):
        """Read statusfiles and update corresponding job structure and file"""
        jobs = []
        for job in self.jobs:
            if os.path.exists(job['statusfile']):
                job.update(cPickle.load(open(job['statusfile'])))
                cPickle.dump(job, open(job['jobfile'], 'w'))
                jobs.append(job)
            else:
                logging.warning('No status file for %s, removing job' % job['jobid'])
        self.jobs = jobs
            
def run_fetch(job, kwargs):
    # Only pay attention to these keys in kwargs.  Others are ignored.
    allowed_keys = ('obsid', 'start', 'stop', 'dt', 'out_format',
                    'time_format', 'colspecs')
    
    fetch_kwargs = dict(outfile=job['outfile'],
                        statusfile=job['statusfile'],
                        status_interval=opt.status_interval,
                        max_size=opt.max_size,
                        ignore_quality=False,
                        mind_the_gaps=False,
                        start=None,
                        stop=None,
                        dt=32.8,
                        out_format='csv',
                        time_format='secs',
                        colspecs=['ephin2eng:'])

    # Update allowed key values in fetch_kwargs
    fetch_kwargs.update((x, kwargs[x]) for x in kwargs if x in allowed_keys )

    # Incorporate fetch keyword args into job and store
    job.update(fetch_kwargs)
        
    pid = os.fork()
    if pid:
        job['pid'] = pid
        return

    logging.info('Running fetch() in pid %d with kwargs:\n%s' % (os.getpid(), pprint.pformat(fetch_kwargs)))

    try:
        headers, data = Ska.TelemArchive.fetch.fetch(**fetch_kwargs)
    except Exception, msg:
        # Something bombed so write jobfile and statusfile here
        logging.warning('Fetch failed with msg: %s' % msg)
        job['status'] = 'error'
        job['error'] = str(msg)
        logging.warning('job = %s' % pprint.pformat(job))
        cPickle.dump(job, open(job['jobfile'], 'w'))
        cPickle.dump(job, open(job['statusfile'], 'w'))

    sys.exit(0)

def server_action(action, jobs):
    cmd = action.get('cmd')
    kwargs = action.get('kwargs')

    jobs.update_jobs()

    logging.info("Server action cmd: %s" % cmd)
    if cmd == 'get_status':
        return jobs.jobs

    elif cmd == 'run_fetch':
        if jobs.n_active >= opt.max_jobs:
            logging.warning('Maximum active jobs (%d) exceeded' % opt.max_jobs)
            return [dict(error='Maximum active jobs (%d) exceeded' % opt.max_jobs)]

        job = jobs.create_job()
        run_fetch(job, kwargs)
        cPickle.dump(job, open(job['jobfile'], 'w'))    

        return jobs.jobs

    elif cmd == 'stop_server':
        logging.info('Stopping fetch server')
        return 'Stopping fetch server'

    else:
        logging.warning("Unknown cmd '%s'" % str(cmd))
        return "Unknown cmd '%s'" % str(cmd)

def sigint_handler(signal, frame):
    raise Exception

def server():
    logging.basicConfig(filename=opt.logfile, level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')

    jobs = FetchJobs()

    # establish server
    try:
        service = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        service.bind(("", opt.port))
        service.listen(1)
    except socket.error:
        logging.debug("Socket already in use (probably normal)")
        sys.exit(0)

    logging.info("Listening on port %d" % opt.port)

    while True:
        # serve forever
        channel, info = service.accept()
        logging.info("Connection from %s on port %d" % (str(info), opt.port))

        prev_alarm_handler = signal.signal(signal.SIGALRM, timeout_handler)
        try:
            signal.alarm(8)

            # Get the message from client
            msg_recv = ''
            while TERMINATOR not in msg_recv:
                msg_recv += channel.recv(1024)
            msg_recv = cPickle.loads(msg_recv[:-len(TERMINATOR)])

            # Respond to the request from the received message
            server_response = server_action(msg_recv, jobs)
            msg_send = cPickle.dumps(server_response) + TERMINATOR

            # Send the response
            while msg_send:
                n = channel.send(msg_send)
                msg_send = msg_send[n:]

            signal.alarm(0)
            signal.signal(signal.SIGALRM, prev_alarm_handler)
        except TimeoutError, e:
            logging.warning('TimeoutError')

        channel.close() # disconnect

        if msg_recv['cmd'] == 'stop_server':
            break

    service.close()

def get_options():
    parser = optparse.OptionParser()
    parser.set_defaults()
    parser.add_option("--logfile",
                      default=os.path.join(SKA_DATA, 'server.log'),
                      help="Logfile")
    parser.add_option("--outroot",
                      default='/proj/sot/ska/www/media/fetch',
                      help="Root directory for fetch output")
    parser.add_option("--urlroot",
                      default='/media/fetch',
                      help="URL root")
    parser.add_option("--outfile",
                      default='telem.dat',
                      help="Output (data) file name")
    parser.add_option("--statusfile",
                      default='status.dat',
                      help="Status file name")
    parser.add_option("--jobfile",
                      default='job.dat',
                      help="Job file name")
    parser.add_option("--status-interval",
                      default=4,
                      type=int,
                      help="Interval between status file updates (sec)")
    parser.add_option("--max-size",
                      default=10000000,
                      type=int,
                      help="Maximum output file size (bytes)")
    parser.add_option("--max-jobs",
                      default=2,
                      type=int,
                      help="Maximum active fetch jobs",)
    parser.add_option("--max-age",
                      default=3,
                      type=float,
                      help="Maximum age for fetch output files before deletion (days)",)
    parser.add_option("--port",
                      default=18001,
                      type=int,
                      help="Socket port")

    (opt, args) = parser.parse_args()
    return (opt, args)

if __name__ == '__main__':
    global opt
    opt, args = get_options()
    server()
