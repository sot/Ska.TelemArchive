import sys
import os
import re
import cPickle
import socket
import signal
import time
import pprint
import shutil
import logging
import Ska.TelemArchive.fetch

SKA = os.getenv('SKA') or '/proj/sot/ska'
SKA_DATA = os.path.join(SKA, 'data', 'telem_archive')

# user-accessible port
PORT = 18039

# Unlikely sequence of characters to terminate conversation
TERMINATOR = "\_$|)<~};!)}]+/)()]\;}&|&*\\%_$^^;;-+=:_;\\<|\'-_/]*?`-"

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError

# Fake optparse object.  Fix this sometime...
class Options(object):
    def __init__(self, **kwargs):
        for key in kwargs:
            setattr(self, key, kwargs[key])

opt = Options(max_age=3,
              logfile=os.path.join(SKA_DATA, 'server.log'),
              outroot='/proj/sot/ska/www/media/fetch',
              urlroot='/media/fetch',
              outfile='telem.dat',
              statusfile='status.dat',
              jobfile='job.dat',
              status_interval=4,
              max_size=10 * 1e6,
              max_jobs=2,
              )

class FetchJobs(object):
    def __init__(self):
        self.jobs = []

        # Read in existing jobs
        for pid in os.listdir(opt.outroot):
            job = cPickle.load(open(os.path.join(opt.outroot, pid, opt.jobfile)))
            self.add(job)

        self.update_status()

    def add(self, job):
        self.jobs.insert(0, job)

    def _n_active(self):
        return len([x for x in self.jobs if x['status'] == 'active'])

    n_active = property(_n_active)

    def update_status(self):
        jobs = []
        for job in self.jobs:
            # Delete jobs and their output directories as needed
            if  (time.time() - job['systime_start'] > opt.max_age * 86400):
                if os.path.exists(job['outdir']):
                    logging.info('Removing directory %s because age exceeds %.1f' % (job['outdir'], opt.max_age))
                    shutil.rmtree(job['outdir'])
            else:
                if os.path.exists(job['statusfile']):
                    job.update(cPickle.load(open(job['statusfile'])))
                jobs.append(job)
                cPickle.dump(job, open(job['jobfile'], 'w'))
        self.jobs = jobs
            
jobs = FetchJobs()

def run_fetch(kwargs):
    # Only pay attention to these keys in kwargs.  Others are ignored.
    allowed_keys = ('start', 'stop', 'dt', 'out_format',
                    'time_format', 'colspecs')

    pid = os.fork()

    outdir = os.path.join(opt.outroot, str(pid or os.getpid()))
    fetch_kwargs = dict(outfile=os.path.join(outdir, opt.outfile),
                        statusfile=os.path.join(outdir, opt.statusfile),
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

    if pid:
        job = dict(pid=pid,
                   systime_start=time.time(),
                   outdir=outdir,
                   status='active',
                   url=os.path.join(opt.urlroot, str(pid), opt.outfile),
                   jobfile=os.path.join(opt.outroot, str(pid), opt.jobfile),
                   )
        job.update(fetch_kwargs)
        return job

    logging.info('Running fetch() with kwargs:\n%s' % pprint.pformat(fetch_kwargs))

    if not os.path.exists(outdir):
        os.makedirs(outdir)
    try:
        headers, data = Ska.TelemArchive.fetch.fetch(**fetch_kwargs)
    except Exception, msg:
        logging.warning('Fetch failed with msg: %s' % msg)
        # Something bombed so write statusfile manually
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        fetch_kwargs['status'] = 'error'
        fetch_kwargs['error'] = msg
        cPickle.dump(fetch_kwargs, open(fetch_kwargs['statusfile'], 'w'))

    sys.exit(0)

def server_action(action):
    cmd = action.get('cmd')
    kwargs = action.get('kwargs')

    jobs.update_status()

    logging.info("Server action cmd: %s" % cmd)
    if cmd == 'get_status':
        return jobs.jobs
    elif cmd == 'run_fetch':
        if jobs.n_active >= opt.max_jobs:
            logging.warning('Maximum active jobs (%d) exceeded' % opt.max_jobs)
            return dict(error='Maximum active jobs (%d) exceeded' % opt.max_jobs)
        job = run_fetch(kwargs)
        jobs.add(job)
        time.sleep(1)
        return jobs.jobs
    elif cmd == 'stop_server':
        return 'Stopping fetch server'
    else:
        return "Unknown cmd '%s'" % str(cmd)

def sigint_handler(signal, frame):
    raise Exception

def server():
    # global opt
    # opt, args = get_options()

    # establish server
    try:
        service = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        service.bind(("localhost", PORT))
        service.listen(1)
    except:
        raise

    logging.basicConfig(filename=opt.logfile, level=logging.INFO)
    logging.info("Listening on port %d" % PORT)

    while True:
        # serve forever
        channel, info = service.accept()
        logging.info("Connection from %s" % str(info))

        prev_alarm_handler = signal.signal(signal.SIGALRM, timeout_handler)
        try:
            signal.alarm(8)

            # Get the message from client
            msg_recv = ''
            while TERMINATOR not in msg_recv:
                msg_recv += channel.recv(1024)
            msg_recv = cPickle.loads(msg_recv[:-len(TERMINATOR)])

            # Respond to the request from the received message
            msg_send = cPickle.dumps(server_action(msg_recv)) + TERMINATOR

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

if __name__ == '__main__':
    server()
