#!/usr/bin/env python
"""
Fetch values from the SKA telemetry archive.
"""
__docformat__ = 'restructuredtext'
import os
import sys
import re
import time
import pyfits
import Chandra.Time
from mx.DateTime import strptime, DateTime, Error, DateTimeDeltaFromSeconds
from Ska.TelemArchive.data_table import DataColumn, DateNotInTable
import cPickle
from pyparsing import (Word, alphanums, delimitedList, ParseException,
                       Optional, lineStart, lineEnd)

SKA = os.getenv('SKA') or '/proj/sot/ska'
SKA_DATA = SKA + '/data/telem_archive'

def main():
    (opt, args) = get_options()
    kwargs = opt.__dict__
    # One difference in function vs. program interface:
    kwargs['out_format'] = kwargs.get('file_format')
    del kwargs['file_format']
    fetch(colspecs=args, **kwargs)

def fetch(obsid=None,
          outfile=None,
          statusfile=None,
          status_interval=5,
          max_size=None,
          ignore_quality=False,
          mind_the_gaps=False,
          start=None,
          stop=None,
          dt=32.8,
          out_format=None,
          time_format='secs',
          sleep=None,
          colspecs=['ephin2eng:']):
    """
    Fetch data from the telemetry archive.

    :param obsid: Return data for obsid
    :param outfile: File for fetch output (default = stdout)
    :param statusfile: Write out fetch status each status-interval seconds
    :param status_interval: Time interval between statusfile update and file size check (sec)
    :param max_size: Approximate output file size limit (default = None)
    :param ignore_quality: Output bad quality rows (suppressed by default)
    :param mind_the_gaps: Abort if data gap detected (instead of just setting quality=1)
    :param start: Start date of processing
    :param stop: Stop date of processing
    :param dt: Sampling interval (sec)
    :param out_format: Format for output ('csv', 'space', 'dmascii', 'tab') (default=None => list)
    :param time_format: Format for output time stamp
    :param sleep: Time to sleep per row for debug (secs) (default=None)
    :param colspecs: List of column specifiers

    :rtype: headers, values = tuple, list of tuples
    """
    table_defs = get_table_defs(SKA_DATA + '/tables')

    # Redirect stdout and stderr if specified
    if outfile:
        sys.stdout = open(outfile, 'w')

    # Create data column objects for each requested column name (along with date and quality)
    column_defs = [{'table':'pseudo_column', 'name':'date'}]
    column_defs.extend(get_column_defs(table_defs, colspecs))
    if ignore_quality:
        column_defs.append({'table':'pseudo_column', 'name':'quality'})

    columns = [ DataColumn(x) for x in column_defs]

    output_headers = write_output(columns, out_format, 'name')
    output_values = []
    
    dates, datestart, datestop, n_dates = get_date_stamps(start, stop, dt, obsid)
    status = FetchStatus(datestart, datestop, n_dates, columns,
                         statusfile=statusfile,
                         status_interval=status_interval,
                         outfile=outfile,
                         max_size=max_size)
    status.write_statusfile()

    for i_date, date in enumerate(dates()):
        quality = 0
        if sleep:
            time.sleep(sleep)
        for column in columns:
            if column.table_type == 'pseudo_column':
                if column.name == 'date':
                    if time_format == 'secs':
                        column.value = date
                    else:
                        column.value = getattr(Chandra.Time.DateTime(date), time_format)
                elif column.name == 'quality':
                    column.value = quality
            else:
                try:
                    column.value, column.quality = column.get_value(date)
                except RuntimeError:
                    if mind_the_gaps:  # Be stringent and raise the exception
                        raise
                    else:
                        column.quality = 1
                        column.value = None
                quality |= column.quality

        if (not quality or ignore_quality):
            vals = write_output(columns, out_format, 'value')
            if out_format is None:
                output_values.append(vals)

        if status.check_now(i_date):
            status.write_statusfile()
            status.check_filesize()

    # Drop all tables currently associated with columns.  This is to force destruction of
    # any pyfits objects that are still being referenced at program exit (leaving tmp files around).
    for column in columns:
        column.drop_table()

    status.check_now(i_date)
    status.write_statusfile('done')

    return output_headers, output_values

class FetchStatus(object):
    """
    Take care of processing status operations:
     - Write a status file
     - Check output file size
    """
    def __init__(self,
                 datestart, datestop, n_dates, columns,
                 statusfile=None,
                 status_interval=None,
                 outfile=None,
                 max_size=None,
                 ):
        self.statusfile= statusfile
        self.status_interval = status_interval
        self.outfile = outfile
        self.max_size = max_size
        self.total_rows = n_dates
        self.percent_complete = 0
        self.row_interval = 100       # Check time every 100 rows
        self.last_time = 0.0
        self.process_start = time.ctime()
        self.current_row = 0
        self.current_time = time.ctime()
        self.datestart = Chandra.Time.DateTime(datestart).date
        self.datestop = Chandra.Time.DateTime(datestop).date
        self.columns = ' '.join([x.name for x in columns])
        self.error = None
        self.filesize = 0
        self.print_attrs = ('current_row', 'total_rows', 'percent_complete',
                            'process_start', 'current_time',
                            'datestart', 'datestop', 
                            'columns', 'status', 'error')
        
    def check_now(self, current_row):
        self.current_row = current_row
        if current_row % self.row_interval == 0:
            t = time.time()
            if t - self.last_time > self.status_interval:
                self.last_time = t
                return True
        else:
            return False

    def write_statusfile(self, status='active'):
        if not self.statusfile:
            return

        self.percent_complete = '%.1f' % (100. * self.current_row / self.total_rows)
        self.current_time = time.ctime()
        self.status = status
        vals = dict((x, getattr(self, x)) for x in self.print_attrs)
        cPickle.dump(vals, open(self.statusfile, 'w'))

    def check_filesize(self):
        if self.max_size and self.outfile:
            self.filesize = os.stat(self.outfile).st_size
            if self.filesize > self.max_size:
                self.error = 'file size limit %d bytes exceeded' % self.max_size
                self.write_statusfile('error')
                sys.exit(1)

class InvalidTableOrColumn(LookupError):
    """Custom exception if no matching table or column is found"""
        
def write_output(columns, out_format, attr):
    values = tuple(getattr(x, attr) for x in columns)

    field_seps = {'dmascii': ' ',
                  'space': ' ',
                  'csv': ',',
                  'tab': '\t',
                  'rdb': '\t'}
    field_sep = field_seps.get(out_format)
                 
    if out_format == 'dmascii' and attr == 'name':
        print '#',
    if out_format in field_seps:
        print field_sep.join(str(x) for x in values)
    elif out_format == 'fits':
        raise RuntimeError('Sorry, FITS output format not yet supported')

    return values

def get_date_stamps(start, stop, timedel, obsid):
    """Generate datetime values corresponding to a uniform sampling between
    start and stop
    """
    # Use Chandra.Time.DateTime to convert most any input format to YYYY:DOY:HH:MM:SS
    if obsid:
        from pysqlite2 import dbapi2 as sqlite
        conn = sqlite.connect(os.path.join(os.environ.get('SKA', '/proj/sot/ska'),
                                           'data/telem_archive/db.sql3'))
        cur = conn.cursor()
        cur.execute("SELECT kalman_tstart, kalman_tstop FROM observations WHERE obsid=?",
                    (obsid,))
        vals = cur.fetchall()
        if len(vals) == 0:
            raise RuntimeError, 'No observations matching obsid = %d' % obsid
        elif len(vals) > 1:
            raise RuntimeError, 'Multiple observations matching obsid = %d' % obsid
        start = vals[0][0]
        stop = vals[0][1]

    if not start or not stop:
        raise RuntimeError, 'Start and/or stop time not provided'
        
    datestart = Chandra.Time.DateTime(start).secs
    datestop  = Chandra.Time.DateTime(stop).secs
    
    def gen_date_stamps():
        date = datestart
        while (date < datestop):
            yield date
            date = date + timedel

    return gen_date_stamps, datestart, datestop, int((datestop - datestart) / timedel)

def get_column_defs(table_defs, args):
    """Parse supplied args to find table/column specifiers, and find
    those columns within the table definitions (table_defs).
    Allowed syntax::

     <table>:<col1>,...   # col1,... in specified table
     <col1>,..            # col1,... in any table (must be unique)
     @<filename>          # File <filename> containing lines in above format
    """
    def get_columns_in_table(table_name, col_names):
        """Find specified col_names in table_name.  If table_name is not
        provided then look in all tables in within table_defs.  Return
        list of dicts (table, name) for each column."""
        columns = []
        if table_name:
            if table_name not in table_defs:
                raise InvalidTableOrColumn, "No table named %s" % table_name

            table_columns = table_defs[table_name]['columns']
            skip_cols = ['time', 'quality']
            skip_cols += [x['name'] for x in table_columns if not x.get('is_output', True)]
            table_col_names = [x['name'] for x in table_columns if x['name'] not in skip_cols]

            if not col_names:
                col_names = table_col_names

            for col_name in col_names:
                if col_name not in table_col_names:
                       raise InvalidTableOrColumn, \
                             "No column %s in %s table" % (col_name, table_name)
                columns.append({'table': table_name,
                                'name' : col_name})
        else:
            for col_name in col_names:
                for table_name in table_defs:
                    try:
                        column = get_columns_in_table(table_name, [col_name])
                        break
                    except InvalidTableOrColumn:
                        pass
                else:
                    raise InvalidTableOrColumn('Column %s not found in any table' % col_name)

                columns += column

        return columns
    
    columns = []
    table_name = Word(alphanums + '_-').setResultsName("table_name") + ':'
    col_name = Word(alphanums + '_-')
    opt_table_and_cols = Optional(table_name) + \
                         delimitedList(col_name).setResultsName("col_names")
    arg_parse = lineStart + ( opt_table_and_cols | table_name ) + lineEnd
    for arg in args:
        try:
            results = arg_parse.parseString(arg)
        except ParseException, msg:
            raise ParseException("Bad column specifier syntax in %s" % arg)
            
        columns += get_columns_in_table(results.table_name, results.col_names)

    return columns

def get_table_defs(table_dir):
    """ Read all the table definition files in specified table_dir """
    from glob import glob
    import yaml
    table_files = glob(table_dir + '/*.yml')
    assert table_files, 'No table files found'

    table_def = {}
    for filename in table_files:
        table_name = re.sub(r'\.yml$', '', os.path.basename(filename))
        table_def[table_name] = yaml.load(open(filename).read())
    return table_def

def get_options():
    from optparse import OptionParser
    parser = OptionParser(usage='fetch.py [options] col_spec1 [col_spec2 ...]')
    parser.set_defaults()
    parser.add_option("--obsid",
                      type="int",
                      help="Return data for OBSID",
                      )
    parser.add_option("--outfile",
                      help="File for fetch output (default = stdout)",
                      )
    parser.add_option("--statusfile",
                      help="Write out fetch status each status-interval seconds",
                      )
    parser.add_option("--status-interval",
                      default=2,
                      type="float",
                      help="Time interval between statusfile update and file size check (sec)",
                      )
    parser.add_option("--max-size",
                      type="int",
                      help="Approximate output file size limit (default = None)",
                      )
    parser.add_option("--ignore-quality",
                      action="store_true",
                      default=False,
                      help="Output bad quality rows (suppressed by default)",
                      )
    parser.add_option("--mind-the-gaps",
                      action="store_true",
                      default=False,
                      help="Abort if data gap detected (instead of just setting quality=1)",
                      )
    parser.add_option("--start",
                      help="Start date of processing",
                      )
    parser.add_option("--stop",
                      help="Stop date of processing",
                      )
    parser.add_option("--dt",
                      type='float',
                      default=32.8,
                      help="Sampling interval (sec)",
                      )
    parser.add_option("--file-format",
                      default='csv',
                      choices=['csv','rdb','space','fits','tab','dmascii'],
                      help="Output data format (csv rdb space fits tab dmascii)",
                      )
    parser.add_option("--time-format",
                      default='date',
                      choices=['date','greta','secs','jd','mjd','fits','unix'],
                      help="Output time format (date greta secs jd mjd fits unix)",
                      )
    (opt, args) = parser.parse_args()
    if not args: args = ['ephin2eng:']

    return opt, args

if __name__ == '__main__':
    main()
    
