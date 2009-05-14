import os
import sys
import re
import pprint
import logging

import pyfits
from mx.DateTime import strptime, DateTime, Error, DateTimeDeltaFromSeconds
import Chandra.Time

SKA = os.getenv('SKA') or '/proj/sot/ska'
SKA_DATA = SKA + '/data/telem_archive'

class NullHandler(logging.Handler):
    def emit(self, record):
        pass

logger = logging.getLogger('data_table')
logger.addHandler(NullHandler())

data_tables = {}                        # Currently active data tables
files_not_found = set()

def add_days(year, doy, delta_days):
    d0 = Chandra.Time.DateTime('%04d:%03d' % (year,doy)).mxDateTime
    d1 = d0 + delta_days
    return d1.year, d1.day_of_year

class BeforeTableStart(RuntimeError):
    pass

class AfterTableStop(RuntimeError):
    pass

class DateNotInTable(RuntimeError):
    pass

class DataTable(object):
    """
    Provide methods to read an archive FITS data table and return column
    values corresponding to a particular time.
    """
    def __init__(self, year, doy, table_type):
        self.file_name = SKA_DATA + '/%04d/%03d/%s.fits.gz' % (year, doy, table_type)
        self.doy = doy
        self.year = year
        self.table_type = table_type

        # Don't try to read if it already failed
        if self.file_name in files_not_found:
            raise IOError
        logger.debug('Reading file %s' % self.file_name)
        try:
            hdulist = pyfits.open(self.file_name)
        except IOError:
            logger.debug('Could not open %s, raising IOError' % self.file_name)
            files_not_found.add(self.file_name)
            raise
        bintbl = hdulist[1]   # First extension of HDU list
        self.col_names = bintbl.columns.names
        self.i_row = 0
        self.n_rows = bintbl.header['naxis2']
        self.references = 1

        # Read the entire table into memory as a dict of lists for efficiency
        self.fits_data_arr = {}
        for col_name in self.col_names:
            col = bintbl.data.field(col_name)
            self.fits_data_arr[col_name] = list(col)

        # Start and stop date for file (as Chandra seconds)
        self.file_tstart = self.fits_data_arr['tstart'][0]
        self.file_tstop = self.fits_data_arr['tstop'][-1]
        # Start and stop date for first record (as Chandra seconds)
        self.tstart = self.fits_data_arr['tstart'][0]
        self.tstop = self.fits_data_arr['tstop'][0]
        hdulist.close()

    def reset(self):
        self.i_row = 0

    def row(self):
        fda = self.fits_data_arr
        while (self.i_row < self.n_rows):
            yield dict([(name, fda[name][self.i_row]) for name in self.col_names])
            self.i_row += 1

    def get_value(self, column_name, date=None):
        """Get column value for bin containing specified date.  Date must be in Chandra secs"""

        fda = self.fits_data_arr
        n_tries = 0

        # Make sure the requested date is within the bounds of the table
        if date < self.file_tstart:
            raise BeforeTableStart
        if date >= self.file_tstop:
            raise AfterTableStop

        # Step through data rows until a row contains the date
        # Allow for a little slop at the edges because of floating point
        # uncertainties in the time bin edges.  Bins and time steps will never
        # be less than 1.025 seconds so a 1 msec slop is fine.
        while not (date > self.tstart - 0.001 and date < self.tstop):
            if (date >= self.tstop):
                self.i_row += 1
            else:
                self.i_row -= 1
            if self.i_row >= self.n_rows or self.i_row < 0:
                raise DateNotInTable, 'Data gap: date %s not in table %s' % (date, self.file_name)
            self.tstart = fda['tstart'][self.i_row]
            self.tstop = fda['tstop'][self.i_row]
            if n_tries > self.n_rows:
                raise DateNotInTable, 'Data gap: date %s not in table %s' % (date, self.file_name)
            n_tries += 1                # Catch oscillation due to a data gap
        
        return fda[column_name][self.i_row], fda['quality'][self.i_row]

class DataColumn(object):
    """
    Access data from a column name from a table type.  The class manages the associated
    FITS data table files, opening FITS data table files and releasing as needed.
    """
    def __init__(self, column_def):
        self.name  = column_def['name']
        self.table_type = column_def['table']
        self.data_table = None
        self.value = None
    
    def drop_table(self):
        if not self.data_table:
            return
        logger.debug('Dropping ref #%d to table %s' % 
                      (self.data_table.references, self.data_table.file_name))
        self.data_table.references -= 1
        if self.data_table.references <= 0:
            logger.debug('Dropping table %s' % self.data_table.file_name)
            del data_tables[self.data_table.file_name]
        self.data_table = None

    def register_table(self, year, doy):
        """Register a reference to the DataTable object that contains this column.
        Create the DataTable object (which connects to the correct FITS data file)
        if needed."""

        # Check if the DataTable object for this date and table type already exists
        for x in data_tables.values():
            if (year, doy, self.table_type) == (x.year, x.doy, x.table_type):
                self.data_table = x
                self.data_table.references += 1
                logger.debug('Adding ref #%d to table file %s' %
                              (self.data_table.references, self.data_table.file_name))
                break
        else:  # Doesn't exist, so create it and add to data_tables registry
            self.data_table = DataTable(year, doy, self.table_type)
            data_tables[self.data_table.file_name] = self.data_table
            logger.debug('Registering new table file %s' % self.data_table.file_name)

    def get_value(self, date):
        """Return value for the column for a particular date.  Register a new data_table
        if needed to satisfy the request.  Release current data_table if it doesn't
        contain the requested date and is not being used by any other column
        """
        year = None
        doy  = None
        
        # This is expensive in an inner loop
        # logger.debug('Trying to get %s at %s' % (self.name, date))

        # If we already have a data table for this column, see if we can
        # get a value.  Respond to exceptions if data value could not be returned.
        if self.data_table:
            sdt = self.data_table
            try:
                return sdt.get_value(self.name, date)
            except AfterTableStop:
                logger.debug('Got AfterTableStop')
                (year, doy) = add_days(sdt.year, sdt.doy, +1)
            # In theory the code below would be good, but it opens the possibility of
            # oscillating if there is a data gap at the day boundary.  Need a recursion
            # count but this could be tricky with all the exceptions being thrown.
            # Instead just let the exception get raised and fetch() treats it as a bad
            # quality column.
            # 
            # except BeforeTableStart:
            #     logger.debug('Got BeforeTableStart')
            #     (year, doy) = add_days(sdt.year, sdt.doy, -1)
        else:
            mxdate = Chandra.Time.DateTime(date).mxDateTime
            (year, doy) = (mxdate.year, mxdate.day_of_year)
            logger.debug('Setting year, doy to %d %d' % (year, doy))

        # If year and doy became defined, then we must need to attach to a different DataTable
        if year != None and doy != None:
            logger.debug('Need table for column %s at %d %d' % (self.name, year, doy))
            self.drop_table()
            self.register_table(year, doy)
            return self.get_value(date)
        else:
            raise RuntimeError, 'Year or doy not defined but should be.'

    def __repr__(self):
        return pprint.pformat(self.__dict__)

