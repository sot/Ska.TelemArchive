from setuptools import setup

from Ska.TelemArchive import __version__

setup(name='Ska.TelemArchive',
      author = 'Tom Aldcroft',
      description='Modules supporting defunct SKA telemetry archive',
      author_email = 'aldcroft@head.cfa.harvard.edu',
      py_modules = ['Ska.TelemArchive.fetch',
                    'Ska.TelemArchive.fetch_client',
                    'Ska.TelemArchive.fetch_server',
                    'Ska.TelemArchive.data_table'],
      version=__version__,
      zip_safe=False,
      packages=['Ska', 'Ska.TelemArchive'],
      package_dir={'Ska' : 'Ska'},
      )
