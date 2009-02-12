from setuptools import setup
setup(name='Ska.TelemArchive',
      author = 'Tom Aldcroft',
      description='Modules supporting SKA telemetry archive',
      author_email = 'aldcroft@head.cfa.harvard.edu',
      py_modules = ['Ska.TelemArchive.fetch',
                    'Ska.TelemArchive.fetch_client',
                    'Ska.TelemArchive.fetch_server',
                    'Ska.TelemArchive.data_table'],
      version='0.02',
      zip_safe=False,
      namespace_packages=['Ska'],
      packages=['Ska', 'Ska.TelemArchive'],
      # package_dir={'Ska' : 'Ska'},
      # package_data={}
      )
