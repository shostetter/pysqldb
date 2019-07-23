from distutils.core import setup
# import pkg_resources  # part of setuptools
# version = pkg_resources.require("ris")[0].version

setup(name='pysqldb',
      version='0.0.3',
      packages=['pysqldb'],
      description='Basic library to connect to SQL databases',
      install_requires=[
            'psycopg2',
            'pyodbc',
            'pandas',
            'numpy',
            'tqdm'
      ]
      )

# to package run (setup.py sdist) from cmd
# to install unzip, and run (python setup.py install) from the cmd in the folder