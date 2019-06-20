import psycopg2 
import pyodbc
import getpass
import datetime


class DbConnect:
    def __init__(self, **kwargs):
        self.user = kwargs.get('user', None)
        self.password = kwargs.get('password', None)
        self.type = kwargs.get('type', None)
        self.server = kwargs.get('server', None)
        self.database = kwargs.get('database', None)
        self.port = kwargs.get('port', 5432)
        self.params = dict()
        self.conn = None
        self.queries = list()
        self.connection_start = None

    def __str__(self):
        return 'Database connection ({typ}) to {db} on {srv} - {usr} \nConnection established {dt}'.format(
            typ=self.type,
            db=self.database,
            srv=self.server,
            usr=self.user,
            dt=self.connection_start
        )

    def connect(self):
        """
        Connects to database
        Requires all connection parameters to be entered and connection type
        :return: 
        """
        # make sure all parameters are populated
        if not all((self.database, self.user, self.password, self.server)):
            self.get_credentials()
        # postgres connection
        if self.type.upper() in ('PG', 'POSTGRESQL', 'POSTGRES'):
            # standardize types
            self.type = 'PG'
            self.params = {
                'dbname': self.database,
                'user': self.user,
                'password': self.password,
                'host': self.server,
                'port': self.port
            }
            self.conn = psycopg2.connect(**self.params)

        if self.type.upper() in ('MS', 'SQL', 'MSSQL', 'SQLSERVER'):
            # standardize types
            self.type = 'MS'
            self.params = {
                'DRIVER': 'SQL Server Native Client 10.0',  # 'SQL Server Native Client 10.0',
                'DATABASE': self.database,
                'UID': self.user,
                'PWD': self.password,
                'SERVER': self.server
            }
            # need catch for missing drivers
            # native client is required for correct handling of datetime2 types in SQL
            try:
                self.conn = pyodbc.connect(**self.params)
            except:
                # revert to SQL driver and show warning
                print ('Warning:\n\tMissing SQL Server Native Client 10.0 \
                datetime2 will not be interpreted correctly\n')
                self.params['DRIVER'] = 'SQL Server'
                self.conn = pyodbc.connect(**self.params)
        self.connection_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def disconnect(self):
        """
        Closes connection to DB
        :return: 
        """
        self.conn.close()

    def get_credentials(self):
        print ('\nAdditional database connection details required:')
        if not self.type:
            self.type = raw_input('Database type (MS/PG)').upper()
        if not self.database:
            self.database = raw_input('Database name:')
        if not self.server:
            self.server = raw_input('Server:')
        if not self.user:
            self.user = raw_input('User name ({}):'.format(
                self.database.lower()))
        self.password = getpass.getpass('Password ({})'.format(
            self.database.lower()))

    def query(self, query):
        qry = Query(self, query)
        self.queries.append(qry)


class Query:
    def __init__(self):
        pass
