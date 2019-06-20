import psycopg2
import pyodbc
import getpass
import datetime
import re
import sys
import cPickle as pickle
import pandas as pd


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
        self.connect()

    def __str__(self):
        return 'Database connection ({typ}) to {db} on {srv} - user: {usr} \nConnection established {dt}'.format(
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

    def dfquery(self, query):
        qry = Query(self, query)
        self.queries.append(qry)
        return qry.dfquery()


class Query:
    def __str__(self):
        if self.query_time.seconds == 0:
            t = self.query_time.microseconds
            ty = 'microseconds'
        else:
            t = self.query_time.seconds
            ty = 'seconds'
        if self.data:
            return '- Query run {dt}\n\tQuery time: {t} {ty}\n\t{q}\n\t* Returned {r} rows *'.format(
                dt=datetime.datetime.now(),
                q=self.query_string,
                r=len(self.data),
                t=t, ty=ty)
        else:
            return '- Query run {dt}\n\tQuery time: {t} {ty}\n\t{q}\n\t* No records returned *'.format(
                dt=datetime.datetime.now(),
                q=self.query_string,
                t=t, ty=ty)

    def __init__(self, dbo, query_string, **kwargs):
        """
        
        :param dbo: DbConnect object 
        :param query_string: String sql query to be run  
        :param kwargs: 
            strict (bool): If true will run sys.exit on failed query attempts 
            comment (bool): If true any new tables will automatically have a comment added to them
            permission (bool): description 
            keep (bool): description
            remove_date (datetime.date): description
        """
        self.dbo = dbo
        self.query_string = query_string
        self.strict = kwargs.get('strict', True)
        self.comment = kwargs.get('comment', True)
        self.permission = kwargs.get('permission', True)
        self.keep = kwargs.get('keep', False)
        self.remove_date = kwargs.get('remove_date', datetime.date.today() + datetime.timedelta(days=7))
        self.query_start = datetime.datetime.now()
        self.query_end = datetime.datetime.now()
        self.query_time = None
        self.has_data = False
        self.data_description = None
        self.data_columns = None
        self.data = None
        self.new_tables = list()
        self.query()
        self.auto_comment()

    def query(self):
        """
        Runs SQL query 
        stores data (if any) in Query class attribute 
        stores the query that was run in Query class attribute 
        stores the query durration in Query class attribute 
        :return: 
        """
        self.query_start = datetime.datetime.now()
        cur = self.dbo.conn.cursor()
        self.query_string = self.query_string.replace('%', '%%')
        self.query_string = self.query_string.replace('-pct-', '%')
        try:
            cur.execute(self.query_string)
        except:
            print ('Failure:\n')
            print ('- Query run {dt}\n\t{q}'.format(
                dt=datetime.datetime.now(),
                q=self.query_string))
            del cur
            cur = self.dbo.conn.cursor()
            if self.strict:
                sys.exit()

        self.query_end = datetime.datetime.now()
        self.query_time = self.query_end-self.query_start
        if cur.description is None:
            self.dbo.conn.commit()
            self.new_tables = self.query_creates_table()
        else:
            self.query_data(cur)
        # write new tables to log
        self.remove_log()
        # drop any tables previously added in log
        self.cleanup_log()

    def dfquery(self):
        """
        Runs SQL query - only availible for select queries  
        stores data in Query class attribute 
        stores the query that was run in Query class attribute 
        stores the query durration in Query class attribute 
        :return: Pandas DataFrame of the results of the query 
        """
        # Cannot use pd.read_sql() because the structure will necessitate running query twice
        # once when Query is initially created and again to query for the df,
        # so this is a work around for pyodbc
        if self.dbo.type == 'MS':
            self.data = [tuple(i) for i in self.data]
            df = pd.DataFrame(self.data, columns=self.data_columns)
        return df

    def query_data(self, cur):
        """
        Parses the results of the query and stores data and columns in Query class attribute 
        :param cur: 
        :return: 
        """
        self.has_data = True
        self.data_description = cur.description
        self.data_columns = [desc[0] for desc in self.data_description]
        self.data = cur.fetchall()

    def query_creates_table(self):
        """
        Checks if query generates new tables 
        :return: list of sets of {[schema.]table}
        """
        new_tables = list()
        create_table = r'(create\s+table\s+)((\w+\.\w+)|(\w+))'
        matches = re.findall(create_table, self.query_string.lower())
        # get all schema and table pairs remove create table match
        new_tables += [set(_[1:]) for _ in matches]
        #select_into = r'(select\s+)((.+)|((.+)\n)+)(into)((\s+\w+\.\w+)|(\s+[^\.]\w+))'
        # adds catch for MS [database].[schema].[table]
        select_into = r'(select\s+)((.+)|((.+)\n)+)(into)\s+(([^\s+]?\w+[^\s+]?\.)*([^\s+]?\w+[^\s+]?))'
        matches = re.findall(select_into, self.query_string.lower())
        # into is position 5 in regex match
        new_tables += [set(_[6:]) for _ in matches]
        # clean up

        for _ in new_tables:
            if '' in _:
                _.remove('')
        return [i.pop() for i in new_tables]

    def auto_comment(self):
        """
        Automatically generates comment for PostgreSQL tables if created with Query 
        :return: 
        """
        if self.comment and self.dbo.type == 'PG':
            for t in self.new_tables:
                # tables in new_tables list will contain schema if provided, otherwise will default to public
                q = """COMMENT ON TABLE {t} IS 'Created by {u} on {d}'""".format(
                    t=t,
                    u=self.dbo.user,
                    d=self.query_start.strftime('%Y-%m-%d %H:%M')
                )
                _ = Query(self.dbo, q)

    def remove_log(self):
        """
        Logs new table attributes that have a remove_date 
        :return: 
        """
        if not self.keep:
            to_log = read_pickle()
            if not to_log:
                to_log = list()
            for tbl in self.new_tables:
                to_log.append({
                    'table': tbl,
                    'created': self.query_start,
                    'removal': self.remove_date,
                    'db_info': {
                        'database': self.dbo.database,
                        'server': self.dbo.server,
                        'db_type': self.dbo.type,
                        'user': self.dbo.user
                    }
                })
        write_pickle(to_log)

    def same_connection(self, db_info):
        """
        Checks if connection params match
        :param db_info: 
        :return: 
        """
        if {
            'db_type': self.dbo.type,
            'server': self.dbo.server,
            'database': self.dbo.database,
            'user': self.dbo.user
        } == db_info:
            return True
        else:
            return False

    def cleanup_log(self):
        new_log = list()
        for tbl in read_pickle():
            if tbl['removal'] < datetime.date.today() and self.same_connection(tbl['db_info']):
                q = Query(self.dbo, "DROP TABLE IF EXISTS {}".format(tbl['table']))
                print q
            else:
                new_log.append(tbl)
        write_pickle(new_log)


def write_pickle(data, data_file='to_remove.lg'):
    ouf = open(data_file, 'w')
    pickle.dump(data, ouf)
    ouf.close()


def read_pickle(data_file='to_remove.lg'):
    # Read in existing queue
    try:
        inf = open(data_file)
    except:
        write_pickle([])
        inf = open(data_file)
    data = pickle.load(inf)
    inf.close()
    return data

db = DbConnect(type='ms',
                        database='RISCRASHDATA',
                        server='dotdevgissql01',
                        user='risadmin ',
                        password='RISADMIN!')
# db.dfquery("SELECT TOP 10 [NODEID] FROM [RISCRASHDATA].[dbo].[__bikesshare_p1_nodes]")
# db.query("SELECT TOP 5 [Street],[FACILITYTY],[FACILITYCL] FROM [RISCRASHDATA].[dbo].[bike_lanes_15]")
db.query("SELECT TOP 5 [Street],[FACILITYTY],[FACILITYCL] into [RISCRASHDATA].[dbo].[_test_bike_lanes_15] FROM [RISCRASHDATA].[dbo].[bike_lanes_15]")
for q in db.queries: print q
print q.new_tables

db2 = DbConnect(type='pg',
                      database='CRASHDATA',
                      server='dotdevpgsql02',
                      user='shostetter',
                      password='Batavia79')
sample_query2 = '''DROP TABLE IF EXISTS working.temp; 
    SELECT nodeid, masterid, is_int, is_cntrln_int into
    working.temp
    FROM public.node WHERE nodeid=9033921;
    '''
db2.query(query=sample_query2)
for q in db2.queries: print q
print q.new_tables
