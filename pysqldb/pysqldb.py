import psycopg2
import pyodbc
import getpass
import datetime
import re
import sys
import os
import csv
import subprocess
import pandas as pd
import numpy as np
from tqdm import tqdm


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

    def connect(self, quiet=False):
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
        if not quiet:
            print (self)

    def disconnect(self, quiet=False):
        """
        Closes connection to DB
        :return: 
        """
        self.conn.close()
        if not quiet:
            print 'Database connection ({typ}) to {db} on {srv} - user: {usr} \nConnection closed {dt}'.format(
                typ=self.type,
                db=self.database,
                srv=self.server,
                usr=self.user,
                dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            )

    def refresh_connection(self):
        self.disconnect(True)
        self.connect(True)

    def get_credentials(self):
        """
        Requests any missing credentials needed for db connection 
        :return: None
        """
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

    def query(self, query, **kwargs):
        """
            runs Query object from input SQL string and adds query to queries
                :param query: String sql query to be run  
                :param kwargs: 
                    strict (bool): If true will run sys.exit on failed query attempts 
                    comment (bool): If true any new tables will automatically have a comment added to them
                    permission (bool): description 
                    temp (bool): if True any new tables will be logged for deletion at a future date 
                    remove_date (datetime.date): description
                    table_log: (bool): defaults to True, will log any new tables created and delete them 
                                       once past removal date 
                """
        strict = kwargs.get('strict', True)
        permission = kwargs.get('permission', True)
        temp = kwargs.get('temp', False)
        table_log = kwargs.get('table_log', False)
        timeme = kwargs.get('timeme', True)
        no_comment = kwargs.get('no_comment', False)
        qry = Query(self, query, strict=strict, permission=permission, temp=temp, table_log=table_log,
                    timeme=timeme, no_comment=no_comment)
        self.queries.append(qry)
        self.refresh_connection()

    def dfquery(self, query, timeme=False):
        """
        Returns dataframe of results of a SQL query. Will though an error if no data is returned
        :param query: SQL statement 
        :param timeme: default to False, adds timing to query run
        :return: 
        """
        qry = Query(self, query, timeme=timeme)
        self.queries.append(qry)
        self.refresh_connection()
        return qry.dfquery()

    def type_decoder(self, typ):
        """
        Lazy type decoding from pandas to SQL.
        This does not try to optimze for smallest size!
        :param typ: Numpy dtype for column   
        :return: 
        """
        if typ == np.dtype('M'):
            return 'timestamp'
        elif typ == np.dtype('int64'):
            return 'bigint'
        elif typ == np.dtype('float64'):
            return 'float'
        else:
            return 'varchar (500)'

    def clean_cell(self, x):
        """
        Formats csv cells for SQL to add to database
        :param x: 
        :return: 
        """
        if type(x) == long:
            return int(x)
        elif type(x) == unicode or type(x) == str:
            if "'" in x:
                return str(x).replace("'", " ")
            if u'\xa0' in x:
                return str(x.replace(u'\xa0', ' '))
            return str(x)  # .replace(u'\xa0', u' ')
        elif type(x) == datetime.date:
            return x.strftime('%Y-%m-%d')
        elif type(x) == datetime.datetime:
            return x.strftime('%Y-%m-%d %H:%M')
        elif type(x) == datetime.datetime:
            return x.strftime('%Y-%m-%d %H:%M')
        elif type(x) == pd.tslib.Timestamp:
            x.to_pydatetime()
            return x.strftime('%Y-%m-%d %H:%M')
        else:
            return x

    def clean_column(self, x):
        """
        Reformats column names to for database
        :param x: column name
        :return: Reformated column name
        """
        a = x.strip().lower()
        b = a.replace(' ', '_')
        c = b.replace('.', '')
        d = c.replace('(s)', '')
        e = d.replace(':', '_')
        return e

    def dataframe_to_table(self, df, table_name, **kwargs):
        """
        Translates Pandas DataFrame to database table. 
        :param df: Pandas DataFrame to be added to database
        :param table_name: Table name to be used in databse
        :param kwargs: 
            schema (str): Define schema, defaults to public (PG)/ dbo (MS)
            overwrite (bool): If table exists in database will overwrite 

        :return: 
        """
        overwrite = kwargs.get('overwrite', False)
        schema = kwargs.get('schema', 'public')
        if self.type == 'MS':
            schema = kwargs.get('schema', 'dbo')
        input_schema = list()

        # parse df for schema
        for col in df.dtypes.iteritems():
            col_name, col_type = col[0], self.type_decoder(col[1])
            input_schema.append([self.clean_column(col_name), col_type])
        if self.type == 'PG':
            it = ' IF EXISTS '
        else:
            it = ''
        # create table in database
        if overwrite:
            qry = """
            DROP TABLE {if_typ}{s}.{t}
        """.format(s=schema, t=table_name, if_typ=it)
            self.query(qry.replace('\n', ' '), strict=False, timeme=False)  # Fail if table not exists MS
        qry = """
            CREATE TABLE {s}.{t} (
            {cols}
            )
        """.format(s=schema, t=table_name, cols=str(['"' + str(i[0]) + '" ' + i[1] for i in input_schema]
                                                    )[1:-1].replace("'", ""))
        self.query(qry.replace('\n', ' '), timeme=False)

        # insert data
        print 'Reading data into Database\n'
        for _, row in tqdm(df.iterrows()):
            row = row.replace({pd.np.nan: None})  # clean up empty cells
            self.query("""
                INSERT INTO {s}.{t} ({cols})
                VALUES ({d})
            """.format(s=schema, t=table_name,
                       cols=str(['"' + str(i[0]) + '"' for i in input_schema])[1:-1].replace("'", ''),
                       d=str([self.clean_cell(i) for i in row.values])[1:-1].replace(
                           'None', 'NULL')), strict=False, table_log=False, timeme=False)

        df = self.dfquery("SELECT COUNT(*) as cnt FROM {s}.{t}".format(s=schema, t=table_name), timeme=False)
        print '\n{c} rows added to {s}.{t}\n'.format(c=df.cnt.values[0], s=schema, t=table_name)

    def csv_to_table(self, **kwargs):
        """
        Imports csv file to database. This uses pandas datatypes to generate the table schema. 
        :param kwargs: 
            input_file (str): File path to csv file
            overwrite (bool): If table exists in database will overwrite 
            schema (str): Define schema, defaults to public (PG)/ dbo (MS)
            table_name: (str): name for database table
        :return: 
        """
        input_file = kwargs.get('input_file', None)
        overwrite = kwargs.get('overwrite', False)
        schema = kwargs.get('schema', 'public')
        table_name = kwargs.get('table_name', '_{u}_{d}'.format(
            u=self.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M')))
        if self.type == 'MS':
            schema = kwargs.get('schema', 'dbo')
        if not input_file:
            input_file = file_loc('file')
        # use pandas to get existing data and schema

        df = pd.read_csv(input_file)
        if not table_name:
            table_name = os.path.basename(input_file).split('.')[0]
        self.dataframe_to_table(df, table_name, overwrite=overwrite, schema=schema)

    def xls_to_table(self, **kwargs):
        """
        Imports csv file to database. This uses pandas datatypes to generate the table schema. 
        :param kwargs: 
            input_file (str): File path to csv file
            sheet_name : str, int, list, or None, default 0
            overwrite (bool): If table exists in database will overwrite 
            schema (str): Define schema, defaults to public (PG)/ dbo (MS)
            table_name: (str): name for database table
        :return: 
        """
        input_file = kwargs.get('input_file', None)
        sheet_name = kwargs.get('sheet_name', 0)
        overwrite = kwargs.get('overwrite', False)
        table_name = kwargs.get('table_name', '_{u}_{d}'.format(
            u=self.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M')))
        schema = kwargs.get('schema', 'public')
        if self.type == 'MS':
            schema = kwargs.get('schema', 'dbo')
        if not input_file:
            input_file = file_loc('file')
        # use pandas to get existing data and schema

        df = pd.read_excel(input_file, sheet_name=sheet_name)
        if not table_name:
            table_name = os.path.basename(input_file).split('.')[0]
        self.dataframe_to_table(df, table_name, overwrite=overwrite, schema=schema)

    def query_to_csv(self, query, **kwargs):
        """
        Exports query results to a csv file. 
        :param query: SQL query as string type 
        :param kwargs: 
            sep: Delimiter
            strict (bool): If true will run sys.exit on failed query attempts 
            output: File path for csv file
            open_file (bool): If true will auto open the output csv file when done   

        :return: 
        """
        strict = kwargs.get('strict', True)
        output = kwargs.get('output',
                            os.path.join(os.getcwd(), 'data_{}.csv'.format(
                                datetime.datetime.now().strftime('%Y%m%d%H%M'))))
        open_file = kwargs.get('open_file', False)
        sep = kwargs.get('sep', ',')
        quote_strings = kwargs.get('quote_strings', False)
        qry = Query(self, query, strict=strict, table_log=False)
        qry.query_to_csv(output=output, open_file=open_file, quote_strings=quote_strings, sep=sep)

    def query_to_shp(self, query, **kwargs):
        strict = kwargs.get('strict', True)
        path = kwargs.get('path', None)
        shp_name = kwargs.get('shp_name', None)
        cmd = kwargs.get('cmd', None)
        gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")
        qry = Query(self, query, strict=strict, table_log=False)
        qry.query_to_shp(path=path,
                         query=query,
                         shp_name=shp_name,
                         cmd=cmd,
                         gdal_data_loc=gdal_data_loc)

    def import_shp(self, **kwargs):
        dbo = kwargs.get('dbo', self)
        path = kwargs.get('path', None)
        table = kwargs.get('table', None)
        schema = kwargs.get('schema', 'public')
        query = kwargs.get('query', None)
        shp_name = kwargs.get('shp_name', None)
        cmd = kwargs.get('cmd', None)
        srid = kwargs.get('srid', '2263')
        gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")
        precision = kwargs.get('precision', False),
        private = kwargs.get('private', False)
        shp = Shapefile(dbo=dbo, path=path, table=table, schema=schema, query=query,
                        shp_name=shp_name, cmd=cmd, srid=srid, gdal_data_loc=gdal_data_loc)
        shp.read_shp(precision, private)

    def import_feature_class(self, **kwargs):
        dbo = kwargs.get('dbo', self)
        path = kwargs.get('path', None)
        table = kwargs.get('table', None)
        schema = kwargs.get('schema', 'public')
        query = kwargs.get('query', None)
        shp_name = kwargs.get('shp_name', None)
        cmd = kwargs.get('cmd', None)
        srid = kwargs.get('srid', '2263')
        gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")
        private = kwargs.get('private', False)
        shp = Shapefile(dbo=dbo, path=path, table=table, schema=schema, query=query,
                        shp_name=shp_name, cmd=cmd, srid=srid, gdal_data_loc=gdal_data_loc)
        shp.read_feature_class(private)


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
            comment (str): String to add to default comment
            permission (bool): description 
            temp (bool): if True any new tables will be logged for deletion at a future date 
            remove_date (datetime.date): description
            table_log: (bool): defaults to True, will log any new tables created and delete them once past removal date 
        """
        self.dbo = dbo
        self.query_string = query_string
        self.strict = kwargs.get('strict', True)
        self.permission = kwargs.get('permission', True)
        self.temp = kwargs.get('temp', False)
        self.table_log = kwargs.get('table_log', False)
        self.comment = kwargs.get('comment', '')
        self.no_comment = kwargs.get('no_comment', False)
        self.timeme = kwargs.get('timeme', True)
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
        self.run_table_logging()

    def query_time_format(self):
        if self.query_time.seconds < 60:
            if self.query_time.seconds < 1:
                return 'Query run in {} microseconds'.format(self.query_time.microseconds)
            else:
                return 'Query run in {} seconds'.format(self.query_time.seconds)
        else:
            return 'Query run in {} seconds'.format(self.query_time)

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
            else:
                # reset connection
                self.dbo.disconnect()
                self.dbo.connect()
        self.query_end = datetime.datetime.now()
        self.query_time = self.query_end - self.query_start
        if self.timeme:
            print self.query_time_format()
        if cur.description is None:
            self.dbo.conn.commit()
            self.new_tables = self.query_creates_table()
        else:
            self.query_data(cur)


    def dfquery(self):
        """
        Runs SQL query - only available for select queries  
        stores data in Query class attribute 
        stores the query that was run in Query class attribute 
        stores the query duration in Query class attribute 
        :return: Pandas DataFrame of the results of the query 
        """
        # Cannot use pd.read_sql() because the structure will necessitate running query twice
        # once when Query is initially created and again to query for the df,
        # so this is a work around for pyodbc
        df = None
        if self.dbo.type == 'MS':
            self.data = [tuple(i) for i in self.data]
            df = pd.DataFrame(self.data, columns=self.data_columns)
        else:
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
        create_table = r'(create table\s+)(\[?[\w]*\]?[.]?\"?\[?[\w]*\]?\.?\[?[\w]*\"?\]?([\w]*\]?))'
        matches = re.findall(create_table, self.query_string.lower())
        # get all schema and table pairs remove create table match
        new_tables += [set(_[1:]) for _ in matches]
        # adds catch for MS [database].[schema].[table]
        select_into = r'(select[^\.]*into\s+)(\[?[\w]*\]?[.]?\"?\[?[\w]*\]?\.?\[?[\w]*\"?\]?([\w]*\]?))'
        matches = re.findall(select_into, self.query_string.lower())
        # [[select ... into], [table], [misc]]
        new_tables += [set(_[1:]) for _ in matches]
        # clean up
        for _ in new_tables:
            if '' in _:
                _.remove('')
        if new_tables and new_tables != [set()]:
            return [i.pop() for i in new_tables]
        else:
            return []

    def auto_comment(self):
        """
        Automatically generates comment for PostgreSQL tables if created with Query 
        :return: 
        """
        if self.dbo.type == 'PG' and not self.no_comment:
            for t in self.new_tables:
                # tables in new_tables list will contain schema if provided, otherwise will default to public
                q = """COMMENT ON TABLE {t} IS 'Created by {u} on {d}\n{cmnt}'""".format(
                    t=t,
                    u=self.dbo.user,
                    d=self.query_start.strftime('%Y-%m-%d %H:%M'),
                    cmnt=self.comment
                )
                _ = Query(self.dbo, q, strict=False, table_log=False, timeme=False)

    def run_table_logging(self):
        """
        Logs new tables and runs clean up on any existing tables in the log file
        :return: 
        """
        for table in self.new_tables:
            # print table
            if '.' in table:
                log_temp_table(self.dbo,
                               table.split('.')[0],
                               table.split('.')[1],
                               self.dbo.user)
            else:
                if self.dbo.type == 'MS':
                    default_schema = 'dbo'
                else:
                    default_schema = 'public'
                log_temp_table(self.dbo,
                               default_schema,
                               table,
                               self.dbo.user)


        pass

    def query_to_csv(self, **kwargs):
        """
        Writes results of the query to a csv file
        :param kwargs:
            output: String for csv output file location (defaults to current directory)
            open_file: Boolean flag to auto open output file    
        :return: 
        """
        output = kwargs.get('output',
                            os.path.join(os.getcwd(), 'data_{}.csv'.format(
                                datetime.datetime.now().strftime('%Y%m%d%H%M'))))
        open_file = kwargs.get('open_file', False)
        quote_strings = kwargs.get('quote_strings', False)
        sep = kwargs.get('sep', ',')

        df = self.dfquery()
        # TODO: convert geom to well known string for outputs
        if quote_strings:
            df.to_csv(output, index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC, sep=sep)
        else:
            df.to_csv(output, index=False, quotechar="'", sep=sep)
        if open_file:
            os.startfile(output)

    def query_to_shp(self, **kwargs):
        query = kwargs.get('query', self.query_string)
        path = kwargs.get('path', None)
        shp_name = kwargs.get('shp_name', None)
        cmd = kwargs.get('cmd', None)
        gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")
        shp = Shapefile(dbo=self.dbo,
                        path=path,
                        query=query,
                        shp_name=shp_name,
                        cmd=cmd,
                        gdal_data_loc=gdal_data_loc)
        shp.write_shp()


class Shapefile:
    def __str__(self):
        pass

    def __init__(self, **kwargs):
        self.dbo = kwargs.get('dbo', None)
        self.path = kwargs.get('path', None)
        self.table = kwargs.get('table', None)
        self.schema = kwargs.get('schema', 'public')
        self.query = kwargs.get('query', None)
        self.shp_name = kwargs.get('shp_name', None)
        self.cmd = kwargs.get('cmd', None)
        self.srid = kwargs.get('srid', '2263')
        self.gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")
        self.connect()

    def connect(self):
        if not self.dbo:
            self.dbo = DbConnect()

    def name_extension(self, name):
        if '.shp' in name:
            return name
        else:
            return name + '.shp'

    def write_shp(self):
        if self.table:
            qry = "SELECT * FROM {s}.{t}".format(s=self.schema, t=self.table)
        else:
            qry = "SELECT * FROM ({q}) x".format(q=self.query)
        if not self.shp_name:
            output_file_name = file_loc('save')
            self.shp_name = os.path.basename(output_file_name)
            self.path = os.path.dirname(output_file_name)
        if not self.path:
            self.path = file_loc('folder')
        if not self.cmd:
            self.cmd = 'ogr2ogr -overwrite -f \"ESRI Shapefile\" \"{export_path}\{shpname}\" ' \
                       'PG:"host={host} user={username} dbname={db} ' \
                       'password={password}" -sql "{pg_sql_select}"'.format(export_path=self.path,
                                                                            shpname=self.name_extension(self.shp_name),
                                                                            host=self.dbo.server,
                                                                            username=self.dbo.user,
                                                                            db=self.dbo.database,
                                                                            password='*' * len(self.dbo.password),
                                                                            pg_sql_select=qry)
        os.system(self.cmd.replace('{}'.format('*' * len(self.dbo.password)), self.dbo.password))
        if self.table:
            print '{t} shapefile \nwritten to: {p}\ngenerated from: {q}'.format(t=self.name_extension(self.shp_name),
                                                                                p=self.path,
                                                                                q=self.table)
        else:
            print '{t} shapefile \nwritten to: {p}\ngenerated from: {q}'.format(t=self.name_extension(self.shp_name),
                                                                                p=self.path,
                                                                                q=self.query)

    def table_exists(self):
        # check if table exists
        self.dbo.query("SELECT table_name FROM information_schema.tables WHERE table_schema = '{s}'".format(
            s=self.schema))
        if self.shp_name.replace('.shp', '').lower() in [i[0] for i in self.dbo.queries[-1].data]:
            exists = True
        else:
            exists = False
        return exists

    def del_indexes(self):
        self.dbo.query("""
            select 
                t.relname as table_name, i.relname as index_name, a.attname as column_name
            from
                pg_class t, pg_class i, pg_index ix, pg_attribute a
            where
                t.oid = ix.indrelid
                and i.oid = ix.indexrelid
                and a.attrelid = t.oid
                and a.attnum = ANY(ix.indkey)
                and t.relkind = 'r'
                and t.relname = '{t}'
        """.format(
                t=self.shp_name.replace('.shp', '').lower()))
        idx = self.dbo.queries[-1].data
        for row in idx:
            if 'pkey' not in row[1]:
                self.dbo.query('DROP INDEX IF EXISTS "{s}"."{i}"'.format(
                    s=self.schema, i=row[1]
                ), strict=False)

    def read_shp(self, precision=False, private=False):
        if precision:
            precision = '-lco precision=NO'
        else:
            precision = ''
        if not all([self.path, self.shp_name]):
            filename = file_loc('file', 'Missing file info - Opening search dialog...')
            self.shp_name = os.path.basename(filename)
            self.path = os.path.dirname(filename)
        if not self.table:
            self.table = self.shp_name.replace('.shp', '').lower()

        if self.table_exists():
            # clean up spatial index
            self.del_indexes()
            print 'Deleting existing table {s}.{t}'.format(s=self.schema, t=self.table)
            self.dbo.query("DROP TABLE IF EXISTS {s}.{t} CASCADE".format(s=self.schema, t=self.table))

        cmd = 'ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs ' \
              'EPSG:{srid} -progress -f "PostgreSQL" PG:"host={host} port=5432 dbname={dbname} ' \
              'user={user} password={password}" "{shp}" -nln {schema}.{tbl_name} {perc} '.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                host=self.dbo.server,
                dbname=self.dbo.database,
                user=self.dbo.user,
                password=self.dbo.password,
                shp=os.path.join(self.path, self.shp_name).lower(),
                schema=self.schema,
                tbl_name=self.table,
                perc=precision
                )

        subprocess.call(cmd, shell=True)

        self.dbo.query("""comment on table {s}."{t}" is '{t} created by {u} on {d}
        - imported using pysql module -'""".format(
            s=self.schema,
            t=self.table,
            u=self.dbo.user,
            d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        ))

        if not private:

            self.dbo.query('grant all on {s}."{t}" to public;'.format(
                s=self.schema,
                t=self.table))

    def read_feature_class(self, private=False):
        if not all([self.path, self.shp_name]):
            return 'Missing path and/or shp_name'
        if not self.table:
            self.table = self.shp_name.lower()

        if self.table_exists():
            # clean up spatial index
            self.del_indexes()
            print 'Deleting existing table {s}.{t}'.format(s=self.schema, t=self.table)
            self.dbo.query("DROP TABLE IF EXISTS {s}.{t} CASCADE".format(s=self.schema, t=self.table))

        cmd = 'ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs ' \
              'EPSG:{srid} -f "PostgreSQL" PG:"host={host} user={user} dbname={dbname} ' \
              'password={password}" "{gdb}" "{feature}" -nln {sch}.{feature} -progress'.format(
                gdal_data=self.gdal_data_loc,
                srid=self.srid,
                host=self.dbo.server,
                dbname=self.dbo.database,
                user=self.dbo.user,
                password=self.dbo.password,
                gdb=self.path,
                feature=self.shp_name,
                sch=self.schema
                )
        print cmd
        subprocess.call(cmd, shell=True)

        self.dbo.query("""comment on table {s}."{t}" is '{t} created by {u} on {d}
               - imported using pysql module -'""".format(
            s=self.schema,
            t=self.table,
            u=self.dbo.user,
            d=datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        ))

        if not private:
            self.dbo.query('grant all on {s}."{t}" to public;'.format(
                s=self.schema,
                t=self.table))


def file_loc(typ='file', print_message=None):
    if not print_message:
        print 'File/folder search dialog...'
    else:
        print print_message
    from Tkinter import Tk
    import tkFileDialog
    # import tkMessageBox
    Tk().withdraw()
    if typ == 'file':
        # tkMessageBox.showinfo("Open file", "Please navigate to the file file you want to process")
        filename = tkFileDialog.askopenfilename(title="Select file")
        return filename
    elif typ == 'folder':
        folder = tkFileDialog.askdirectory(title="Select folder")
        return folder
    elif typ == 'save':
        output_file_name = tkFileDialog.asksaveasfilename(
            filetypes=(("Shapefile", "*.shp"), ("All Files", "*.*")),
            defaultextension=".shp"
        )
        return output_file_name


def log_temp_table(dbo, schema, table, owner, expiration=datetime.datetime.now() + datetime.timedelta(days=7)):
    log_table = '__temp_log_table_{}__'.format(owner)
    # check if log exists if not make one
    if dbo.type == 'MS':
        dbo.query("""
                SELECT * 
                FROM sys.tables t 
                JOIN sys.schemas s 
                ON t.schema_id = s.schema_id
                WHERE s.name = '{s}' AND t.name = '{log}'
            """.format(s=schema, log=log_table), timeme=False)
        if not dbo.queries[-1].data:
            dbo.query("""
                CREATE TABLE {s}.{log} (
                    tbl_id int IDENTITY(1,1) PRIMARY KEY,
                    table_owner varchar(255),
                    table_schema varchar(255),
                    table_name varchar(255),
                    created_on datetime, 
                    expires date
                )
            """.format(s=schema, log=log_table), timeme=False)
    elif dbo.type == 'PG':
        # useed the check rather than create if not exists because it was breaking my auto_comment
        dbo.query("""
            SELECT EXISTS (
            SELECT 1
            FROM pg_catalog.pg_tables
            WHERE schemaname = '{s}'
            AND tablename = '{log}'
            )        
        """.format(s=schema, log=log_table), timeme=False)
        x = dbo.queries[-1].data[0][0]
        if not x:
            dbo.query("""
                CREATE TABLE {s}.{log}  (
                    tbl_id SERIAL,
                    table_owner varchar,
                    table_schema varchar,
                    table_name varchar,
                    created_on timestamp, 
                    expires date
                    )
                """.format(s=schema, log=log_table), timeme=False)
    # add new table to log
    if table != log_table:
        dbo.query("""
            INSERT INTO {s}.{log} (
                table_owner,
                table_schema,
                table_name,
                created_on , 
                expires
            )
            VALUES (
                '{u}',
                '{s}',
                '{t}',
                '{dt}',
                '{ex}'
            )
        """.format(
            s=schema,
            log=log_table,
            u=owner,
            t=table,
            dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
            ex=expiration
        ), strict=False, timeme=False)


def clean_up_from_log(dbo, schema, owner):
    log_table = '__temp_log_table_{}__'.format(owner)
    dbo.query("""
        SELECT table_schema, table_name FROM {s}.{l}
        WHERE expires < '{dt}'
        """.format(
        s=schema,
        l=log_table,
        dt=datetime.datetime.now().strftime('%Y-%m-%d')
    ), timeme=False)
    to_clean = dbo.queries[-1].data
    for sch, table in tqdm(to_clean):
        dbo.query('DROP TABLE {}.{}'.format(sch, table), strict=False, timeme=False, no_comment=True)
        clean_out_log(dbo, sch, table, owner)


def clean_out_log(dbo, schema, table, owner):
    dbo.query("DELETE FROM {ls}.{lt} where table_schema='{ts}' and table_name='{tn}'".format(
        ls=schema,
        lt='__temp_log_table_{}__'.format(owner),
        ts=schema,
        tn=table
        ), strict=False, timeme=False, no_comment=True)

