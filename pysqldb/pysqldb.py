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
import decimal


class DbConnect:
    """
    Database Connection class. Contains db connection, query, inport/export tools
    """

    def __init__(self, **kwargs):
        """

        :param kwargs: 
            user (string):
            password (string):
            ldap (bool):
            type (string):
            server (string):
            database (string):
            port (int):
        """
        self.user = kwargs.get('user', None)
        self.password = kwargs.get('password', None)
        self.LDAP = kwargs.get('ldap', False)
        self.type = kwargs.get('type', None)
        self.server = kwargs.get('server', None)
        self.database = kwargs.get('database', None)
        self.port = kwargs.get('port', 5432)
        self.params = dict()
        self.conn = None
        self.queries = list()
        self.connection_start = None
        self.connect()
        self.clean_logs()
        self.data = None
        # self.pid = self.get_pid()

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
        :return: None
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
            # self.pid = self.get_pid()

        if self.type.upper() in ('MS', 'SQL', 'MSSQL', 'SQLSERVER'):
            # standardize types
            self.type = 'MS'
            if self.LDAP:
                self.params = {
                    'DRIVER': 'SQL Server Native Client 10.0',  # 'SQL Server Native Client 10.0',
                    'DATABASE': self.database,
                    'SERVER': self.server
                }
            else:
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

    def get_pid(self):
        if self.type == 'PG':
            cur = self.conn.cursor()
            try:
                cur.execute("SELECT pg_backend_pid()")
                return [i[0] for i in cur].pop()
            except:
                print ('Failure:\nOn get_pid()')
                return None
        else:
            return None

    def disconnect(self, quiet=False):
        """
        Closes connection to DB
        :return: None
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
        """
        Disconnects and reconnects from database, to avoid open blocking connections
        :return: None
        """
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
        if not self.server:
            self.server = raw_input('Server:')
        if not self.database:
            self.database = raw_input('Database name:')
        if not self.user:
            if not self.LDAP:
                self.user = raw_input('User name ({}):'.format(
                    self.database.lower()))
        if not self.password:
            if not self.LDAP:
                self.password = getpass.getpass('Password ({})'.format(
                    self.database.lower()))

    def query(self, query, **kwargs):
        """
            Runs Query object from input SQL string and adds query to queries
                :param query: String sql query to be run  
                :param kwargs: 
                    strict (bool): If true will run sys.exit on failed query attempts 
                    comment (bool): If true any new tables will automatically have a comment added to them
                    permission (bool): description 
                    temp (bool): if True any new tables will be logged for deletion at a future date 
                    remove_date (datetime.date): description
            :return: None
        """
        strict = kwargs.get('strict', True)
        permission = kwargs.get('permission', True)
        temp = kwargs.get('temp', True)
        timeme = kwargs.get('timeme', True)
        no_comment = kwargs.get('no_comment', False)
        comment = kwargs.get('comment', '')
        qry = Query(self, query, strict=strict, permission=permission, temp=temp,
                    timeme=timeme, no_comment=no_comment, comment=comment)
        self.queries.append(qry)
        self.refresh_connection()
        self.data = qry.data

    def dfquery(self, query, timeme=False):
        """
        Generates a pandas Dataframe for the results of select SQL query. 
        This will throw an error if no data is returned. 
        :param query: SQL statement 
        :param timeme: default to False, adds timing to query run
        :return: Pandas DataFrame
        """
        qry = Query(self, query, timeme=timeme)
        self.queries.append(qry)
        self.refresh_connection()
        self.data = qry.data
        return qry.dfquery()

    def type_decoder(self, typ):
        """
        Lazy type decoding from pandas to SQL. There are problems assoicated with NaN values for numeric types when 
        stored as Object dtypes. 

        This does not try to optimize for smallest size datatype.

        :param typ: Numpy dtype for column   
        :return: String representing data type 
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
        :param x: Raw csv cell value 
        :return: Formatted csv cell value as python object
        """
        if pd.isnull(x):
            return None
        if type(x) == long:
            return float(x)
        if type(x) == decimal.Decimal:
            return float(x)
        elif type(x) == unicode or type(x) == str:
            if "'" in x:
                x = str(x).replace("'", " ")
            if u'\xa0' in x:
                x = str(x.replace(u'\xa0', ' '))
            if u"\u2019" in x:
                x = str(x.replace(u"\u2019", "'"))
            if u"\u2018" in x:
                x = str(x.replace(u"\u2019", "'"))
            if "'" in x:
                x = x.replace("'", '-qte-chr-')
            return str(x)  # .replace(u'\xa0', u' ')
        elif type(x) == datetime.date:
            return x.strftime('%Y-%m-%d')
        elif type(x) == datetime.datetime:
            return x.strftime('%Y-%m-%d %H:%M')
        elif type(x) == datetime.datetime:
            return x.strftime('%Y-%m-%d %H:%M')
        # elif type(x) == pd.tslib.Timestamp: # depricated
        elif type(x) == pd.Timestamp:
            x.to_pydatetime()
            return x.strftime('%Y-%m-%d %H:%M')
        else:
            return x

    def clean_column(self, x):
        """
        Reformats column names to for database
        :param x: column name
        :return: Reformatted column name with special characters replaced 
        """
        if type(x) == int:
            x = str(x)
        a = x.strip().lower()
        b = a.replace(' ', '_')
        c = b.replace('.', '')
        d = c.replace('(s)', '')
        e = d.replace(':', '_')
        return e

    def dataframe_to_table_schema(self, df, table_name, **kwargs):
        """
        Translates Pandas DataFrame intto empty database table. 
        :param df: Pandas DataFrame to be added to database
        :param table_name: Table name to be used in database
        :param kwargs: 
            :schema (str): Database schema to use for destination in database (defaults to public (PG)/ dbo (MS))
            :overwrite (bool): If table exists in database will overwrite if True (defaults to False)
            :temp (bool): Optional flag to make table as not-temporary (defaults to False)
        :return: Table schema that was created from DataFrame
        """
        overwrite = kwargs.get('overwrite', False)
        schema = kwargs.get('schema', 'public')
        if self.type == 'MS':
            schema = kwargs.get('schema', 'dbo')
        temp = kwargs.get('temp', True)
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
        self.query(qry.replace('\n', ' '), timeme=False, temp=temp)
        return input_schema

    def dataframe_to_table(self, df, table_name, **kwargs):
        """
        Adds data from Pandas DataFrame to existing table
        :param df: Pandas DataFrame to be added to database
        :param table_name: Table name to be used in database
        :param kwargs: 
            :table_schema: schema of dataframe (returned from dataframe_to_table_schema)
            :schema (str): Database schema to use for destination in database (defaults to public (PG)/ dbo (MS))
            :overwrite (bool): If table exists in database will overwrite if True (defaults to False)
            :temp (bool): Optional flag to make table as not-temporary (defaults to False)
        :return: None
        """
        overwrite = kwargs.get('overwrite', False)
        temp = kwargs.get('temp', True)
        table_schema = kwargs.get('table_schema', None)
        schema = kwargs.get('schema', 'public')
        if not table_schema:
            table_schema = self.dataframe_to_table_schema(df, table_name, overwrite=overwrite, schema=schema, temp=temp)
        if self.type == 'MS':
            schema = kwargs.get('schema', 'dbo')
        # insert data
        print 'Reading data into Database\n'
        for _, row in tqdm(df.iterrows()):
            row = row.replace({pd.np.nan: None})  # clean up empty cells
            self.query("""
                INSERT INTO {s}.{t} ({cols})
                VALUES ({d})
            """.format(s=schema, t=table_name,
                       cols=str(['"' + str(i[0]) + '"' for i in table_schema])[1:-1].replace("'", ''),
                       d=str([self.clean_cell(i) for i in row.values])[1:-1].replace(
                           'None', 'NULL')), strict=False, timeme=False)

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
            sep (str): Separator for csv file, defaults to comma (,)
        :return: 
        """
        input_file = kwargs.get('input_file', None)
        overwrite = kwargs.get('overwrite', False)
        schema = kwargs.get('schema', 'public')
        table_name = kwargs.get('table_name', '_{u}_{d}'.format(
            u=self.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M')))
        temp = kwargs.get('temp', True)
        sep = kwargs.get('sep', ',')
        if self.type == 'MS':
            schema = kwargs.get('schema', 'dbo')
        if not input_file:
            input_file = file_loc('file')
        # use pandas to get existing data and schema

        df = pd.read_csv(input_file, sep=sep)
        if 'ogc_fid' in df.columns:
            df = df.drop('ogc_fid', 1)
        if not table_name:
            table_name = os.path.basename(input_file).split('.')[0]
        input_schema = self.dataframe_to_table_schema(df, table_name, overwrite=overwrite, schema=schema, temp=temp)
        # for larger files use GDAL to import
        if df.shape[0] > 999:
            # try to bulk load on failure should revert to insert method
            if not self.bulk_csv_to_table(input_schema=input_schema, **kwargs):
                self.dataframe_to_table(df, table_name, table_schema=input_schema, overwrite=overwrite, schema=schema,
                                        temp=temp)
        else:
            self.dataframe_to_table(df, table_name, table_schema=input_schema, overwrite=overwrite, schema=schema,
                                    temp=temp)

    def bulk_csv_to_table(self, **kwargs):
        print 'Bulk loading data...'
        input_file = kwargs.get('input_file', None)

        if self.type == 'MS':
            schema = kwargs.get('schema', 'dbo')
        else:
            schema = kwargs.get('schema', 'public')
        table_name = kwargs.get('table_name', '_{u}_{d}'.format(
            u=self.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M')))
        sep = kwargs.get('sep', ',')
        input_schema = kwargs.get('input_schema', None)
        types = {'PG': 'PostgreSQL', 'MS': 'MSSQLSpatial'}
        # Build staging table using GDAL to import data
        # This is needed because GDAL doesnt parse datatypes (all to varchar)
        if self.type == 'PG':
            cmd = """ogr2ogr -f "{t}" 
            PG:"host={server} 
            user={user} 
            dbname={db} 
            password={password}" 
            {f} 
            -oo EMPTY_STRING_AS_NULL=YES
            -nln "{schema}.stg_{tbl}" 
            """.format(t=types[self.type],
                       server=self.server,
                       user=self.user,
                       password=self.password,
                       db=self.database,
                       f=input_file,
                       schema=schema,
                       tbl=table_name
                       )
        else:
            cmd = """ogr2ogr -f "{t}" "MSSQL:server={server}; 
                     UID={user}; database={db}; PWD={password}" 
                     -nln "{schema}.stg_{tbl}" 
                     {f} 
                     -oo EMPTY_STRING_AS_NULL=YES
                     --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
                     """.format(t=types[self.type],
                                server=self.server,
                                user=self.user,
                                password=self.password,
                                db=self.database,
                                f=input_file,
                                schema=schema,
                                tbl=table_name
                                )
        # print cmd.replace('\n', ' ')
        subprocess.call(cmd.replace('\n', ' '), shell=True)
        # move data to final table
        self.query("ALTER TABLE {s}.stg_{t} DROP COLUMN IF EXISTS ogc_fid".format(
            s=schema, t=table_name), strict=False, timeme=False)
        # need to get staging field names, GDAL sanitizes differently
        if self.type == 'SQL':
            sq, p = 'TOP 1', ''
        else:
            sq, p = '', 'LIMIT 1'
        qry = Query(self, "select {sq} * from {s}.stg_{t} {p}".format(
            s=schema, t=table_name, sq=sq, p=p),
                    strict=False, timeme=False)
        qry = '''INSERT INTO {s}.{t}
                SELECT
                {cols}
                FROM {s}.stg_{t}
            '''.format(
            s=schema,
            t=table_name,
            # cast all fields to new type to move from stg to final table
            # take staging field name from stg table
            cols=str(['CAST("' + qry.data_columns[i[0]] + '" as ' + i[1][1] + ')' for i in enumerate(input_schema)
                      if i[1][0] != 'ogc_fid']).replace(
                "'", "")[1:-1]
        )
        self.query(qry, timeme=False)
        self.query("DROP TABLE {s}.stg_{t}".format(s=schema, t=table_name), timeme=False)
        df = self.dfquery("SELECT COUNT(*) as cnt FROM {s}.{t}".format(s=schema, t=table_name), timeme=False)
        print '\n{c} rows added to {s}.{t}\n'.format(c=df.cnt.values[0], s=schema, t=table_name)
        return True

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
        temp = kwargs.get('temp', True)
        if self.type == 'MS':
            schema = kwargs.get('schema', 'dbo')
        else:
            schema = kwargs.get('schema', 'public')
        if not input_file:
            input_file = file_loc('file')
        # use pandas to get existing data and schema

        df = pd.read_excel(input_file, sheet_name=sheet_name)
        if not table_name:
            table_name = os.path.basename(input_file).split('.')[0]
        self.dataframe_to_table(df, table_name, overwrite=overwrite, schema=schema, temp=temp)

    def query_to_csv(self, query, **kwargs):
        """
        Exports query results to a csv file. 
        :param query: SQL query as string type 
        :param kwargs: 
            sep: Delimiter
            strict (bool): If true will run sys.exit on failed query attempts 
            output: File path for csv file
            open_file (bool): If true will auto open the output csv file when done   
            quote_strings (bool): if true will use quote strings 
        :return: 
        """
        strict = kwargs.get('strict', True)
        output = kwargs.get('output',
                            os.path.join(os.getcwd(), 'data_{}.csv'.format(
                                datetime.datetime.now().strftime('%Y%m%d%H%M'))))
        open_file = kwargs.get('open_file', False)
        sep = kwargs.get('sep', ',')
        quote_strings = kwargs.get('quote_strings', False)
        qry = Query(self, query, strict=strict)
        print 'Writing to %s' % output
        qry.query_to_csv(output=output, open_file=open_file, quote_strings=quote_strings, sep=sep)

    def query_to_shp(self, query, **kwargs):
        """
                    Exports query results to a shp file. 
                    :param query: SQL query as string type 
                    :param kwargs: 
                        strict (bool): If true will run sys.exit on failed query attempts 
                        path: folder path for output shp
                        shp_name: file name for shape (should end in '.shp'
                        cmd: GDAL command line script that can be used to override default
                        gdal_data_loc: path to gdal data, if not stored in system env correctly
                    :return: 
                """
        strict = kwargs.get('strict', True)
        path = kwargs.get('path', None)
        shp_name = kwargs.get('shp_name', None)
        cmd = kwargs.get('cmd', None)
        gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")
        qry = Query(self, query, strict=strict)
        qry.query_to_shp(path=path,
                         # query=query,
                         shp_name=shp_name,
                         cmd=cmd,
                         gdal_data_loc=gdal_data_loc)

    def table_to_csv(self, table_name, **kwargs):

        # TODO: Build this
        pass

    def shp_to_table(self, **kwargs):
        """
        Imports shape file to database. This uses GDAL to generate the table.
        :param kwargs: 
            :dbo: DbConnect object 
            :path: File path of the shapefile
            :table: Table name to use in the database
            :schema: Schema to use in the database
            :shp_name: = Shapefile name (ends in .shp) 
            :cmd: Optional ogr2ogr command to overwrite default
            :srid: SRID to use (defaults to 2263)
            :gdal_data_loc: file path fo the GDAL data (defaults to C:\Program Files (x86)\GDAL\gdal-data)
            :precision: Sets percision flag in ogr (defaults to -lco precision=NO)
            :private: Flag for permissions in database (Defaults to false - will grant select to public)
        :return: 
        """
        dbo = kwargs.get('dbo', self)
        path = kwargs.get('path', None)
        table = kwargs.get('table', None)
        schema = kwargs.get('schema', 'public')
        shp_name = kwargs.get('shp_name', None)
        cmd = kwargs.get('cmd', None)
        srid = kwargs.get('srid', '2263')
        port = dbo.port
        gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")
        precision = kwargs.get('precision', False),
        private = kwargs.get('private', False)
        shp = Shapefile(dbo=dbo, path=path, table=table, schema=schema, shp_name=shp_name,
                        cmd=cmd, srid=srid, gdal_data_loc=gdal_data_loc, port=port)
        shp.read_shp(precision, private)

    def feature_class_to_table(self, **kwargs):
        """
        Imports shape file to database. This uses GDAL to generate the table.
        :param kwargs: 
            :dbo: DbConnect object 
            :path: Filepath to the geodatabase 
            :table: Table name to use in the database
            :schema: Schema to use in the database
            :shp_name: = FeatureClass name 
            :cmd: Optional ogr2ogr command to overwrite default
            :srid: SRID to use (defaults to 2263)
            :gdal_data_loc: file path fo the GDAL data (defaults to C:\Program Files (x86)\GDAL\gdal-data)
            :precision: Sets percision flag in ogr (defaults to -lco precision=NO)
            :private: Flag for permissions in database (Defaults to false - will grant select to public)
        :return: 
        """
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

    def clean_logs(self):
        log_table = '__temp_log_table_{}__'.format(self.user)
        if self.type == 'PG':
            self.query(" SELECT schema_name FROM information_schema.schemata", timeme=False)
            for sch in self.queries[-1].data:
                schema = sch[0]
                # check if log table exists
                self.query("""
                            SELECT EXISTS (
                            SELECT 1
                            FROM pg_catalog.pg_tables
                            WHERE schemaname = '{s}'
                            AND tablename = '{log}'
                            )        
                        """.format(s=schema, log=log_table), timeme=False)
                x = self.queries[-1].data[0][0]
                if x:
                    clean_up_from_log(self, schema, self.user)
        elif self.type == 'MS':
            self.query("""
                select s.name as schema_name, 
                    s.schema_id,
                    u.name as schema_owner
                from sys.schemas s
                    inner join sys.sysusers u
                        on u.uid = s.principal_id
                where u.name not like 'db_%' 
                    and u.name != 'INFORMATION_SCHEMA' 
                    and u.name != 'sys'
                    and u.name != 'guest'
                order by s.schema_id
                """)
            for sch in self.queries[-1].data:
                schema = sch[0]
                # check if log table exists
                self.query("""
                                SELECT * 
                                FROM sys.tables t 
                                JOIN sys.schemas s 
                                ON t.schema_id = s.schema_id
                                WHERE s.name = '{s}' AND t.name = '{log}'
                            """.format(s=schema, log=log_table), timeme=False)
                if self.queries[-1].data:
                    x = self.queries[-1].data[0][0]
                    if x:
                        clean_up_from_log(self, schema, self.user)

    def blocking_me(self):
        """
        Runs dfquery to find which queries or users are blocking the user defined in the connection. Postgres Only.
        :return: Pandas DataFrame of blocking queries
        """
        if self.type == 'PG':
            return self.dfquery("""
            SELECT blocked_locks.pid     AS blocked_pid,
                 blocked_activity.usename  AS blocked_user,
                 blocking_locks.pid     AS blocking_pid,
                 blocking_activity.usename AS blocking_user,
                 blocked_activity.query    AS blocked_statement,
                 blocking_activity.query   AS current_statement_in_blocking_process
           FROM  pg_catalog.pg_locks         blocked_locks
            JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
            JOIN pg_catalog.pg_locks         blocking_locks 
                ON blocking_locks.locktype = blocked_locks.locktype
                AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
                AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                AND blocking_locks.pid != blocked_locks.pid
            JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
           WHERE NOT blocked_locks.GRANTED
           AND blocked_activity.usename = '%s'
           ORDER BY blocking_activity.usename;
        """ % self.user)

    def kill_blocks(self):
        """
        Will kill any queries that are blocking, that the user (defined in the connection) owns. Postgres Only.
        :return: None
        """
        if self.type == 'PG':
            self.query("""
            SELECT blocking_locks.pid AS blocking_pid
               FROM  pg_catalog.pg_locks         blocked_locks
                JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
                JOIN pg_catalog.pg_locks         blocking_locks 
                    ON blocking_locks.locktype = blocked_locks.locktype
                    AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
                    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                    AND blocking_locks.pid != blocked_locks.pid
                JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
               WHERE NOT blocked_locks.GRANTED
               and blocking_activity.usename = '%s'
            """ % self.user)
            to_kill = [i[0] for i in self.queries[-1].data]
            if to_kill:
                print 'Killing %i connections' % len(to_kill)
                for pid in tqdm(to_kill):
                    self.query("""SELECT pg_terminate_backend(%i);""" % pid)

    def my_tables(self, schema='public'):
        """
        Get a list of tables for which you are the owner (PG only).
        :param schema: Schema to look in (defaults to public)
        :return: Pandas DataFRame of the table list
        """
        if self.type == 'PG':
            return self.dfquery("""
                SELECT
                    tablename, tableowner
                FROM
                    pg_catalog.pg_tables
                WHERE
                    schemaname ='{s}'
                    AND tableowner='{u}'
                ORDER BY 
                    tablename
            """.format(s=schema, u=self.user))


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
        """
        self.dbo = dbo
        self.query_string = query_string
        self.strict = kwargs.get('strict', True)
        self.permission = kwargs.get('permission', True)
        self.temp = kwargs.get('temp', True)
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
        self.renamed_tables = list()
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
        self.query_string = self.query_string.replace('-qte-chr-', "''")
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
            self.renamed_tables = self.query_renames_table()
            if self.permission:
                for t in self.new_tables:
                    self.dbo.query('grant select on {t} to public;'.format(t=t), timeme=False)
            if self.renamed_tables:
                for i in self.renamed_tables.keys():
                    self.rename_index(i, self.renamed_tables[i])
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

    def query_renames_table(self):
        new_tables = dict()
        rename_tables = r'(alter table\s+)([\w]*\.)?([\w]*)\s+(rename to\s+)([\w]*)'
        matches = re.findall(rename_tables, self.query_string.lower())
        for row in matches:
            if row:
                old_schema = row[1]
                old_table = row[2]
                new_table = row[-1]
                new_tables[old_schema+new_table] = old_table
        return new_tables

    def rename_index(self, new_table, old_table):
        # get indecies for new table
        sch, tbl = new_table.split('.')
        self.dbo.query("""
            SELECT indexname
            FROM pg_indexes
            WHERE tablename = '{t}'
             AND schemaname='{s}';
        """.format(t=tbl, s=sch), timeme=False)
        indecies = self.dbo.data
        for idx in indecies:
            if old_table in idx[0]:
                new_idx = idx[0].replace(old_table, tbl)
                self.dbo.query("ALTER INDEX IF EXISTS {s}.{i} RENAME to {i2}".format(
                    s=sch, i=idx[0], i2=new_idx
                ), timeme=False)



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
                _ = Query(self.dbo, q, strict=False, timeme=False)

    def run_table_logging(self):
        """
        Logs new tables and runs clean up on any existing tables in the log file
        :return: 
        """
        if self.temp:
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

    def chunked_write_csv(self, **kwargs):
        """
        Writes results of the query to a csv file. 
        Performs the same operations as query_to_csv, but brakes data into chunks 
        to deal with memory errors for large files. 
        :param kwargs:
            :output: String for csv output file location (defaults to current directory)
            :open_file: Boolean flag to auto open output file
            :quote_strings: Boolean flag for adding quote strings to output
            :sep: Separator for csv (defaults to ',')
        :return: None
        """
        print 'Large file...Writing %i rows of data...' % len(self.data)
        output = kwargs.get('output',
                            os.path.join(os.getcwd(), 'data_{}.csv'.format(
                                datetime.datetime.now().strftime('%Y%m%d%H%M'))))
        open_file = kwargs.get('open_file', False)
        quote_strings = kwargs.get('quote_strings', False)
        sep = kwargs.get('sep', ',')

        # break data into chunks
        def chunks(size=100000):
            """
            Breaks large datasets into smaller subsets
            :param size: Integer for the size of the chunks (defaults to 100,000)
            :return: Generator for data in 100,000 record chunks (list of lists)
            """
            n = len(self.data) / size
            for i in range(0, n):
                yield self.data[i::n], i

        # write to csv
        l = chunks()
        for (chunk, pos) in l:
            # convert to data frame
            if self.dbo.type == 'MS':
                self.data = [tuple(i) for i in self.data]
                df = pd.DataFrame(self.data, columns=self.data_columns)
            else:
                df = pd.DataFrame(chunk, columns=self.data_columns)
            # Only write header for 1st chunk
            if pos == 0:
                # Write out 1st chunk
                if quote_strings:
                    df.to_csv(output, index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC, sep=sep)
                else:
                    df.to_csv(output, index=False, quotechar="'", sep=sep)
            else:
                if quote_strings:
                    df.to_csv(output, index=False, quotechar="'", quoting=csv.QUOTE_NONNUMERIC, sep=sep,
                              mode='a', header=False)
                else:
                    df.to_csv(output, index=False, quotechar="'", sep=sep, mode='a', header=False)
        if open_file:
            os.startfile(output)

    def query_to_csv(self, **kwargs):
        """
        Writes results of the query to a csv file
        :param kwargs:
            :output: String for csv output file location (defaults to current directory)
            :open_file: Boolean flag to auto open output file
            :quote_strings: Boolean flag for adding quote strings to output
            :sep: Separator for csv (defaults to ',')
        :return: None
        """
        output = kwargs.get('output',
                            os.path.join(os.getcwd(), 'data_{}.csv'.format(
                                datetime.datetime.now().strftime('%Y%m%d%H%M'))))
        open_file = kwargs.get('open_file', False)
        quote_strings = kwargs.get('quote_strings', False)
        sep = kwargs.get('sep', ',')

        if len(self.data) > 100000:
            self.chunked_write_csv(**kwargs)

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
        self.port = kwargs.get('port', 5432)
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
        os.system(self.cmd.replace('{}'.format('*' * len(self.dbo.password)), self.dbo.password).replace('\n', ' '))
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
        port = self.port
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
        self.table = self.table.lower()

        if self.table_exists():
            # clean up spatial index
            self.del_indexes()
            print 'Deleting existing table {s}.{t}'.format(s=self.schema, t=self.table)
            self.dbo.query("DROP TABLE IF EXISTS {s}.{t} CASCADE".format(s=self.schema, t=self.table))

        cmd = 'ogr2ogr --config GDAL_DATA "{gdal_data}" -nlt PROMOTE_TO_MULTI -overwrite -a_srs ' \
              'EPSG:{srid} -progress -f "PostgreSQL" PG:"host={host} port={port} dbname={dbname} ' \
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
            perc=precision,
            port=port
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
            self.dbo.query('grant select on {s}."{t}" to public;'.format(
                s=self.schema,
                t=self.table))

        self.rename_geom()

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
            self.dbo.query('grant select on {s}."{t}" to public;'.format(
                s=self.schema,
                t=self.table))

        self.rename_geom()

    def rename_geom(self):
        self.dbo.query("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = '{s}'
            AND table_name   = '{t}';
        """.format(s=self.schema, t=self.table))
        if 'wkb_geometry' in [i[0] for i in self.dbo.queries[-1].data]:
            # rename column
            self.dbo.query("""
                ALTER TABLE {s}.{t} 
                RENAME wkb_geometry to geom
            """.format(s=self.schema, t=self.table))
            # rename index
            self.dbo.query("""
                ALTER INDEX IF EXISTS
                {s}.{t}_wkb_geometry_geom_idx
                RENAME to {t}_geom_idx
            """.format(s=self.schema, t=self.table))


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
                    expires date, 
                    primary key (table_schema, table_name)
                    )
                """.format(s=schema, log=log_table), timeme=False)
    # add new table to log
    if table != log_table:
        if dbo.type == 'PG':
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
            ON CONFLICT (table_schema, table_name) DO 
            UPDATE SET expires = EXCLUDED.expires, created_on=EXCLUDED.created_on
            """.format(
                s=schema,
                log=log_table,
                u=owner,
                t=table,
                dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                ex=expiration
            ), strict=False, timeme=False)
        elif dbo.type == 'MS':
            dbo.query("""
            MERGE dbo.__temp_log_table_risadmin__ AS [Target] 
            USING (
                SELECT 
                    '{u} 'as table_owner,
                    '{s}' as table_schema,
                    '{t}' as table_name,
                    '{dt}' as created_on , 
                    '{ex}' as expires
            ) AS [Source] ON [Target].table_schema = [Source].table_schema 
                and [Target].table_name = [Source].table_name 
            WHEN MATCHED THEN UPDATE 
                SET [Target].created_on = [Source].created_on,
                [Target].expires = [Source].expires
            WHEN NOT MATCHED THEN INSERT (table_owner, table_schema, table_name, created_on, expires) 
                VALUES ([Source].table_owner, [Source].table_schema, [Source].table_name, 
                    [Source].created_on, [Source].expires);
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
    cleaned = 0
    for sch, table in to_clean:
        dbo.query('DROP TABLE {}.{}'.format(sch, table), strict=False, timeme=False, no_comment=True)
        clean_out_log(dbo, sch, table, owner)
        cleaned += 1
    if cleaned > 0:
        'Removed {} temp tables'.format(cleaned)


def clean_out_log(dbo, schema, table, owner):
    dbo.query("DELETE FROM {ls}.{lt} where table_schema='{ts}' and table_name='{tn}'".format(
        ls=schema,
        lt='__temp_log_table_{}__'.format(owner),
        ts=schema,
        tn=table
    ), strict=False, timeme=False, no_comment=True)


def print_cmd_string(password_list, cmd_string):
    for p in password_list:
        cmd_string = cmd_string.replace(p, '*'*len(p))
    return cmd_string


def pg_to_sql(pg, ms, org_table, **kwargs):
    """
    Migrates tables from Postgres to SQL Server, generates spatial tables in MS if spatial in PG.
    :param pg: DbConnect instance connecting to PostgreSQL source database
    :param ms: DbConnect instance connecting to SQL Server destination database
    :param org_table: table name of table to migrate
    :param kwargs: 
        :ldap (bool): Flag for using LDAP credentials (defaults to False)
        :org_schema: PostgreSQL schema for origin table (defaults to public)
        :dest_schema: SQL Server schema for destination table (defaults to dbo) 
        :print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :return: 
    """
    LDAP = kwargs.get('ldap', False)
    spatial = kwargs.get('spatial', False)
    org_schema = kwargs.get('org_schema', 'public')
    dest_schema = kwargs.get('dest_schema', 'dbo')
    print_cmd = kwargs.get('print_cmd', False)
    if spatial:
        spatial = '-a_srs EPSG:2263 '
    else:
        spatial = ' '
    if LDAP:
        cmd = """
                ogr2ogr -overwrite -update -f MSSQLSpatial "MSSQL:server={ms_server};database={ms_db};UID={ms_user};PWD={ms_pass}" 
                PG:"host={pg_host} port={pg_port} dbname={pg_database} user={pg_user} password={pg_pass}" 
                {pg_schema}.{pg_table} -lco OVERWRITE=yes -lco SCHEMA={ms_schema} {spatial}-progress 
                --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
                """.format(
            ms_pass='',
            ms_user='',
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            pg_database=pg.database,
            pg_schema=org_schema,
            pg_table=org_table,
            ms_schema=dest_schema,
            spatial=spatial
        )
    else:
        cmd = """
        ogr2ogr -overwrite -update -f MSSQLSpatial "MSSQL:server={ms_server};database={ms_db};UID={ms_user};PWD={ms_pass}" 
        PG:"host={pg_host} port={pg_port} dbname={pg_database} user={pg_user} password={pg_pass}" 
        {pg_schema}.{pg_table} -lco OVERWRITE=yes -lco SCHEMA={ms_schema} {spatial}-progress 
        --config MSSQLSPATIAL_USE_GEOMETRY_COLUMNS NO
        """.format(
            ms_pass=ms.password,
            ms_user=ms.user,
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            pg_database=pg.database,
            pg_schema=org_schema,
            pg_table=org_table,
            ms_schema=dest_schema,
            spatial=spatial
        )
    if print_cmd:
        print print_cmd_string([ms.password, pg.password], cmd)
    subprocess.call(cmd.replace('\n', ' '), shell=True)


def sql_to_pg_qry(ms, pg, query, **kwargs):
    LDAP = kwargs.get('ldap', False)
    spatial = kwargs.get('spatial', True)
    dest_schema = kwargs.get('dest_schema', 'public')
    print_cmd = kwargs.get('print_cmd', False)
    table_name = kwargs.get('table_name', '_{u}_{d}'.format(
        u=pg.user, d=datetime.datetime.now().strftime('%Y%m%d%H%M')))

    if spatial:
        spatial = 'MSSQLSpatial'
    else:
        spatial = 'MSSQL'
    if LDAP:
        cmd = """
            ogr2ogr -overwrite -update -f "PostgreSQL" PG:"host={pg_host} port={pg_port} dbname={pg_database} 
            user={pg_user} password={pg_pass}" -f {spatial} "MSSQL:server={ms_server};database={ms_database};
            UID={ms_user};PWD={ms_pass}" -sql "{sql_select}" -lco OVERWRITE=yes 
            -lco SCHEMA={pg_schema} -nln {table_name} -progress
            """.format(
            ms_pass='',
            ms_user='',
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            ms_database=ms.database,
            pg_database=pg.database,
            pg_schema=dest_schema,
            sql_select=query,
            spatial=spatial,
            table_name=table_name
        )
    else:
        cmd = """
                ogr2ogr -overwrite -update -f "PostgreSQL" PG:"host={pg_host} port={pg_port} dbname={pg_database} 
                user={pg_user} password={pg_pass}" -f {spatial} "MSSQL:server={ms_server};database={ms_database};
                UID={ms_user};PWD={ms_pass}" -sql "{sql_select}" -lco OVERWRITE=yes 
                -lco SCHEMA={pg_schema} -nln {table_name} -progress
                """.format(
            ms_pass=ms.password,
            ms_user=ms.user,
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            ms_database=ms.database,
            pg_database=pg.database,
            pg_schema=dest_schema,
            sql_select=query,
            spatial=spatial,
            table_name=table_name
        )
    if print_cmd:
        print print_cmd_string([ms.password, pg.password], cmd)
    subprocess.call(cmd.replace('\n', ' '), shell=True)
    clean_geom_column(pg, table_name, dest_schema)


def sql_to_pg(ms, pg, org_table, **kwargs):
    """    
    Migrates tables from SQL Server to PostgreSQL, generates spatial tables in PG if spatial in MS.
    :param ms: DbConnect instance connecting to SQL Server destination database
    :param pg: DbConnect instance connecting to PostgreSQL source database
    :param org_table: table name of table to migrate
    :param kwargs: 
        :ldap (bool): Flag for using LDAP credentials (defaults to False)
        :org_schema: SQL Server schema for origin table (defaults to dbo) 
        :dest_schema: PostgreSQL schema for destination table (defaults to public)
        :print_cmd: Option to print he ogr2ogr command line statement (defaults to False) - used for debugging
    :return: 
    """
    LDAP = kwargs.get('ldap', False)
    spatial = kwargs.get('spatial', True)
    org_schema = kwargs.get('org_schema', 'dbo')
    dest_schema = kwargs.get('dest_schema', 'public')
    print_cmd = kwargs.get('print_cmd', False)
    if spatial:
        spatial = 'MSSQLSpatial'
    else:
        spatial = 'MSSQL'
    if LDAP:
        cmd = """
        ogr2ogr -overwrite -update -f "PostgreSQL" PG:"host={pg_host} port={pg_port} dbname={pg_database} 
        user={pg_user} password={pg_pass}" -f {spatial} "MSSQL:server={ms_server};database={ms_database};
        UID={ms_user};PWD={ms_pass}" {ms_schema}.{ms_table} -lco OVERWRITE=yes 
        -lco SCHEMA={pg_schema} -progress
        """.format(
            ms_pass='',
            ms_user='',
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            ms_database=ms.database,
            pg_database=pg.database,
            pg_schema=dest_schema,
            pg_table=org_table,
            ms_table=org_table,
            ms_schema=org_schema,
            spatial=spatial
        )
    else:
        cmd = """
        ogr2ogr -overwrite -update -f "PostgreSQL" PG:"host={pg_host} port={pg_port} dbname={pg_database} 
        user={pg_user} password={pg_pass}" -f {spatial} "MSSQL:server={ms_server};database={ms_database};
        UID={ms_user};PWD={ms_pass}" {ms_schema}.{ms_table} -lco OVERWRITE=yes 
        -lco SCHEMA={pg_schema} -progress
        """.format(
            ms_pass=ms.password,
            ms_user=ms.user,
            pg_pass=pg.password,
            pg_user=pg.user,
            ms_server=ms.server,
            ms_db=ms.database,
            pg_host=pg.server,
            pg_port=pg.port,
            ms_database=ms.database,
            pg_database=pg.database,
            pg_schema=dest_schema,
            pg_table=org_table,
            ms_table=org_table,
            ms_schema=org_schema,
            spatial=spatial
        )
    if print_cmd:
        print print_cmd_string([ms.password, pg.password], cmd)
    subprocess.call(cmd.replace('\n', ' '), shell=True)
    clean_geom_column(pg, org_table, dest_schema)


def pg_to_pg(from_pg, to_pg, org_table, **kwargs):
    org_schema = kwargs.get('org_schema', 'public')
    dest_schema = kwargs.get('dest_schema', 'public')
    print_cmd = kwargs.get('print_cmd', False)
    dest_name = kwargs.get('dest_name', org_table)
    cmd = """
    ogr2ogr -overwrite -update -f "PostgreSQL" PG:"host={to_pg_host} port={to_pg_port} dbname={to_pg_database} 
    user={to_pg_user} password={to_pg_pass}" PG:"host={from_pg_host} port={from_pg_port} 
    dbname={from_pg_database}  user={from_pg_user} password={from_pg_pass}" {from_pg_schema}.{from_pg_table} 
    -lco OVERWRITE=yes -lco SCHEMA={to_pg_schema} -nln {to_pg_name} -progress
    """.format(
        from_pg_host=from_pg.server,
        from_pg_port=from_pg.port,
        from_pg_database=from_pg.database,
        from_pg_user=from_pg.user,
        from_pg_pass=from_pg.password,
        to_pg_host=to_pg.server,
        to_pg_port=to_pg.port,
        to_pg_database=to_pg.database,
        to_pg_user=to_pg.user,
        to_pg_pass=to_pg.password,
        from_pg_schema=org_schema,
        to_pg_schema=dest_schema,
        from_pg_table=org_table,
        to_pg_name=dest_name
    )
    if print_cmd:
        print print_cmd_string([from_pg.password, to_pg.password], cmd)
    subprocess.call(cmd.replace('\n', ' '), shell=True)
    clean_geom_column(to_pg, dest_name, dest_schema)


def clean_geom_column(db, table, schema):
    # check if there is a geom column
    # rename column to geom (only if wkb_geom) otherwise could cause issues if more than 1 geom
    db.query("""SELECT COLUMN_NAME 
                FROM information_schema.COLUMNS 
                WHERE data_type='USER-DEFINED' 
                and TABLE_NAME='{t}'
                and table_schema = '{s}'
            """.format(t=table, s=schema), timeme=False)
    if db.data:
        if db.data[-1][0] == 'wkb_geometry':
            db.query("ALTER TABLE {s}.{t} RENAME COLUMN wkb_geometry to geom".format(t=table, s=schema),
                     timeme=False)

