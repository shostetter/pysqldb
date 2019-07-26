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
        qry = Query(self, query, strict=strict, permission=permission, temp=temp, table_log=table_log)
        self.queries.append(qry)

    def dfquery(self, query):
        """
        Returns dataframe of results of a SQL query. Will though an error if no data is returned
        :param query: SQL statement 
        :return: 
        """
        qry = Query(self, query)
        self.queries.append(qry)
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
            self.query(qry.replace('\n', ' '), strict=False)  # Fail if table not exists MS
        qry = """
            CREATE TABLE {s}.{t} (
            {cols}
            )
        """.format(s=schema, t=table_name, cols=str(['"' + str(i[0]) + '" ' + i[1] for i in input_schema]
                                                    )[1:-1].replace("'", ""))
        self.query(qry.replace('\n', ' '))

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
                           'None', 'NULL')), strict=False, table_log=False)

        df = self.dfquery("SELECT COUNT(*) as cnt FROM {s}.{t}".format(s=schema, t=table_name))
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
        self.query_time = self.query_end - self.query_start
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
        return [i.pop() for i in new_tables]

    def auto_comment(self):
        """
        Automatically generates comment for PostgreSQL tables if created with Query 
        :return: 
        """
        if self.dbo.type == 'PG':
            for t in self.new_tables:
                # tables in new_tables list will contain schema if provided, otherwise will default to public
                q = """COMMENT ON TABLE {t} IS 'Created by {u} on {d}\n{cmnt}'""".format(
                    t=t,
                    u=self.dbo.user,
                    d=self.query_start.strftime('%Y-%m-%d %H:%M'),
                    cmnt=self.comment
                )
                _ = Query(self.dbo, q, strict=False, table_log=False)

    def run_table_logging(self):
        """
        Logs new tables and runs clean up on any existing tables in the log file
        :return: 
        """
        # if self.table_log:
        #     run_log_process(self)
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
        path = kwargs.get('path', None)
        query = self.query_string
        shp_name = kwargs.get('shp_name', None)
        cmd = kwargs.get('cmd', None)
        gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")
        shp = Shapefile(self.dbo,
                        path=path,
                        query=query,
                        shp_name=shp_name,
                        cmd=cmd,
                        gdal_data_loc=gdal_data_loc)
        shp.write_shp()


class Shapefile:
    def __str__(self):
        pass

    def __init__(self, dbo, **kwargs):
        self.dbo = dbo
        self.path = kwargs.get('path', None)
        self.table = kwargs.get('table', None)
        self.schema = kwargs.get('schema', 'public')
        self.query = kwargs.get('query', None)
        self.shp_name = kwargs.get('shp_name', None)
        self.cmd = kwargs.get('cmd', None)
        self.gdal_data_loc = kwargs.get('gdal_data_loc', r"C:\Program Files (x86)\GDAL\gdal-data")

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

    def read_shp(self, precision=False):
        # TODO
        pass

    def read_feature_class(self):
        # TODO
        pass


def file_loc(typ='file'):
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


########################################################################################################################
############################################ TESTING ###################################################################
########################################################################################################################
# shp = Shapefile(db,shp_name='test', path=r'C:\Users\SHostetter\Desktop', query="select * from lion limit 5")

import configparser
config = configparser.ConfigParser()
config.read(r'C:\Users\SHostetter\Desktop\GIT\pysql\db.cfg')
db = DbConnect(type='postgres',
               server=config['PG DB']['SERVER'],
               database=config['PG DB']['DB_NAME'],
               user=config['PG DB']['DB_USER'],
               password=config['PG DB']['DB_PASSWORD']
               )
db.query_to_shp(query="select * from lion limit 5")
