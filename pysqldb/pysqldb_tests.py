import configparser
from pysqldb.pysqldb import *
from table_log import *


def test_ms():
    # SET UP DB CONNECTION
    config = configparser.ConfigParser()
    config.read('db.cfg')

    db = DbConnect(type='ms',
                   server=config['MS DB']['SERVER'],
                   database=config['MS DB']['DB_NAME'],
                   user=config['MS DB']['DB_USER'],
                   password=config['MS DB']['DB_PASSWORD']
                   )
    print '\n\nTesting DB CONNECTION (MS)\n'
    print db
    print '\nTesting CREATE TABLE QUERIES (MS)\n'
    db.query("""
    CREATE TABLE {db}.{schema}._test_table_ (
        id int,
        name varchar(50)
    ); """.format(db=db.database, schema='dbo'), temp=True, strict=False)
    print 'TABLES CREATED:\n' + str(db.queries[-1].new_tables)
    db.query("""
    INSERT INTO {db}.{schema}._test_table_ (id, name)
        values (0,'test0')
    """.format(db=db.database, schema='dbo'))

    print '\nTesting UPDATE QUERIES (MS)\n'
    db.query("""
        UPDATE {db}.{schema}._test_table_ 
        SET id = 1, name = 'test';
    """.format(db=db.database, schema='dbo'))

    print '\nTesting SELECT QUERIES (MS)\n'
    db.query("""
            SELECT * FROM {db}.{schema}._test_table_;
    """.format(db=db.database, schema='dbo'))
    print db.queries[-1].data
    print '-'*25
    db.dfquery("""
            SELECT * FROM {db}.{schema}._test_table_ ;
    """.format(db=db.database, schema='dbo'))
    print '\nTesting DROP TABLE QUERIES (MS)\n'

    db.query("DROP TABLE {db}.{schema}._test_table_".format(db=db.database, schema='dbo'))
    db.disconnect()


def test_pg():
    # SET UP DB CONNECTION
    config = configparser.ConfigParser()
    config.read('db.cfg')

    db = DbConnect(type='postgres',
                   server=config['PG DB']['SERVER'],
                   database=config['PG DB']['DB_NAME'],
                   user=config['PG DB']['DB_USER'],
                   password=config['PG DB']['DB_PASSWORD']
                   )
    print '\n\nTesting DB CONNECTION (PG)\n'
    print db
    print '\nTesting CREATE TABLE QUERIES (PG)\n'
    db.query("""
        CREATE TABLE {schema}._test_table_ (
            id int,
            name varchar(50)
        ); """.format(schema='working'), temp=True)
    print 'TABLES CREATED:\n' + str(db.queries[-1].new_tables)

    db.query("""
        INSERT INTO {schema}._test_table_ (id, name)
            values (0,'test0')
        """.format(schema='working'))
    print '\nTesting UPDATE QUERIES (PG)\n'
    db.query("""
            UPDATE {schema}._test_table_ 
            SET id = 1, name = 'test';
        """.format(schema='working'))

    print '\nTesting SELECT QUERIES (PG)\n'
    db.query("""
                SELECT * FROM {schema}._test_table_;
        """.format(schema='working'))
    print db.queries[-1].data
    print '-' * 25
    db.dfquery("""
                SELECT * FROM {schema}._test_table_ ;
        """.format(schema='working'))
    print '\nTesting DROP TABLE QUERIES (PG)\n'

    db.query("DROP TABLE {schema}._test_table_".format(schema='working'))
    db.disconnect()


def test_log():
    test_ms()
    test_pg()
    config = configparser.ConfigParser()
    config.read('db.cfg')

    db = DbConnect(type='postgres',
                   server=config['PG DB']['SERVER'],
                   database=config['PG DB']['DB_NAME'],
                   user=config['PG DB']['DB_USER'],
                   password=config['PG DB']['DB_PASSWORD']
                   )
    # print '\n\nTesting DB CONNECTION (PG)\n'
    print db
    db.query("""
            CREATE TABLE {schema}._test_table_2 (
                id int,
                name varchar(50)
            ); """.format(schema='working'), strict=False, temp=True)
    #q = db.queries[-1]
    # print 'TABLES CREATED:\n' + str(q.new_tables)
    # log_table(q)
    #run_log_process(q)

    # change date and run some other query
    d = read_log()
    for i in range(len(d)):
        d[i]['removal'] = datetime.date.today() + datetime.timedelta(days=-7)

    write_log(d)
    df = db.dfquery("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    AND table_type='BASE TABLE'
                """)
    # q = db.queries[-1]
    print df.head()
    db.query("""
                CREATE TABLE {schema}._test_table_3 (
                    id int,
                    name varchar(50)
                ); """.format(schema='working'), strict=False, temp=True)


def test_read_csv():
    config = configparser.ConfigParser()
    config.read('db.cfg')

    db = DbConnect(type='postgres',
                   server=config['PG DB']['SERVER'],
                   database=config['PG DB']['DB_NAME'],
                   user=config['PG DB']['DB_USER'],
                   password=config['PG DB']['DB_PASSWORD']
                   )
    db.query("drop table if exists working.test_input", table_log=False)
    db.csv_to_table(input_file=r'C:\Users\SHostetter\Desktop\GIT\pysql\data\test_input.csv', schema='working')
    df = db.dfquery("select * from working.test_input")
    print df.head()
    # use TK
    db.query("drop table if exists working.test_input", table_log=False)
    db.csv_to_table(schema='working')
    df = db.dfquery("select * from working.test_input")
    print df.head()

    # MS
    config = configparser.ConfigParser()
    config.read('db.cfg')

    db = DbConnect(type='ms',
                   server=config['MS DB']['SERVER'],
                   database=config['MS DB']['DB_NAME'],
                   user=config['MS DB']['DB_USER'],
                   password=config['MS DB']['DB_PASSWORD']
                   )
    db.query("drop table RISCRASHDATA.dbo.test_input", strict=False, table_log=False)
    db.csv_to_table(input_file=r'C:\Users\SHostetter\Desktop\GIT\pysql\data\test_input.csv', schema='dbo')
    df = db.dfquery("select * from RISCRASHDATA.dbo.test_input")
    print df.head()


def test_write_csv():
    config = configparser.ConfigParser()
    config.read('db.cfg')

    db = DbConnect(type='postgres',
                   server=config['PG DB']['SERVER'],
                   database=config['PG DB']['DB_NAME'],
                   user=config['PG DB']['DB_USER'],
                   password=config['PG DB']['DB_PASSWORD']
                   )
    db.query_to_csv("""SELECT ntacode, geom, signalized, unsignalized, total_ints FROM working.nta_sig;""",
                    open_file=True)

    # MS
    config = configparser.ConfigParser()
    config.read('db.cfg')

    db = DbConnect(type='ms',
                   server=config['MS DB']['SERVER'],
                   database=config['MS DB']['DB_NAME'],
                   user=config['MS DB']['DB_USER'],
                   password=config['MS DB']['DB_PASSWORD']
                   )

    db.query_to_csv(""" SELECT TOP 1000 cause, cf, cf2, v2cf, v2cf2
                        FROM [RISCRASHDATA].[risadmin].[fatality_nycdot_current]
                        where cf2 in (22, 23, 24, 31, 32)""",
                    open_file=True, quote_strings=False, sep='\t')




if __name__ == '__main__':
    # test_ms()
    # test_pg()
    q = test_log()
    test_read_csv()

# test_log()
# test_read_csv()