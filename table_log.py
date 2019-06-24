import cPickle as pickle
import datetime

def write_log(data, data_file='to_remove.lg'):
    """
    Write pickle data to log file 
    :param data: Dictionary with table name, created date, removal date, and database connection params 
    :param data_file: 
    :return: 
    """
    ouf = open(data_file, 'w')
    pickle.dump(data, ouf)
    ouf.close()


def read_log(data_file='to_remove.lg'):
    """
    Read pickle data from log file
    :param data_file: tables to remove log file
    :return: unpickled data from log file 
    """
    # Read in existing queue
    try:
        inf = open(data_file)
    except:
        write_log([])
        inf = open(data_file)
    data = pickle.load(inf)
    inf.close()
    return data


def log_table(query, remove_date=datetime.date.today() + datetime.timedelta(days=7)):
    """
    Log tables for deletion. 
    Assumes the log file will never get very large  
    :param query: pysqldb.Query instance
    :param remove_date: Datetime.date after which the table will be deleted 
    :return: None
    """
    if query.temp:
        # get existing log
        to_log = read_log()
        for tbl in query.new_tables:
            # generate log data
            to_log.append({
                'table': tbl,
                'created': query.query_start,
                'removal': remove_date,
                'db_info': {
                    'database': query.dbo.database,
                    'server': query.dbo.server,
                    'db_type': query.dbo.type,
                    'user': query.dbo.user
                }
            })
        # write back data to log
        write_log(to_log)


def check_db_connection(query, db_info):
    """
    Checks if connection params match
    :param db_info: 
    :return: 
    """
    if {
        'db_type': query.dbo.type,
        'server': query.dbo.server,
        'database': query.dbo.database,
        'user': query.dbo.user
    } == db_info:
        return True
    else:
        return False


def cleanup_database(query, data_file='to_remove.lg'):
    new_log = list()
    rp = read_log(data_file)
    print '{} tables found in queue'.format(len(rp))
    for tbl in rp:
        if tbl['removal'] < datetime.date.today():
            if check_db_connection(query, tbl['db_info']):
                # new db connection
                db2 = query.dbo.__class__(database=query.dbo.database,
                                          server=query.dbo.server,
                                          user=query.dbo.user,
                                          password=query.dbo.password,
                                          type=query.dbo.type)
                try:
                    # Drop table from DB using a new DB connection so it doesnt block any other work if table is locked
                    db2.query('DROP TABLE {}'.format(tbl['table']), strict=False, table_log=False)
                except:
                    print 'Clean up failed {}'.format(tbl)
                db2.disconnect(True)
        else:
            new_log.append(tbl)
    if len(new_log) > 0:
        print '{} tables still in queue'.format(len(new_log))
    write_log(new_log)


def run_log_process(query):
    log_table(query)
    cleanup_database(query)
