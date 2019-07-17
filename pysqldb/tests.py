# from pysqldb import *


def test(typ):
    if typ == 'MS':
        dbc = DbConnect(type='ms',
                        database='FORMS',
                        server='dot55sql01',
                        user='arcgis',
                        password='arcgis')
        return dbc
    elif typ == 'PG':
        dbc = DbConnect(type='pg',
                      database='CRASHDATA',
                      server='dotdevpgsql02',
                      user='shostetter',
                      password='Batavia79')
        return dbc
    else:
        return None

if __name__ == '__main__':
    # # testing

    ms_sample_query = 'SELECT TOP 10 INTEGRATION_ID, SRC_ADDRESS_TYPE, SRC_ON_STREET, SRC_CROSS_STREET,\
    SRC_OFF_STREET  FROM [FORMS].[dbo].[WC_ACCIDENT_F]'
    ms_sample_query2 = 'SELECT TOP 5 ACCIDENT_ID, VEHICLE_NUM, VEHICLE_TYPE_CODE\
    FROM [FORMS].[dbo].[WC_ACCIDENT_VEHICLE_F]'
    db = test('MS')
    print (db)
    qry = Query(db, query_string=ms_sample_query)
    print (qry)
    db.query(query=ms_sample_query)
    db.query(query=ms_sample_query2)
    df = db.dfquery(query=ms_sample_query)
    for i in db.queries:
        print '\n'
        print db
        print i

    db = test('PG')
    print (db)
    sample_query = '''SELECT nodeid, masterid, is_int, is_cntrln_int
    FROM public.node 
    WHERE nodeid=9033921'''
    sample_query2 = '''DROP TABLE IF EXISTS working.temp; CREATE TABLE working.temp as 
    SELECT nodeid, masterid, is_int, is_cntrln_int
    FROM public.node WHERE nodeid=9033921;
    
    DROP TABLE IF EXISTS working.temp2; CREATE TABLE working.temp2 as 
    SELECT nodeid, masterid, is_int, is_cntrln_int
    FROM public.node WHERE nodeid=9033921
    '''

    sample_query3 = '''SELECT * FROM working.temp;'''
    qry = Query(db, query_string=sample_query)
    qry = Query(db, query_string=sample_query2)
    print (qry)
    db.query(query=sample_query2)
    db.query(query=sample_query2)
    db.query(query=sample_query3)
    for i in db.queries:
        print '\n'
        print db
        print i

    df = Query(db, query_string=sample_query).dfquery()
    print df
    df = db.dfquery(query=sample_query)
    print df
    print db.queries[-1]
    # ------------------------------------------------------------------------------------------------------------
    db = DbConnect(type='ms',
                        database='RISCRASHDATA',
                        server='dotdevgissql01',
                        user='risadmin ',
                        password='RISADMIN!')
    db.dfquery("SELECT TOP 10 [NODEID] FROM [RISCRASHDATA].[dbo].[__bikesshare_p1_nodes]")
    db.query("SELECT TOP 5 [Street],[FACILITYTY],[FACILITYCL] FROM [RISCRASHDATA].[dbo].[bike_lanes_15]")
    db.query("SELECT TOP 5 [Street],[FACILITYTY],[FACILITYCL] into [RISCRASHDATA].[dbo].[_test_bike_lanes_15] \
    FROM [RISCRASHDATA].[dbo].[bike_lanes_15]")
    for q in db.queries: print q


