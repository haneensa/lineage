# pip3 install flask flask_compress flask_cors
import duckdb
from scorpion import runscorpion

def load_intel_db():
    create_sql = """CREATE TABLE readings as SELECT * FROM 'data/intel.csv'"""
    con.execute(create_sql)
    # hack since we don't have guards for null values
    con.execute("""UPDATE readings SET temp = COALESCE(temp, 0);""")
    con.execute("""UPDATE readings SET light = COALESCE(light, 0);""")
    con.execute("""UPDATE readings SET voltage = COALESCE(voltage, 0);""")
    con.execute("""UPDATE readings SET humidity = COALESCE(humidity, 0);""")
    con.execute("""UPDATE readings SET moteid = COALESCE(moteid, -1);""")
    #print(con.execute("select * from readings").df())

#sql = "SELECT hrint, sum(temp) as agg0, stddev(temp) AS agg1, count() as agg2, avg(temp) as agg3 FROM readings AS readings GROUP BY hrint"
sql = "SELECT hrint, stddev(temp) as agg0 FROM readings AS readings GROUP BY hrint"
agg_alias = 'agg0'

badids = [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55]
goodids = [291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320]

con = duckdb.connect(config={'allow_unsigned_extensions' : 'true'})
load_intel_db()
print(con.execute("LOAD 'build/release/repository/v1.3.0/osx_amd64/lineage.duckdb_extension'").df())

o = runscorpion(con, sql, agg_alias, goodids, badids)
for r in o['results']:
    print(r['score'])
    print(r['clauses'])
print("===============")
