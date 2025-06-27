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
badids = [34, 35, 36, 37, 38, 39, 40, 41, 42, 43]
goodids = [292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307]

badids = [9, 25, 43, 126, 199, 200, 223, 224, 243, 269]
goodids = [34, 61, 77, 78, 92, 144, 167, 168, 169, 170, 194, 195, 214, 215, 216, 282]

badids = [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46]
goodids = [294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320]
badids = [17, 32, 88, 133, 134, 135, 150, 175, 176, 214, 262, 263, 288, 309, 310, 334]
goodids = [11, 53, 68, 126, 127, 128, 147, 170, 209, 210, 236, 254, 255, 256, 257, 258, 259, 284, 305, 306, 330]

badids = [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55]
goodids = [291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320]
#goodids = [180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193]
#badids = [252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280, 281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294 , 295] 



#badids = [125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136]
#goodids = [238, 239, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279, 280 , 281, 282]                  

con = duckdb.connect(config={'allow_unsigned_extensions' : 'true'})
load_intel_db()
con.execute("LOAD 'build/release/repository/v1.3.0/linux_amd64/fade.duckdb_extension'").df()

o = runscorpion(con, sql, agg_alias, goodids, badids)
for r in o['results']:
    print(r['score'])
    print(r['clauses'])
