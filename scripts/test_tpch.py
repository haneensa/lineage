import re
import ast
import json
import duckdb
import argparse
from timeit import default_timer as timer

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--sf', type=float, help="sf scale", default=0.1)
parser.add_argument('--qid', type=int, help="query", default=1)
parser.add_argument('--debug', type=bool, help="debug", default=False)
parser.add_argument('--folder', type=str, help='queries folder', default='queries/')
args = parser.parse_args()


dbname = f'tpch_{args.sf}.db'
con = duckdb.connect(config={'allow_unsigned_extensions' : 'true'})
con.execute("CALL dbgen(sf="+str(args.sf)+");")

qid = args.qid
print(con.execute("LOAD 'build/release/repository/v1.3.0/osx_amd64/lineage.duckdb_extension'").df())
qfile = f"{args.folder}q{str(qid).zfill(2)}.sql"
text_file = open(qfile, "r")
query = text_file.read().strip()
query = ' '.join(query.split())
text_file.close()
con.execute("PRAGMA threads=1")
if args.debug:
    con.execute("PRAGMA set_debug_lineage(True)")
con.execute("PRAGMA set_lineage(True)")
print(query)
print(con.execute(query).df())
con.execute("PRAGMA set_lineage(False)")

start = timer()
lineage = con.execute("select * from global_lineage()").df()
end = timer()
print(end - start)

print(lineage)

start = timer()
lineage = con.execute("select * from LQ()").df()
end = timer()

print(end - start)
print(lineage)

con.execute("pragma clear_lineage")
