# pip install plotly==5.18.0 ipywidgets
import duckdb;
import pandas as pd
import numpy as np
import time
import sys
import requests
import zipfile
import os
import io

import argparse


print("DuckDB version:", duckdb.__version__) # 1.3.0

################################
# Step 1. download duckdb extention
################################

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--sf', type=float, help="sf scale", default=0.1)
parser.add_argument('--qid', type=int, help="query", default=1)
parser.add_argument('--aggid', type=int, help="aggid", default=0)
parser.add_argument('--oid', type=int, help="oid", default=0)
parser.add_argument('--debug', type=bool, help="debug", default=False)
parser.add_argument('--sparse', type=bool, help="sparse", default=False)
parser.add_argument('--folder', type=str, help='queries folder', default='queries/')
args = parser.parse_args()

dbname = f'tpch_{args.sf}.db'
if not os.path.exists(dbname):
    con = duckdb.connect(dbname, config={'allow_unsigned_extensions' : 'true'})
    con.execute("CALL dbgen(sf="+str(args.sf)+");")
else:
    con = duckdb.connect(dbname, config={'allow_unsigned_extensions' : 'true'})

con.execute("LOAD lineage")

def run_with_lineage(sql):
    con.execute("PRAGMA clear_lineage;")
    con.execute("PRAGMA threads=1")
    con.execute("PRAGMA set_debug_lineage(false);").df()
    con.execute("PRAGMA set_lineage(true);")
    start = time.time()
    out = con.execute(sql).df()
    end = time.time()
    con.execute("PRAGMA set_lineage(false);")
    print(end-start)
    return out


qid = args.qid
qfile = f"{args.folder}q{str(qid).zfill(2)}.sql"
text_file = open(qfile, "r")
query = text_file.read().strip()
query = ' '.join(query.split())
text_file.close()

out = run_with_lineage(query)
print(out)


meta = con.execute("select * from pragma_latest_qid()").df()
print(meta)
assert(len(meta) > 0)
latest = len(meta)-1
qid = meta['query_id'][latest]
model = "count"

con.execute(f"PRAGMA PrepareLineage({qid})")

start = time.time()
lineage = con.execute(f"select * from PolyEval({qid}, {model})").df()
end = time.time()
print(end - start)
print(lineage)

con.execute("pragma clear_lineage")
