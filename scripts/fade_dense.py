from fade_utils import PrepTargetMatrix
from fade_utils import timed, Get_DB

import os
import argparse
from pathlib import Path
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import duckdb

# -----------------------------
# Command-line arguments
# -----------------------------
parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument("--sf", type=float, default=1,
                    help="TPCH scale factor")
parser.add_argument("--qid", type=int, default=1,
                    help="TPCH query id")
parser.add_argument("--aggid", type=int, default=0,
                    help="Aggregation id (reserved)")
parser.add_argument("--oids", nargs="+", type=int, default=[0],
                    help="List of output row ids to analyze")
parser.add_argument("--workers", type=int, default=1,
                    help="Number of execution threads")
parser.add_argument("--debug", action="store_true",
                    help="Enable lineage debug mode")
parser.add_argument("--folder", type=str, default="queries/",
                    help="Folder containing TPCH SQL queries")

args = parser.parse_args()

# -----------------------------
# Initialize TPCH database and Load lineage extension
# -----------------------------
dbname = f'tpch_{args.sf}.db'
extensions = ["build/release/repository/v1.3.0/osx_amd64/lineage.duckdb_extension"]
con = Get_DB(dbname, args.sf, extensions)

# -----------------------------
# Prep intervention matrix (target matrix)
# User provided predicates projection list.
# For each table with interventions, this generates:
# 1) {table_name}_tm -> iid, mask_0, .., mask_n
# 2) {table_name}_preds -> i, j, predicate string
# -----------------------------
predicates_per_table = {"lineitem": []}

res =  con.execute("""select distinct l_extendedprice as v
                   from lineitem order by l_extendedprice limit 10""")
for row in res.fetchall():
    predicates_per_table["lineitem"].append(f"l_extendedprice<='{row[0]}'")

res =  con.execute("select distinct l_shipmode as v from lineitem")
for row in res.fetchall():
    predicates_per_table["lineitem"].append(f"l_shipmode='{row[0]}'")

print(predicates_per_table)

# TODO: create a function that takes n_interventions, and generate random masks
PrepTargetMatrix(con, predicates_per_table)

# -----------------------------
# Read TPCH query
# -----------------------------
query = " ".join((Path(args.folder) / f"q{args.qid:02d}.sql").read_text().split())
print(f"1. Base Query:\n{query}")

con.execute(f"PRAGMA threads={args.workers}")
if args.debug:
    con.execute("PRAGMA set_debug_lineage(True)")

# -----------------------------
# Execute query with lineage
# -----------------------------

con.execute("PRAGMA set_lineage(True)")

base_result = timed("Query Result", lambda: con.execute(query).df())
print(f"{base_result}")

# Preserve row identity for lineage joins
base_result["idx"] = base_result.index

con.execute("PRAGMA set_lineage(False)")

oids = args.oids
n_output = len(oids)
if len(base_result) < n_output:
    n_output = len(base_result)
    oids = [x for x in range(n_output)]

# Internal Query id
qid = con.execute("select max(query_id) from lineage_meta()").df().iat[0,0]
print(f"Query ID: {qid}")

start = timer()
con.execute(f"PRAGMA PrepareLineage({qid})")
end = timer()
print(f"Lineage Post Processing took: {end - start:.5f}s")

# TODO: make pragma that returns table names, operator ids
# lineage table -> one for output id, the others are for input ids for that table
spja_block = con.execute(f"""select * from read_block({qid})
                         where output_id in {oids}""").df()
print(f"SPJAU Lineage Block Columns: {spja_block.columns}")

# Identify lineage column referring to lineitem
lineitem_cols = [col for col in spja_block.columns if 'lineitem' in col]
assert lineitem_cols, "No lineitem lineage column found"
lineitem_col = lineitem_cols[0]

##################
### SQL: dense target matrix
### for row in dict_codes.itertuples(index=True):
###    for k in range(n_interventions):
# ##       out1[row.out, k] += TM[]
##################
n_interventions = con.execute("select count(*) as c from lineitem_preds").df().iat[0, 0]

## run this for every 64 bit
n_masks = (n_interventions + 63) // 64
width = min(64, n_interventions)
results = []

### TODO: specfiy n_interventions, use random masks
start = timer()
for mid in range(n_masks):
    q = f"""
    WITH temp AS (
        SELECT
            b.output_id AS out,
            {mid} AS mid,
            k,
            SUM(get_bit(l.mask_{mid}, k::INT) * base.l_extendedprice) AS v
        FROM spja_block b
        JOIN lineitem_tm l ON l.iid = b.{lineitem_col}
        JOIN lineitem base ON base.rowid = l.iid
        CROSS JOIN range({width}) r(k)
        GROUP BY out, k
    )
    SELECT
        out, mid, k, v, pred
    FROM temp
    JOIN lineitem_preds ON (i = mid * 64 AND j = k)
    ORDER BY v DESC
    LIMIT 3
    """
    res = con.execute(q).df()
    results.append(res)
    # JOIN base_result base ON (out = idx)
end = timer()

print(results)
print(f"Took: {end-start:.5f}s")

con.execute("pragma clear_lineage")
