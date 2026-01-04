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
# Initialize TPCH database
# -----------------------------
dbname = f'tpch_{args.sf}.db'
con = duckdb.connect(
    dbname,
    config={"allow_unsigned_extensions": "true"}
)
if not os.path.exists(dbname):
    print(f"Creating TPCH database (sf={args.sf})")
    con.execute("CALL dbgen(sf="+str(args.sf)+");")

# -----------------------------
# Load lineage extension
# -----------------------------
# This assumes a locally built extension
con.execute("LOAD 'build/release/repository/v1.3.0/osx_amd64/lineage.duckdb_extension'")

# -----------------------------
# Predicate definitions
# User provided predicates projection list.
# For each table with interventions, this generates:
# 1) {table_name}_tm -> iid, mask_0, .., mask_n
# 2) {table_name}_preds -> i, j, predicate string
# -----------------------------
predicates_per_table = {"lineitem": ["l_shipmode='MAIL'", "l_shipmode='SHIP'", "l_shipmode='REG_AIR'",
              "l_shipmode='TRUCK'", "l_shipmode='RAIL'", "l_shipmode='AIR'", "l_shipmode='FOR'"]}
for table, predicates in predicates_per_table.items():
    proj_exprs = []
    n_predicates = len(predicates)
    mask_id = 0
    preds_meta = []
    # Pack predicates into into 64-bit chunks
    for i in range(0, n_predicates, 64):
        n_masks = min(n_predicates, 64)
        mask_exprs = []
        for j, pred in enumerate(predicates[i:i+64]):
            mask_exprs.append(f"set_bit( bitstring('0', 64), {j}, ({pred})::int )")
            preds_meta.append( [i, j, predicates[i+j] ])
        mask_sql = " | ".join(mask_exprs)
        proj_exprs.append( f"({mask_sql})::BITSTRING as mask_{mask_id}")
        mask_id += 1

    con.execute(f"DROP TABLE IF EXISTS {table}_tm")
    create_tm_sql = f"""create table {table}_tm as
    select rowid as iid, {", ".join(proj_exprs)} from {table}"""
    con.execute(create_tm_sql)
    print(create_tm_sql)

    preds_df = pd.DataFrame(preds_meta, columns=["i", "j", "pred"])
    con.register("preds_tmp", preds_df)
    con.execute(f"DROP TABLE IF EXISTS {table}_preds")
    con.execute(f"create table {table}_preds as select * from preds_tmp")

# -----------------------------
# Read TPCH query
# -----------------------------
query = " ".join(
    (Path(args.folder) / f"q{args.qid:02d}.sql").read_text().split()
)
print(f"1. Base Query:\n{query}")

con.execute(f"PRAGMA threads={args.workers}")
if args.debug:
    con.execute("PRAGMA set_debug_lineage(True)")

# -----------------------------
# Execute query with lineage
# -----------------------------

con.execute("PRAGMA set_lineage(True)")

start = timer()
base_result = con.execute(query).df()
end = timer()
print(f"2. Query Result:\n {base_result}")
print(f"Runtime took: {end - start:.5f}s")

# Preserve row identity for lineage joins
base_result["idx"] = base_result.index

con.execute("PRAGMA set_lineage(False)")

n_output = len(args.oids)
if len(base_result) < n_output:
    n_output = len(base_result)
    args.oids = [x for x in range(n_output)]

##### Internal Query id
qid = con.execute("select max(query_id) from pragma_latest_qid()").df().iat[0,0]
print(f"Query ID: {qid}")

start = timer()
con.execute(f"PRAGMA PrepareLineage({qid})")
end = timer()
print(f"Lineage Post Processing took: {end - start:.5f}s")

# TODO: make pragma that returns table names, operator ids
# lineage table -> one for output id, the others are for input ids for that table
spja_block = con.execute(f"""select * from read_block({qid})
                         where output_id in {args.oids}""").df()
print(f"SPJAU Lineage Block Columns: {spja_block.columns}")

# Identify lineage column referring to lineitem
lineitem_cols = [col for col in spja_block.columns if 'lineitem' in col]
assert lineitem_cols, "No lineitem lineage column found"
lineitem_col = lineitem_cols[0]

#### Dense target matrix
n_interventions = con.execute("select count(*) as c from lineitem_preds").df().iat[0, 0]

q = f"""
with temp as (SELECT b.output_id as out, k, 
sum( get_bit(l.mask_0, k::INT) * base.l_extendedprice) as v
from spja_block as b, lineitem_tm as l, lineitem as base
CROSS JOIN range({n_interventions}) AS r(k)
where l.iid=b.{lineitem_col} and base.rowid=l.iid
group by out, k)

select out, k, v, pred, base.* from temp JOIN lineitem_preds ON (k=i+j)
JOIN base_result base ON (out=idx) order by v desc limit 3
"""
start = timer()
top_3 = con.execute(q).df()
end = timer()

print(f"Top 3: {top_3}\n Took: {end-start:.5f}s")

con.execute("pragma clear_lineage")
