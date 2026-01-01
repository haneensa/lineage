# aggs: add default ones: count, sum, avg, stddev, max, min
# to use complex ones, we need to compile the agg implementatoin
import numpy as np
import re
import os
import ast
import json
import duckdb
import argparse
from pathlib import Path
from timeit import default_timer as timer

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--qid', type=int, help="query", default=1)
parser.add_argument('--aggid', type=int, help="aggid", default=0)
parser.add_argument('--oid', type=int, help="oid", default=0)
parser.add_argument('--workers', type=int, help="workers", default=1)
parser.add_argument('--debug', type=bool, help="debug", default=False)
parser.add_argument('--folder', type=str, help='queries folder', default='queries/')
args = parser.parse_args()


######## Initialize DB
dbname = f'tpch_{args.sf}.db'
if not os.path.exists(dbname):
    con = duckdb.connect(dbname, config={'allow_unsigned_extensions' : 'true'})
    con.execute("CALL dbgen(sf="+str(args.sf)+");")
else:
    con = duckdb.connect(dbname, config={'allow_unsigned_extensions' : 'true'})

con.execute("LOAD 'build/release/repository/v1.3.0/osx_amd64/lineage.duckdb_extension'")


#### Prep intervention matrix (target matrix)
#### TODO: wrap this in apragma
codes = {'lineitem': ['l_extendedprice', 'l_shipmode'], 'orders': ['o_orderpriority']}
for table in codes:
    cols = ""
    for col in codes[table]:
        # assign each unique value of col an  integer id
        if len(cols) > 0: cols += " , "
        cols += f""" cast(dense_rank() over (order by {col}) as integer) - 1 as {col}_int """
        # store the dictionary encoding of the col in {table}_{col}_dict
        codes_q = f"""create table if not exists {table}_{col}_dict as
        select distinct val, code  from (select rowid,
        cast(dense_rank() over (order by {col}) as integer) - 1 as code,
        {col} as val from {table}) order by code
        """
        con.execute(codes_q)
    # table_mv -> one for rowid and the others are for all possible parametrized attribute
    # if exists, then drop it and create another one.
    con.execute(f"DROP TABLE IF EXISTS {table}_mv")
    views = f"""CREATE TABLE {table}_mv AS
    select rowid as iid,
    {cols}
    from {table}
    """
    print(views)
    con.execute(views)

#### Example of dense target matrix
# User provided predicates projection list.
# prep target matrices per table
predicates_per_table = {"lineitem": ["l_shipmode='MAIL'", "l_shipmode='SHIP'", "l_shipmode='REG_AIR'",
              "l_shipmode='TRUCK'", "l_shipmode='RAIL'", "l_shipmode='AIR'", "l_shipmode='FOR'"
              ]}
for table, predicates in predicates_per_table.items():
    proj_list = ""
    n_predicates = len(predicates)
    m_id = 0
    for i in range(0, n_predicates, 64):
        # pack every 64 into one integer
        n_masks = min(n_predicates, 64)
        mask = ""
        for j in range(n_masks):
            if len(mask) > 0: mask += " | "
            mask += f"( {predicates[i+j]} )::int << {j} "
        if len(proj_list) > 0: proj_list += " , "
        proj_list += f" {mask} as mask_{m_id} "
        m_id += 1

    q = f"create table if not exists {table}_tm as select rowid, {proj_list} from {table}"
    con.execute(q)
    print(q)
    target_matrix = con.execute(f"select * from {table}_tm").df()
    print(target_matrix)
    mask = target_matrix["mask_0"].to_numpy()
    is_mail = (mask & 1) != 0
    is_ship = (mask & 32) != 0
    print(is_mail)
    print(is_ship)

######## Read query
qfile = f"{args.folder}q{args.qid:02d}.sql"
query = " ".join(
    (Path(args.folder) / f"q{args.qid:02d}.sql").read_text().split()
)
print(f"1. Base Query:\n{query}")

con.execute(f"PRAGMA threads={args.workers}")
if args.debug:
    con.execute("PRAGMA set_debug_lineage(True)")

con.execute("PRAGMA set_lineage(True)")

start = timer()
res = con.execute(query).df()
end = timer()
print(f"2. Query Result:\n {res} \nRuntime took: {end - start}")

n_output = len(res)

con.execute("PRAGMA set_lineage(False)")

##### internal query id
qid = con.execute("select max(query_id) from pragma_latest_qid()").df().iat[0,0]
print(f"Query ID: {qid}")

start = timer()
con.execute(f"PRAGMA PrepareLineage({qid})")
end = timer()
print("fLineage Post Processing took: {end - start}")

# TODO: make pragma that returns table names, operator ids
# lineage table -> one for output id, the others are for input ids for that table
spja_block = con.execute(f"select * from read_block({qid})").df()
print(f"SPJAU Lineage Block:\n{spja_block}")


##### single table Sparse target matrix evaluation
##### sparse: dictionary encode  each unique value, stored in {table_name}_mv.{col_name}_int
# input: sparse target matrix, spja_block
# output: dense target matrix, dense output agg values
# if we need values, then join_codes should include them

start = timer()
q = f"""select l.l_shipmode_int as code, b.output_id as out
from spja_block as b, lineitem_mv as l where l.iid=b.opid_10_lineitem
"""
dict_codes = con.execute(q).df()
end = timer()
print("Runtime: ", end - start)
print(dict_codes)

#### 1) conjunctive equality predicate
n_interventions = con.execute("select max(code)+1 from dict_codes").df().iat[0, 0]
removed = np.zeros((n_output, n_interventions))
agg = np.zeros((n_output, n_interventions))

for row in dict_codes.itertuples(index=True):
    code = row.code
    o = row.out
    # equality predicate is true when code==column_idx
    for k in range(n_interventions):
        pval = not (code == k)
        agg[o, k] += pval

for  row in dict_codes.itertuples(index=True):
    code = row.code
    o = row.out
    # equality predicate is true when code==column_idx
    # optimization for additive aggs, only count removed tuples
    removed[o, code] += 1

print(agg)
print("=====")
print(removed)

#### 2) conjunctive inequality predicate
#### 3) disjunctive equality predicate

#### 4) range pradicates
q = f"""select l.l_extendedprice_int as code, b.output_id as out
from spja_block as b, lineitem_mv as l where l.iid=b.opid_10_lineitem
"""
dict_codes = con.execute(q).df()
end = timer()
print("Runtime: ", end - start)
print(dict_codes)

n_interventions = con.execute("select max(code)+1 from dict_codes").df().iat[0, 0]
print(f"|l_extendedprice|:{n_interventions}")
removed = np.zeros((n_output, n_interventions))
agg = np.zeros((n_output, n_interventions))
agg_2 = np.zeros((n_output, n_interventions))


start = timer()
for row in dict_codes.itertuples(index=True):
    code = row.code
    o = row.out
    agg[o, code:] += 1
end = timer()
print("T1: ", end - start)

### optimization: use prefix sum
start = timer()
for row in dict_codes.itertuples(index=True):
    code = row.code
    o = row.out
    agg_2[o, code] += 1

for r in range(n_output):
    for c in range(1, n_interventions):
        agg_2[r, c] += agg_2[r, c-1]
end = timer()

print("T2: ", end - start)

print(agg)
print(agg_2)

### Multiple parameterized predicates
# combine multiple codes. use shift to give each code unique code space

# when combining range and equality predicates, we can use the stricker evaluation since it is conjunctive predicate
# for incrementally removable aggregates (based on count, sum)
    
# if we have another inequality predicate, we need to check if eq_code <= range_code
# two conditions have to be satisfied


#### Dense target matrix

# multiple tables? join using spjau block, then multiple masks
# rule: t1.mask * t2.mask columns are aligned
# e.g. t1.mask[0] and t2.mask[0] are part of the same predicate

con.execute("pragma clear_lineage")
