import numpy as np
import re
import os
import ast
import json
import duckdb
import argparse
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


dbname = f'tpch_{args.sf}.db'
if not os.path.exists(dbname):
    con = duckdb.connect(dbname, config={'allow_unsigned_extensions' : 'true'})
    con.execute("CALL dbgen(sf="+str(args.sf)+");")
else:
    con = duckdb.connect(dbname, config={'allow_unsigned_extensions' : 'true'})

con.execute("LOAD 'build/release/repository/v1.3.0/osx_amd64/lineage.duckdb_extension'")

qfile = f"{args.folder}q{str(args.qid).zfill(2)}.sql"
text_file = open(qfile, "r")
query = text_file.read().strip()
query = ' '.join(query.split())
text_file.close()
print(query)

con.execute(f"PRAGMA threads={args.workers}")
if args.debug:
    con.execute("PRAGMA set_debug_lineage(True)")

con.execute("PRAGMA set_lineage(True)")

start = timer()
res = con.execute(query).df()
end = timer()
print(res)
print(end - start)

con.execute("PRAGMA set_lineage(False)")

meta = con.execute("select * from pragma_latest_qid()").df()
print(meta)
assert(len(meta) > 0)
latest = len(meta)-1
qid = meta['query_id'][latest]
model = "count"

start = timer()
con.execute(f"PRAGMA PrepareLineage({qid})")
end = timer()
print("Prepare: ", end - start)

start = timer()
spja_block = con.execute(f"select * from read_block({qid})").df()
end = timer()
print("Read: ", end - start)
print(spja_block)


# for each column, construct the monomial block
monomial = ''
for col in spja_block.columns:
    if col == 'output_id': continue
    if len(monomial) > 0: monomial += " || 'x' || "
    name = col.split('_')[-1]
    monomial +=  f"'{name}' ||" + col
print(monomial)

# todo: need better names for columns variables
start = timer()
q = f"select string_agg({monomial}, '+') from spja_block where output_id=0"
print(con.execute(q).df())
end = timer()
print("formula: ", end - start)

# select sum(in1.bool * in2.bool) existance from JLineage as t, in1, in2  where t.in1=in1 and t.in2=in2
# where t.oid = 0
lineitem_col = [col for col in spja_block.columns if 'lineitem' in col]
if len(lineitem_col) > 0:
    start = timer()
    q = f"select sum(1) from spja_block as b, lineitem as l where l.rowid=b.{lineitem_col[0]} and b.output_id=0"
    print(con.execute(q).df())
    end = timer()
    print("count: ", end - start)

# security
# whatif
# cross filter
# where prov
# probability evaluate


# sparse: dictionary encode  each unique value
# -> get_codes() -> single column that encodes where the value is true
# -> for join maybe we can do it by sql?
# -> for agg: use if
# dense fade ?


codes = {'lineitem': ['l_extendedprice', 'l_shipmode'], 'orders': ['o_orderpriority']}
distinct_codes = {} # key table.column -> relation
for table in codes:
    cols = ""
    for col in codes[table]:
        if len(cols) > 0:
            cols += " , "
        cols += f""" cast(dense_rank() over (order by {col}) as integer) - 1 as {col}_int """
        print(table)
        
        codes_q = f"""create table if not exists {table}_{col}_mv as
        select distinct val, code  from (select rowid,
        cast(dense_rank() over (order by {col}) as integer) - 1 as code,
        {col} as val from {table}) order by code
        """
        print(codes_q)
        lcodes = con.execute(codes_q).df()
        print(lcodes)
    views = f"""CREATE TABLE IF NOT EXISTS {table}_mv AS
    select rowid as iid,
    {cols}
    from {table}
    """
    print(views)
    ann = con.execute(views).df()
    print(ann)

# table_mv -> one for rowid and the others are for all possible parametrized attribute
# lineage table -> one for output id, the others are for input ids for that table
# l.l_shipmode = ?

# if single parametrized column, then that all we would need
# else combine multiple codes. use shift to give each code unique code space
start = timer()
q = f"""select l.l_shipmode_int * (select max codes from orders_o_orderpriority) 
+ o.o_orderpriority_int as code
from spja_block as b, lineitem_mv as l, orders_mv as o where l.iid=b.opid_10_lineitem
and o.iid=b.opid_8_orders
and b.output_id=0
"""
q = f"""select l.l_shipmode_int as code, b.output_id as out
from spja_block as b, lineitem_mv as l where l.iid=b.opid_10_lineitem
"""
join_codes = con.execute(q).df()
end = timer()
print("count: ", end - start)
print(join_codes)

# input: sparse target matrix, spja_block
# output: dense target matrix, dense output agg values
# if we need values, then join_codes should include them

"""
q = f"select sum(case when code=5 then 1 else 0 end) from join_codeswhere output_id=0"
print(con.execute(q).df())
# with fade, construct RxC where R=#outputs, C=|interventions|=|distinct codes|
"""
n_interventions = con.execute("select max(code)+1 from join_codes").df().iat[0, 0]
print(n_interventions)
n_output = len(res)
removed = np.zeros((n_output, n_interventions))
agg = np.zeros((n_output, n_interventions))

for  row in join_codes.itertuples(index=True):
    code = row.code
    o = row.out
    #val = row[val_col]
    for k in range(n_interventions):
        pval = not (code == k)
        agg[o, k] += pval # incr(val * pval, agg[o, k]) 

# when combining range and equality predicates, we can use the stricker evaluation since it is conjunctive predicate
# for incrementally removable aggregates (based on count, sum)
for  row in join_codes.itertuples(index=True):
    code = row.code
    o = row.out
    # if we have another inequality predicate, we need to check if eq_code <= range_code
    # two conditions have to be satisfied
    removed[o, code] += 1

print(agg)
print("=====")
print(removed)

q = f"""select l.l_extendedprice_int as code, b.output_id as out
from spja_block as b, lineitem_mv as l where l.iid=b.opid_10_lineitem
"""
join_codes = con.execute(q).df()
end = timer()
print("count: ", end - start)
print(join_codes)

# input: sparse target matrix, spja_block
# output: dense target matrix, dense output agg values
# if we need values, then join_codes should include them

n_interventions = con.execute("select max(code)+1 from join_codes").df().iat[0, 0]
print(n_interventions)
n_output = len(res)
removed = np.zeros((n_output, n_interventions))
agg = np.zeros((n_output, n_interventions))
for  row in join_codes.itertuples(index=True):
    code = row.code
    o = row.out
    #val = row[val_col]
    agg[o, code:] += 1

print(agg)

# what about inequalities sparse encoding? how to evaluate it using sql?
# what about compining mulitple sparse encodings?

# dense: how to specify target matrix?
# target matrix data types?
# evaluatioin?
# optimizations?

con.execute("pragma clear_lineage")


# provsql API:
# setup: 
# create type formula_state as (formula text, nbargs int);
# create function formula_plus_state(state formula_state, value text) return formula_state;
# create function formula_times_state(state formula_state, value text) return formula_sate;
# create function formula_monus(formula1 text, formula2 text) return text
# create function formula_state2formula(state formula_state)
# create function formula(token UUID, token2value regclass) return text
#   -> provenance_evaluate(token, token2value, 1::text, 'formula_plus', 'formula_times', 'formula_monus')

# create aggregate formula_plus(text) (sfunc = formula_plus_state, stype = formula_state, initcond = '(0,0)', finalfunc = formula_state2formula)
# create aggregate formula_times(text) (sfunc = formula_time_state, stype = formula_state, initcond = '(1,0)', finalfunc = formula_state2formula)

# 1. select add_provenance(REL);
# 2. select create_provenance_mapping(ATTR, REL, ATTR);
# 3. select formula(provenance(), ATTR) from ...
# -> formula(), security(), where_provenance(), probability_evaluate(), boolean_st()
