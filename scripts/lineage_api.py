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

def formula(spja_block):
    monomial = ''
    for col in spja_block.columns:
        if col == 'output_id': continue
        if len(monomial) > 0: monomial += " || 'x' || "
        table_name = col.split('_')[-1]
        var = table_name[0].capitalize() # TODO: resolve duplicates
        monomial +=  f"'{var}' ||" + col
    start = timer()
    q = f"select string_agg({monomial}, '+') from spja_block where output_id=0"
    eq = con.execute(q).df()
    end = timer()
    print("formula: ", end - start)
    return eq

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

print(formula(spja_block))

# TODO: need an easy way to figure out the column name in the spja block
# select sum(in1.bool * in2.bool) existance from lineage_idx as t, in1, in2  where t.in1=in1 and t.in2=in2
# where t.oid = 0
lineitem_col = [col for col in spja_block.columns if 'lineitem' in col]
if len(lineitem_col) > 0: # else, what to do?
    start = timer()
    q = f"select sum((l_shipmode='AIR')::int),sum((l_shipmode='MAIL')::int), sum(1) from spja_block as b, lineitem as l where l.rowid=b.{lineitem_col[0]} and b.output_id=0"
    print(con.execute(q).df())
    end = timer()
    print("count: ", end - start)

con.execute("pragma clear_lineage")

# security
# whatif
# cross filter
# where prov
# probability evaluate

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
