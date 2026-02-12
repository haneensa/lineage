from fade_utils import Get_DB, timed
import argparse
from pathlib import Path
from timeit import default_timer as timer

import numpy as np
import pandas as pd
import duckdb

NESTED_LIMIT = 3000

# -----------------------------
# Command-line arguments
# -----------------------------
parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument("--sf", type=float, default=1,
                    help="TPCH scale factor")
parser.add_argument("--qid", type=int, default=1,
                    help="TPCH query id")
parser.add_argument("--oids", nargs="+", type=int, default=[0],
                    help="List of output row ids to analyze")
parser.add_argument("--n_interventions_list", nargs="+", type=int, default=[64, 1024],
                    help="List of unique values (interventions)")
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
# Read TPCH query
# -----------------------------
query = " ".join((Path(args.folder) / f"q{args.qid:02d}.sql").read_text().split())
print(f"1. Base Query:\n{query}")

con.execute(f"PRAGMA threads={args.workers}")
if args.debug:
    con.execute("PRAGMA set_debug_lineage(True)")

con.execute("PRAGMA set_lineage(True)")

base_result = timed("Query Result", lambda: con.execute(query).df())
print(f"{base_result}")

con.execute("PRAGMA set_lineage(False)")

oids = args.oids
n_output = len(oids)
if len(base_result) < n_output:
    n_output = len(base_result)
    oids = [x for x in range(n_output)]

##### Internal query id
qid = con.execute("select max(query_id) from lineage_meta()").df().iat[0,0]
print(f"Query ID: {qid}")

timed("Lineage Post Processing:", lambda: con.execute(f"PRAGMA PrepareLineage({qid})"))

spja_block = con.execute(f"""select * from read_block({qid})
                         where output_id in {oids}""").df()
print(f"SPJAU Lineage Block Columns: {spja_block.columns}")

# -----------------------------
# Sparse target matrix evaluation
# Dictionary encode each row a unique value, stored in {table_name}_mv.{col_name}_int
# -----------------------------
# input: sparse target matrix, spja_block
# output: dense target matrix, dense output agg values

"""
Intervention semantics:
- Each row is assigned a discrete 'code' in [0, n_interventions)
- Intervention k removes rows where code == k
- Aggregates measure the effect of removing each code
"""
n_interventions_list =  args.n_interventions_list
for n_interventions in n_interventions_list:
    print(f"*********** Number of interventions: {n_interventions} ***********")

    q = f"""SELECT  CAST(floor(random() * {n_interventions}) AS INTEGER) AS eq_predicate,
                    CAST(floor(random() * {n_interventions}) AS INTEGER) AS range_predicate,
                    output_id AS out
    FROM spja_block
    """
    sparse_tm = timed("Sparse target matrix retrieval runtime", lambda: con.execute(q).df(), False)

    ##################
    ### 1) SQL: nested loops usign cross product
    # - Approach 1: O(|rows| × |interventions|)
    ### Pros: flexible interventions predicates, takes any aggregate function
    ### Cons: slow as number of unique values increase.
    ### Intervention Predicate: 
    ###     remove all rows where col=? -> not (row.code == k)
    ###     remove all rows where col<>? -> (row.code == k)
    ### for row in sparse_tm.itertuples(index=True):
    ###    for k in range(n_interventions):
    ###       out[row.out, k] += not (row.code == k)
    ##################
    if n_interventions < NESTED_LIMIT:
        nested_sql_result = timed("Approach (SQL, sparse, nested):",
                            lambda: con.execute(f"""SELECT out, k, sum( (k!=eq_predicate)::int )
                           FROM sparse_tm CROSS JOIN range({n_interventions}) r(k)
                           GROUP BY out, k
                           """).df())

    ##################
    ### 2) SQL: single loop, take advantage that each row belong
    ### to a single intervention AND aggregate function is incrementally
    ### removable.
    # - Approach 2: O(|rows|)
    ### Pros: fast
    ### Cons: limited predicates, only aggregate functions based on count
    ### Intervention Predicate: 
    ###     remove all rows where col=? -> total - removed
    ###     remove all rows where col<>? -> removed
    ### for row in sparse_tm.itertuples(index=True):
    ###    removed[row.out, row.code] += 1
    ### for row in sparse_tm.itertuples(index=True):
    ###    total[row.out] += 1
    ### out = total - removed
    ##################
    single_pass_result = timed("Approach (SQL, sparse, single):",
                      lambda: con.execute(f"""WITH temp_remo AS (SELECT out, eq_predicate, sum(1) as removed
                       FROM sparse_tm 
                       GROUP BY out, eq_predicate),

                       temp_all AS (SELECT out, COUNT(*) as total
                       FROM sparse_tm 
                       GROUP BY out)

                       SELECT out, eq_predicate, total-removed
                       FROM temp_remo JOIN temp_all USING (out)
                       """).df())
    
    ##################
    ### 3) SQL: nested loops usign cross product
    # - Approach 1: O(|rows| × |interventions|)
    ### Pros: flexible interventions predicates, takes any aggregate function
    ### Cons: slow as number of unique values increase.
    ### Intervention Predicate: 
    ###     remove all rows where col=? -> not (row.code == k)
    ###     remove all rows where col<>? -> (row.code == k)
    ### for row in sparse_tm.itertuples(index=True):
    ###    for k in range(n_interventions):
    ###       out[row.out, k] += not (row.code > k)
    ##################
    if n_interventions < NESTED_LIMIT:
        nested_sql_result = timed("Approach (SQL, sparse, nested):",
        lambda: con.execute(f"""SELECT out, k, sum( ( range_predicate > k)::int )
                           FROM sparse_tm CROSS JOIN range({n_interventions}) r(k)
                           GROUP BY out, k
                           ORDER BY out, k
                           """).df())

    ##################
    ### 4) SQL: single loop, take advantage that each row belong
    ### to a single intervention AND aggregate function is incrementally
    ### removable.
    # - Approach 2: O(|rows|)
    ### Pros: fast
    ### Cons: limited predicates, only aggregate functions based on count
    ##################
    prefix_sum = timed("Prefix sum:",
                    lambda: con.execute("""
                    WITH temp as (
                      SELECT out, range_predicate, sum(1) AS v
                      FROM sparse_tm GROUP BY out, range_predicate
                    ),
                    temp_all AS (SELECT out, COUNT(*) as total
                    FROM sparse_tm 
                    GROUP BY out)

                    SELECT out, range_predicate, total - sum(v) OVER (
                        ORDER BY range_predicate
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) as prefix_sum
                    FROM temp JOIN temp_all USING (out)
                    ORDER BY out, range_predicate""").df())
    
    prefix_sum = timed("Prefix sum:",
                    lambda: con.execute("""
                    WITH temp as (
                      SELECT out, range_predicate, eq_predicate, sum(1) AS v
                      FROM sparse_tm GROUP BY out, range_predicate, eq_predicate
                    ),
                    temp_all AS (SELECT out, COUNT(*) as total
                    FROM sparse_tm 
                    GROUP BY out)

                    SELECT out, range_predicate, eq_predicate, sum(v) OVER (
                        PARTITION BY out, eq_predicate
                        ORDER BY range_predicate
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) as prefix_sum, total-prefix_sum
                    FROM temp JOIN temp_all USING (out)
                    ORDER BY out, eq_predicate, range_predicate""").df())
    
    # x < ? y < ?, [x,y]
    # Rule: partition by the equality predicate, apply prefix sum over inequeality predicate
    ## ADD partition by equality columns
    ## sum all values that belong to an exact value
    if n_interventions < 100:
        nested_sql_result = timed("Approach (SQL, sparse, nested):",
                    lambda: con.execute(f"""SELECT out, rk, k,
                           sum( (((k=eq_predicate) and (range_predicate <= rk)))::int ) as eq,
                           sum( not (((k=eq_predicate) and (range_predicate <= rk)))::int ) as complement,
                           FROM sparse_tm
                           CROSS JOIN range({n_interventions}) r(k)
                           CROSS JOIN range({n_interventions}) r(rk)
                           GROUP BY out, k, rk
                           ORDER BY out, k, rk
                           """).df())
    

con.execute("pragma clear_lineage")
