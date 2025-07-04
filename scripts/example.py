import re
import ast
import json
import duckdb
import argparse

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--sf', type=float, help="sf scale", default=0.1)
parser.add_argument('--qid', type=int, help="query", default=1)
parser.add_argument('--workers', type=int, help="threads", default=1)
parser.add_argument('--debug', type=bool, help="debug", default=False)
parser.add_argument('--end2end', type=bool, help="end to end lineage", default=False)
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
con.execute(f"PRAGMA threads={args.workers}")
if args.debug:
    con.execute("PRAGMA set_debug_lineage(True)")
con.execute("PRAGMA set_lineage(True)")
print(query)
print(con.execute(query).df())
con.execute("PRAGMA set_lineage(False)")

lineage = con.execute("select * from global_lineage()").df()
print(lineage)


end2end_lineage = f"""
WITH RECURSIVE lineage_tree as   (
    -- Base case: start from root
    SELECT
        source_table,
        source_opid,
        sink_opid,
        out_rowid,
        in_rowid,
        0 AS depth,
        CAST(source_opid AS VARCHAR) AS path
    FROM global_lineage()
    WHERE sink_opid =-1

    UNION ALL

    -- Recursive step: find children
    SELECT
        c.source_table,
        c.source_opid,
        c.sink_opid,
        p.out_rowid,
        c.in_rowid,
        p.depth + 1,
        path || ' -> ' || c.source_opid
    FROM global_lineage() c
    JOIN lineage_tree p ON c.sink_opid = p.source_opid and c.out_rowid=p.in_rowid
),

-- end-to-end lineage
lineage_e2e AS (
    SELECT source_table, source_opid,
        out_rowid,
        LIST(DISTINCT in_rowid) AS prov
    FROM lineage_tree
    GROUP BY out_rowid, source_opid, source_table
)

SELECT *
FROM lineage_e2e WHERE  CAST(source_table AS VARCHAR) NOT LIKE 'LOGICAL_%'
"""

if args.end2end:
    print(con.execute(end2end_lineage).df())
con.execute("pragma clear_lineage")

# TODO: add detailed stats per operator n_input, n_output, skew 1:1, 1:n, n:1 (in a list), storage
