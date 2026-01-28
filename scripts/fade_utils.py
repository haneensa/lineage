import numpy as np
import pandas as pd
import duckdb
import os
from timeit import default_timer as timer


def timed(label, fn, debug=True):
    print("==================")
    start = timer()
    res = fn()
    print(f"{label}: {timer() - start:.5f}s")
    if debug:
        print(res)
    print("==================")
    return res

def PrepTargetMatrix(con, predicates_per_table):
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

        con.execute(f"drop table if exists {table}_tm")
        create_tm_sql = f"""create table if not exists {table}_tm as
        select rowid as iid, {", ".join(proj_exprs)} from {table}"""
        con.execute(create_tm_sql)
        print(create_tm_sql)

        con.execute(f"drop table if exists {table}_preds")
        preds_df = pd.DataFrame(preds_meta, columns=["i", "j", "pred"])
        con.register("preds_tmp", preds_df)
        con.execute(f"create table if not exists {table}_preds as select * from preds_tmp")

def DictionaryEncodeAttrs(con, codes):
    for table in codes:
        cols = []
        for col in codes[table]:
            # assign each unique value of col an  integer id
            cols.append(f""" cast(dense_rank() over (order by {col}) as integer) - 1 as {col}_int """)
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
        {", ".join(cols)}
        from {table}
        """
        print(views)
        con.execute(views)

def Get_DB(dbname, sf, extensions=[]):
    con = duckdb.connect(
        dbname,
        config={"allow_unsigned_extensions": "true"}
    )
    if sf and not os.path.exists(dbname):
        print(f"Creating TPCH database (sf={sf})")
        con.execute("CALL dbgen(sf="+str(sf)+");")
    for ex in extensions:
        con.execute(f"LOAD '{ex}'")
    return con

