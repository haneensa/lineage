# pip3 install flask flask_compress flask_cors
import duckdb
from scorpion import runscorpion

def load_intel_db():
    create_sql = """CREATE TABLE readings as SELECT * FROM 'data/intel.csv'"""
    con.execute(create_sql)
    # hack since we don't have guards for null values
    con.execute("""UPDATE readings SET temp = COALESCE(temp, 0);""")
    con.execute("""UPDATE readings SET light = COALESCE(light, 0);""")
    con.execute("""UPDATE readings SET voltage = COALESCE(voltage, 0);""")
    con.execute("""UPDATE readings SET humidity = COALESCE(humidity, 0);""")
    con.execute("""UPDATE readings SET moteid = COALESCE(moteid, -1);""")
    #print(con.execute("select * from readings").df())

#sql = "SELECT hrint, sum(temp) as agg0, stddev(temp) AS agg1, count() as agg2, avg(temp) as agg3 FROM readings AS readings GROUP BY hrint"
sql = "SELECT hrint, stddev(temp) as agg0 FROM readings AS readings GROUP BY hrint"
agg_alias = 'agg0'

badids = [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55]
goodids = [291, 292, 293, 294, 295, 296, 297, 298, 299, 300, 301, 302, 303, 304, 305, 306, 307, 308, 309, 310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 320]

con = duckdb.connect(config={'allow_unsigned_extensions' : 'true'})
load_intel_db()
print(con.execute("LOAD 'build/release/repository/v1.3.0/osx_amd64/lineage.duckdb_extension'").df())

o = runscorpion(con, sql, agg_alias, goodids, badids)
for r in o['results']:
    print(r['score'])
    print(r['clauses'])
print("===============")
specs = [
    ["readings.moteid"],
    ["readings.voltage"],
    ["readings.light"],
    ["readings.moteid","readings.voltage"],
    ["readings.moteid","readings.light"],
    ["readings.voltage","readings.light"],
    #["readings.moteid","readings.light","readings.voltage"],
]


def runscorpion(con, sql, agg_alias, goodids, badids, goodvals, badvals, query_id=None):
    print("runscorpion")
    query_id = 0 #con.query_id
    #sorted_index = out['hrint'].sort_values().index
    #goodids = [sorted_index[i] for i in goodids]
    #badids = [sorted_index[i] for i in badids]
    #out = cached_df
    allids = goodids + badids
    #goodvals = out.loc[goodids, agg_alias]
    #badvals = out.loc[badids, agg_alias]
    #print("##### out #######")
    #print(out)
    print("##### badids #######")
    print(badids)
    print("##### goodids #######")
    print(goodids)
    print("##### badvals #######")
    print(badvals)
    print("##### goodvals #######")
    print(goodvals)
    #print(out.loc[goodids])
    #print(out.loc[badids])

    #allids = goodids + badids
    print("allids *******", allids)
    # TODO: this uses pre vals. should access it from fade API instead of recomputing it
    mg, mb = np.mean(goodvals), np.mean(badvals)
    ng, nb = len(goodvals), len(badvals)
    goodids = [f"g{id}" for id in range(len(goodids))]
    print(badids)
    badids = [f"g{id + len(goodids)}" for id in range(len(badids))]
    print(badids)

    ges = [f"abs({id}-{val})**0.5" for id, val in zip(goodids, goodvals)]
    bes = [f"coalesce(({val}-{id}),0)**2" for id, val in zip(badids, badvals)]
    fade_q = f"""
    WITH good AS (
        SELECT pid, unnest([{','.join(ges)}]) as y
        FROM faderes
    ), bad AS (
        SELECT pid, unnest([{','.join(bes)}]) as y
        FROM faderes
    ), good2 AS (
        SELECT pid, max(y) as y
        FROM good
        GROUP BY pid
    ), bad2 AS (
        SELECT pid, 
        median(y) AS y50,
        avg(y) AS avgy,
        min(y) as miny, max(y) as maxy
        FROM bad
        GROUP BY pid
    ), scorpion AS (
        SELECT g.pid, y50/{mb}/{nb} - g.y/{mg} as score
        FROM good2 as g, bad2 as b
        WHERE g.pid = b.pid
    )
    SELECT * from scorpion ORDER BY score desc LIMIT 10
    """

    results = []
    agg_idx = 0
    for spec in specs:
        results.extend(run_fade(con, query_id, agg_idx, allids, spec, fade_q))
    results.sort(key=lambda d: d['score'], reverse=True)
    print(results)
    return dict(
        status="final",
        results=results[:10]
    )

def run_fade(con, query_id, agg_idx, allids, spec_list, fade_q):
    allids = [int(x) for x in allids]
    print(fade_q)
    print(f"Run fade with spec {spec_list}")
    results = []
    try: 
        q = f"PRAGMA Whatif({query_id}, {agg_idx}, {allids}, {spec_list})"
        print(q)
        start = time.time()
        con.execute(q).fetchdf()
        end = time.time()
        print('whatif: ', end - start)
        # TODO: expose another than has original data (pre)
        faderes = con.execute(f"select * from fade_reader({query_id}, {agg_idx});").df()
        print(faderes)
        faderesults = con.execute(fade_q).fetchdf()
        print(faderesults)
        for i in range(10):
            score = float(faderesults['score'][i])
            pid = faderesults['pid'][i]
            q = f"select * from GetPredicates({pid});"
            predicate = con.execute(q).fetchdf().iloc[0,0]
            print(predicate)
            predicate = predicate.replace("_", ".")
            clauses = [p.strip() for p in predicate.split("AND")]
            results.append(dict(score=score, clauses=clauses))
        print("done")
        return results
    except Exception as e:
        print("exception")
        print(e)
        return []
