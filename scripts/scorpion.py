import pdb
import json
import os
import numpy as np
import time
import duckdb

from collections import *
from flask import Flask, request, g, Response, jsonify
from flask_compress import Compress
from flask_cors import CORS, cross_origin

from flask import session
import uuid

_connection = None

specs = [
    ["readings.moteid"],
    ["readings.voltage"],
    ["readings.light"],
    ["readings.moteid","readings.voltage"],
    ["readings.moteid","readings.light"],
    ["readings.voltage","readings.light"],
    #["readings.moteid","readings.light","readings.voltage"],
]


def get_db():
    global _connection
    if _connection is None:
        print("Create new connection")
        _connection = duckdb.connect('intel.db', config={"allow_unsigned_extensions": "true"})
        _connection.execute("LOAD 'build/release/repository/v1.3.0/osx_amd64/lineage.duckdb_extension'")
    return _connection

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)


tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
print(tmpl_dir)
app = Flask(__name__, template_folder=tmpl_dir)
CORS(app)#, supports_credentials=True)


def build_preflight_response():
    response = make_response()
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add('Access-Control-Allow-Headers', "*")
    response.headers.add('Access-Control-Allow-Methods', "*")
    return response

def build_actual_response(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    return response

@app.teardown_appcontext
def close_db(error):
    conn = g.pop("conn", None)
    if conn is not None:
        conn.close()

def clear(c):
    c.execute("PRAGMA clear_lineage")

@app.route("/query", methods=["get"])
def query():
  con = get_db()
  q = request.args.get("q")
  print("*****")
  print(q)
  if "hrint" in q:
    q  = q + " order by hrint"
  if 'stddev' in q and 'GROUP BY' in q and 'not' not in q:
      con.execute("PRAGMA threads=12")
      con.execute("PRAGMA set_debug_lineage(false);").df()
      con.execute("PRAGMA set_lineage(true);").df()
  start = time.time()
  df = con.execute(q).df()
  end = time.time()
  print(df)
  if 'stddev' in q and 'GROUP BY' in q and 'not' not in q:
      con.execute("PRAGMA set_lineage(false);").df()
      query_id = 0 #con.query_id
      con.execute(f"PRAGMA PrepareLineage({query_id})").df()

  query_timing = end - start

  print(df)
  print("*****", query_timing)
  dicts = df.to_dict(orient='records')
  keys = df.columns.tolist()
  values = [[row[key] for key in keys] for row in dicts]
  # TODO: save positions of alias
  resp = [dict(
    columns=keys,
    values=values
  )]
  return jsonify(resp)



@app.route('/api/scorpion/', methods=['POST', 'GET'])
#@cross_origin(origins="*")
def scorpion():
  try:
    if request.method == "GET":
      data =  json.loads(str(request.args['json']))
    else:
      data =  json.loads(str(request.form['json']))

    print(data)
    print("running scorpion")

    conn = get_db()

    sql = data['sql']
    if "hrint" in sql:
        sql  = sql + " order by hrint"
    badselection = data['badselection']
    goodselection = data['goodselection']
    alias = data['alias']
    badids = [d['id'] for d in badselection[alias]]
    goodids = [d['id'] for d in goodselection[alias]]
    goodvals = [d['y'] for d in goodselection[alias]]
    badvals = [d['y'] for d in badselection[alias]]
    print(sql)
    print("alias", alias)
    print(goodids)
    print(badids)
    print(goodvals)
    print(badvals)

    ret = runscorpion(conn, sql, alias, goodids, badids, goodvals, badvals)
    print(ret)

  except Exception as e:
      import traceback
      traceback.print_exc()
      print(e)
      ret = dict(
              status="final",
              results=[
          dict(score=0.1, clauses=["voltage < 0.1"]),
          dict(score=0.2, clauses=["moteid = 18"])
      ])

  response = json.dumps(ret, cls=NumpyEncoder)
  return response, 200, {'Content-Type': 'application/json'}


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

    

if __name__ == "__main__":
  app.config['SECRET_KEY'] = os.environ.get("FLASK_SECRET_KEY", "dev-secret")
  app.run(host="0.0.0.0", port="8111", debug=True, threaded=False)
