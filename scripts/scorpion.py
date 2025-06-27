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

def load_intel_db(con):
    create_sql = """CREATE TABLE readings as SELECT * FROM 'data/intel.csv'"""
    con.execute(create_sql)
    # hack since we don't have guards for null values
    con.execute("""UPDATE readings SET temp = COALESCE(temp, 0);""")
    con.execute("""UPDATE readings SET light = COALESCE(light, 0);""")
    con.execute("""UPDATE readings SET voltage = COALESCE(voltage, 0);""")
    con.execute("""UPDATE readings SET humidity = COALESCE(humidity, 0);""")

    con.execute("""UPDATE readings SET moteid = COALESCE(moteid, -1);""")
    #print(con.execute("select * from readings").df())

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

@app.before_request
def before_request():
  """
  This function is run at the beginning of every web request 
  (every time you enter an address in the web browser).
  We use it to setup a database connection that can be used throughout the request

  The variable g is globally accessible
  """
  try:
    g.conn = duckdb.connect(config={'allow_unsigned_extensions' : 'true'})
    load_intel_db(g.conn)
    g.conn.execute("LOAD 'build/release/repository/v1.3.0/linux_amd64/fade.duckdb_extension'").df()
  except:
    print("uh oh, problem connecting to database")
    import traceback; traceback.print_exc()
    g.conn = None

@app.teardown_request
def teardown_request(exception):
  """
  At the end of the web request, this makes sure to close the database connection.
  If you don't the database could run out of memory!
  """
  try:
    g.conn = None
  except Exception as e:
    pass

def clear(c):
    c.execute("PRAGMA clear_lineage")

@app.route("/query", methods=["get"])
def query():
  q = request.args.get("q")
  print("*****")
  print(q)
  if "hrint" in q:
    q  = q + " order by hrint"
  df = g.conn.execute(q).df()
  print(df)
  print("*****")
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

    sql = data['sql']
    if "hrint" in sql:
        sql  = sql + " order by hrint"
    badselection = data['badselection']
    goodselection = data['goodselection']
    alias = data['alias']
    badids = [d['id'] for d in badselection[alias]]
    goodids = [d['id'] for d in goodselection[alias]]
    print(sql)
    print("alias", alias)
    print(goodids)
    print(badids)

    ret = runscorpion(g.conn, sql, alias, goodids, badids)
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



def runscorpion(con, sql, agg_alias, goodids, badids, query_id=None):
    clear(con)
    print("runscorpion")
    con.execute("PRAGMA threads=1")
    con.execute("PRAGMA set_debug_lineage(false);").df()
    con.execute("PRAGMA set_lineage(true);").df()
    start = time.time()
    out = con.execute(sql).df()
    end = time.time()
    print(out)
    con.execute("PRAGMA set_lineage(false);").df()
    query_timing = end - start
    query_id = 0 #con.query_id
    #sorted_index = out['hrint'].sort_values().index
    #goodids = [sorted_index[i] for i in goodids]
    #badids = [sorted_index[i] for i in badids]
    allids = goodids + badids
    goodvals = out.loc[goodids, agg_alias]
    badvals = out.loc[badids, agg_alias]
    print("##### out #######")
    print(out)
    print("##### badids #######")
    print(badids)
    print("##### goodids #######")
    print(goodids)
    print("##### badvals #######")
    print(badvals)
    print("##### goodvals #######")
    print(goodvals)
    print(out.loc[goodids])
    print(out.loc[badids])

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

    specs = [
        ["readings.moteid"],
        ["readings.voltage"],
        ["readings.light"],
        ["readings.moteid","readings.voltage"],
        #["readings.moteid","readings.light"],
        #["readings.voltage","readings.light"],
        #["readings.moteid","readings.light","readings.voltage"],
    ]


    con.execute(f"PRAGMA prepare_lineage({query_id})").df()

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
        q = f"PRAGMA whatif({query_id}, {agg_idx}, {allids}, {spec_list})"
        print(q)
        con.execute(q).fetchdf()
        # TODO: expose another than has original data (pre)
        faderes = con.execute("select * from fade_reader(0);").df()
        print(faderes)
        faderesults = con.execute(fade_q).fetchdf()
        print(faderesults)
        for i in range(10):
            score = float(faderesults['score'][i])
            pid = faderesults['pid'][i]
            q = f"select * from get_predicate({pid});"
            predicate = con.execute(q).fetchdf().iloc[0,0]
            print(predicate)
            predicate = predicate.replace("_", ".")
            clauses = [p.strip() for p in predicate.split("AND")]
            results.append(dict(score=score, clauses=clauses))
        return results
    except Exception as e:
        print("exception")
        print(e)
        return []

    

if __name__ == "__main__":


  app.run(host="0.0.0.0", port="8111", debug=True, threaded=True)
