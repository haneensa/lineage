# FaDE: Fast Hypothetical Deletions Extension for DuckDB

**FaDE** is a DuckDB extension that enables **fast, interactive "what-if" analysis** by supporting hypothetical deletions
and scaling updates for SPJAU queries (Select–Project–Join–Aggregate–Union).
FaDE empowers users to run interventions over analytical queries with extremely low latency and high throughput,
making it ideal for interactive data applications and explanation engines.

---

## Installation

To install **FaDE** from the latest [GitHub release](https://github.com/haneensa/fade/releases), download and extract the extension into DuckDB’s extension directory manually, run the following script:

```bash
python scripts/download_fade.py
```

## Running the Extension

To load FaDE, DuckDB must allow unsigned extensions. This requires launching DuckDB with the unsigned flag.

``` Python
import duckdb

con = duckdb.connect(config={'allow_unsigned_extensions': True})
con.execute("LOAD 'fade';")
```
