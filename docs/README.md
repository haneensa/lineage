# Lineage enabled DuckDB

**lineage** is a DuckDB extension that provies row-level lineage.

---

## Installation

To install **lineage** from the latest [GitHub release](https://github.com/haneensa/lineage/releases),
download and extract the extension into DuckDB’s extension directory manually, run the following script:

```bash
python scripts/download_extension.py --name lineage --arch ARCH
```

Replace ARCH with your architecture (e.g., linux_amd64, osx_amd64)

## Running the Extension

To load lineage, DuckDB must allow unsigned extensions. This requires launching DuckDB with the unsigned flag.

``` Python
import duckdb

con = duckdb.connect(config={'allow_unsigned_extensions': True})
con.execute("LOAD 'lineage';")
```

## Lineage Schema

We capture row-level lineage at the boundaries of pipeline breakers, and expose it through the global_lineage() table function. This relation has the following schema:

```
lineage = con.execute("select * from global_lineage()").df()
source_table: VARCHAR
sink_table:   VARCHAR
source_opid:  INT
sink_opid:    INT
out_rowid:    BIGINT
in_rowid:     BIGINT
```


## Example

You can find an example in scripts/example.py on how to enable lineage capture, access operator lineage, and query end to end lineage.
