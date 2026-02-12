# DuckDB Lineage Extension – Development Plan

## Overview
The DuckDB Lineage Extension automatically captures fine-grained lineage of queries at the physical plan level.  
It tracks which input tuples contribute to each output row and generates SPJUA-level lineage blocks for complete queries.  

This extension allows users to capture full lineage without rewriting queries or modifying workflow.

---

## Installation

1. Download the extension:
```bash
python3 scripts/download_extension.py
````

2. Use the extension in DuckDB (allow unsigned extensions):

```python
import duckdb

con = duckdb.connect(config={'allow_unsigned_extensions': True})
con.execute("LOAD 'lineage';")
```

3. Enable lineage capture:

```sql
PRAGMA set_lineage(True);
```

4. Execute any query as usual:

```sql
SELECT c.name, SUM(o.value) AS total_spend
FROM customer c
JOIN orders o USING (cid)
GROUP BY c.name;
```

5. Disable lineage capture:

```sql
PRAGMA set_lineage(False);
```

6. Access captured lineage through SPJUA blocks:

```python
lineage_edges = con.execute("""
SELECT *
FROM read_block(
    (SELECT max(query_id) FROM lineage_meta())
);
""").fetchdf()
```

---

## 1. Logical Operators to Support

Lineage capture should be extended to handle:

* `UNION`
* `ANY JOIN`, `DEPENDENT JOIN`, `OPTIONAL JOIN`
* `INTERSECT`
* `UNNEST`
* `SAMPLE`
* `WINDOW`
* `DISTINCT`
* `RECURSIVE CTE`

Short-circuit logic (e.g., semi joins) must be disabled to ensure full lineage capture.

---

## 2. SPJUA-Level Lineage Blocks

### 2.1 Per-SPJUA Block Creation

For each SPJUA portion of a query:

* Collect operator-level lineage for all operators in that SPJUA.
* Compose a single SPJUA block with:

  * One column per base table accessed
  * One column for output tuple IDs
  * Rows encoding full input-to-output tuple mapping

### 2.2 Generalization

* Refactor `CreateJoinAggBlocks` and `InitGlobalLineageBuff` to support multiple SPJUA blocks per query.
* Ensure recursive handling for nested queries and subqueries.

---

## 3. Disable Short-Circuit Logic
* Ensure all input annotations are captured regardless of operator-specific optimizations.

---

## 4. Testing & Validation

### 4.1 Unit Tests

* Validate input-to-output lineage for each new operator.
* Check SPJUA block consistency.

### 4.2 End-to-End Tests

Test the extension on complex queries:

**Test Cases:**


1. **Unions and Intersections**

   * Queries using `UNION` or `INTERSECT` operators.
   * Confirm lineage is captured for each branch.
   * Validate combined SPJUA block correctly represents input-to-output mapping for all branches.

2. **Recursive CTEs**

   * Queries using recursive common table expressions.
   * Ensure lineage capture works for each recursion iteration.
   * Confirm SPJUA blocks reflect lineage across recursive steps.

**Validation Steps:**

1. **Manual Comparison**

   * Write equivalent SQL queries with `LIST()` and `UNNEST()` rewrites to manually track lineage.
   * Compare manual lineage vs extension output.

2. **SPJUA Block Inspection**

   * Use:

```sql
SELECT *
FROM read_block(
    (SELECT max(query_id) FROM lineage_meta))
);
```

* Ensure:

  * Each table accessed has a corresponding column.
  * Output tuple mapping is complete and matches expected input-to-output relationships.

3. **Edge Cases**

   * Queries with mark/semi/anti joins, and delim joins.
   * Disable short-circuit logic.
   * Validate SPJUA block consistency even when some operators do not contribute output rows.
