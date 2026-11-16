<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# SQL dialect extension series

Based on `e2dc8b048b542e7abdc296254aa54e26eb89d76d` (57.0.0-dev).
This index links the implementation PRs; it does not merge them.

| PR                                                | API / fix                                            | Purpose                                                                             | Base |
| ------------------------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------- | ---- |
| [#1](https://github.com/kuza55/datafusion/pull/1) | QUALIFY validation before window rebasing            | Fix the regression introduced by apache/datafusion#21173.                           | main |
| [#2](https://github.com/kuza55/datafusion/pull/2) | `ExprPlanner::plan_numeric_literal`                  | Preserve literal spelling, scale, sign and suffix for dialect typing.               | main |
| [#3](https://github.com/kuza55/datafusion/pull/3) | `ExprPlanner::plan_identifier` + query context       | Choose alias visibility and identifier precedence with clause/scope information.    | main |
| [#5](https://github.com/kuza55/datafusion/pull/5) | `RelationPlanner::plan_column_aliases`               | Apply partial column-alias policy consistently to relations and CTEs.               | main |
| [#6](https://github.com/kuza55/datafusion/pull/6) | `RelationPlanner::resolve_cte_conflict`              | Choose inner/outer CTE precedence before duplicate-name rejection.                  | main |
| [#7](https://github.com/kuza55/datafusion/pull/7) | `RelationPlanner::plan_join` / `plan_wildcard`       | Build merged-key projections, preserve qualified inputs and control wildcard order. | #3   |
| [#8](https://github.com/kuza55/datafusion/pull/8) | `RelationPlanner::plan_select_input` + `materialize` | Add computed columns before WHERE or SELECT planning and reuse their values.        | #7   |

## API contracts

The literal and identifier hooks retain their default fallbacks. Identifier policy
receives the current projection, clause, input schema and outer schemas.

Column-alias extensions return normalized output names; DataFusion validates
cardinality and uniqueness. CTE conflict policy applies only to names inherited
from enclosing queries: duplicates within one WITH block still fail.

Join extensions receive planned inputs before the SQL constraint is bound. Their
output feeds subsequent joins. Wildcard extensions own the complete expansion,
including modifiers, and can preserve qualified source keys alongside merged keys.
The execution example uses a private qualifier for merged keys; it does not
establish SQL-unparser round-trip support.

SELECT-input extensions run after FROM and after WHERE. `materialize` adds scalar
projection slots, preserves reuse of computed values, and hides helper columns
from built-in and custom wildcard expansion. Aggregates, windows, subqueries,
outer references and UNNEST are rejected by this input-stage helper. JOIN input
placement uses the join hook. Alias visibility and evaluation placement remain
explicit choices in the dialect package.

Duplicate SQL output labels still require a mapping to unique internal names.
Custom type storage and execution use the existing Arrow/UDF interfaces; SQL
planning hooks do not introduce physical scalar variants.

## Related upstream work

- [Spark numeric literals #27213](https://github.com/apache/datafusion/pull/27213):
  the literal hook exposes the needed token information without a Spark branch in core parsing.
- [Lateral/WHERE aliases #17362](https://github.com/apache/datafusion/pull/17362)
  and [volatile HAVING/QUALIFY aliases #26850](https://github.com/apache/datafusion/pull/26850):
  identifier policy and expression placement are distinct parts of alias support.
- [Spark lateral-alias analyzer](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveLateralColumnAliasReference.scala):
  rewrites alias references through projections.
- [CTE scope exposure #24755](https://github.com/apache/datafusion/pull/24755)
  exposes existing lookup; [Spark CTE precedence](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-cte.html)
  motivates a policy before conflict rejection.
- [Join-key fix #22998](https://github.com/apache/datafusion/pull/22998):
  discusses merged values, qualified keys, chained joins and wildcard order.
  The join/wildcard extension dispatch does not replace that default correctness fix.
- [PostgreSQL partial column aliases](https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-TABLE-ALIASES):
  a shorter list preserves the remaining input names.

## QUALIFY regression evidence

The same planner test was executed on the adjacent commits below:

| Revision                                                                                                                                                     | Result                                                                                     |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| [`20434b0`](https://github.com/apache/datafusion/commit/20434b01721905769c9a8f85e8b497a245ccf07c), parent                                                    | PASS: Projection → Filter → WindowAggr → Values                                            |
| [`1e3b956`](https://github.com/apache/datafusion/commit/1e3b956fa79b279bdf02197eb5b35c99a21eb6b4), [#21173](https://github.com/apache/datafusion/pull/21173) | FAIL: `QUALIFY clause requires window functions in the SELECT list or QUALIFY clause`      |
| Current main + #1                                                                                                                                            | PASS: `qualify.slt`, including source-column, constant, collision and aggregate predicates |

```sql
SELECT ROW_NUMBER() OVER (ORDER BY x) AS rn
FROM (VALUES (1), (2)) t(x)
QUALIFY x = 2;
```

To reproduce the historical planner comparison, append this test to
`datafusion/sql/tests/sql_integration.rs` at each revision:

```rust
#[test]
fn qualify_projection_regression_21173() {
    let sql = "SELECT ROW_NUMBER() OVER (ORDER BY x) AS rn FROM (VALUES (1), (2)) t(x) QUALIFY x = 2";
    let plan = logical_plan(sql).expect("SELECT contains a window function");
    println!("{}", plan.display_indent());
}
```

Run `cargo test --profile ci -p datafusion-sql --test sql_integration qualify_projection_regression_21173 -- --nocapture`.
The comparison used Rust 1.99.1; no historical implementation changes were made.
Current `main` with only the new SLT cases also fails six checks; #1 passes them.
#21173 moved the eligibility scan after window calls in SELECT became column
references. #1 validates earlier and also excludes windows appearing only in
ORDER BY from the deferred-volatile path.

## Validation

The [combined branch](https://github.com/kuza55/datafusion/tree/kuza/sql-dialect-hooks-validation-v2)
contains all seven implementation PRs and an additional execution test combining
CTE precedence, partial aliases, merged join keys, custom wildcard expansion and
scalar materialization in one query.

- 109 extension execution tests pass.
- Formatting and all-target/all-feature Clippy pass.
- Extended workspace tests and the full nonfunctional suite are running.

The original three-PR validation record is preserved at
[the original index revision](https://github.com/kuza55/datafusion/blob/5fbefcd84206851f2d2cb62b247ad562965e3420/dev/sql-dialect-hooks.md).
