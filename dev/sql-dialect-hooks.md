<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# SQL dialect extension series

Based on `e2dc8b048b542e7abdc296254aa54e26eb89d76d` (57.0.0-dev).
All implementation PRs independently target `kuza55/datafusion:main`.
This index does not merge the implementations.

| PR | Extension or fix | Dependency |
| --- | --- | --- |
| [#1](https://github.com/kuza55/datafusion/pull/1) | Validate QUALIFY before rebasing SELECT windows | None |
| [#2](https://github.com/kuza55/datafusion/pull/2) | `ExprPlanner::plan_numeric_literal(RawNumericLiteral)` | None |
| [#3](https://github.com/kuza55/datafusion/pull/3) | `ExprPlanner::plan_identifier` with query-local `IdentifierPlannerContext` | None |

## Scope

The numeric hook preserves the SQL token's precision, scale, exponent, separators,
long suffix and directly applied minus before ScalarValue conversion. Extensions
can return an expression or defer to the existing parser.

The identifier hook exposes the current SELECT projection, clause, input schema
and outer schemas. Extensions choose precedence and alias visibility. Recursive
replacement planning stays in DataFusion, with scope isolation and cycle checks.
The execution tests demonstrate immutable aliases in SELECT, WHERE, JOIN ON and
window arguments. Volatile alias materialization remains the caller's responsibility;
the example planner rejects it. This is an extension point, not full Snowflake binding.

Existing UDF, type, scalar-function, operator and relation hooks remain useful.
Merged join keys, wildcard ordering, partial column aliases, duplicate output
labels and nested CTE policy are separate residual compatibility work.

## QUALIFY regression evidence

The same planner test was executed on the adjacent commits below:

| Revision | Result |
| --- | --- |
| [`20434b0`](https://github.com/apache/datafusion/commit/20434b01721905769c9a8f85e8b497a245ccf07c), parent | PASS: Projection → Filter → WindowAggr → Values |
| [`1e3b956`](https://github.com/apache/datafusion/commit/1e3b956fa79b279bdf02197eb5b35c99a21eb6b4), [#21173](https://github.com/apache/datafusion/pull/21173) | FAIL: `QUALIFY clause requires window functions in the SELECT list or QUALIFY clause` |
| Current main + #1 | PASS: `qualify.slt`, including source-column, constant, collision and aggregate predicates |

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
#21173 moved the eligibility scan after window calls in SELECT became column
references. #1 validates earlier and also excludes windows appearing only in
ORDER BY from the deferred-volatile path.

## Upstream precedents

- [#24846](https://github.com/apache/datafusion/pull/24846) and [#26177](https://github.com/apache/datafusion/pull/26177): scalar-function hooks and schema access, merged.
- [#24755](https://github.com/apache/datafusion/pull/24755): expose CTE scope instead of reconstructing it downstream, merged.
- [#27213](https://github.com/apache/datafusion/pull/27213): draft Spark-specific numeric parsing; #2 provides reusable dispatch instead.
- [#17362](https://github.com/apache/datafusion/pull/17362): lateral aliases/WHERE aliases, closed unmerged. #3 supplies a policy hook rather than changing default SQL behavior.
- [#22998](https://github.com/apache/datafusion/pull/22998): merged USING/NATURAL join keys, open; not duplicated in this series.
