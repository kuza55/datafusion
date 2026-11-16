<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# SQL dialect extension series

Based on `e2dc8b048b542e7abdc296254aa54e26eb89d76d` (57.0.0-dev).
All implementation PRs independently target `kuza55/datafusion:main`.
This index does not merge the implementations.
The [validation commit](https://github.com/kuza55/datafusion/commit/15f1412062daf0513468f52e7f1cda09c9df755c)
combines #1–#3 and resolves their additive planner/test-file conflicts.

| PR                                                | Extension or fix                                                           | Dependency |
| ------------------------------------------------- | -------------------------------------------------------------------------- | ---------- |
| [#1](https://github.com/kuza55/datafusion/pull/1) | Validate QUALIFY before rebasing SELECT windows                            | None       |
| [#2](https://github.com/kuza55/datafusion/pull/2) | `ExprPlanner::plan_numeric_literal(RawNumericLiteral)`                     | None       |
| [#3](https://github.com/kuza55/datafusion/pull/3) | `ExprPlanner::plan_identifier` with query-local `IdentifierPlannerContext` | None       |

## Scope

The numeric hook preserves the SQL token's precision, scale, exponent, separators,
long suffix and directly applied minus before ScalarValue conversion. Extensions
can return an expression or defer to the existing parser.

The identifier hook exposes the current SELECT projection, clause, input schema
and outer schemas. Extensions choose precedence and alias visibility. Recursive
replacement planning stays in DataFusion, with scope isolation and cycle checks.
The execution tests demonstrate non-volatile aliases in SELECT, WHERE, JOIN ON and
window arguments and array elements. Volatile alias materialization remains the caller's responsibility;
the example planner rejects it. This is an extension point, not full Snowflake binding.

Existing UDF, type, scalar-function, operator and relation hooks remain useful.
Merged join keys, wildcard ordering, partial column aliases, duplicate output
labels and nested CTE policy are separate residual compatibility work. These SQL
hooks do not add physical scalar variants; custom types still need supported
Arrow storage, metadata and execution functions.

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

Rust 1.99.1, on the combined validation commit linked above:

| Check                                                      | Result                                                                                            |
| ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Focused expression-planner execution tests                 | 10 passed                                                                                         |
| Extended workspace suite                                   | 13,455 Rust tests passed, 8 ignored; all 307 SQL logic files passed                               |
| `cargo fmt --all -- --check`                               | Passed                                                                                            |
| `cargo clippy --all-targets --all-features -- -D warnings` | Passed                                                                                            |
| Nonfunctional suite                                        | All steps passed; the final HTML-docs step was rerun after installing Graphviz and cargo-depgraph |

The extended suite used:

```sh
RUST_BACKTRACE=1 cargo test --profile ci \
  --exclude datafusion-examples --exclude datafusion-benchmarks --exclude datafusion-cli \
  --workspace --lib --tests --bins \
  --features avro,json,backtrace,extended_tests,recursive_protection,parquet_encryption
```

The existing `sql_planner` benchmarks were compared with `main` using the
unoptimized `ci` profile, 30 samples, a 2-second warmup and 5-second measurement.
The table shows Criterion's point estimates from the repeat after the test suite
finished; the first run overlapped fuzz tests and was discarded as noisy.

| Benchmark                                           | Main      | Combined hooks |
| --------------------------------------------------- | --------- | -------------- |
| `logical_select_one_from_700`                       | 1.841 ms  | 1.718 ms       |
| `logical_select_all_from_1000`                      | 23.721 ms | 16.397 ms      |
| `logical_wide_case_50_exprs`                        | 10.326 ms | 8.103 ms       |
| `optimizer_select_one_from_700` (unchanged control) | 1.673 ms  | 1.584 ms       |

The unchanged optimizer control also shifted. These are shared-runner smoke
measurements; release-build overhead remains unmeasured.

To repeat, share `CARGO_TARGET_DIR` between the main and validation worktrees,
set `CLICKBENCH_DATA_PATH` to `datafusion/core/tests/data/clickbench_hits_10.parquet`
using an absolute path, and run:

```sh
cargo bench --profile ci -p datafusion --bench sql_planner -- \
  'logical_select_one_from_700|logical_select_all_from_1000|logical_wide_case_50_exprs|optimizer_select_one_from_700' \
  --warm-up-time 2 --measurement-time 5 --sample-size 30 --save-baseline quiet-main
```

On the validation branch, replace `--save-baseline quiet-main` with
`--baseline quiet-main`.

## Upstream precedents

- [#24846](https://github.com/apache/datafusion/pull/24846) and [#26177](https://github.com/apache/datafusion/pull/26177): scalar-function hooks and schema access, merged.
- [#24755](https://github.com/apache/datafusion/pull/24755): expose CTE scope instead of reconstructing it downstream, merged.
- [#27213](https://github.com/apache/datafusion/pull/27213): draft Spark-specific numeric parsing; #2 provides reusable dispatch instead.
- [#17362](https://github.com/apache/datafusion/pull/17362): lateral aliases/WHERE aliases, closed unmerged. #3 supplies a policy hook rather than changing default SQL behavior.
- [#22998](https://github.com/apache/datafusion/pull/22998): merged USING/NATURAL join keys, open; not duplicated in this series.
