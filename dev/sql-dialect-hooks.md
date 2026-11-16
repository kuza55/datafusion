<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# SQL dialect extension series

Based on `e2dc8b048b542e7abdc296254aa54e26eb89d76d` (57.0.0-dev).
API contracts, rationale and related upstream discussions are in each PR.

| PR                                                | API / fix                                 | Purpose                                                                          | Base |
| ------------------------------------------------- | ----------------------------------------- | -------------------------------------------------------------------------------- | ---- |
| [#1](https://github.com/kuza55/datafusion/pull/1) | QUALIFY validation before window rebasing | Fix the regression introduced by apache/datafusion#21173.                        | main |
| [#2](https://github.com/kuza55/datafusion/pull/2) | `ExprPlanner::plan_numeric_literal`       | Preserve literal spelling, scale, sign and suffix for dialect typing.            | main |
| [#3](https://github.com/kuza55/datafusion/pull/3) | `ExprPlanner::resolve_column`             | Choose alias visibility and identifier precedence with clause/scope information. | main |
| [#5](https://github.com/kuza55/datafusion/pull/5) | `RelationPlanner::plan_column_aliases`    | Apply partial column-alias policy consistently to relations and CTEs.            | main |
| [#6](https://github.com/kuza55/datafusion/pull/6) | `RelationPlanner::resolve_cte_conflict`   | Choose inner/outer CTE precedence before duplicate-name rejection.               | main |
| [#7](https://github.com/kuza55/datafusion/pull/7) | Joined bindings + `expand_wildcard`       | Select merged keys and wildcard order from engine-owned bindings.                | #3   |

[PR #4](https://github.com/kuza55/datafusion/pull/4) contains the hook APIs and
high-level binding alternatives. Guaranteed alias-value reuse is not a binding
requirement; the SELECT input materialization experiment is withdrawn.

[PR #1](https://github.com/kuza55/datafusion/pull/1) records the QUALIFY regression:
the same query passes at `20434b0` and fails at its child `1e3b956`
([apache/datafusion#21173](https://github.com/apache/datafusion/pull/21173)).
The [historical reproduction](https://github.com/kuza55/datafusion/blob/9f2646ee620f3694e9eb1a7f907f1464bfb9650d/dev/sql-dialect-hooks.md#qualify-regression-evidence)
includes the test and command.

The previous [identifier](https://github.com/kuza55/datafusion/blob/348dcbee16a0c359a7abc6d4003fe9a74a21f542/datafusion/expr/src/planner.rs)
and [join/wildcard](https://github.com/kuza55/datafusion/blob/f246f4c8df07c387a6a2c742bb5d7bed95805ee9/datafusion/expr/src/planner.rs)
replacement APIs remain alternatives in PR #4. The active hooks select validated
binding IDs; DataFusion owns expression expansion and join construction.
