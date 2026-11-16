<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# SQL dialect extension series

Based on `e2dc8b048b542e7abdc296254aa54e26eb89d76d` (57.0.0-dev).
API contracts, rationale and related upstream discussions are in each PR.

| PR                                                | API / fix                                            | Purpose                                                                             | Base |
| ------------------------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------------- | ---- |
| [#1](https://github.com/kuza55/datafusion/pull/1) | QUALIFY validation before window rebasing            | Fix the regression introduced by apache/datafusion#21173.                           | main |
| [#2](https://github.com/kuza55/datafusion/pull/2) | `ExprPlanner::plan_numeric_literal`                  | Preserve literal spelling, scale, sign and suffix for dialect typing.               | main |
| [#3](https://github.com/kuza55/datafusion/pull/3) | `ExprPlanner::plan_identifier` + query context       | Choose alias visibility and identifier precedence with clause/scope information.    | main |
| [#5](https://github.com/kuza55/datafusion/pull/5) | `RelationPlanner::plan_column_aliases`               | Apply partial column-alias policy consistently to relations and CTEs.               | main |
| [#6](https://github.com/kuza55/datafusion/pull/6) | `RelationPlanner::resolve_cte_conflict`              | Choose inner/outer CTE precedence before duplicate-name rejection.                  | main |
| [#7](https://github.com/kuza55/datafusion/pull/7) | `RelationPlanner::plan_join` / `plan_wildcard`       | Build merged-key projections, preserve qualified inputs and control wildcard order. | #3   |
| [#8](https://github.com/kuza55/datafusion/pull/8) | `RelationPlanner::plan_select_input` + `materialize` | Add computed columns before WHERE or SELECT planning and reuse their values.        | #7   |

The [binding-policy proposal](https://github.com/kuza55/datafusion/blob/9f2646ee620f3694e9eb1a7f907f1464bfb9650d/dev/sql-binding-policy-proposal.md)
explores typed decisions over engine-owned binding identities instead of mutable
planning callbacks. The implementation PRs retain their original prototype APIs;
they do not implement that proposed redesign.

[PR #1](https://github.com/kuza55/datafusion/pull/1) records the QUALIFY regression:
the same query passes at `20434b0` and fails at its child `1e3b956`
([apache/datafusion#21173](https://github.com/apache/datafusion/pull/21173)).
The [historical reproduction](https://github.com/kuza55/datafusion/blob/9f2646ee620f3694e9eb1a7f907f1464bfb9650d/dev/sql-dialect-hooks.md#qualify-regression-evidence)
includes the test and command.

The [combined validation branch](https://github.com/kuza55/datafusion/tree/kuza/sql-dialect-hooks-validation-v2)
composes all seven implementations and retains the larger merged-key demonstration
and cross-layer execution test outside the individual API diffs.
