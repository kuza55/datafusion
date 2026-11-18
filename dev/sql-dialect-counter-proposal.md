<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# SQL dialect extension series: counter-proposal

A response to [PR #4](https://github.com/kuza55/datafusion/pull/4), on the same
base (`e2dc8b048b542e7abdc296254aa54e26eb89d76d`, 57.0.0-dev). The design
rationale is in [PR #13](https://github.com/kuza55/datafusion/pull/13).

| PR                                                  | Change                                               | Replaces                           | Base                            |
| --------------------------------------------------- | ---------------------------------------------------- | ---------------------------------- | ------------------------------- |
| [#9](https://github.com/kuza55/datafusion/pull/9)   | Nested `WITH` shadows an enclosing CTE name          | `UseInner` in #6                   | main                            |
| [#10](https://github.com/kuza55/datafusion/pull/10) | Column alias lists may be shorter than the relation  | `plan_column_aliases` in #5        | main                            |
| [#11](https://github.com/kuza55/datafusion/pull/11) | `SqlBindingPolicy::resolve_column`                   | `ExprPlanner::resolve_column` (#3) | main                            |
| [#12](https://github.com/kuza55/datafusion/pull/12) | `nested_cte_precedence`, `column_alias_coverage`     | `KeepOuter` in #6, strict lists in #5 | `kuza/sql-dialect-counter-base` |

Unchanged from the original series: #1 (QUALIFY fix) and #2
(`ExprPlanner::plan_numeric_literal`).

Not reimplemented: the merged `USING`/`NATURAL` key from #7 should land as engine
behaviour through [apache/datafusion#22998](https://github.com/apache/datafusion/pull/22998),
and `expand_wildcard` is deferred until a use case remains after that fix.
