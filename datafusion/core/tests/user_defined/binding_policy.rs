// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_expr::planner::{
    BindingOrigin, BindingScope, ColumnAliasCoverage, ColumnAliasSite, ColumnBinding,
    ColumnLookup, CtePrecedence, IdentifierClause, SqlBindingPolicy,
};

fn context(policy: impl SqlBindingPolicy + 'static) -> SessionContext {
    SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_binding_policy(Arc::new(policy))
            .build(),
    )
}

/// Earlier SELECT aliases are visible to later items and to WHERE and JOIN ON,
/// but only for names that no column provides.
#[derive(Debug)]
struct LateralAliases;

impl SqlBindingPolicy for LateralAliases {
    fn resolve_column<'a>(
        &self,
        lookup: &ColumnLookup<'a>,
    ) -> Result<Option<&'a ColumnBinding>> {
        let mut aliases = vec![];
        for candidate in lookup.candidates() {
            match (candidate.origin(), lookup.clause()) {
                (BindingOrigin::Input, _) => return Ok(None),
                (
                    BindingOrigin::SelectAlias { index },
                    IdentifierClause::Select { index: current },
                ) if index < current => aliases.push(candidate),
                (
                    BindingOrigin::SelectAlias { .. },
                    IdentifierClause::Where | IdentifierClause::From,
                ) => aliases.push(candidate),
                _ => {}
            }
        }
        match aliases[..] {
            [] => Ok(None),
            [alias] => Ok(Some(alias)),
            _ => plan_err!("Ambiguous projection alias"),
        }
    }
}

async fn rows(ctx: &SessionContext, sql: &str) -> Result<Vec<Vec<i64>>> {
    let mut rows = vec![];
    for batch in ctx.sql(sql).await?.collect().await? {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        match ScalarValue::try_from_array(column, row).unwrap() {
                            ScalarValue::Int64(Some(value)) => value,
                            value => panic!("unexpected value {value:?} for {sql}"),
                        }
                    })
                    .collect(),
            );
        }
    }
    Ok(rows)
}

#[tokio::test]
async fn policy_binds_select_aliases() -> Result<()> {
    let ctx = context(LateralAliases);
    for (sql, expected) in [
        ("SELECT 1 AS a, a + 1 AS b, b + 1 AS c", vec![vec![1, 2, 3]]),
        (
            "SELECT SUM(x) AS a, a + 1 AS b FROM (VALUES (1), (2)) t(x)",
            vec![vec![3, 4]],
        ),
        ("SELECT 1 AS a, [a][1] AS b", vec![vec![1, 1]]),
        (
            "SELECT x + 1 AS a FROM (VALUES (1), (2)) t(x) WHERE a > 2",
            vec![vec![3]],
        ),
        (
            "SELECT a.x + b.y AS z FROM (VALUES (1)) a(x) JOIN (VALUES (2)) b(y) ON z = 3",
            vec![vec![3]],
        ),
        (
            "SELECT x + 1 AS a, SUM(a) OVER () AS s FROM (VALUES (1), (2)) t(x) ORDER BY a",
            vec![vec![2, 5], vec![3, 5]],
        ),
        (
            "SELECT SUM(x) OVER w AS a, a + 1 AS b FROM (VALUES (1), (2)) t(x) WINDOW w AS ()",
            vec![vec![3, 4], vec![3, 4]],
        ),
        // A column of the same name wins over the alias
        (
            "SELECT x + 1 AS x, x AS y FROM (VALUES (4)) t(x)",
            vec![vec![5, 4]],
        ),
        ("SELECT 7 AS \"A\", \"A\" + 1 AS b", vec![vec![7, 8]]),
        ("SELECT b FROM (SELECT 2 AS a, a + 1 AS b) q", vec![vec![3]]),
        (
            "SELECT 1 AS a, a + 1 AS b UNION ALL SELECT 3 AS a, a + 1 AS b",
            vec![vec![1, 2], vec![3, 4]],
        ),
        (
            "WITH q AS (SELECT 5 AS a, a + 1 AS b) SELECT b FROM q",
            vec![vec![6]],
        ),
    ] {
        assert_eq!(rows(&ctx, sql).await?, expected, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn policy_keeps_engine_scoping_and_errors() -> Result<()> {
    let ctx = context(LateralAliases);
    for (sql, message) in [
        ("SELECT a AS b, 1 AS a", "No field named a"),
        // Aliases belong to their own query, not to subqueries
        ("SELECT 1 AS a, (SELECT a) AS b", "No field named a"),
        ("SELECT 1 AS \"A\", a AS b", "No field named a"),
        ("SELECT b AS a, a AS b WHERE a > 0", "No field named b"),
        (
            "SELECT 1 AS z, z FROM (VALUES (2)) a(z) CROSS JOIN (VALUES (3)) b(z)",
            "Ambiguous reference",
        ),
        (
            "SELECT 1 AS a, 2 AS a WHERE a = 1",
            "Ambiguous projection alias",
        ),
    ] {
        let error = ctx.sql(sql).await.unwrap_err().to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }
    // A failed query leaves no alias scope or cycle guard behind
    assert_eq!(rows(&ctx, "SELECT 1 AS a, a AS b").await?, [[1, 1]]);
    // Without a policy the same query does not bind
    assert!(
        SessionContext::new()
            .sql("SELECT 1 AS a, a AS b")
            .await
            .is_err()
    );
    Ok(())
}

/// Picks the nearest scope, then the last candidate in it.
#[derive(Debug)]
struct LastInNearestScope;

impl SqlBindingPolicy for LastInNearestScope {
    fn resolve_column<'a>(
        &self,
        lookup: &ColumnLookup<'a>,
    ) -> Result<Option<&'a ColumnBinding>> {
        let depth = |binding: &ColumnBinding| match binding.scope() {
            BindingScope::Outer { depth } => depth + 1,
            _ => 0,
        };
        let nearest = lookup.candidates().iter().map(depth).min();
        Ok(lookup
            .candidates()
            .iter()
            .rfind(|binding| Some(depth(binding)) == nearest))
    }
}

#[tokio::test]
async fn policy_chooses_among_columns_and_scopes() -> Result<()> {
    let ctx = context(LastInNearestScope);
    for (sql, expected) in [
        // DataFusion alone reports an ambiguous reference
        (
            "SELECT k FROM (VALUES (1)) a(k) CROSS JOIN (VALUES (2)) b(k)",
            2,
        ),
        // A qualifier limits the candidates
        (
            "SELECT a.k FROM (VALUES (1)) a(k) CROSS JOIN (VALUES (2)) b(k)",
            1,
        ),
        // The inner query's own column beats the correlated one
        (
            "SELECT (SELECT k FROM (VALUES (7)) u(k)) FROM (VALUES (1)) t(k)",
            7,
        ),
        // With no inner match, the outer column is chosen and correlated
        (
            "SELECT (SELECT max(v) FROM (VALUES (4)) u(v) WHERE v = k) FROM (VALUES (4)) t(k)",
            4,
        ),
    ] {
        assert_eq!(rows(&ctx, sql).await?, [[expected]], "{sql}");
    }
    let error = ctx.sql("SELECT b AS a, a AS b").await.unwrap_err();
    assert!(
        error.to_string().contains("Cyclic identifier expansion"),
        "{error}"
    );
    Ok(())
}

/// Outer CTEs win, and CTE column lists must be complete.
#[derive(Debug)]
struct OuterCtes;

impl SqlBindingPolicy for OuterCtes {
    fn nested_cte_precedence(&self) -> CtePrecedence {
        CtePrecedence::Outer
    }

    fn column_alias_coverage(&self, site: ColumnAliasSite) -> ColumnAliasCoverage {
        match site {
            ColumnAliasSite::Cte => ColumnAliasCoverage::All,
            _ => ColumnAliasCoverage::Prefix,
        }
    }
}

#[derive(Debug)]
struct UniqueCtes;

impl SqlBindingPolicy for UniqueCtes {
    fn nested_cte_precedence(&self) -> CtePrecedence {
        CtePrecedence::Error
    }
}

#[tokio::test]
async fn policy_rejects_nested_cte_names() -> Result<()> {
    let ctx = context(UniqueCtes);
    let error = ctx
        .sql(
            "WITH t AS (SELECT 1) SELECT * FROM (WITH t AS (SELECT 2) SELECT * FROM t) u",
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("specified more than once"), "{error}");
    // Sibling queries may still reuse a name
    assert_eq!(
        rows(
            &ctx,
            "SELECT * FROM (WITH t AS (SELECT 1 AS x) SELECT x FROM t) a CROSS JOIN (WITH t AS (SELECT 2 AS y) SELECT y FROM t) b"
        )
        .await?,
        [[1, 2]]
    );
    Ok(())
}

#[tokio::test]
async fn policy_sets_cte_precedence_and_alias_coverage() -> Result<()> {
    let ctx = context(OuterCtes);
    for sql in [
        "WITH t AS (SELECT 1 AS x) SELECT (WITH t AS (SELECT 2 AS x) SELECT x FROM t) AS nested, x FROM t",
        "WITH t AS (SELECT 1 AS x), u AS (WITH t AS (SELECT 2 AS x) SELECT x FROM t) SELECT u.x, t.x FROM u CROSS JOIN t",
        "WITH t AS (SELECT 1 AS x) SELECT * FROM (WITH RECURSIVE t(x) AS (SELECT 2 UNION ALL SELECT x + 1 FROM t WHERE x < 3) SELECT max(x) FROM t) u CROSS JOIN t",
    ] {
        assert_eq!(rows(&ctx, sql).await?, [[1, 1]], "{sql}");
    }
    assert_eq!(
        rows(&ctx, "SELECT x, b FROM (SELECT 1 AS a, 2 AS b) t(x)").await?,
        [[1, 2]]
    );
    for (sql, message) in [
        // The ignored definition is still validated
        (
            "WITH t AS (SELECT 1) SELECT * FROM (WITH t AS (SELECT missing) SELECT * FROM t) u",
            "No field named missing",
        ),
        (
            "WITH t(x) AS (SELECT 1 AS a, 2 AS b) SELECT * FROM t",
            "contains 2 columns but only 1 names",
        ),
    ] {
        let error = ctx.sql(sql).await.unwrap_err().to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }
    Ok(())
}
