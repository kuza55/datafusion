// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::RecordBatch;
use datafusion::common::test_util::batches_to_string;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::common::DFSchema;
use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::Operator;
use datafusion::prelude::*;
use datafusion::sql::sqlparser::ast::BinaryOperator;
use datafusion_common::ScalarValue;
use datafusion_expr::BinaryExpr;
use datafusion_expr::expr::Alias;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};

#[derive(Debug)]
struct MyCustomPlanner;

#[derive(Debug)]
struct DeferColonPlanner {
    colon_seen: Arc<AtomicBool>,
}

impl ExprPlanner for DeferColonPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        if matches!(&expr.op, BinaryOperator::Custom(op) if op == ":") {
            self.colon_seen.store(true, Ordering::Relaxed);
        }
        Ok(PlannerResult::Original(expr))
    }
}

impl ExprPlanner for MyCustomPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        match &expr.op {
            BinaryOperator::Arrow => {
                Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(expr.left.clone()),
                    right: Box::new(expr.right.clone()),
                    op: Operator::StringConcat,
                })))
            }
            BinaryOperator::LongArrow => {
                Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(expr.left.clone()),
                    right: Box::new(expr.right.clone()),
                    op: Operator::Plus,
                })))
            }
            BinaryOperator::Question => {
                Ok(PlannerResult::Planned(Expr::Alias(Alias::new(
                    Expr::Literal(ScalarValue::Boolean(Some(true)), None),
                    None::<&str>,
                    format!("{} ? {}", expr.left, expr.right),
                ))))
            }
            BinaryOperator::Custom(op) if op == ":" => {
                Ok(PlannerResult::Planned(Expr::Alias(Alias::new(
                    Expr::Literal(ScalarValue::Boolean(Some(true)), None),
                    None::<&str>,
                    "custom colon",
                ))))
            }
            _ => Ok(PlannerResult::Original(expr)),
        }
    }
}

async fn plan_and_collect(sql: &str) -> Result<Vec<RecordBatch>> {
    let config =
        SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
    let mut ctx = SessionContext::new_with_config(config);
    ctx.register_expr_planner(Arc::new(MyCustomPlanner))?;
    ctx.sql(sql).await?.collect().await
}

#[tokio::test]
async fn test_custom_operators_arrow() {
    let actual = plan_and_collect("select 'foo'->'bar';").await.unwrap();
    insta::assert_snapshot!(batches_to_string(&actual), @r#"
    +----------------------------+
    | Utf8("foo") || Utf8("bar") |
    +----------------------------+
    | foobar                     |
    +----------------------------+
    "#);
}

#[tokio::test]
async fn test_custom_operators_long_arrow() {
    let actual = plan_and_collect("select 1->>2;").await.unwrap();
    insta::assert_snapshot!(batches_to_string(&actual), @r"
    +---------------------+
    | Int64(1) + Int64(2) |
    +---------------------+
    | 3                   |
    +---------------------+
    ");
}

#[tokio::test]
async fn test_question_select() {
    let actual = plan_and_collect("select a ? 2 from (select 1 as a);")
        .await
        .unwrap();
    insta::assert_snapshot!(batches_to_string(&actual), @r"
    +--------------+
    | a ? Int64(2) |
    +--------------+
    | true         |
    +--------------+
    ");
}

#[tokio::test]
async fn test_question_filter() {
    let actual = plan_and_collect("select a from (select 1 as a) where a ? 2;")
        .await
        .unwrap();
    insta::assert_snapshot!(batches_to_string(&actual), @r"
    +---+
    | a |
    +---+
    | 1 |
    +---+
    ");
}

#[tokio::test]
async fn test_custom_struct_colon_operator() {
    let config =
        SessionConfig::new().set_str("datafusion.sql_parser.dialect", "snowflake");
    let mut ctx = SessionContext::new_with_config(config);
    ctx.register_expr_planner(Arc::new(MyCustomPlanner))
        .unwrap();

    let actual = ctx
        .sql("select {'a': 1}:a;")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    insta::assert_snapshot!(batches_to_string(&actual), @r"
    +--------------+
    | custom colon |
    +--------------+
    | true         |
    +--------------+
    ");
}

#[tokio::test]
async fn test_deferred_struct_colon_operator() {
    let config =
        SessionConfig::new().set_str("datafusion.sql_parser.dialect", "snowflake");
    let mut ctx = SessionContext::new_with_config(config);
    let colon_seen = Arc::new(AtomicBool::new(false));
    ctx.register_expr_planner(Arc::new(DeferColonPlanner {
        colon_seen: Arc::clone(&colon_seen),
    }))
    .unwrap();

    let actual = ctx
        .sql("select {'a': 1}:a;")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(colon_seen.load(Ordering::Relaxed));
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].num_rows(), 1);
    assert_eq!(
        ScalarValue::try_from_array(actual[0].column(0).as_ref(), 0).unwrap(),
        ScalarValue::Int64(Some(1))
    );
}

#[derive(Debug)]
struct ProjectionAliasPlanner;

impl ExprPlanner for ProjectionAliasPlanner {
    fn resolve_column(
        &self,
        request: &datafusion_expr::planner::ColumnLookup<'_>,
    ) -> Result<Option<datafusion_expr::planner::BindingId>> {
        use datafusion_expr::planner::{ColumnBindingKind, IdentifierClause};
        if request
            .candidates()
            .iter()
            .any(|c| !matches!(c.kind(), ColumnBindingKind::SelectAlias { .. }))
        {
            return Ok(None);
        }
        let mut aliases =
            request
                .candidates()
                .iter()
                .filter(|c| match (c.kind(), request.clause()) {
                    (
                        ColumnBindingKind::SelectAlias { index },
                        IdentifierClause::Select { index: current },
                    ) => index < current,
                    (
                        ColumnBindingKind::SelectAlias { .. },
                        IdentifierClause::Where | IdentifierClause::From,
                    ) => true,
                    _ => false,
                });
        let result = aliases.next().map(|c| c.id());
        if aliases.next().is_some() {
            return datafusion_common::plan_err!("Ambiguous projection alias");
        }
        Ok(result)
    }
}

#[tokio::test]
async fn identifier_planner_binds_query_local_aliases() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_expr_planner(Arc::new(ProjectionAliasPlanner))?;
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
            "SELECT x AS a, (SELECT MAX(v) FROM (VALUES (4)) u(v) WHERE v = t.x) AS b FROM (VALUES (4)) t(x)",
            vec![vec![4, 4]],
        ),
        (
            "WITH q AS (SELECT 5 AS a, a + 1 AS b) SELECT b FROM q",
            vec![vec![6]],
        ),
    ] {
        let batches = ctx.sql(sql).await?.collect().await?;
        let mut actual = vec![];
        for batch in batches {
            for row in 0..batch.num_rows() {
                actual.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| {
                            match ScalarValue::try_from_array(column, row).unwrap() {
                                ScalarValue::Int64(Some(value)) => value,
                                value => panic!("unexpected value {value:?} for {sql}"),
                            }
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        assert_eq!(actual, expected, "{sql}");
    }
    Ok(())
}

#[tokio::test]
async fn identifier_planner_preserves_errors_and_scope() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_expr_planner(Arc::new(ProjectionAliasPlanner))?;
    for (sql, message) in [
        ("SELECT a AS b, 1 AS a", "No field named a"),
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
    // An earlier error must not contaminate a later query's scope or cycle guard.
    assert_eq!(
        ctx.sql("SELECT 1 AS a, a AS b").await?.collect().await?[0].num_rows(),
        1
    );
    let stock = SessionContext::new();
    assert!(stock.sql("SELECT 1 AS a, a + 1 AS b").await.is_err());
    Ok(())
}

#[derive(Debug, Default)]
struct StaleColumnBinding(std::sync::Mutex<Option<datafusion_expr::planner::BindingId>>);

impl ExprPlanner for StaleColumnBinding {
    fn resolve_column(
        &self,
        request: &datafusion_expr::planner::ColumnLookup<'_>,
    ) -> Result<Option<datafusion_expr::planner::BindingId>> {
        let mut previous = self.0.lock().unwrap();
        if previous.is_some() {
            return Ok(*previous);
        }
        *previous = request.candidates().first().map(|binding| binding.id());
        Ok(None)
    }
}

#[tokio::test]
async fn column_bindings_reject_ids_from_another_lookup() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_expr_planner(Arc::new(StaleColumnBinding::default()))?;
    let error = ctx
        .sql("SELECT t.x, t.y FROM (VALUES (1, 2)) t(x, y)")
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("does not belong to this lookup"),
        "{error}"
    );
    Ok(())
}

#[derive(Debug)]
struct FirstColumnBinding;

impl ExprPlanner for FirstColumnBinding {
    fn resolve_column(
        &self,
        request: &datafusion_expr::planner::ColumnLookup<'_>,
    ) -> Result<Option<datafusion_expr::planner::BindingId>> {
        Ok(request.candidates().first().map(|binding| binding.id()))
    }
}

#[tokio::test]
async fn column_bindings_select_qualified_names_and_reject_alias_cycles() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_expr_planner(Arc::new(FirstColumnBinding))?;
    let batches = ctx
        .sql("SELECT b.k FROM (VALUES (1)) a(k) CROSS JOIN (VALUES (2)) b(k)")
        .await?
        .collect()
        .await?;
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0)?,
        ScalarValue::Int64(Some(2))
    );
    let error = ctx.sql("SELECT b AS a, a AS b").await.unwrap_err();
    assert!(
        error.to_string().contains("Cyclic identifier expansion"),
        "{error}"
    );
    Ok(())
}

#[tokio::test]
async fn alias_binding_allows_volatile_expressions() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_expr_planner(Arc::new(ProjectionAliasPlanner))?;
    let batches = ctx
        .sql("SELECT random() AS r, r AS s")
        .await?
        .collect()
        .await?;
    assert_eq!((batches[0].num_rows(), batches[0].num_columns()), (1, 2));
    Ok(())
}
