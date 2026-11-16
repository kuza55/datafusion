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
struct NumericLiteralPlanner;

impl ExprPlanner for NumericLiteralPlanner {
    fn plan_numeric_literal(
        &self,
        literal: datafusion_expr::planner::RawNumericLiteral,
    ) -> Result<PlannerResult<datafusion_expr::planner::RawNumericLiteral>> {
        let value = match literal.value.as_str() {
            "1.2300" => ScalarValue::Decimal128(
                Some(if literal.negative { -12300 } else { 12300 }),
                10,
                4,
            ),
            "1e2" if !literal.negative => ScalarValue::Int32(Some(100)),
            "7" if literal.long && !literal.negative => ScalarValue::Int32(Some(7)),
            "1_000" if !literal.negative => ScalarValue::Int32(Some(1000)),
            "999" => {
                return datafusion_common::plan_err!("literal rejected by extension");
            }
            _ => return Ok(PlannerResult::Original(literal)),
        };
        Ok(PlannerResult::Planned(lit(value)))
    }
}

#[tokio::test]
async fn custom_numeric_literals_preserve_token_and_sign() -> Result<()> {
    use datafusion_expr::planner::RawNumericLiteral;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct Observe(Arc<Mutex<Vec<RawNumericLiteral>>>);
    impl ExprPlanner for Observe {
        fn plan_numeric_literal(
            &self,
            literal: RawNumericLiteral,
        ) -> Result<PlannerResult<RawNumericLiteral>> {
            self.0.lock().unwrap().push(literal.clone());
            Ok(PlannerResult::Original(literal))
        }
    }

    let seen = Arc::new(Mutex::new(vec![]));
    let config = SessionConfig::new().set_str("datafusion.sql_parser.dialect", "spark");
    let mut ctx = SessionContext::new_with_config(config);
    ctx.register_expr_planner(Arc::new(Observe(Arc::clone(&seen))))?;
    ctx.register_expr_planner(Arc::new(NumericLiteralPlanner))?;
    let batches = ctx
        .sql("SELECT 1.2300, -1.2300, 1e2, 7L, 1_000, -9223372036854775808 AS min_int")
        .await?
        .collect()
        .await?;
    let expected = [
        ScalarValue::Decimal128(Some(12300), 10, 4),
        ScalarValue::Decimal128(Some(-12300), 10, 4),
        ScalarValue::Int32(Some(100)),
        ScalarValue::Int32(Some(7)),
        ScalarValue::Int32(Some(1000)),
        ScalarValue::Int64(Some(i64::MIN)),
    ];
    for (column, expected) in batches[0].columns().iter().zip(expected) {
        assert_eq!(ScalarValue::try_from_array(column, 0)?, expected);
    }
    assert!(seen.lock().unwrap().contains(&RawNumericLiteral {
        value: "1.2300".into(),
        negative: true,
        long: false,
    }));
    assert!(seen.lock().unwrap().contains(&RawNumericLiteral {
        value: "7".into(),
        negative: false,
        long: true,
    }));
    let error = ctx.sql("SELECT 999").await.unwrap_err().to_string();
    assert!(error.contains("literal rejected by extension"), "{error}");
    Ok(())
}

#[tokio::test]
async fn numeric_literal_fallthrough_preserves_defaults() -> Result<()> {
    let mut ctx = SessionContext::new();
    ctx.register_expr_planner(Arc::new(NumericLiteralPlanner))?;
    let batches = ctx.sql("SELECT 42, -42, 0.5").await?.collect().await?;
    for (column, expected) in batches[0].columns().iter().zip([
        ScalarValue::Int64(Some(42)),
        ScalarValue::Int64(Some(-42)),
        ScalarValue::Float64(Some(0.5)),
    ]) {
        assert_eq!(ScalarValue::try_from_array(column, 0)?, expected);
    }
    Ok(())
}
