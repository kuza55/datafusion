// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::expr::reject_window_functions;
use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use datafusion_common::{
    Column, HashMap, Result, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::planner::ColumnBindingKind;
use datafusion_expr::{Case, Expr};
use datafusion_expr::{JoinType, LogicalPlan, LogicalPlanBuilder};
use sqlparser::ast::{
    Join, JoinConstraint, JoinOperator, ObjectName, TableFactor, TableWithJoins,
};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct JoinColumnBinding {
    pub column: Column,
    pub expression: Expr,
    pub kind: ColumnBindingKind,
    pub visible: bool,
}

pub(crate) fn column_bindings(
    plan: &LogicalPlan,
    context: &PlannerContext,
) -> Result<Vec<JoinColumnBinding>> {
    if let Some((_, bindings)) = context
        .join_bindings
        .iter()
        .find(|(schema, _)| Arc::ptr_eq(schema, plan.schema()))
    {
        return Ok(bindings.clone());
    }
    match plan {
        LogicalPlan::Filter(filter) => column_bindings(&filter.input, context),
        LogicalPlan::Join(join) => {
            let mut left = column_bindings(&join.left, context)?;
            let mut right = column_bindings(&join.right, context)?;
            if matches!(join.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                return Ok(left);
            }
            if matches!(join.join_type, JoinType::RightSemi | JoinType::RightAnti) {
                return Ok(right);
            }
            let mut merged = vec![];
            if join.join_constraint == datafusion_expr::JoinConstraint::Using {
                for (left_key, right_key) in &join.on {
                    let name = match left_key {
                        Expr::Column(column) => column.name.clone(),
                        Expr::Alias(alias) => alias.name.clone(),
                        _ => continue,
                    };
                    let expression = match join.join_type {
                        JoinType::Right => right_key.clone().unalias(),
                        JoinType::Full => Expr::Case(Case::new(
                            None,
                            vec![(
                                Box::new(left_key.clone().unalias().is_not_null()),
                                Box::new(left_key.clone().unalias()),
                            )],
                            Some(Box::new(right_key.clone().unalias())),
                        )),
                        _ => left_key.clone().unalias(),
                    }
                    .alias(name.clone());
                    for binding in left.iter_mut().chain(right.iter_mut()) {
                        if binding.column.name == name {
                            binding.visible = false;
                            if binding.column.relation.is_some() {
                                binding.kind = ColumnBindingKind::UsingInput;
                            }
                        }
                    }
                    merged.push(JoinColumnBinding {
                        column: Column::from_name(name),
                        expression,
                        kind: ColumnBindingKind::Using,
                        visible: true,
                    });
                }
            }
            merged.extend(
                left.into_iter().chain(right).filter(|binding| {
                    binding.visible || binding.column.relation.is_some()
                }),
            );
            Ok(merged)
        }
        _ => Ok(plan
            .schema()
            .columns()
            .into_iter()
            .map(|column| JoinColumnBinding {
                expression: Expr::Column(column.clone()),
                column,
                kind: ColumnBindingKind::Input,
                visible: true,
            })
            .collect()),
    }
}

const JOIN_ON_HELP: &str =
    "Compute the window function in a subquery and join on its result";

impl<S: ContextProvider> SqlToRel<'_, S> {
    pub(crate) fn plan_table_with_joins(
        &self,
        t: TableWithJoins,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let mut left = if is_lateral(&t.relation) {
            self.create_relation_subquery(t.relation, planner_context)?
        } else {
            self.create_relation(t.relation, planner_context)?
        };
        let old_outer_from_schema = planner_context.outer_from_schema();
        for join in t.joins {
            planner_context.extend_outer_from_schema(left.schema())?;
            left = self.parse_relation_join(left, join, planner_context)?;
        }
        planner_context.set_outer_from_schema(old_outer_from_schema);
        Ok(left)
    }

    pub(crate) fn parse_relation_join(
        &self,
        left: LogicalPlan,
        join: Join,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let right = if is_lateral_join(&join)? {
            self.create_relation_subquery(join.relation, planner_context)?
        } else {
            self.create_relation(join.relation, planner_context)?
        };
        match join.join_operator {
            JoinOperator::LeftOuter(constraint) | JoinOperator::Left(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Left, planner_context)
            }
            JoinOperator::RightOuter(constraint) | JoinOperator::Right(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Right, planner_context)
            }
            JoinOperator::Inner(constraint) | JoinOperator::Join(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Inner, planner_context)
            }
            JoinOperator::LeftSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftSemi,
                planner_context,
            ),
            JoinOperator::RightSemi(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightSemi,
                planner_context,
            ),
            JoinOperator::LeftAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::LeftAnti,
                planner_context,
            ),
            JoinOperator::RightAnti(constraint) => self.parse_join(
                left,
                right,
                constraint,
                JoinType::RightAnti,
                planner_context,
            ),
            JoinOperator::FullOuter(constraint) => {
                self.parse_join(left, right, constraint, JoinType::Full, planner_context)
            }
            JoinOperator::CrossJoin(JoinConstraint::None) => {
                self.parse_cross_join(left, right)
            }
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => self.parse_asof_join(
                left,
                right,
                match_condition,
                constraint,
                planner_context,
            ),
            other => not_impl_err!("Unsupported JOIN operator {other:?}"),
        }
    }

    fn parse_asof_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        sql_match_condition: sqlparser::ast::Expr,
        constraint: JoinConstraint,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        let join_schema = left.schema().join(right.schema())?;
        let match_condition =
            self.sql_to_expr(sql_match_condition, &join_schema, planner_context)?;

        match constraint {
            JoinConstraint::On(sql_on) => {
                let window_span = self.window_function_span(&sql_on, &HashMap::new());
                let on = self.sql_to_expr(sql_on, &join_schema, planner_context)?;
                reject_window_functions(&on, "JOIN ON", JOIN_ON_HELP, window_span)?;
                LogicalPlanBuilder::from(left)
                    .asof_join_on(right, Some(on), match_condition)?
                    .build()
            }
            JoinConstraint::Using(object_names) => {
                let keys = object_names
                    .into_iter()
                    .map(|object_name| {
                        let ObjectName(mut object_names) = object_name;
                        if object_names.len() != 1 {
                            return not_impl_err!(
                                "Invalid identifier in ASOF USING clause. Expected single identifier, got {}",
                                ObjectName(object_names)
                            );
                        }
                        let id = object_names.swap_remove(0);
                        id.as_ident()
                            .ok_or_else(|| {
                                plan_datafusion_err!(
                                    "Expected identifier in ASOF USING clause"
                                )
                            })
                            .map(|ident| {
                                Column::from_name(
                                    self.ident_normalizer.normalize(ident.clone()),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                LogicalPlanBuilder::from(left)
                    .asof_join_using(right, keys, match_condition)?
                    .build()
            }
            JoinConstraint::None => LogicalPlanBuilder::from(left)
                .asof_join_on(right, None, match_condition)?
                .build(),
            JoinConstraint::Natural => plan_err!("NATURAL ASOF JOIN is not supported"),
        }
    }

    fn parse_cross_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
    ) -> Result<LogicalPlan> {
        LogicalPlanBuilder::from(left).cross_join(right)?.build()
    }

    fn plan_using_bindings(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        keys: Vec<Column>,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        use datafusion_expr::expr_rewriter::normalize_col;
        let previous_input = planner_context.identifier_input.take();
        let projection =
            std::mem::replace(&mut planner_context.identifier_projection, Arc::from([]));
        let result = (|| {
            let mut handled = false;
            let mut on = vec![];
            for key in &keys {
                let mut expressions = vec![];
                for input in [&left, &right] {
                    planner_context.identifier_input = Some(Arc::new(input.clone()));
                    // The SQL name is already normalized; quoting prevents normalizing it again.
                    let id = sqlparser::ast::Ident::with_quote('"', key.name.clone());
                    let expr = self.resolve_column_reference(
                        &[id],
                        input.schema(),
                        planner_context,
                    )?;
                    handled |= expr.is_some();
                    expressions.push(match expr {
                        Some(expr) => expr.unalias().alias(key.name.clone()),
                        None => normalize_col(Expr::Column(key.clone()), input)?,
                    });
                }
                let right = expressions.pop().unwrap();
                on.push((expressions.pop().unwrap(), right));
            }
            Ok::<_, datafusion_common::DataFusionError>((handled, on))
        })();
        planner_context.identifier_input = previous_input;
        planner_context.identifier_projection = projection;
        let (handled, on) = result?;
        if !handled {
            return LogicalPlanBuilder::from(left)
                .join_using(right, join_type, keys)?
                .build();
        }
        let mut join = datafusion_expr::Join::try_new(
            Arc::new(left),
            Arc::new(right),
            on,
            None,
            join_type,
            datafusion_expr::JoinConstraint::Using,
            datafusion_common::NullEquality::NullEqualsNothing,
            false,
        )?;
        if join.on.iter().all(|(left, right)| {
            matches!(left.clone().unalias(), Expr::Column(_))
                && matches!(right.clone().unalias(), Expr::Column(_))
        }) {
            join.on = join
                .on
                .into_iter()
                .map(|(left, right)| (left.unalias(), right.unalias()))
                .collect();
            return Ok(LogicalPlan::Join(join));
        }
        let bindings =
            column_bindings(&LogicalPlan::Join(join.clone()), planner_context)?;
        // The logical USING representation requires column keys. Lower selected
        // merged expressions to ON, retaining SQL visibility only while binding.
        join.join_constraint = datafusion_expr::JoinConstraint::On;
        join.on = join
            .on
            .into_iter()
            .map(|(left, right)| (left.unalias(), right.unalias()))
            .collect();
        planner_context
            .join_bindings
            .push((Arc::clone(&join.schema), bindings));
        Ok(LogicalPlan::Join(join))
    }

    fn parse_join(
        &self,
        left: LogicalPlan,
        right: LogicalPlan,
        constraint: JoinConstraint,
        join_type: JoinType,
        planner_context: &mut PlannerContext,
    ) -> Result<LogicalPlan> {
        match constraint {
            JoinConstraint::On(sql_expr) => {
                let join_schema = left.schema().join(right.schema())?;
                // parse ON expression
                self.warn_on_null_equality_predicate(&sql_expr);
                let window_span = self.window_function_span(&sql_expr, &HashMap::new());
                let input = LogicalPlanBuilder::from(left.clone())
                    .cross_join(right.clone())?
                    .build()?;
                let previous_input =
                    planner_context.identifier_input.replace(Arc::new(input));
                let result = self.sql_to_expr(sql_expr, &join_schema, planner_context);
                planner_context.identifier_input = previous_input;
                let expr = result?;
                // A join condition is evaluated before window functions are
                // computed, so they may not appear in it
                reject_window_functions(&expr, "JOIN ON", JOIN_ON_HELP, window_span)?;
                LogicalPlanBuilder::from(left)
                    .join_on(right, join_type, Some(expr))?
                    .build()
            }
            JoinConstraint::Using(object_names) => {
                let keys = object_names
                    .into_iter()
                    .map(|object_name| {
                        let ObjectName(mut object_names) = object_name;
                        if object_names.len() != 1 {
                            not_impl_err!(
                                "Invalid identifier in USING clause. Expected single identifier, got {}", ObjectName(object_names)
                            )
                        } else {
                            let id = object_names.swap_remove(0);
                            id.as_ident()
                                .ok_or_else(|| {
                                    plan_datafusion_err!(
                                        "Expected identifier in USING clause"
                                    )
                                })
                                .map(|ident| Column::from_name(self.ident_normalizer.normalize(ident.clone())))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                self.plan_using_bindings(left, right, join_type, keys, planner_context)
            }
            JoinConstraint::Natural => {
                let left_cols: HashSet<&String> =
                    left.schema().fields().iter().map(|f| f.name()).collect();
                let keys: Vec<Column> = right
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .filter(|f| left_cols.contains(f))
                    .map(Column::from_name)
                    .collect();
                if keys.is_empty() {
                    self.parse_cross_join(left, right)
                } else {
                    self.plan_using_bindings(
                        left,
                        right,
                        join_type,
                        keys,
                        planner_context,
                    )
                }
            }
            JoinConstraint::None => LogicalPlanBuilder::from(left)
                .join_on(right, join_type, [])?
                .build(),
        }
    }
}

/// Returns `true` if the given [`TableFactor`] is lateral.
pub(crate) fn is_lateral(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Derived { lateral, .. } => *lateral,
        TableFactor::Function { lateral, .. } => *lateral,
        TableFactor::UNNEST { .. } => true,
        _ => false,
    }
}

/// Returns `true` if the given [`Join`] is lateral.
pub(crate) fn is_lateral_join(join: &Join) -> Result<bool> {
    let is_lateral_syntax = is_lateral(&join.relation);
    let is_apply_syntax = match join.join_operator {
        JoinOperator::FullOuter(..)
        | JoinOperator::Right(..)
        | JoinOperator::RightOuter(..)
        | JoinOperator::RightAnti(..)
        | JoinOperator::RightSemi(..)
            if is_lateral_syntax =>
        {
            return not_impl_err!(
                "LATERAL syntax is not supported for \
                 FULL OUTER and RIGHT [OUTER | ANTI | SEMI] joins"
            );
        }
        JoinOperator::CrossApply | JoinOperator::OuterApply => true,
        _ => false,
    };
    Ok(is_lateral_syntax || is_apply_syntax)
}
