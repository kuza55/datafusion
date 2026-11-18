// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`ContextProvider`] and [`ExprPlanner`] APIs to customize SQL query planning

use std::fmt::Debug;
use std::sync::Arc;

use crate::expr::NullTreatment;
#[cfg(feature = "sql")]
use crate::logical_plan::LogicalPlan;
use crate::{
    AggregateUDF, Expr, GetFieldAccess, HigherOrderUDF, ScalarUDF, SortExpr, TableSource,
    WindowFrame, WindowFunctionDefinition, WindowUDF,
};
use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datafusion_common::datatype::DataTypeExt;
use datafusion_common::{
    DFSchema, Result, TableReference, config::ConfigOptions,
    file_options::file_type::FileType, not_impl_err,
};
#[cfg(feature = "sql")]
use sqlparser::ast::{Expr as SQLExpr, Ident, ObjectName, TableAlias, TableFactor};

/// Provides the `SQL` query planner meta-data about tables and
/// functions referenced in SQL statements, without a direct dependency on the
/// `datafusion` Catalog structures such as [`TableProvider`]
///
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html
pub trait ContextProvider {
    /// Returns a table by reference, if it exists
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;

    /// Return the type of a file based on its extension (e.g. `.parquet`)
    ///
    /// This is used to plan `COPY` statements
    fn get_file_type(&self, _ext: &str) -> Result<Arc<dyn FileType>> {
        not_impl_err!("Registered file types are not supported")
    }

    /// Getter for a table function
    fn get_table_function_source(
        &self,
        _name: &str,
        _args: Vec<Expr>,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("Table Functions are not supported")
    }

    /// Provides an intermediate table that is used to expose a recursive CTE
    /// self-reference during planning and execution.
    ///
    /// CTE stands for "Common Table Expression"
    ///
    /// # Notes
    /// We don't directly implement this in [`SqlToRel`] as implementing this function
    /// often requires access to a table that contains
    /// execution-related types that can't be a direct dependency
    /// of the sql crate (for example [`CteWorkTable`]).
    ///
    /// The [`ContextProvider`] provides a way to "hide" this dependency.
    /// The schema argument is the schema to expose for scans of the recursive
    /// self-reference, which may be more conservative than the final recursive
    /// query output schema.
    ///
    /// [`SqlToRel`]: https://docs.rs/datafusion/latest/datafusion/sql/planner/struct.SqlToRel.html
    /// [`CteWorkTable`]: https://docs.rs/datafusion/latest/datafusion/datasource/cte_worktable/struct.CteWorkTable.html
    fn create_cte_work_table(
        &self,
        _name: &str,
        _schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        not_impl_err!("Recursive CTE is not implemented")
    }

    /// Return [`ExprPlanner`] extensions for planning expressions
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &[]
    }

    /// Return [`RelationPlanner`] extensions for planning table factors
    #[cfg(feature = "sql")]
    fn get_relation_planners(&self) -> &[Arc<dyn RelationPlanner>] {
        &[]
    }

    /// Return [`TypePlanner`] extensions for planning data types
    #[cfg(feature = "sql")]
    fn get_type_planner(&self) -> Option<Arc<dyn TypePlanner>> {
        None
    }

    /// Return the [`SqlBindingPolicy`] that decides how SQL names bind
    #[cfg(feature = "sql")]
    fn get_binding_policy(&self) -> Option<&dyn SqlBindingPolicy> {
        None
    }

    /// Return the scalar function with a given name, if any
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;

    /// Return the higher order function with a given name, if any
    fn get_higher_order_meta(&self, name: &str) -> Option<Arc<HigherOrderUDF>>;

    /// Return the aggregate function with a given name, if any
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;

    /// Return the window function with a given name, if any
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>>;

    /// Return the system/user-defined variable type, if any
    ///
    /// A user defined variable is typically accessed via `@var_name`
    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType>;

    /// Return metadata about a system/user-defined variable, if any.
    ///
    /// By default, this wraps [`Self::get_variable_type`] in an Arrow [`Field`]
    /// with nullable set to `true` and no metadata. Implementations that can
    /// provide richer information (such as nullability or extension metadata)
    /// should override this method.
    fn get_variable_field(&self, variable_names: &[String]) -> Option<FieldRef> {
        self.get_variable_type(variable_names)
            .map(|data_type| data_type.into_nullable_field_ref())
    }

    /// Return overall configuration options
    fn options(&self) -> &ConfigOptions;

    /// Return all scalar function names
    fn udf_names(&self) -> Vec<String>;

    /// Return all higher order function names
    fn higher_order_function_names(&self) -> Vec<String>;

    /// Return all aggregate function names
    fn udaf_names(&self) -> Vec<String>;

    /// Return all window function names
    fn udwf_names(&self) -> Vec<String>;
}

/// Customize planning of SQL AST expressions to [`Expr`]s
///
/// For more background, please also see the [Extending SQL in DataFusion: from ->> to TABLESAMPLE blog]
///
/// [Extending SQL in DataFusion: from ->> to TABLESAMPLE blog]: https://datafusion.apache.org/blog/2026/01/12/extending-sql
pub trait ExprPlanner: Debug + Send + Sync {
    /// Plan the binary operation between two expressions, returns original
    /// BinaryExpr if not possible
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan the field access expression, such as `foo.bar`
    ///
    /// returns original [`RawFieldAccessExpr`] if not possible
    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan an array literal, such as `[1, 2, 3]`
    ///
    /// Returns original expression arguments if not possible
    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(exprs))
    }

    /// Plan a `POSITION` expression, such as `POSITION(<expr> in <expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_position(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan a dictionary literal, such as `{ key: value, ...}`
    ///
    /// Returns original expression arguments if not possible
    fn plan_dictionary_literal(
        &self,
        expr: RawDictionaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawDictionaryExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plan an extract expression, such as`EXTRACT(month FROM foo)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_extract(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan an substring expression, such as `SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])`
    ///
    /// Returns original expression arguments if not possible
    fn plan_substring(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plan `<expr> AT TIME ZONE '<tz>'`.
    ///
    /// `args` is `[Utf8 literal tz, expr]`, the argument order of the
    /// PostgreSQL function form `timezone(zone, expr)`.
    ///
    /// The result type depends on whether the type of `expr` *after type
    /// coercion* has a timezone, and the SQL planner runs before type
    /// coercion. So an implementation must return an expression that decides
    /// later, such as the `timezone` function of `datafusion-functions`.
    ///
    /// Returns original expression arguments if not possible
    fn plan_at_time_zone(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans a struct literal, such as  `{'field1' : expr1, 'field2' : expr2, ...}`
    ///
    /// This function takes a vector of expressions and a boolean flag
    /// indicating whether the struct uses the optional name
    ///
    /// Returns the original input expressions if planning is not possible.
    fn plan_struct_literal(
        &self,
        args: Vec<Expr>,
        _is_named_struct: bool,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans an overlay expression, such as `overlay(str PLACING substr FROM pos [FOR count])`
    ///
    /// Returns original expression arguments if not possible
    fn plan_overlay(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans a `make_map` expression, such as `make_map(key1, value1, key2, value2, ...)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_make_map(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Original(args))
    }

    /// Plans compound identifier such as `db.schema.table` for non-empty nested names
    ///
    /// # Note:
    /// Currently compound identifier for outer query schema is not supported.
    ///
    /// Returns original expression if not possible
    fn plan_compound_identifier(
        &self,
        _field: &Field,
        _qualifier: Option<&TableReference>,
        _nested_names: &[String],
    ) -> Result<PlannerResult<Vec<Expr>>> {
        not_impl_err!(
            "Default planner compound identifier hasn't been implemented for ExprPlanner"
        )
    }

    /// Plans scalar functions, such as `ABS(<expr>)`
    ///
    /// Returns the original scalar function if not possible
    fn plan_scalar(
        &self,
        expr: RawScalarExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawScalarExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plans aggregate functions, such as `COUNT(<expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_aggregate(
        &self,
        expr: RawAggregateExpr,
    ) -> Result<PlannerResult<RawAggregateExpr>> {
        Ok(PlannerResult::Original(expr))
    }

    /// Plans window functions, such as `COUNT(<expr>)`
    ///
    /// Returns original expression arguments if not possible
    fn plan_window(&self, expr: RawWindowExpr) -> Result<PlannerResult<RawWindowExpr>> {
        Ok(PlannerResult::Original(expr))
    }
}

/// A SQL dialect's rules for binding names.
///
/// DataFusion finds the definitions a name could refer to and the policy picks
/// one. A policy never builds expressions or plans, so scoping, alias expansion
/// and correlation stay with the SQL planner.
///
/// Name binding is one decision per session rather than a chain of handlers,
/// so a single policy is registered through
/// [`ContextProvider::get_binding_policy`], like [`TypePlanner`]. Without a
/// policy the SQL planner does no extra work.
#[cfg(feature = "sql")]
pub trait SqlBindingPolicy: Debug + Send + Sync {
    /// Choose the definition a column reference binds to.
    ///
    /// Return one of [`ColumnLookup::candidates`], or `None` to keep
    /// DataFusion's own lookup, including its ambiguity checks. An error
    /// rejects the reference. Covers bare and qualified names; nested field
    /// access keeps its existing planning.
    fn resolve_column<'a>(
        &self,
        _lookup: &ColumnLookup<'a>,
    ) -> Result<Option<&'a ColumnBinding>> {
        Ok(None)
    }

    /// Which definition a name refers to when a nested `WITH` reuses the name
    /// of a CTE from an enclosing query. Both definitions are planned and
    /// validated either way.
    fn nested_cte_precedence(&self) -> CtePrecedence {
        CtePrecedence::Inner
    }

    /// Whether a column alias list may be shorter than the relation it
    /// renames, leaving the remaining columns' names unchanged.
    fn allows_partial_column_aliases(&self, _site: ColumnAliasSite) -> bool {
        true
    }
}

/// Precedence between CTEs of the same name in nested queries.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CtePrecedence {
    /// The nested definition shadows the enclosing one, as in standard SQL.
    Inner,
    /// References keep binding to the enclosing definition.
    Outer,
}

/// Where a column alias list appears.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ColumnAliasSite {
    /// A table alias in FROM, such as `t(a, b)`.
    Table,
    /// A CTE definition, such as `WITH t(a, b) AS (...)`.
    Cte,
    /// A view definition, such as `CREATE VIEW v(a, b) AS ...`.
    View,
}

/// SQL clause containing the identifier being planned.
///
/// Window arguments inherit their containing clause. `From` includes join
/// predicates and table-function arguments; nested queries have their own scope.
#[cfg(feature = "sql")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum IdentifierClause {
    /// A projection item, indexed before wildcard expansion.
    Select {
        /// Zero-based position in the unexpanded SELECT list.
        index: usize,
    },
    /// FROM relations, join predicates and table-function arguments.
    From,
    /// WHERE predicate.
    Where,
    /// GROUP BY expression.
    GroupBy,
    /// HAVING predicate.
    Having,
    /// QUALIFY predicate.
    Qualify,
    /// ORDER BY expression.
    OrderBy,
    /// An expression outside the clauses above.
    Other,
}

/// The query whose scope defines a [`ColumnBinding`].
#[cfg(feature = "sql")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BindingScope {
    /// The query containing the reference.
    Current,
    /// An enclosing query; depth zero is the nearest.
    Outer {
        /// Number of queries between the reference's query and this one.
        depth: usize,
    },
}

/// What a [`ColumnBinding`] refers to within its [`BindingScope`].
#[cfg(feature = "sql")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BindingOrigin {
    /// A column of the query's input relations.
    Input,
    /// An explicitly named SELECT expression.
    SelectAlias {
        /// Zero-based position in the unexpanded SELECT list.
        index: usize,
    },
}

/// A definition that a column reference could bind to.
#[cfg(feature = "sql")]
#[derive(Debug)]
pub struct ColumnBinding {
    column: datafusion_common::Column,
    scope: BindingScope,
    origin: BindingOrigin,
}

#[cfg(feature = "sql")]
impl ColumnBinding {
    /// Normalized name and optional relation qualifier.
    pub fn column(&self) -> &datafusion_common::Column {
        &self.column
    }
    /// The query that defines this binding.
    pub fn scope(&self) -> BindingScope {
        self.scope
    }
    /// What this binding refers to in that query.
    pub fn origin(&self) -> BindingOrigin {
        self.origin
    }

    /// Construct binding metadata for SQL planner dispatch.
    ///
    /// # Public Only for Internal Use:
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn new(
        column: datafusion_common::Column,
        scope: BindingScope,
        origin: BindingOrigin,
    ) -> Self {
        Self {
            column,
            scope,
            origin,
        }
    }
}

/// A column reference and the definitions it could bind to.
///
/// Binding to a SELECT alias plans the alias's expression in the scope that
/// defines it; it does not guarantee the expression is evaluated once.
#[cfg(feature = "sql")]
#[derive(Debug)]
pub struct ColumnLookup<'a> {
    identifiers: &'a [Ident],
    clause: IdentifierClause,
    candidates: &'a [ColumnBinding],
}

#[cfg(feature = "sql")]
impl<'a> ColumnLookup<'a> {
    /// Original identifier parts, including quoting and source spans.
    pub fn identifiers(&self) -> &'a [Ident] {
        self.identifiers
    }
    /// Clause containing this reference.
    pub fn clause(&self) -> IdentifierClause {
        self.clause
    }
    /// Definitions matching the name. Precedence and ambiguity are the
    /// policy's to decide.
    pub fn candidates(&self) -> &'a [ColumnBinding] {
        self.candidates
    }

    /// Construct a lookup for SQL planner dispatch.
    ///
    /// # Public Only for Internal Use:
    /// This is not a public API and is for internal use only; see [API policy] for details.
    ///
    /// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
    #[doc(hidden)]
    pub fn new(
        identifiers: &'a [Ident],
        clause: IdentifierClause,
        candidates: &'a [ColumnBinding],
    ) -> Self {
        Self {
            identifiers,
            clause,
            candidates,
        }
    }
}

/// An operator with two arguments to plan
///
/// Note `left` and `right` are DataFusion [`Expr`]s but the `op` is the SQL AST
/// operator.
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawBinaryExpr {
    #[cfg(not(feature = "sql"))]
    pub op: datafusion_expr_common::operator::Operator,
    #[cfg(feature = "sql")]
    pub op: sqlparser::ast::BinaryOperator,
    pub left: Expr,
    pub right: Expr,
}

/// An expression with GetFieldAccess to plan
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawFieldAccessExpr {
    pub field_access: GetFieldAccess,
    pub expr: Expr,
}

/// A Dictionary literal expression `{ key: value, ...}`
///
/// This structure is used by [`ExprPlanner`] to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawDictionaryExpr {
    pub keys: Vec<Expr>,
    pub values: Vec<Expr>,
}

/// A scalar function to plan with [`ExprPlanner`].
#[derive(Debug, Clone)]
pub struct RawScalarExpr {
    pub func: Arc<ScalarUDF>,
    pub args: Vec<Expr>,
}

/// This structure is used by `AggregateFunctionPlanner` to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawAggregateExpr {
    pub func: Arc<AggregateUDF>,
    pub args: Vec<Expr>,
    pub distinct: bool,
    pub filter: Option<Box<Expr>>,
    pub order_by: Vec<SortExpr>,
    pub null_treatment: Option<NullTreatment>,
}

/// This structure is used by `WindowFunctionPlanner` to plan operators with
/// custom expressions.
#[derive(Debug, Clone)]
pub struct RawWindowExpr {
    pub func_def: WindowFunctionDefinition,
    pub args: Vec<Expr>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<SortExpr>,
    pub window_frame: WindowFrame,
    pub filter: Option<Box<Expr>>,
    pub null_treatment: Option<NullTreatment>,
    pub distinct: bool,
}

/// Result of planning a raw expr with [`ExprPlanner`]
#[derive(Debug, Clone)]
pub enum PlannerResult<T> {
    /// The raw expression was successfully planned as a new [`Expr`]
    Planned(Expr),
    /// The raw expression could not be planned, and is returned unmodified
    Original(T),
}

/// Result of planning a relation with [`RelationPlanner`]
#[cfg(feature = "sql")]
#[derive(Debug, Clone)]
pub struct PlannedRelation {
    /// The logical plan for the relation
    pub plan: LogicalPlan,
    /// Optional table alias for the relation
    pub alias: Option<TableAlias>,
}

#[cfg(feature = "sql")]
impl PlannedRelation {
    /// Create a new `PlannedRelation` with the given plan and alias
    pub fn new(plan: LogicalPlan, alias: Option<TableAlias>) -> Self {
        Self { plan, alias }
    }
}

/// Result of attempting to plan a relation with extension planners
#[cfg(feature = "sql")]
#[derive(Debug)]
pub enum RelationPlanning {
    /// The relation was successfully planned by an extension planner
    Planned(Box<PlannedRelation>),
    /// No extension planner handled the relation, return it for default processing
    Original(Box<TableFactor>),
}

/// Customize planning SQL table factors to [`LogicalPlan`]s.
#[cfg(feature = "sql")]
/// For more background, please also see the [Extending SQL in DataFusion: from ->> to TABLESAMPLE blog]
///
/// [Extending SQL in DataFusion: from ->> to TABLESAMPLE blog]: https://datafusion.apache.org/blog/2026/01/12/extending-sql
pub trait RelationPlanner: Debug + Send + Sync {
    /// Plan a table factor into a [`LogicalPlan`].
    ///
    /// Returning [`RelationPlanning::Planned`] short-circuits further planning and uses the
    /// provided plan. Returning [`RelationPlanning::Original`] allows the next registered planner,
    /// or DataFusion's default logic, to handle the relation.
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning>;
}

/// Provides utilities for relation planners to interact with DataFusion's SQL
/// planner.
///
/// This trait provides SQL planning utilities specific to relation planning,
/// such as converting SQL expressions to logical expressions and normalizing
/// identifiers. It uses composition to provide access to session context via
/// [`ContextProvider`].
#[cfg(feature = "sql")]
pub trait RelationPlannerContext {
    /// Provides access to the underlying context provider for reading session
    /// configuration, accessing tables, functions, and other metadata.
    fn context_provider(&self) -> &dyn ContextProvider;

    /// Plans the specified relation through the full planner pipeline, starting
    /// from the first registered relation planner.
    fn plan(&mut self, relation: TableFactor) -> Result<LogicalPlan>;

    /// Returns the common table expression (CTE) visible for the specified
    /// table reference in the current query scope, if any.
    ///
    /// Relation planners run before DataFusion's built-in relation planning. A
    /// planner that recognizes ordinary tables by name can use this method to
    /// pass a same-named relation to the remaining planners. When every
    /// ordinary name-based planner does so, built-in CTE resolution is
    /// preserved. Use [`Self::object_name_to_table_reference`] to normalize an
    /// [`ObjectName`] before looking it up.
    ///
    /// The lookup follows the same string-key matching as DataFusion's built-in
    /// relation planning, including for qualified table references. Different
    /// [`TableReference`] variants with the same string representation therefore
    /// use the same CTE lookup key.
    ///
    /// The default implementation returns `None` for compatibility with custom
    /// implementations of this context.
    fn get_cte(&self, _name: &TableReference) -> Option<&LogicalPlan> {
        None
    }

    /// Converts a SQL expression into a logical expression using the current
    /// planner context.
    fn sql_to_expr(&mut self, expr: SQLExpr, schema: &DFSchema) -> Result<Expr>;

    /// Converts a SQL expression into a logical expression without DataFusion
    /// rewrites.
    fn sql_expr_to_logical_expr(
        &mut self,
        expr: SQLExpr,
        schema: &DFSchema,
    ) -> Result<Expr>;

    /// Normalizes an identifier according to session settings.
    fn normalize_ident(&self, ident: Ident) -> String;

    /// Normalizes a SQL object name into a [`TableReference`].
    fn object_name_to_table_reference(&self, name: ObjectName) -> Result<TableReference>;
}

/// Customize planning SQL types to DataFusion (Arrow) types.
#[cfg(feature = "sql")]
/// For more background, please also see the [Extending SQL in DataFusion: from ->> to TABLESAMPLE blog]
///
/// [Extending SQL in DataFusion: from ->> to TABLESAMPLE blog]: https://datafusion.apache.org/blog/2026/01/12/extending-sql
pub trait TypePlanner: Debug + Send + Sync {
    /// Plan SQL [`sqlparser::ast::DataType`] to DataFusion [`DataType`]
    ///
    /// Returns None if not possible
    #[deprecated(since = "53.0.0", note = "Use plan_type_field()")]
    fn plan_type(
        &self,
        _sql_type: &sqlparser::ast::DataType,
    ) -> Result<Option<DataType>> {
        Ok(None)
    }

    /// Plan SQL [`sqlparser::ast::DataType`] to DataFusion [`FieldRef`]
    ///
    /// Returns None if not possible. Unlike [`Self::plan_type`], `plan_type_field()`
    /// makes it possible to express extension types (e.g., `arrow.uuid`) or otherwise
    /// insert metadata into the DataFusion type representation. The default implementation
    /// falls back on [`Self::plan_type`] for backward compatibility and wraps the result
    /// in a nullable field reference.
    fn plan_type_field(
        &self,
        sql_type: &sqlparser::ast::DataType,
    ) -> Result<Option<FieldRef>> {
        #[expect(deprecated)]
        Ok(self
            .plan_type(sql_type)?
            .map(|data_type| data_type.into_nullable_field_ref()))
    }
}
