<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Proposal: SQL binding policy over engine-owned bindings

Status: design proposal, not implemented or endorsed upstream. Reconsiders the
contracts in fork PRs #3 and #5–#8. The existing branches remain runnable prototypes.
Research checked November 16, 2026; implementation baseline is DataFusion
`e2dc8b048b542e7abdc296254aa54e26eb89d76d` (57.0.0-dev).

The durable boundary should be **which SQL binding a name denotes**, with
DataFusion responsible for turning that binding into a valid plan. Returning a
replacement expression or mutating SELECT at particular points in today's
planner makes the extension responsible for too much of that mechanism.

Keep specific, typed callbacks. Introduce one query-independent `SqlBindingPolicy`
object per session, defaulting to existing behavior; store all mutable binding
state in the engine's per-query context. Keep `ExprPlanner` for expression lowering
and `RelationPlanner` for custom table factors. The single policy object makes
identifier, CTE and wildcard decisions coherent; it does not replace those
existing ordered extension registries. A dialect package can explicitly compose
policy implementations if needed.

## What earlier reviews actually established

| Discussion                                                                                                                                                                                      | Outcome and implication                                                                                                                                                                                                                                                                                                                  |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Original hook design #11168](https://github.com/apache/datafusion/pull/11168#issuecomment-2198106261)                                                                                          | A generic AST callback was considered. Andrew Lamb preferred specific methods for supported SQL constructs because they were easier to understand and use. Keep typed hooks; do not invent a universal planner event interface.                                                                                                          |
| [Operator lowering #11168](https://github.com/apache/datafusion/pull/11168#discussion_r1659097446)                                                                                              | The motivation was to lower syntax directly through extension implementations, avoiding placeholder operators and hardcoded function names. Keep binding references internal until they lower to ordinary expressions/plans; do not introduce public expression variants just to rewrite them away later.                                |
| [First implementation #11180](https://github.com/apache/datafusion/pull/11180#issuecomment-2202679775) and [registration follow-up #11208](https://github.com/apache/datafusion/pull/11208)     | The downstream JSON integration discovered there was no external registration path after the first PR merged. Validate through a real downstream crate and normal session entry points before freezing the API.                                                                                                                          |
| [Composition #11180](https://github.com/apache/datafusion/pull/11180#issuecomment-2213584028), [RelationPlanner #17843](https://github.com/apache/datafusion/pull/17843#discussion_r2533035450) | Ordered first-handler dispatch was explicit; later RelationPlanner review clarified overrides and requested meaningful execution examples. Our proposed single semantic policy is a deliberate difference, intended to avoid two dialects accidentally splitting one query's binding rules.                                              |
| [CTE context #24755](https://github.com/apache/datafusion/pull/24755#pullrequestreview-5068509896)                                                                                              | A downstream consumer confirmed that exposing existing lexical scope removed its workaround. Review also [asked for inherited-scope coverage](https://github.com/apache/datafusion/pull/24755#discussion_r3911181050) while preserving existing duplicate-name rejection. Exposing scope and changing scope policy are separate changes. |
| [Scalar context #26177](https://github.com/apache/datafusion/pull/26177#pullrequestreview-5533658385)                                                                                           | `plan_scalar` needed a schema parameter after its initial merge. Review requested a schema-using example and migration documentation; the [author noted it was still unreleased](https://github.com/apache/datafusion/pull/26177#discussion_r4290023866). This is pre-release contract refinement, not evidence of a released-API break. |
| [Timezone planning #25165](https://github.com/apache/datafusion/pull/25165#issuecomment-5927441190)                                                                                             | Early type inspection selected the wrong behavior before coercion; the revised design moved the decision into UDF return-field/execution logic. Binding hooks must not promise final coerced types.                                                                                                                                      |
| [USING joins #22998](https://github.com/apache/datafusion/pull/22998#pullrequestreview-5620569989)                                                                                              | Review preferred constructing the correct join output over globally repairing unresolved names. The [follow-up proposal](https://github.com/apache/datafusion/pull/22998#issuecomment-6368869277) explicitly raises raw-schema, wildcard-order and serialization questions; it is not an accepted binding-metadata design.               |
| [Lateral aliases #17361](https://github.com/apache/datafusion/issues/17361#issuecomment-3271503236)                                                                                             | Interest from multiple users mattered to prioritization. The associated #17362 closed unmerged, with no substantive architectural rejection in its PR discussion. Demonstrate real consumers; do not infer upstream approval from adjacent hooks.                                                                                        |

The general direction below is our inference from these discussions. None of the
reviewers has reviewed this proposal.

## Public contract

Illustrative signatures, not a compilable API specification:

```rust,ignore
trait SqlBindingPolicy: Debug + Send + Sync {
    fn resolve_identifier(&self, request: &IdentifierLookup<'_>)
        -> Result<Decision<BindingId>>;
    fn check_cte_declaration(&self, request: &CteDeclaration<'_>)
        -> Result<Decision<()>>;
    fn resolve_cte(&self, request: &CteLookup<'_>)
        -> Result<Decision<CteId>>;
    fn join_output(&self, request: &JoinOutputRequest<'_>)
        -> Result<Decision<JoinOutputSpec>>;
    fn expand_wildcard(&self, request: &WildcardRequest<'_>)
        -> Result<Decision<WildcardSelection>>;
    fn column_aliases(&self, request: &ColumnAliasRequest<'_>)
        -> Result<Decision<ColumnNames>>;
}
```

Every method has a default `Decision::Default` implementation; `Use(value)` opts
into a decision, and `Err` rejects it. Default means the existing engine policy,
not an empty candidate list or successful resolution. Engine validation still
applies to handled results. There is no public `BeforeWhere`/`BeforeProjection`
callback and no mutable SELECT AST in this policy interface.

Request structs have private fields and read-only accessors. They expose source
identifiers including quoting, normalized lookup keys, clause/use location,
scope relationships and declaration position where relevant. Builders construct
validated output specifications. IDs are opaque handles valid only within their
own planning invocation; the engine rejects foreign/stale handles. Adding context
accessors or default methods should not require changing existing signatures.
Extensible public enums are non-exhaustive. This reduces source churn; it does
not make future semantic changes automatically compatible.

### Identifier lookup and expression placement: replace #3/#8's core contract

The engine supplies binding candidates with identity, origin (input column,
SELECT alias, outer reference), declaration position and scope. The policy chooses
visibility and precedence using those facts. Multiple equally preferred candidates
remain ambiguous unless the dialect explicitly specifies a tie-breaking rule.
Source spelling and final output labels are not identities.

Choosing a SELECT alias returns its identity, not its defining SQL expression.
DataFusion binds that definition once in its definition scope, records dependency
edges, detects cycles and resolves references to the same value. Distinct alias
definitions containing the same volatile expression remain distinct computations.
Declaration identity must survive wildcard expansion, so a mutable SELECT index
cannot serve as the identity.

Internally, a bound SELECT representation records alias dependencies and the
semantic row domain in which each expression can be evaluated: relation input,
join input/output, grouped rows or window output. The engine derives legal
placement from dependencies and use sites. It inserts projections and rebases
references while preserving grouping, filtering, outer-join null extension,
DISTINCT and LIMIT semantics. It must not simply move every computation to its
earliest available input: that can change volatile evaluation and error behavior.

For example, a scalar alias reused by WHERE and SELECT can be computed below the
filter; an alias of `SUM(x)` reused by HAVING is produced after aggregation. A
window alias cannot be made available to WHERE merely by selecting it: reject an
illegal use unless a separately supported dialect rewrite defines that behavior.
JOIN ON reuse needs dependencies on the relevant join inputs and must respect
outer-join semantics. Unproven cases receive explicit errors, not expression
substitution as a fallback.

Value reuse must survive logical optimization and physical filter pushdown. The
[volatile-alias fix #26850](https://github.com/apache/datafusion/pull/26850#pullrequestreview-5708180427)
needed both projection placement and a physical guard. This is a query-semantics
contract, not an exactly-once side-effect guarantee across execution retries.
Final type-sensitive behavior remains in existing coercion/UDF interfaces.

Spark's [lateral-alias analyzer](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ResolveLateralColumnAliasReference.scala)
similarly separates recognition of alias references from projection/aggregate
rewriting. It is evidence for separating the two responsibilities, not a claim
that its alias rules match Snowflake's.

### CTE declaration and lookup: replace #6

Represent CTE declarations and lexical scopes before planning their bodies;
retain declaration identity rather than overwriting a flat name-to-plan map.

Two decisions are necessary. `check_cte_declaration` decides whether a nested
same-name declaration is permitted before any lookup; otherwise unused conflicting
definitions could silently stop producing the current error. `resolve_cte` chooses
among legal candidates at a reference site. Context includes declaration/use
scope, sibling position, query/CTE-body location and recursive status. Returning
an ID selects an engine-owned declaration; the extension cannot manufacture a plan
or change the enclosing query's scope.

The engine continues to enforce same-WITH duplicate rejection, dependency and
recursion rules, definition-site binding and current validation of unused bodies.
Default normalization and qualified-name lookup must agree with existing
`RelationPlannerContext::get_cte`. Relations intercepted by existing custom
planners retain their documented interception precedence.

This supports scoped shadowing and explicit outer/inner preference without tying
the API to a duplicate-insertion error. It does not claim full Spark LEGACY mode:
[Spark's implementation](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/CTESubstitution.scala)
also changes substitution traversal and inlining, rejects recursive CTEs in legacy
mode, and makes EXCEPTION behavior depend on query location. Those are separate
mechanisms that need their own demonstrated use cases.

### Join output and wildcard selection: narrow #7

Give each planned relation an internal binding catalog alongside its plan.
Record resolved left/right key identities, merged-key identity and expression,
qualified visibility, unqualified visibility and SQL output order. Do not infer
these relationships from a private qualifier or an unqualified string.

The engine constructs and validates USING/NATURAL equality and merged values.
`join_output` chooses visibility and output layout through `JoinOutputSpec`;
it does not replace the whole join algorithm. Later joins bind against the catalog
produced by earlier joins, so an unrelated `k` from a CROSS JOIN stays ambiguous.
General correctness fixes belong in shared join construction, independently of
dialect-specific SQL layout policy and independently of this extension proposal.

`expand_wildcard` chooses an ordered list of binding IDs from that same catalog.
Normal EXCLUDE/RENAME/REPLACE handling remains engine-owned; extension-only modifier
syntax needs a separately specified syntax hook. Qualified stars can select source
keys while an unqualified star selects a merged key. Internal computed slots are
hidden by binding visibility, not by filtering columns whose names look generated.

Initially keep the catalog private to SQL planning and lower to normal plans.
This avoids committing every optimizer and codec to a new public binding IR.
However, it is not enough to discard provenance and assume unparsing will work:
the prototype must establish how optimized-plan unparsing and duplicate output
labels retain the required output mapping. Compare legal generated SQL aliases
with a query output descriptor; keep this representation private until validated.
If durable plan metadata proves necessary, its propagation, equality and same-
version serialization are a separate prerequisite. Do not change raw DataFrame
join output merely to carry hidden SQL bindings.

### Column aliases: strengthen #5's context

Use a request carrying alias origin (table, derived relation, CTE or recursive
seed), original identifiers, normalized names and input fields. The policy returns
names for existing output slots; it cannot change their identities or physical
types. Engine cardinality and internal-uniqueness checks remain. Partial aliases
are one consumer; quoting and relation-specific rules no longer require another
method signature. Duplicate client-visible labels remain a separate output-mapping
problem until the preceding prototype establishes a sound representation.

## Delivery and proof

1. Keep the QUALIFY correctness fix and numeric-literal hook independent. Keep
   #3/#5–#8 as prototypes until the replacement contracts are proved.
2. Add private scope/binding identities and characterization tests with no policy
   overrides. Preserve current default behavior except separately reviewed bugs.
3. Prove merged-key identity and wildcard layout through chained joins, aliases,
   optimized/unoptimized execution, unparsing and same-version plan round trips.
4. Implement alias dependency resolution and value placement over those identities.
   Demonstrate scalar, aggregate and window reuse, rejection of illegal uses, and
   optimizer/physical-plan preservation before exposing a placement API.
5. Wire the real df2rs adapter through normal SessionContext construction and a
   custom SessionState. Add an independent consumer using Spark lateral-alias/CTE
   requirements or PostgreSQL join/alias rules. Neither counts as full dialect
   compatibility; compare explicitly documented/recorded cases.
6. Expose the typed policy methods once those consumers work. Separate behavioral
   defaults from registration and provide upgrade notes for any public changes.

Minimum interaction matrix:

| Area            | Required cases                                                                                                                                                                                 |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Name resolution | Input/alias collision in each clause; earlier/later aliases; duplicate aliases; cycles; quoted names; correlated scopes.                                                                       |
| Evaluation      | Instrumented volatile reuse and separate definitions; aggregate-to-window aliases; HAVING/QUALIFY; short-circuit/error expressions; DISTINCT/LIMIT; outer joins.                               |
| Join bindings   | LEFT/RIGHT/FULL USING and NATURAL; chained joins; unrelated same-name columns; `*` vs `t.*`; modifiers; relation aliases; reversed alphabetical table names.                                   |
| CTE scope       | Nested query/CTE-body/scalar-subquery scopes; inherited names; unused conflicts; earlier siblings; recursive seed/step; qualified and quoted dotted names.                                     |
| Integration     | Default/no-policy behavior; custom SessionState; concurrent queries; existing ExprPlanner/RelationPlanner precedence; schema labels/order/nullability; SQL round trips and plan serialization. |

The implementation is larger than a handful of dispatch hooks: most work is in
binding identities, dependency resolution and preserving semantics through plan
rewrites. That investment belongs in reusable engine machinery if upstream wants
the capability. The compatibility package would keep dialect policy, UDFs,
coercion and any unproven output-label adapter. Do not promise a smaller total
maintenance burden or new line-count estimate until the real adapter is ported.

This proposal reduces foreseeable redesign by stabilizing semantic decisions and
keeping representation private. It cannot guarantee upstream acceptance or cover
every future dialect rule. Each engine change should still be justified by a
concrete consumer and reviewed independently.
