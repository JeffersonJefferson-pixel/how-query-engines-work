package org.example.kquery.queryplanner

import org.example.kquery.datatypes.KQuerySchema
import org.example.kquery.logicalplan.*
import org.example.kquery.physicalplan.*
import org.example.kquery.physicalplan.expressions.*
import java.sql.SQLException

class QueryPlanner {
    /**
     * Translate logical plan tree to physical plan.
     */
    fun createPhysicalPlan(plan: LogicalPlan): PhysicalPlan {
        return when (plan) {
            is Scan -> ScanExec(plan.dataSource, plan.projection)
            is Projection -> {
                // create physical plan for projection's input.
                val input = createPhysicalPlan(plan.input)
                // convert projection's logical expressions to physical expressions.
                val projectionExpr = plan.expr.map { createPhysicalPlan(it, plan.input) }
                val projectionSchema = KQuerySchema(plan.expr.map { it.toField(plan.input) } )
                ProjectionExec(input, projectionSchema, projectionExpr)
            }
            is Selection -> {
                val input = createPhysicalPlan(plan.input)
                val filterExpr = createPhysicalPlan(plan.expr, plan.input)
                SelectionExec(input, filterExpr)
            }
            is Aggregate -> {
                // create physical plan for aggregate input.
                val input = createPhysicalPlan(plan.input)
                // create physical plan for grouping expression and aggregate expressions.
                val groupExpr = plan.groupExpr.map { createPhysicalPlan(it, plan.input) }
                val aggregateExpr = plan.aggregateExpr.map {
                    val physicalAggregateExpr = createPhysicalPlan(it.expr, plan.input)
                    when (it) {
                        is Max -> MaxExpression(physicalAggregateExpr)
                        is Min -> MinExpression(physicalAggregateExpr)
                        is Sum -> SumExpression(physicalAggregateExpr)
                        else -> throw IllegalStateException("Unsupported aggregate function: $it")
                    }
                }
                HashAggregateExec(input, groupExpr, aggregateExpr, plan.schema())
            }
            else -> throw IllegalStateException("Unsupported logical plan: $plan")
        }
    }

    /**
     * Translate logical expressions to physical expressions recursively.
     */
    fun createPhysicalPlan(expr: LogicalExpr, input: LogicalPlan): Expression =
        when (expr) {
            // literal expression only needs to copy the literal value.
            is LiteralLong -> LiteralLongExpression(expr.n)
            is LiteralDouble -> LiteralDoubleExpression(expr.n)
            is LiteralString -> LiteralStringExpression(expr.str)
            is Column -> {
                // logical column expression  references columns by name, but physical expression uses column indices.
                // query planner needs to translate from column names to column indices.
                val i = input.schema().fields.indexOfFirst { it.name == expr.name }
                if (i == -1) {
                    throw SQLException("No column named '${expr.name}'")
                }
                ColumnExpression(i)
            }
            is BinaryExpr -> {
                // create physical plan for left and right expression.
                val l = createPhysicalPlan(expr.l, input)
                val r = createPhysicalPlan(expr.r, input)
                when (expr) {
                    // comparison
                    is Eq -> EqExpression(l, r)
                    is Neq -> NeqExpression(l, r)
                    is Gt -> GtExpression(l, r)
                    is GtEq -> GtEqExpression(l, r)
                    is Lt -> LtExpression(l, r)
                    is LtEq -> LtEqExpression(l, r)
                    // boolean
                    is And -> AndExpression(l, r)
                    is Or -> OrExpression(l, r)
                    // math
                    is Add -> AddExpression(l, r)
                    is Subtract -> SubtractExpression(l, r)
                    is Multiply -> MultiplyExpression(l, r)
                    is Divide -> DivideExpression(l, r)
                    is Modulus -> ModulusExpression(l, r)

                    else -> throw IllegalStateException("Unsupported binary expression: $expr")
                }
            }
            else -> throw IllegalStateException("Unsupported logical expression: $expr")
        }
}