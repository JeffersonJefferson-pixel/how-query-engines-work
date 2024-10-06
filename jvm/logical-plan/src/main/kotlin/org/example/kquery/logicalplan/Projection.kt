package org.example.kquery.logicalplan

import org.example.kquery.datatypes.Schema

/**
 * Projection logical plan applies a projection to its input.
 * It is a list of expressions to be evaluated against the input data.
 */
class Projection(
    val input: LogicalPlan,
    val expr: List<LogicalExpr>
): LogicalPlan {
    override fun schema(): Schema {
        return Schema(expr.map { it.toField(input) })
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Projection: ${ expr.map { it.toString() }.joinToString(", ") }"
    }
}