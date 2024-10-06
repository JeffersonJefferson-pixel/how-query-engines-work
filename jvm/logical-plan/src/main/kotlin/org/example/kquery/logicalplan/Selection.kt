package org.example.kquery.logicalplan

import org.example.kquery.datatypes.Schema

/**
 * Selection logical plan applies a filter expression to determin
 * Filter expression needs to evaluate to Boolean.
 */
class Selection(
    val input: LogicalPlan,
    val expr: LogicalExpr
): LogicalPlan {
    override fun schema(): Schema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Selection: $expr"
    }
}