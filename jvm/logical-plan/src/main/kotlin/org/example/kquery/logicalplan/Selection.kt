package org.example.kquery.logicalplan

import org.example.kquery.datatypes.KQuerySchema

/**
 * Selection logical plan applies a filter expression to determin
 * Filter expression needs to evaluate to Boolean.
 */
class Selection(
    val input: LogicalPlan,
    val expr: LogicalExpr
): LogicalPlan {
    override fun schema(): KQuerySchema {
        return input.schema()
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Selection: $expr"
    }
}