package org.example.kquery.logicalplan

import org.example.kquery.datatypes.Schema

/**
 * Aggregate logical plan calculate aggregates of underlying data based on some grouping.
 */
class Aggregate(
    val input: LogicalPlan,
    val groupExpr: List<LogicalExpr>,
    val aggregateExpr: List<LogicalExpr>
): LogicalPlan {
    override fun schema(): Schema {
        return Schema(groupExpr.map { it.toField(input) } + aggregateExpr.map { it.toField(input) })
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Aggregate: groupExpr=$groupExpr, aggregateExpr=$aggregateExpr"
    }
}