package org.example.kquery.logicalplan

import org.example.kquery.datatypes.KQuerySchema

/**
 * Aggregate logical plan calculate aggregates of underlying data based on some grouping.
 */
class Aggregate(
    val input: LogicalPlan,
    val groupExpr: List<LogicalExpr>,
    val aggregateExpr: List<AggregateExpr>
): LogicalPlan {
    override fun schema(): KQuerySchema {
        return KQuerySchema(groupExpr.map { it.toField(input) } + aggregateExpr.map { it.toField(input) })
    }

    override fun children(): List<LogicalPlan> {
        return listOf(input)
    }

    override fun toString(): String {
        return "Aggregate: groupExpr=$groupExpr, aggregateExpr=$aggregateExpr"
    }
}