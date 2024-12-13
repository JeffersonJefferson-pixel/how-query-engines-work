package org.example.kquery.optimizer

import org.example.kquery.logicalplan.*

interface OptimizerRule {
    fun optimize(plan: LogicalPlan) : LogicalPlan

}

class Optimizer() {
    fun optimize(plan: LogicalPlan): LogicalPlan {
        var optimizedPlan = plan
        val rules = listOf(ProjectionPushDownRule())
        rules.forEach { optimizedPlan = it.optimize(optimizedPlan) }
        return optimizedPlan
    }
}

fun extractColumns(expr: List<LogicalExpr>, input: LogicalPlan, accum: MutableSet<String>) {
    expr.forEach { extractColumns(it, input, accum) }
}

fun extractColumns(expr: LogicalExpr, input: LogicalPlan, accum: MutableSet<String>) {
    when (expr) {
        is ColumnIndex -> accum.add(input.schema().fields[expr.i].name)
        is Column -> accum.add(expr.name)
        is BinaryExpr -> {
            extractColumns(expr.l, input, accum)
            extractColumns(expr.r, input, accum)
        }
        is Alias -> extractColumns(expr.expr, input, accum)
        is CastExpr -> extractColumns(expr.expr, input, accum)
        is LiteralString -> {}
        is LiteralLong -> {}
        is LiteralDouble -> {}
        else -> throw IllegalStateException("extractColumns does not support expression: $expr")
    }
}