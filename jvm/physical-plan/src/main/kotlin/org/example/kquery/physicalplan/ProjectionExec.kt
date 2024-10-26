package org.example.kquery.physicalplan

import org.example.kquery.datatypes.KQuerySchema
import org.example.kquery.datatypes.RecordBatch
import org.example.kquery.physicalplan.expressions.Expression

/**
 * Projection execution plan simply evaluates the projection expressions against the input columns
 * and produces a record batch containing the derived columns.
 */
class ProjectionExec(
    val input: PhysicalPlan,
    val schema: KQuerySchema,
    val expr: List<Expression>
) : PhysicalPlan {
    override fun schema(): KQuerySchema {
        return schema
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

    override fun execute(): Sequence<RecordBatch> {
        return input.execute().map { batch ->
            val columns = expr.map { it.evaluate(batch) }
            RecordBatch(schema, columns)
        }
    }

    override fun toString(): String {
        return "ProjectionExec: $expr"
    }
}