package org.example.kquery.physicalplan

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.example.kquery.datatypes.ArrowFieldVector
import org.example.kquery.datatypes.ArrowVectorBuilder
import org.example.kquery.datatypes.KQuerySchema
import org.example.kquery.datatypes.RecordBatch
import org.example.kquery.physicalplan.expressions.Accumulator
import org.example.kquery.physicalplan.expressions.AggregateExpression
import org.example.kquery.physicalplan.expressions.Expression

/**
 * Hash Aggregate process all incoming batches and maintain a hashmap of accumulators and
 * update the accumulators for each row being processed.
 */
class HashAggregateExec(
    val input: PhysicalPlan,
    val groupExpr: List<Expression>,
    val aggregateExpr: List<AggregateExpression>,
    val schema: KQuerySchema
) : PhysicalPlan {
    override fun schema(): KQuerySchema {
        return schema
    }

    override fun execute(): Sequence<RecordBatch> {
        val map = HashMap<List<Any?>, List<Accumulator>>()

        // process batches
        for (batch in input.execute()) {
            // evaluate grouping expression
            val groupKeys = groupExpr.map { it.evaluate(batch) }
            // evaluate aggregate expressions.
            val aggrInputValues = aggregateExpr.map {
                it.inputExpression().evaluate(batch)
            }

            // row in batch
            (0 until batch.rowCount()).forEach { rowIndex ->
                // create key for map
                val rowKey = groupKeys.map {
                    val value = it.getValue(rowIndex)
                    when (value) {
                        is ByteArray -> String(value)
                        else -> value
                    }
                }

                // get or create  accumulator for grouping key
                val accumulators = map.getOrPut(rowKey) {
                    aggregateExpr.map { it.createAccumulator() }
                }

                // do accumulation
                accumulators.withIndex().forEach { accum ->
                    val value =  aggrInputValues[accum.index].getValue(rowIndex)
                    accum.value.accumulate(value)
                }
            }
        }

        // create result batch
        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = map.size

        val builders = root.fieldVectors.map { ArrowVectorBuilder(it) }

        map.entries.withIndex().forEach { entry ->
            val rowIndex = entry.index
            val groupingKey = entry.value.key
            val accumulators = entry.value.value
            groupExpr.indices.forEach { builders[it].set(rowIndex, groupingKey[it]) }
            aggregateExpr.indices.forEach {
                builders[groupExpr.size + it].set(rowIndex, accumulators[it].finalValue())
            }
        }

        val outputBatch = RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })

        return listOf(outputBatch).asSequence()
    }

    override fun children(): List<PhysicalPlan> {
        return listOf(input)
    }

}