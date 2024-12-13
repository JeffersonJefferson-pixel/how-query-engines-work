package org.example.kquery.physcialplan

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.example.kquery.datatypes.*
import org.example.kquery.physicalplan.expressions.ColumnExpression
import org.example.kquery.physicalplan.expressions.GtEqExpression
import kotlin.test.Test
import kotlin.test.assertEquals

class BooleanExpressionTest {
    @Test
    fun `gteq bytes`() {
        val schema = KQuerySchema(listOf(KQueryField("a", ArrowTypes.Int8Type), KQueryField("b", ArrowTypes.Int8Type)))

        val a = listOf(10, 20, 30, Byte.MIN_VALUE, Byte.MAX_VALUE)
        val b = listOf(10, 30, 20, Byte.MAX_VALUE, Byte.MIN_VALUE)

        val batch = createRecordBatch(schema, listOf(a, b))

        val expr = GtEqExpression(ColumnExpression(0), ColumnExpression(1))
        val result = expr.evaluate(batch)

        assertEquals(a.size, result.size())
        (0 until result.size()).forEach { assertEquals(a[it] >= b[it], result.getValue(it)) }
    }

    private fun createRecordBatch(schema: KQuerySchema,columns: List<List<Any?>> ): RecordBatch {
        val rowCount = columns[0].size

        val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
        root.allocateNew()

        (0 until rowCount).forEach { row ->
            columns.indices.forEach { col ->
                val v = root.getVector(col)
                val value = columns[col][row]
                when (v) {
                    is TinyIntVector -> v.set(row, value as Byte)
                    else -> throw IllegalStateException()
                }
            }
        }

        root.rowCount = rowCount

        return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
    }
}