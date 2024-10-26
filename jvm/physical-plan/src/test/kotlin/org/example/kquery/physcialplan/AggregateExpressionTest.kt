package org.example.kquery.physcialplan

import org.example.kquery.physicalplan.expressions.ColumnExpression
import org.example.kquery.physicalplan.expressions.MinExpression
import kotlin.test.Test
import kotlin.test.assertEquals

class AggregateExpressionTest {
    @Test
    fun `min accumulator`() {
        val a = MinExpression(ColumnExpression(0)).createAccumulator()
        val values = listOf(10, 14, 4)
        values.forEach { a.accumulate(it) }
        assertEquals(4, a.finalValue())
    }
}