package org.example.kquery.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.IntVector
import kotlin.test.Test
import kotlin.test.assertEquals

class ColumnVectorTest {
    @Test
    fun `build int vector`() {
        val size = 10;
        val fieldVector = IntVector("foo", RootAllocator(Long.MAX_VALUE))
        fieldVector.allocateNew(size)
        fieldVector.valueCount = size
        val b = ArrowVectorBuilder(fieldVector)
        (0 until size).forEach { b.set(it, it ) }
        val v = b.build()

        assertEquals(10, v.size())
        (0 until v.size()).forEach { assertEquals(it, v.getValue(it)) }
    }
}