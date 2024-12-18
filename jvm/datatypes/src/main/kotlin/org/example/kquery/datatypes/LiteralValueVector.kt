package org.example.kquery.datatypes

import org.apache.arrow.vector.types.pojo.ArrowType
import java.lang.IndexOutOfBoundsException

/**
 * LiteralValueVector provides the same value for every index in a column.
 */
class LiteralValueVector(
    private val arrowType: ArrowType,
    private val value: Any?,
    private val size: Int
) : ColumnVector {
    override fun getType(): ArrowType {
        return arrowType
    }

    override fun getValue(i: Int): Any? {
        if (i <  0 || i >= size) {
            throw IndexOutOfBoundsException()
        }
        return value
    }

    override fun size(): Int {
        return size;
    }
}