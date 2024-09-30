package org.example.kquery.datatypes

import main.kotlin.org.example.kquery.datatypes.ColumnVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.pojo.ArrowType

class ArrowFieldVector(private val field: FieldVector) : ColumnVector {
    override fun getType(): ArrowType {
        return when (field) {
            is IntVector -> ArrowTypes.Int32Type
            else -> throw IllegalStateException()
        }
    }

    override fun getValue(i: Int): Any? {
        if (field.isNull(i)) {
            return null
        }

        return when (field) {
            is IntVector -> field.get(i)
            else -> throw IllegalStateException()
        }
    }

    override fun size(): Int {
        return field.valueCount
    }
}