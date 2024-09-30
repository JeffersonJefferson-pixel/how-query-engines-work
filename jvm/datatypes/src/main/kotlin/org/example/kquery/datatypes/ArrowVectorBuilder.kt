package org.example.kquery.datatypes

import org.example.kquery.datatypes.ColumnVector
import org.apache.arrow.vector.FieldVector
import org.apache.arrow.vector.IntVector

class ArrowVectorBuilder(val fieldVector: FieldVector) {
    fun set(i: Int, value: Any?) {
        when (fieldVector) {
            is IntVector -> {
                if (value == null) {
                    fieldVector.setNull(i)
                } else if (value is Number) {
                    fieldVector.set(i, value.toInt())
                } else if (value is String) {
                    fieldVector.set(i, value.toInt())
                } else {
                    throw IllegalStateException()
                }
            }
        }
    }

    fun setValueCount(n: Int) {
        fieldVector.valueCount = n
    }

    fun build(): ColumnVector {
        return ArrowFieldVector(fieldVector)
    }
}
