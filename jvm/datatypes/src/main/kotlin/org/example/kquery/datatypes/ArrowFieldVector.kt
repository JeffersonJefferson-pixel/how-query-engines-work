package org.example.kquery.datatypes

import org.apache.arrow.vector.*
import org.example.kquery.datatypes.ColumnVector
import org.apache.arrow.vector.types.pojo.ArrowType

class ArrowFieldVector(private val field: FieldVector) : ColumnVector {
    override fun getType(): ArrowType {
        return when (field) {
            is BitVector -> ArrowTypes.BooleanType
            is TinyIntVector -> ArrowTypes.Int8Type
            is SmallIntVector -> ArrowTypes.Int16Type
            is IntVector -> ArrowTypes.Int32Type
            is BigIntVector -> ArrowTypes.Int64Type
            is Float4Vector -> ArrowTypes.FloatType
            is Float8Vector -> ArrowTypes.DoubleType
            is VarCharVector -> ArrowTypes.StringType
            else -> throw IllegalStateException()
        }
    }

    override fun getValue(i: Int): Any? {
        if (field.isNull(i)) {
            return null
        }

        return when (field) {
            is BitVector -> field.get(i) == 1
            is TinyIntVector -> field.get(i)
            is SmallIntVector -> field.get(i)
            is IntVector -> field.get(i)
            is BigIntVector -> field.get(i)
            is Float4Vector -> field.get(i)
            is Float8Vector -> field.get(i)
            is VarCharVector -> {
                val bytes = field.get(i)
                if (bytes == null ) {
                    null
                } else {
                    String(bytes)
                }
            }
            else -> throw IllegalStateException()
        }
    }

    override fun size(): Int {
        return field.valueCount
    }
}