package org.example.kquery.datatypes

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.ArrowType

object FieldVectorFactory {
    fun create(arrowType: ArrowType, initialCapacity: Int): FieldVector {
        val rootAllocator = RootAllocator(Long.MAX_VALUE)
        val fieldVector : FieldVector =
            when (arrowType) {
                ArrowTypes.BooleanType -> BitVector("v", rootAllocator)
                ArrowTypes.Int8Type -> TinyIntVector("v", rootAllocator)
                ArrowTypes.Int16Type -> SmallIntVector("v", rootAllocator)
                ArrowTypes.Int32Type -> IntVector("v", rootAllocator)
                ArrowTypes.Int64Type -> BigIntVector("v", rootAllocator)
                ArrowTypes.UInt8Type -> UInt1Vector("v", rootAllocator)
                ArrowTypes.UInt16Type -> UInt2Vector("v", rootAllocator)
                ArrowTypes.UInt32Type -> UInt4Vector("v", rootAllocator)
                ArrowTypes.UInt64Type -> UInt8Vector("v", rootAllocator)
                ArrowTypes.FloatType -> Float4Vector("v", rootAllocator)
                ArrowTypes.DoubleType -> Float8Vector("v", rootAllocator)
                ArrowTypes.StringType -> VarCharVector("v", rootAllocator)
                else -> throw IllegalStateException()
            }
        if (initialCapacity > 0) {
            fieldVector.setInitialCapacity(initialCapacity)
        }
        fieldVector.allocateNew()
        return fieldVector
    }
}

class ArrowFieldVector(val field: FieldVector) : ColumnVector {
    override fun getType(): ArrowType {
        return when (field) {
            is BitVector -> ArrowTypes.BooleanType
            is TinyIntVector -> ArrowTypes.Int8Type
            is SmallIntVector -> ArrowTypes.Int16Type
            is IntVector -> ArrowTypes.Int32Type
            is BigIntVector -> ArrowTypes.Int64Type
            is UInt1Vector -> ArrowTypes.UInt8Type
            is UInt2Vector -> ArrowTypes.UInt16Type
            is UInt4Vector -> ArrowTypes.UInt32Type
            is UInt8Vector -> ArrowTypes.UInt64Type
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
            is TinyIntVector, is SmallIntVector, is IntVector, is BigIntVector, is UInt1Vector, is UInt2Vector, is UInt4Vector,
            is UInt8Vector, is Float4Vector, is Float8Vector -> field.getObject(i)
            is VarBinaryVector -> {
                val bytes = field.get(i)
                if (bytes == null ) {
                    null
                } else {
                    String(bytes)
                }
            }
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