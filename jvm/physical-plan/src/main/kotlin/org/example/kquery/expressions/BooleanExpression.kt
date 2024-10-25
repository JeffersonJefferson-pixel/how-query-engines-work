package org.example.kquery.expressions

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BitVector
import org.apache.arrow.vector.types.pojo.ArrowType
import org.example.kquery.datatypes.ArrowFieldVector
import org.example.kquery.datatypes.ArrowTypes
import org.example.kquery.datatypes.ColumnVector
import org.example.kquery.datatypes.RecordBatch

abstract class BooleanExpression(
    val l: Expression,
    val r: Expression
) : Expression {
    override fun evaluate(input: RecordBatch) : ColumnVector {
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        if (ll.getType() != rr.getType()) {
            throw IllegalStateException(
                "Cannot compare values of different type: ${ll.getType()} != ${rr.getType()}")
        }
        return compare(ll, rr)
    }

    fun compare(l: ColumnVector, r: ColumnVector) : ColumnVector {
        val v = BitVector("v", RootAllocator(Long.MAX_VALUE))
        v.allocateNew()
        (0 until l.size()).forEach {
            val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
            v.set(it, if (value) 1 else 0)
        }
        v.valueCount = l.size()
        return ArrowFieldVector(v)
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean
}

class AndExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return toBool(l) && toBool(r)
    }
}

class OrExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return toBool(l) || toBool(r)
    }
}

class EqExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) == (r as Byte)
            ArrowTypes.Int16Type -> (l as Short)  == (r as Short)
            ArrowTypes.Int32Type -> (l as Int) == (r as Int)
            ArrowTypes.Int64Type -> (l as Long) == (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) == (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort)  == (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) == (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) == (r as ULong)
            ArrowTypes.FloatType -> (l as Float) == (r as Float)
            ArrowTypes.DoubleType -> (l as Double) == (r as Double)
            ArrowTypes.StringType -> toString(l) == toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class NeqExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) != (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) != (r as Short)
            ArrowTypes.Int32Type -> (l as Int) != (r as Int)
            ArrowTypes.Int64Type -> (l as Long) != (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) != (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) != (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) != (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) != (r as ULong)
            ArrowTypes.FloatType -> (l as Float) != (r as Float)
            ArrowTypes.DoubleType -> (l as Double) != (r as Double)
            ArrowTypes.StringType -> toString(l) != toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class LtExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) < (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) < (r as Short)
            ArrowTypes.Int32Type -> (l as Int) < (r as Int)
            ArrowTypes.Int64Type -> (l as Long) < (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) < (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) < (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) < (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) < (r as ULong)
            ArrowTypes.FloatType -> (l as Float) < (r as Float)
            ArrowTypes.DoubleType -> (l as Double) < (r as Double)
            ArrowTypes.StringType -> toString(l) < toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class LtEqExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) <= (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) <= (r as Short)
            ArrowTypes.Int32Type -> (l as Int) <= (r as Int)
            ArrowTypes.Int64Type -> (l as Long) <= (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) <= (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) <= (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) <= (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) <= (r as ULong)
            ArrowTypes.FloatType -> (l as Float) <= (r as Float)
            ArrowTypes.DoubleType -> (l as Double) <= (r as Double)
            ArrowTypes.StringType -> toString(l) <= toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class GtExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) > (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) > (r as Short)
            ArrowTypes.Int32Type -> (l as Int) > (r as Int)
            ArrowTypes.Int64Type -> (l as Long) > (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) > (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) > (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) > (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) > (r as ULong)
            ArrowTypes.FloatType -> (l as Float) > (r as Float)
            ArrowTypes.DoubleType -> (l as Double) > (r as Double)
            ArrowTypes.StringType -> toString(l) > toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

class GtEqExpression(l: Expression, r: Expression): BooleanExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Boolean {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) >= (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) >= (r as Short)
            ArrowTypes.Int32Type -> (l as Int) >= (r as Int)
            ArrowTypes.Int64Type -> (l as Long) >= (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) >= (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) >= (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) >= (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) >= (r as ULong)
            ArrowTypes.FloatType -> (l as Float) >= (r as Float)
            ArrowTypes.DoubleType -> (l as Double) >= (r as Double)
            ArrowTypes.StringType -> toString(l) >= toString(r)
            else -> throw IllegalStateException("Unsupported data type in comparison expression: $arrowType")
        }
    }
}

private fun toString(v: Any?): String {
    return when (v) {
        is ByteArray -> String(v)
        else -> v.toString()
    }
}

private fun toBool(v: Any?): Boolean {
    return when (v) {
        is Boolean -> v
        is Number -> v.toInt() == 1
        else -> throw IllegalStateException()
    }
}