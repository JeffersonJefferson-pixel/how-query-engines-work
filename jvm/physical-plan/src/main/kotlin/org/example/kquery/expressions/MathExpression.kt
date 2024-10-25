package org.example.kquery.expressions

import org.apache.arrow.vector.types.pojo.ArrowType
import org.example.kquery.datatypes.ArrowTypes
import org.example.kquery.datatypes.ArrowVectorBuilder
import org.example.kquery.datatypes.ColumnVector
import org.example.kquery.datatypes.FieldVectorFactory

abstract class MathExpression(
    l: Expression,
    r: Expression
): BinaryExpression(l, r) {

    override fun evaluate(l: ColumnVector, r: ColumnVector): ColumnVector {
        val fieldVector = FieldVectorFactory.create(l.getType(), l.size())
        val builder = ArrowVectorBuilder(fieldVector)
        (0 until l.size()).forEach {
            val value = evaluate(l.getValue(it), r.getValue(it), l.getType())
            builder.set(it, value)
        }
        builder.setValueCount(l.size())
        return builder.build()
    }

    abstract fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any?
}

class AddExpression(l: Expression, r: Expression): MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) + (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) + (r as Short)
            ArrowTypes.Int32Type -> (l as Int) + (r as Int)
            ArrowTypes.Int64Type -> (l as Long) + (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) + (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) + (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) + (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) + (r as ULong)
            ArrowTypes.FloatType -> (l as Float) + (r as Float)
            ArrowTypes.DoubleType -> (l as Double) + (r as Double)
            else -> throw IllegalStateException("Unsupported data type in ADD expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l+$r"
    }
}

class SubtractExpression(l: Expression, r: Expression): MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) - (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) - (r as Short)
            ArrowTypes.Int32Type -> (l as Int) - (r as Int)
            ArrowTypes.Int64Type -> (l as Long) - (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) - (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) - (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) - (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) - (r as ULong)
            ArrowTypes.FloatType -> (l as Float) - (r as Float)
            ArrowTypes.DoubleType -> (l as Double) - (r as Double)
            else -> throw IllegalStateException("Unsupported data type in SUBTRACT expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l-$r"
    }
}

class MultiplyExpression(l: Expression, r: Expression): MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) * (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) * (r as Short)
            ArrowTypes.Int32Type -> (l as Int) * (r as Int)
            ArrowTypes.Int64Type -> (l as Long) * (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) * (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) * (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) * (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) * (r as ULong)
            ArrowTypes.FloatType -> (l as Float) * (r as Float)
            ArrowTypes.DoubleType -> (l as Double) * (r as Double)
            else -> throw IllegalStateException("Unsupported data type in MULTIPLY expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l*$r"
    }
}

class DivideExpression(l: Expression, r: Expression): MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) / (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) / (r as Short)
            ArrowTypes.Int32Type -> (l as Int) / (r as Int)
            ArrowTypes.Int64Type -> (l as Long) / (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) / (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) / (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) / (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) / (r as ULong)
            ArrowTypes.FloatType -> (l as Float) / (r as Float)
            ArrowTypes.DoubleType -> (l as Double) / (r as Double)
            else -> throw IllegalStateException("Unsupported data type in DIVIDE expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l/$r"
    }
}

class ModulusExpression(l: Expression, r: Expression): MathExpression(l, r) {
    override fun evaluate(l: Any?, r: Any?, arrowType: ArrowType): Any? {
        return when (arrowType) {
            ArrowTypes.Int8Type -> (l as Byte) % (r as Byte)
            ArrowTypes.Int16Type -> (l as Short) % (r as Short)
            ArrowTypes.Int32Type -> (l as Int) % (r as Int)
            ArrowTypes.Int64Type -> (l as Long) % (r as Long)
            ArrowTypes.UInt8Type -> (l as UByte) % (r as UByte)
            ArrowTypes.UInt16Type -> (l as UShort) % (r as UShort)
            ArrowTypes.UInt32Type -> (l as UInt) % (r as UInt)
            ArrowTypes.UInt64Type -> (l as ULong) % (r as ULong)
            ArrowTypes.FloatType -> (l as Float) % (r as Float)
            ArrowTypes.DoubleType -> (l as Double) % (r as Double)
            else -> throw IllegalStateException("Unsupported data type in MODULUS expression: $arrowType")
        }
    }

    override fun toString(): String {
        return "$l/$r"
    }
}