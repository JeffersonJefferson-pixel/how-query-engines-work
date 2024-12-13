package org.example.kquery.physicalplan.expressions

import org.apache.arrow.vector.types.pojo.ArrowType
import org.example.kquery.datatypes.*

class CastExpression(val expr: Expression, val dataType: ArrowType): Expression {
    override fun toString(): String {
        return "CAST($expr AS $dataType)"
    }

    override fun evaluate(input: RecordBatch): ColumnVector {
        val value: ColumnVector = expr.evaluate(input)
        val fieldVector = FieldVectorFactory.create(dataType, input.rowCount())
        val builder = ArrowVectorBuilder(fieldVector)

        when (dataType) {
            ArrowTypes.FloatType -> {
                (0 until value.size()).forEach {
                    val vv = value.getValue(it)
                    if (vv == null) {
                        builder.set(it, null)
                    } else {
                        val castValue =
                            when (vv) {
                                is ByteArray -> String(vv).toFloat()
                                is String -> vv.toFloat()
                                is Number -> vv.toFloat()
                                else -> throw IllegalStateException("Cannot set value to Float: $vv")
                            }
                        builder.set(it, castValue)
                    }
                }
            }
        }

        builder.setValueCount(value.size())
        return builder.build()
    }
}