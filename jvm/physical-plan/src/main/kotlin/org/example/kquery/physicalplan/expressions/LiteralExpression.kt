package org.example.kquery.physicalplan.expressions

import org.example.kquery.datatypes.ArrowTypes
import org.example.kquery.datatypes.ColumnVector
import org.example.kquery.datatypes.LiteralValueVector
import org.example.kquery.datatypes.RecordBatch

class LiteralLongExpression(private val value: Long) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(ArrowTypes.Int64Type, value, input.rowCount())
    }
}

class LiteralDoubleExpression(private val value: Double): Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(ArrowTypes.DoubleType, value, input.rowCount())
    }
}

class LiteralStringExpression(private val value: String): Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return LiteralValueVector(ArrowTypes.StringType, value, input.rowCount())
    }
}