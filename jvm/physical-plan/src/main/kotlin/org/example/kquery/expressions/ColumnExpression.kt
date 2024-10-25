package org.example.kquery.expressions

import org.example.kquery.datatypes.ColumnVector
import org.example.kquery.datatypes.RecordBatch

/**
 * Column expression evaluates to a reference to the column vector in the record batch.
 * It references columns by index.
 */
class ColumnExpression(val i: Int)  : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        return input.field(i)
    }

    override fun toString(): String {
        return "#$i"
    }
}