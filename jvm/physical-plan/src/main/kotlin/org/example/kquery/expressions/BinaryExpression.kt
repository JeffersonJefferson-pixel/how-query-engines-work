package org.example.kquery.expressions

import org.example.kquery.datatypes.ColumnVector
import org.example.kquery.datatypes.RecordBatch

abstract class BinaryExpression(
    val l: Expression,
    val r: Expression
) : Expression {
    override fun evaluate(input: RecordBatch): ColumnVector {
        // evaluate left and right expressions.
        val ll = l.evaluate(input)
        val rr = r.evaluate(input)
        // validate size and type of results.
        assert(ll.size() == rr.size())
        if (ll.getType() != rr.getType())
            throw IllegalStateException("Binary expression operands do not have the same type: " + "${ll.getType()} != ${rr.getType()}")
        // evaluate binary operator against input values.
        return evaluate(ll, rr)
    }

    abstract fun evaluate(l: ColumnVector, r: ColumnVector) : ColumnVector
}