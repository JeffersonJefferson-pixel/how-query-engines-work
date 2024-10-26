package org.example.kquery.physicalplan.expressions

import org.example.kquery.datatypes.ColumnVector
import org.example.kquery.datatypes.RecordBatch

interface Expression {
    fun evaluate(input: RecordBatch): ColumnVector
}