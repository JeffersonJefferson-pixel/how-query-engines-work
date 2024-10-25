package org.example.kquery.expressions


interface AggregateExpression {
    fun inputExpression(): Expression
    fun createAccumulator(): Accumulator
}

interface Accumulator {
    fun accumulate(value: Any?)
    fun finalValue(): Any?
}