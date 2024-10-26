package org.example.kquery.physicalplan.expressions


/**
 * Aggregate expression aggregates values across multiple batches of data
 * and produce one final value.
 */
interface AggregateExpression {
    fun inputExpression(): Expression
    fun createAccumulator(): Accumulator
}

interface Accumulator {
    fun accumulate(value: Any?)
    fun finalValue(): Any?
}