package org.example.kquery.physicalplan.expressions

class SumExpression(private val expr: Expression) : AggregateExpression {
    override fun inputExpression(): Expression {
        return expr
    }

    override fun createAccumulator(): Accumulator {
        return SumAccumulator()
    }

    override fun toString(): String {
        return "SUM($expr)"
    }
}

class SumAccumulator : Accumulator {
    var value: Any? = null

    override fun accumulate(value: Any?) {
        if (value != null) {
            if (this.value == null) {
                this.value = value
            } else {
                when (val currentValue = this.value) {
                    is Byte -> this.value = currentValue + value as Byte
                    is Short -> this.value = currentValue + value as Short
                    is Int -> this.value = currentValue + value as Int
                    is Long -> this.value = currentValue + value as Long
                    is UByte -> this.value = currentValue + value as UByte
                    is UShort -> this.value = currentValue + value as UShort
                    is UInt -> this.value = currentValue + value as UInt
                    is ULong -> this.value = currentValue + value as ULong
                    is Float -> this.value = currentValue + value as Float
                    is Double -> this.value = currentValue + value as Double
                    else -> throw UnsupportedOperationException("SUM is not implemented for data type: ${value.javaClass.name}")
                }
            }
        }
    }

    override fun finalValue(): Any? {
        return value
    }
}