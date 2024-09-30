package main.kotlin.org.example.kquery.datatypes

import org.apache.arrow.vector.types.pojo.ArrowType

// abstraction over different implementation of field vector (columnar storage for a field)
interface ColumnVector {
    fun getType(): ArrowType
    fun getValue(i: Int) : Any?
    fun size(): Int
}