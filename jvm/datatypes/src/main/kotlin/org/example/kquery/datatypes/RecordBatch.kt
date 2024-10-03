package org.example.kquery.datatypes

class RecordBatch(val schema: Schema, private val fields: List<ColumnVector>) {
    fun rowCount() = fields.first().size()

    fun columnCount() = fields.size

    // access one column
    fun field(i: Int): ColumnVector {
        return fields[i]
    }
}