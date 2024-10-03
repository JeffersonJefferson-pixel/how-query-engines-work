package org.example.kquery.datasource

import org.example.kquery.datatypes.RecordBatch
import org.example.kquery.datatypes.Schema

interface DataSource {
    /** Return the schema for the underlying data source. */
    fun schema(): Schema

    /** Scan the data source, selecting the specified columns. */
    fun scan(projection: List<String>):  Sequence<RecordBatch>
}