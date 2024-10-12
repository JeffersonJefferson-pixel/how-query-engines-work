package org.example.kquery.execution

import org.example.kquery.datasource.CsvDataSource
import org.example.kquery.logicalplan.DataFrame
import org.example.kquery.logicalplan.DataFrameImpl
import org.example.kquery.logicalplan.Scan

class ExecutionContext(val settings: Map<String, String>) {
    val batchSize: Int = settings.getOrDefault("kquery.csv.batchSize", "1024").toInt()

    fun csv(filename: String): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, null, true, batchSize), listOf()), )
    }
}