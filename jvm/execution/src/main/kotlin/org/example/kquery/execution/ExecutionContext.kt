package org.example.kquery.execution

import org.example.kquery.datasource.CsvDataSource
import org.example.kquery.datatypes.RecordBatch
import org.example.kquery.logicalplan.DataFrame
import org.example.kquery.logicalplan.DataFrameImpl
import org.example.kquery.logicalplan.LogicalPlan
import org.example.kquery.logicalplan.Scan
import org.example.kquery.optimizer.Optimizer
import org.example.kquery.queryplanner.QueryPlanner

class ExecutionContext(val settings: Map<String, String>) {
    val batchSize: Int = settings.getOrDefault("kquery.csv.batchSize", "1024").toInt()

    fun csv(filename: String): DataFrame {
        return DataFrameImpl(Scan(filename, CsvDataSource(filename, null, true, batchSize), listOf()), )
    }

    fun execute(df: DataFrame): Sequence<RecordBatch> {
        return execute(df.logicalPlan())
    }

    fun execute(plan: LogicalPlan): Sequence<RecordBatch> {
        val optimizedPan = Optimizer().optimize(plan)
        val physicalPlan = QueryPlanner().createPhysicalPlan(optimizedPan)
        return physicalPlan.execute()
    }
}