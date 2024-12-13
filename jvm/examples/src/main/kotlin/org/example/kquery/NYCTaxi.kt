package org.example.kquery

import org.example.kquery.datatypes.ArrowTypes
import org.example.kquery.execution.ExecutionContext
import org.example.kquery.logicalplan.cast
import org.example.kquery.logicalplan.col
import org.example.kquery.logicalplan.format
import org.example.kquery.logicalplan.max
import org.example.kquery.optimizer.Optimizer
import kotlin.system.measureTimeMillis

fun main() {
    val ctx = ExecutionContext(mapOf())

    val time = measureTimeMillis {
        val df =  ctx.csv("testdata/yellow_tripdata_2019-01.csv")
            .aggregate(
                listOf(col("passenger_count")),
                // need to do an explicit cast to numeric type.
                listOf(max(cast(col("fare_amount"), ArrowTypes.FloatType))))

        println("Logical Plan:\t${format(df.logicalPlan())}")

        val optimizedPlan = Optimizer().optimize(df.logicalPlan())
        println("Optimized Plan:\t${format(optimizedPlan)}")

        val results = ctx.execute(optimizedPlan)

        results.forEach {
            println(it.schema)
            println(it.toCSV())
        }
    }

    println("Query took $time ms")
}