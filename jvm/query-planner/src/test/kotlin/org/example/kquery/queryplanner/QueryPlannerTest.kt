package org.example.kquery.queryplanner

import org.example.kquery.datasource.InMemoryDataSource
import org.example.kquery.datatypes.ArrowTypes
import org.example.kquery.datatypes.KQueryField
import org.example.kquery.datatypes.KQuerySchema
import org.example.kquery.logicalplan.DataFrameImpl
import org.example.kquery.logicalplan.Scan
import org.example.kquery.logicalplan.col
import org.example.kquery.logicalplan.max
import kotlin.test.Test
import kotlin.test.assertEquals

class QueryPlannerTest {
    @Test
    fun `plan aggregate query`() {
        val schema = KQuerySchema(listOf(KQueryField("passenger_count", ArrowTypes.UInt32Type),
            KQueryField("max_fare", ArrowTypes.DoubleType)))
        val dataSource = InMemoryDataSource(schema, listOf())

        val df = DataFrameImpl(Scan("", dataSource, listOf("passenger_count", "max_fare")))

        val plan = df.aggregate(listOf(col("passenger_count")), listOf(max(col("max_fare"))))

        val physicalPlan = QueryPlanner().createPhysicalPlan(plan.logicalPlan())

        assertEquals(
            "HashAggregateExec: groupExpr=[#0], aggrExpr=[MAX(#1)]\n" +
            "\tScanExec: schema=KQuerySchema(fields=[KQueryField(name=passenger_count, dataType=Int(32, false)), KQueryField(name=max_fare, dataType=FloatingPoint(DOUBLE))]), projection=[passenger_count, max_fare]\n",
            physicalPlan.pretty())
    }
}