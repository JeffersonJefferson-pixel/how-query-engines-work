package org.example.kquery.optimizer

import org.example.kquery.datasource.CsvDataSource
import org.example.kquery.logicalplan.DataFrame
import org.example.kquery.logicalplan.DataFrameImpl
import org.example.kquery.logicalplan.Scan
import org.example.kquery.logicalplan.col
import kotlin.test.Test
import kotlin.test.assertEquals

class OptimizerTest {
    @Test
    fun `projection push down`() {
        val df = csv().project(listOf(col("id"), col("first_name"), col("last_name")))

        val rule = ProjectionPushDownRule()
        val optimizedPlan = rule.optimize(df.logicalPlan())

        val expected = "Projection: #id, #first_name, #last_name\n" +
                "\tScan: employee; projection=[first_name, id, last_name]\n"

        assertEquals(expected, optimizedPlan.pretty())
    }

    private fun csv(): DataFrame {
        val employeeCsv = "../testdata/employee.csv"
        return DataFrameImpl(Scan("employee", CsvDataSource(employeeCsv, null, true, 1024), listOf()))
    }
}