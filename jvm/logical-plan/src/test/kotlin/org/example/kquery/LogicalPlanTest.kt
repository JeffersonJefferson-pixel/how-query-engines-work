package org.example.kquery

import org.example.kquery.datasource.CsvDataSource
import org.example.kquery.logicalplan.*
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class LogicalPlanTest {
    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `build logical plan`() {
        val csv = CsvDataSource(employeeCsv, null, true, 10)
        val scan = Scan("employee", csv, listOf())
        val filterExpr = Eq(col("state"), LiteralString("CO"))
        val selection = Selection(scan, filterExpr)
        val plan = Projection(selection, listOf(col("id"), col("first_name"), col("last_name")))

        assertEquals(
            "Projection: #id, #first_name, #last_name\n" +
            "\tSelection: #state = 'CO'\n" +
            "\t\tScan: employee; projection=None\n",
            format(plan)
        )
    }
}