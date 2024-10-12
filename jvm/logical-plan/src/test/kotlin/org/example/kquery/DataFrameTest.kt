package org.example.kquery

import org.example.kquery.datasource.CsvDataSource
import org.example.kquery.logicalplan.*
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class DataFrameTest {
    @Test
    fun `build DataFrame`() {
        val df = csv()
            .filter(col("state") eq lit("CO"))
            .project(listOf(col("id"), col("first_name"), col("last_name")))

        val expected =
            "Projection: #id, #first_name, #last_name\n" +
                "\tSelection: #state = 'CO'\n" +
                    "\t\tScan: employee; projection=None\n"

        assertEquals(expected, format(df.logicalPlan()))
    }

    private fun csv(): DataFrame {
        val employeeCsv = "../testdata/employee.csv"
        return DataFrameImpl(Scan("employee", CsvDataSource(employeeCsv, null, true, 1024), listOf()))
    }
}