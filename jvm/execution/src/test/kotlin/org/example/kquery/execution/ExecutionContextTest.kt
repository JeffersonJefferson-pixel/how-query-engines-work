package org.example.kquery.execution

import org.example.kquery.logicalplan.Column
import org.example.kquery.logicalplan.Eq
import org.example.kquery.logicalplan.LiteralString
import org.example.kquery.logicalplan.format
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class ExecutionContextTest {
    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `employees in CO using DataFrame`() {
        // create context
        val ctx = ExecutionContext(mapOf())

        // construct a query using the DataFrame API
        val df = ctx.csv(employeeCsv)
            .filter(Eq(Column("state"), LiteralString("CO")))
            .project(listOf(Column("id"),
                Column("first_name"),
                Column("last_name"),
                Column("state"),
                Column("salary")))

        assertEquals(
            "Projection: #id, #first_name, #last_name, #state, #salary\n" +
                    "\tSelection: #state = 'CO'\n" +
                    "\t\tScan: D:\\project\\how-query-engines-work\\jvm\\execution\\..\\testdata\\employee.csv; projection=None\n",
            format(df.logicalPlan())
        )
    }
}