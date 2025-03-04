package org.example.kquery.sql

import org.example.kquery.datasource.CsvDataSource
import org.example.kquery.logicalplan.DataFrameImpl
import org.example.kquery.logicalplan.LogicalPlan
import org.example.kquery.logicalplan.Scan
import org.example.kquery.logicalplan.format
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals

class SqlPlannerTest {
    val dir = "../testdata"

    val employeeCsv = File(dir, "employee.csv").absolutePath

    @Test
    fun `simple select`() {
        val plan = plan("SELECT state FROM employee")
        assertEquals("Projection: #state\n" + "\tScan: ; projection=None\n", format(plan))
    }

    @Test
    fun `select with filter`() {
        val plan = plan("SELECT state FROM employee WHERE state = 'CA'")
        assertEquals(
            "Selection: #state = 'CA'\n" +
                    "\tProjection: #state\n" +
                    "\t\tScan: ; projection=None\n",
            format(plan)
        )
    }

    @Test
    fun `select with filter not in projection`() {
        val plan = plan("SELECT last_name FROM employee WHERE state = 'CA'")
        assertEquals(
            "Projection: #last_name\n" +
            "\tSelection: #state = 'CA'\n" +
            "\t\tProjection: #last_name, #state\n" +
            "\t\t\tScan: ; projection=None\n",
            format(plan)
        )
    }

    @Test
    fun `plan aggregate query`() {
        val plan = plan("SELECT state, MAX(salary) FROM employee GROUP BY state")
        assertEquals(
            "Projection: #0, #1\n" +
            "\tAggregate: groupExpr=[#state], aggregateExpr=[MAX(#salary)]\n" +
            "\t\tScan: ; projection=None\n",
            format(plan)
        )
    }

    private fun plan(sql: String): LogicalPlan {
        val tokens = SqlTokenizer(sql).tokenize()
        val parsedQuery = SqlParser(tokens).parse()
        val tables =
            mapOf(
                "employee" to
                    DataFrameImpl(Scan("", CsvDataSource(employeeCsv, null, true, 1024), listOf()))
            )

        val df = SqlPlanner().createDataFrame(parsedQuery as SqlSelect, tables)

        return df.logicalPlan()
    }
}