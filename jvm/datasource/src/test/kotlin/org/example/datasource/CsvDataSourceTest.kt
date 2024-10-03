package org.example.datasource

import org.example.kquery.datasource.CsvDataSource
import java.io.File
import kotlin.test.Test

class CsvDataSourceTest {
    val dir = "../testdata"

    @Test
    fun `read csv with no projection`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

        val headers = listOf("id", "first_name", "last_name", "state", "job_title", "salary")
        val result = csv.scan(listOf())

        result.forEach {
            val field = it.field(0)
            assert(field.size() == 3)

            assert(it.schema.fields.size == headers.size)
            assert(it.schema.fields.map { h -> h.name }.containsAll(headers))
        }

    }
}