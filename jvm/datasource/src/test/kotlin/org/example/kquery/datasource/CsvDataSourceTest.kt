package org.example.kquery.datasource

import org.example.kquery.datasource.CsvDataSource
import java.io.File
import kotlin.test.Test

class CsvDataSourceTest {
    val dir = "../testdata"

    @Test
    fun `read csv with no projection`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

        val headers = listOf("id", "first_name", "last_name", "state", "job_title", "salary")
        val result = csv.scan(listOf()).toList()

        assert(result.size == 1)

        val batch = result[0];

        val field = batch.field(0)
        assert(field.size() == 3)

        assert(batch.schema.fields.size == headers.size)
        assert(batch.schema.fields.map { h -> h.name }.containsAll(headers))

    }

    @Test
    fun `read csv with projection`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1024)

        val headers = listOf("first_name", "last_name", "state", "job_title", "salary")
        val result = csv.scan(headers).toList()

        assert(result.size == 1)

        val batch = result[0];

        val field = batch.field(0)
        assert(field.size() == 3)

        assert(batch.schema.fields.size == headers.size)
        assert(batch.schema.fields.map { h -> h.name }.containsAll(headers))
    }

    @Test
    fun `read csv in small batch`() {
        val csv = CsvDataSource(File(dir, "employee.csv").absolutePath, null, true, 1)

        val result = csv.scan(listOf()).toList()

        assert(result.size == 3)

        result.forEach() {
            val field = it.field(0)
            assert(field.size() == 1)
        }
    }

    @Test
    fun `read csv with no header`() {
        val csv = CsvDataSource(File(dir, "employee_no_header.csv").absolutePath, null, true, 1024)

        val result = csv.scan(listOf()).toList()

        assert(result.size == 1)

        val field = result[0].field(0)
        assert(field.size() == 3)
    }
}