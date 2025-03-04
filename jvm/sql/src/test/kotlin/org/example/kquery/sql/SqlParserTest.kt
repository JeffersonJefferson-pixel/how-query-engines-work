package org.example.kquery.sql

import kotlin.test.Test
import kotlin.test.assertEquals

class SqlParserTest {
    @Test
    fun `1 + 2 * 3`() {
        val expr = parse("1 + 2 * 3")
        val expected = SqlBinaryExpr(SqlLong(1), "+", SqlBinaryExpr(SqlLong(2), "*", SqlLong(3)))
        assertEquals(expected, expr)
    }

    @Test
    fun `simple SELECT`() {
        val select = parseSelect("SELECT id, first_name, last_name FROM employee")
        assertEquals("employee", select.tableName)
        assertEquals(
            listOf(SqlIdentifier("id"), SqlIdentifier("first_name"), SqlIdentifier("last_name")),
            select.projection)
    }

    @Test
    fun `parse SELECT with WHERE`() {
        val select = parseSelect("SELECT id, first_name, last_name FROM employee WHERE state = 'CO'")
        assertEquals(
            listOf(SqlIdentifier("id"), SqlIdentifier("first_name"), SqlIdentifier("last_name")),
            select.projection
        )
        assertEquals(SqlBinaryExpr(SqlIdentifier("state"), "=", SqlString("CO")), select.selection)
        assertEquals("employee", select.tableName)
    }

    @Test
    fun `parse SELECT with aggregates`() {
        val select = parseSelect("SELECT state, MAX(salary) FROM employee GROUP BY state")
        assertEquals(
            listOf(SqlIdentifier("state"), SqlFunction("MAX", listOf(SqlIdentifier("salary")))),
            select.projection
        )
        assertEquals(
            listOf(SqlIdentifier("state")),
            select.groupBy
        )
        assertEquals("employee", select.tableName)
    }


    private fun parseSelect(sql: String): SqlSelect {
        return parse(sql) as SqlSelect
    }

    private fun parse(sql: String): SqlExpr? {
        val tokens = SqlTokenizer(sql).tokenize()
        return SqlParser(tokens).parse()
    }
}