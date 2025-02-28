package org.example.kquery.sql

import org.junit.jupiter.api.TestInstance
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

    private fun parseSelect(sql: String): SqlSelect {
        return parse(sql) as SqlSelect
    }

    private fun parse(sql: String): SqlExpr? {
        val tokens = SqlTokenizer(sql).tokenize()
        return SqlParser(tokens).parse()
    }
}