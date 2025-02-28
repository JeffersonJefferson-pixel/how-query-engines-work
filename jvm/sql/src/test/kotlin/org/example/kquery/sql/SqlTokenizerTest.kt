package org.example.kquery.sql

import kotlin.test.Test
import kotlin.test.assertEquals

class SqlTokenizerTest {
    @Test
    fun `tokenize simple SELECT`() {
        val expected = listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("id", Literal.IDENTIFIER, 9),
            Token(",", Symbol.COMMA, 10),
            Token("first_name", Literal.IDENTIFIER, 21),
            Token(",", Symbol.COMMA, 22),
            Token("last_name", Literal.IDENTIFIER, 32),
            Token("FROM", Keyword.FROM, 37),
            Token("employee", Literal.IDENTIFIER, 46),
        )
        val actual = SqlTokenizer("SELECT id, first_name, last_name FROM employee").tokenize().tokens

        assertEquals(expected, actual)
    }
}