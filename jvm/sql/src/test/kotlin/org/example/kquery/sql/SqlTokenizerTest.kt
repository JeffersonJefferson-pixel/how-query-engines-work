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

    @Test
    fun `tokenize SELECT with WHERE`() {
        val expected = listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("a", Literal.IDENTIFIER, 8),
            Token(",",  Symbol.COMMA, 9),
            Token("b", Literal.IDENTIFIER, 11),
            Token("FROM", Keyword.FROM, 16),
            Token("employee", Literal.IDENTIFIER, 25),
            Token("WHERE", Keyword.WHERE, 31),
            Token("state", Literal.IDENTIFIER, 37),
            Token("=", Symbol.EQ, 39),
            Token("CO", Literal.STRING,  44)
        )
        val actual = SqlTokenizer("SELECT a, b FROM employee WHERE state = 'CO'").tokenize().tokens

        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize SELECT with aggregates`() {
        val expected =
            listOf(
                Token("SELECT", Keyword.SELECT, 6),
                Token("state", Literal.IDENTIFIER, 12),
                Token(",", Symbol.COMMA, 13),
                Token("MAX", Keyword.MAX, 17),
                Token("(", Symbol.LEFT_PAREN, 18),
                Token("salary", Literal.IDENTIFIER, 24),
                Token(")", Symbol.RIGHT_PAREN, 25),
                Token("FROM", Keyword.FROM, 30),
                Token("employee", Literal.IDENTIFIER, 39),
                Token("GROUP", Keyword.GROUP, 45),
                Token("BY", Keyword.BY, 48),
                Token("state", Literal.IDENTIFIER, 54)
            )
        val actual = SqlTokenizer("SELECT state, MAX(salary) FROM employee GROUP BY state").tokenize().tokens

        assertEquals(expected, actual)
    }
}