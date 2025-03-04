package org.example.kquery.sql

import java.sql.SQLException

/** implementation of a Pratt Parser for sql */
class SqlParser(val tokens: TokenStream) : PrattParser {
    override fun nextPrecedence(): Int {
        val token = tokens.peek() ?: return 0
        return when (token.type) {
            // math symbols
            Symbol.EQ -> 40

            Symbol.PLUS, Symbol.SUB -> 50
            Symbol.STAR, Symbol.SLASH -> 60
            else -> 0
        }
    }

    override fun parsePrefix(): SqlExpr? {
        val token = tokens.next() ?: return null
        return when (token.type) {
            // keywords
            Keyword.SELECT -> parseSelect()
            Keyword.MAX -> parseSqlFunction(token.text)

            // literals
            Literal.IDENTIFIER -> SqlIdentifier(token.text)
            Literal.LONG -> SqlLong(token.text.toLong())
            Literal.STRING -> SqlString(token.text)
            else -> throw IllegalStateException("Unexpected token $token")
        }
    }

    override fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr {
        val token = tokens.peek()!!
        return when (token.type) {
            // math symbols
            Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH,
            Symbol.EQ -> {
                tokens.next()
                SqlBinaryExpr(left, token.text, parse(precedence) ?: throw SQLException("Error parsing infix"))
            }
            else -> throw IllegalStateException("Unexpected infix token $token")
        }
    }

    private fun parseSelect(): SqlSelect {
        val projection = parseExprList()

        if (tokens.consumeKeyword(Keyword.FROM)) {
            val table = parseExpr() as SqlIdentifier

            // where clause
            var selection: SqlExpr? = null
            if (tokens.consumeKeyword(Keyword.WHERE)) {
                selection = parseExpr()
            }

            // group by clause
            var groupBy: List<SqlExpr> = listOf()
            if (tokens.consumeKeywords(listOf(Keyword.GROUP, Keyword.BY))) {
                groupBy = parseExprList()
            }

            return SqlSelect(projection, selection, groupBy, table.id)
        } else {
            throw IllegalStateException("Expected FROM keyword, found ${tokens.peek()}")
        }
    }

    private fun parseSqlFunction(name: String): SqlFunction {
        if (tokens.consumeSymbol(Symbol.LEFT_PAREN)) {
            val args = parseExprList()
            if (tokens.consumeSymbol(Symbol.RIGHT_PAREN)) {
                return SqlFunction(name, args)
            } else {
                throw IllegalStateException("Expected RIGHT_PAREN, found ${tokens.peek()}")
            }
        } else {
            throw IllegalStateException("Expect LEFT PAREN symbol, found ${tokens.peek()}")
        }
    }

    private fun parseExprList(): List<SqlExpr> {
        val list = mutableListOf<SqlExpr>()
        var expr = parseExpr()
        // loop until end of expression list.
        while (expr != null) {
            list.add(expr)
            // check for comma.
            if (tokens.peek()?.type == Symbol.COMMA) {
                tokens.next()
            } else {
                break
            }
            // parse expression.
            expr = parseExpr()
        }

        return list
    }

    private fun parseExpr() = parse(0)
}