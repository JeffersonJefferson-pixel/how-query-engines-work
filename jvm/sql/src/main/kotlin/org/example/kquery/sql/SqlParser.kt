package org.example.kquery.sql

import java.sql.SQLException

/** implementation of a Pratt Parser for sql */
class SqlParser(val tokens: TokenStream) : PrattParser {
    override fun nextPrecedence(): Int {
        val token = tokens.peek() ?: return 0
        return when (token.type) {
            // keywords
            Keyword.AS, Keyword.ASC, Keyword.DESC -> 10
            Keyword.AND -> 30
            // math symbols
            Symbol.EQ, Symbol.LT, Symbol.GT -> 40

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
            Literal.DOUBLE -> SqlDouble(token.text.toDouble())
            Literal.STRING -> SqlString(token.text)
            else -> throw IllegalStateException("Unexpected token $token")
        }
    }

    override fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr {
        val token = tokens.peek()!!
        return when (token.type) {
            // math symbols
            Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH,
            Symbol.EQ, Symbol.LT, Symbol.GT -> {
                tokens.next()
                SqlBinaryExpr(left, token.text, parse(precedence) ?: throw SQLException("Error parsing infix"))
            }
            // keywords
            Keyword.AS -> {
                tokens.next()
                SqlAlias(left, parseIdentifier())
            }
            Keyword.AND -> {
                tokens.next()
                SqlBinaryExpr(left, token.text, parse(precedence) ?: throw SQLException("Error parsing infix"))
            }
            Keyword.ASC, Keyword.DESC -> {
                tokens.next()
                SqlSort(left, token.type == Keyword.ASC)
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

            // having clause
            var having: SqlExpr? = null
            if (tokens.consumeKeyword(Keyword.HAVING)) {
                having = parseExpr()
            }

            // order by clause
            var orderBy: List<SqlExpr> = listOf()
            if (tokens.consumeKeywords(listOf(Keyword.ORDER, Keyword.BY))) {
                orderBy = parseOrder()
            }

            return SqlSelect(projection, selection, groupBy, orderBy, having, table.id)
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

    private fun parseIdentifier(): SqlIdentifier {
        val expr = parseExpr() ?: throw SQLException("Expected identifier, found EOF")
        return when (expr) {
            is SqlIdentifier -> expr
            else -> throw SQLException("Expected identifier, found $expr")
        }
    }

    private fun parseOrder(): List<SqlSort> {
        val sortList = mutableListOf<SqlSort>()
        // loop over sorts
        var sort = parseExpr()
        while (sort != null) {
            sort = when (sort) {
                is SqlIdentifier -> SqlSort(sort, true)
                is SqlSort -> sort
                else -> throw IllegalStateException("Unexpected expression $sort after order by.")
            }
            sortList.add(sort)

            // move to next sort
            if (tokens.peek()?.type == Symbol.COMMA) {
                tokens.next()
            } else {
                break
            }
            sort = parseExpr()
        }
        return sortList
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