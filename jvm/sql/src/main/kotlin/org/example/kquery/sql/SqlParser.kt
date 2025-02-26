package org.example.kquery.sql

import jdk.incubator.foreign.SymbolLookup
import java.sql.SQLException

/** implementation of a Pratt Parser for sql */
class SqlParser(val tokens: TokenStream) : PrattParser {
    override fun nextPrecedence(): Int {
        val token = tokens.peek() ?: return 0
        return when (token.type) {
            // math symbols
            Symbol.PLUS, Symbol.SUB -> 50
            Symbol.STAR, Symbol.SLASH -> 60
            else -> 0
        }
    }

    override fun parsePrefix(): SqlExpr? {
        val token = tokens.next() ?: return null
        return when (token.type) {
            Literal.LONG -> SqlLong(token.text.toLong())
            else -> throw IllegalStateException("Unexpected token $token")
        }
    }

    override fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr {
        val token = tokens.peek()!!
        return when (token.type) {
            // math symbols
            Symbol.PLUS, Symbol.SUB, Symbol.STAR, Symbol.SLASH -> {
                tokens.next()
                SqlBinaryExpr(left, token.text, parse(precedence) ?: throw SQLException("Error parsing infix"))
            }
            else -> throw IllegalStateException("Unexpected infix token $token")
        }
    }
}