package org.example.kquery.sql

interface PrattParser {
    fun parse(precedence: Int = 0): SqlExpr? {
        var expr = parsePrefix() ?: return null
        while (precedence < nextPrecedence()) {
            expr = parseInfix(expr, nextPrecedence())
        }
        return expr
    }

    // get precedence of next token.
    fun nextPrecedence(): Int

    // parse the next prefix expression.
    fun parsePrefix(): SqlExpr?

    // parse the next infix expression.
    fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr
}