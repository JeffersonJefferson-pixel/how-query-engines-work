package org.example.kquery.sql

/** wrapper for list of tokens. */
class TokenStream(val tokens: List<Token>) {

    var i = 0

    fun peek(): Token? {
        if (i < tokens.size) {
            return tokens[i]
        } else {
            return null
        }
    }

    fun next(): Token? {
        if (i < tokens.size) {
            return tokens[i++]
        } else {
            return null
        }
    }

    fun consumeKeyword(s: String): Boolean {
        val peek = peek()
        return if (peek?.type is Keyword && peek.text == s) {
            i++
            true
        } else {
            false
        }
    }
}