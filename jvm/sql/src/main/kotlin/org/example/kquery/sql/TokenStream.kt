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
}