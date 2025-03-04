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

    fun consumeKeyword(k: Keyword): Boolean {
        val peek = peek()
        return if (peek?.type == k) {
            i++
            true
        } else {
            false
        }
    }

    fun consumeKeywords(keywords: List<Keyword>): Boolean {
        val save = i
        for (k in keywords) {
            if (!consumeKeyword(k)) {
                i = save
                return false
            }
        }

        return true
    }

    fun consumeSymbol(s: Symbol): Boolean {
        val peek = peek()
        return if (peek?.type is Symbol && peek.text == s.text) {
            i++
            true
        } else {
            false
        }
    }
}