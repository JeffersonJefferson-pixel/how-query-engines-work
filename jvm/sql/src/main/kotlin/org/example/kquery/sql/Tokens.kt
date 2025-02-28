package org.example.kquery.sql

interface TokenType

enum class Literal : TokenType {
    LONG,
    DOUBLE,
    STRING,
    IDENTIFIER;

    companion object {
        fun isNumberStart(ch: Char): Boolean {
            return ch.isDigit() || '.' == ch
        }

        fun isIdentifierStart(ch: Char): Boolean {
            return ch.isLetter()
        }

        fun isIdentifierPart(ch: Char): Boolean {
            return ch.isLetter() || ch.isDigit() || ch == '_'
        }

    }
}

enum class Keyword : TokenType {
    SCHEMA,
    SELECT,
    FROM;

    companion object {
        private val keywords = values().associateBy(Keyword::name)
        fun textOf(text: String) = keywords[text.uppercase()]
    }
}

enum class Symbol(val text: String) : TokenType {
    COMMA(","),
    PLUS("+"),
    SUB("-"),
    STAR("*"),
    SLASH("/");

    companion object {
        private val symbols = values().associateBy(Symbol::text)
        private val symbolStartSet = values().flatMap { it.text.toList() }.toSet()
        fun textOf(text: String) = symbols[text]
        fun isSymbol(ch: Char): Boolean {
            return symbolStartSet.contains(ch)
        }

        fun isSymbolStart(ch: Char): Boolean {
            return isSymbol(ch)
        }
    }
}

data class Token(
    val text: String,
    val type: TokenType,
    val endOffset: Int
) {

}