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
    SCHEMA;

    companion object {
        private val keywords = entries.associateBy(Keyword::name)
        fun textOf(text: String) = keywords[text.uppercase()]
    }
}

enum class Symbol(val text: String) : TokenType {
    PLUS("+"),
    SUB("-"),
    STAR("*"),
    SLASH("/");
}

data class Token(
    val text: String,
    val type: TokenType,
    val endOffset: Int
) {

}