package org.example.kquery.sql

class SqlTokenizer(val sql: String) {
    var offset = 0

    // convert query string into a token stream
    fun tokenize(): TokenStream {
        var token = nextToken()
        val list = mutableListOf<Token>()
        while (token != null) {
            list.add(token)
            token = nextToken()
        }
        return TokenStream(list)
    }

    private fun nextToken(): Token? {
        offset = skipWhitespace(offset)
        var token: Token? = null
        when {
            offset >= sql.length -> {
                return token
            }
            Literal.isIdentifierStart(sql[offset]) -> {
                token = scanIdentifier(offset)
                offset = token.endOffset
            }
            Literal.isNumberStart(sql[offset]) -> {
                token = scanNumber(offset)
                offset = token.endOffset
            }
            Symbol.isSymbolStart(sql[offset]) -> {
                token = scanSymbol(offset)
                offset = token.endOffset
            }
            Literal.isCharsStart(sql[offset]) -> {
                token = scanChars(offset, sql[offset])
                offset = token.endOffset
            }
        }
        return token
    }

    private fun skipWhitespace(startOffset: Int): Int {
        return sql.indexOfFirst(startOffset) { ch -> !ch.isWhitespace() }
    }

    private fun scanNumber(startOffset: Int): Token {
        var endOffset = if ('-' == sql[startOffset]) {
            // negative number
            sql.indexOfFirst(startOffset + 1) { ch -> !ch.isDigit() }
        } else {
            sql.indexOfFirst(startOffset) {ch -> !ch.isDigit() }
        }

        return Token(sql.substring(startOffset, endOffset), Literal.LONG, endOffset)
    }

    private fun scanIdentifier(startOffset: Int): Token {
        val endOffset = sql.indexOfFirst(startOffset) { ch -> !Literal.isIdentifierPart(ch) }
        val text = sql.substring(startOffset, endOffset)
        val tokenType: TokenType = Keyword.textOf(text) ?: Literal.IDENTIFIER
        return Token(text, tokenType, endOffset)
    }

    private fun scanSymbol(startOffset: Int): Token {
        var endOffset = sql.indexOfFirst(startOffset) { ch -> !Symbol.isSymbol(ch) }
        var text = sql.substring(offset, endOffset)
        var symbol: Symbol?
        while (null == Symbol.textOf(text).also { symbol = it }) {
            text = sql.substring(offset, --endOffset)
        }
        return Token(text, symbol ?: throw TokenizeException("$text must be a Symbol!"), endOffset)
    }

    private fun scanChars(startOffset: Int, terminatedChar: Char): Token {
        val endOffset = getOffsetUntilTerminatedChar(terminatedChar, startOffset + 1)
        return Token(sql.substring(startOffset + 1, endOffset), Literal.STRING, endOffset + 1)
    }

    private fun getOffsetUntilTerminatedChar(terminatedChar: Char, startOffset: Int): Int {
        val offset = sql.indexOf(terminatedChar, startOffset)
        return if (offset != -1) offset else
            throw TokenizeException("Must contain $terminatedChar in remain sql[$startOffset .. end]")
    }

    private inline fun CharSequence.indexOfFirst(startIndex: Int = 0, predicate: (Char) -> Boolean): Int {
        for (index in startIndex until this.length) {
            if (predicate(this[index])) {
                return index
            }
        }
        return sql.length
    }
}

class TokenizeException(val msg: String) : Throwable()