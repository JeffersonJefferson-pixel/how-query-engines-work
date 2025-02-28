package org.example.kquery.sql

interface SqlExpr

/** sql identifier such as table or column name */
data class SqlIdentifier(val id: String) : SqlExpr {
    override fun toString() = id
}

/** sql binary expression */
data class SqlBinaryExpr(val l: SqlExpr, val op: String, val r: SqlExpr) : SqlExpr {
    override fun toString() = "$l $op $r"
}


/** sql literal string */
data class SqlString(val value: String) : SqlExpr {
    override fun toString() = "'$value'"
}

interface SqlRelation : SqlExpr

/** sql long literal */
data class SqlLong(val value: Long) : SqlExpr {
    override fun toString() = "$value"
}

/** single table query with a projection and selection. */
data class SqlSelect(
    val projection: List<SqlExpr>,
    val selection: SqlExpr?,
    val tableName: String
) : SqlRelation

