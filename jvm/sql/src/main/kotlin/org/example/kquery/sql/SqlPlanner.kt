package org.example.kquery.sql

import org.example.kquery.logicalplan.*
import java.sql.SQLException
import java.util.concurrent.atomic.LongAccumulator

/** Sql planner convert sql query tree into a logical plan. */
class SqlPlanner {
    fun createDataFrame(select: SqlSelect, tables: Map<String, DataFrame>) : DataFrame {
        var df = tables[select.tableName] ?: throw SQLException("No table named '${select.tableName}'")

        // convert sql projection to logical projection
        val projectionExpr = select.projection.map { createLogicalExpr(it, df) }

        if (select.selection == null) {
            return df.project(projectionExpr)
        }

        // create logical filter expression
        val filterExpr = createLogicalExpr(select.selection, df)

        val columnsInProjection = projectionExpr.map { it.toField(df.logicalPlan()).name }
            .toSet()

        val columnNames = mutableSetOf<String>()
        visit(filterExpr, columnNames)

        val missing = columnNames - columnsInProjection

        if (missing.isEmpty()) {
            return df.project(projectionExpr)
                .filter(filterExpr)
        }

        // add filter columns to projection, then filter, then project only required columns
        return df.project(projectionExpr + missing.map { Column(it) })
            .filter(filterExpr)
            .project(projectionExpr.map {
                Column(it.toField(df.logicalPlan()).name)
            })
    }

    private fun createLogicalExpr(expr: SqlExpr, input: DataFrame) : LogicalExpr {
        return when (expr) {
            is SqlIdentifier -> Column(expr.id)
            is SqlString -> LiteralString(expr.value)
            is SqlLong -> LiteralLong(expr.value)
            is SqlBinaryExpr -> {
                val l = createLogicalExpr(expr.l, input)
                val r = createLogicalExpr(expr.r, input)
                when (expr.op) {
                    Symbol.PLUS.text -> Add(l, r)
                    Symbol.SUB.text -> Subtract(l, r)
                    Symbol.STAR.text -> Multiply(l, r)
                    Symbol.SLASH.text -> Divide(l, r)
                    Symbol.EQ.text -> Eq(l, r)
                    else -> throw SQLException("Invalid operator ${expr.op}")
                }
            }

            else -> throw UnsupportedOperationException()
        }
    }

    private fun visit(expr: LogicalExpr, accumulator: MutableSet<String>) {
        when (expr) {
            is Column -> accumulator.add(expr.name)
            is Alias -> visit(expr.expr, accumulator)
            is BinaryExpr -> {
                visit(expr.l, accumulator)
                visit(expr.r, accumulator)
            }
        }
    }
}