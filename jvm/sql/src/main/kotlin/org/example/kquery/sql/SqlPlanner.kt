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

        val aggregateExprCount = projectionExpr.count { isAggregateExpr(it) }
        if (aggregateExprCount == 0 && select.groupBy.isNotEmpty()) {
            throw SQLException("GROUP BY without aggregate expressions is not supported")
        }

        var plan = df

        if (aggregateExprCount == 0) {
            return planNonAggregatedQuery(select, df, projectionExpr)
        } else {
            plan = planAggregatedQuery(select, plan, projectionExpr)
        }
        return plan
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
            is SqlFunction ->
                when (expr.id) {
                    Keyword.MAX.name -> {
                        return Max(createLogicalExpr(expr.args.first(), input))
                    }
                    else -> throw SQLException("Invalid aggregate function $expr")
                }
            else -> throw UnsupportedOperationException()
        }
    }

    private fun planNonAggregatedQuery(
        select: SqlSelect,
        df: DataFrame,
        projectionExpr: List<LogicalExpr>
    ): DataFrame {
        var plan = df

        if (select.selection != null) {
            // handle case with selection
            val columnNamesInSelection = getReferencedColumnsBySelection(select, df)

            val columnNamesInProjection = getReferencedColumns(projectionExpr)

            val missing = (columnNamesInSelection - columnNamesInProjection)

            plan = planProjectAndFilter(select, plan, projectionExpr, missing)

            if (missing.isNotEmpty()) {
                // handle case with missing
                val n = projectionExpr.size
                // drop column that are added from selection
                val expr = (0 until n).map { i -> Column(plan.schema().fields[i].name) }
                plan = plan.project(expr)
            }
        } else {
            plan = plan.project(projectionExpr)
        }

        return plan
    }

    private fun planAggregatedQuery(
        select: SqlSelect,
        df: DataFrame,
        projectionExpr: List<LogicalExpr>
    ): DataFrame {
        val projection = mutableListOf<LogicalExpr>()
        val aggregateExpr = mutableListOf<AggregateExpr>()
        val numGroupCols = select.groupBy.size
        var groupCount = 0

        // loop projection expression
        projectionExpr.forEach { expr ->
            when (expr) {
                // handle aggregate in projection
                is AggregateExpr -> {
                    projection.add(ColumnIndex(numGroupCols + aggregateExpr.size))
                    aggregateExpr.add(expr)
                }
                // expression other than aggregate must be column in group by
                else -> {
                    projection.add(ColumnIndex(groupCount))
                    groupCount += 1
                }
            }
        }

        var plan = df
        val projectionWithoutAggregates = projectionExpr.filterNot { it is AggregateExpr}

        if (select.selection != null) {
            // handle case with selection
            val columnNamesInSelection = getReferencedColumnsBySelection(select, df)

            val columnNamesInProjection = getReferencedColumns(projectionExpr)

            val missing = (columnNamesInSelection - columnNamesInProjection)

            plan = planProjectAndFilter(select, plan, projectionWithoutAggregates, missing)
        }

        val groupByExpr = select.groupBy.map {createLogicalExpr(it, plan)}
        plan = plan.aggregate(groupByExpr, aggregateExpr)

        return plan.project(projection)
    }

    private fun planProjectAndFilter(
        select: SqlSelect,
        df: DataFrame,
        projectionExpr: List<LogicalExpr>,
        missing: Set<String>
    ): DataFrame {
        // only for sql select with selection
        var plan = df

        if (missing.isEmpty()) {
            // projection and selection have same set of columns
            plan = plan.project(projectionExpr)
            plan = plan.filter(createLogicalExpr(select.selection!!, plan))
        } else {
            // project with column in selection that are missing in original projection
            plan = plan.project(projectionExpr + missing.map { Column(it) })
            plan = plan.filter(createLogicalExpr(select.selection!!, plan))
        }

        return plan
    }

    private fun getReferencedColumnsBySelection(select: SqlSelect, table: DataFrame): Set<String> {
        val accumulator = mutableSetOf<String>()
        if (select.selection != null) {
            val filterExpr = createLogicalExpr(select.selection, table)
            visit(filterExpr, accumulator)
            val validColumnNames = table.schema().fields.map { it.name }
            accumulator.removeIf { name -> !validColumnNames.contains(name) }
        }
        return accumulator
    }

    private fun getReferencedColumns(exprs: List<LogicalExpr>): Set<String> {
        val accumulator = mutableSetOf<String>()
        exprs.forEach { visit(it, accumulator) }
        return accumulator
    }

    private fun isAggregateExpr(expr: LogicalExpr): Boolean {
        return expr is AggregateExpr
    }

    private fun visit(expr: LogicalExpr, accumulator: MutableSet<String>) {
        when (expr) {
            is Column -> accumulator.add(expr.name)
            is Alias -> visit(expr.expr, accumulator)
            is BinaryExpr -> {
                visit(expr.l, accumulator)
                visit(expr.r, accumulator)
            }
            is AggregateExpr -> visit(expr.expr, accumulator)
        }
    }
}