package org.example.kquery.logicalplan

import org.example.kquery.datatypes.KQueryField

/**
 * Logical expression that can be evaluated against some data.
 */
interface LogicalExpr {
    /**
     * Return metadata about value that is produced by the expression when evaluated
     * against an input.
     */
    fun toField(input: LogicalPlan): KQueryField
}