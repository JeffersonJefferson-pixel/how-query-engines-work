package org.example.kquery.logicalplan

import org.example.kquery.datatypes.KQuerySchema

/**
 * A logical plan represents a relation with a known schema.
 * It can have zero or more logical plans as inputs.
 */
interface LogicalPlan {
    /** Returns schema that will be produced by this logical plan. */
    fun schema(): KQuerySchema
    /** Returns the children (inputs) of this logical plan. */
    fun children(): List<LogicalPlan>

    fun pretty(): String {
        return format(this)
    }
}

/** Format logical plan in human-readable form. */
fun format(plan: LogicalPlan, ident: Int = 0): String {
    val b = StringBuilder()
    0.until(ident).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, ident + 1)) }
    return b.toString()
}