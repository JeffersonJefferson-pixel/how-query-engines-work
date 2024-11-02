package org.example.kquery.physicalplan

import org.example.kquery.datatypes.KQuerySchema
import org.example.kquery.datatypes.RecordBatch

interface PhysicalPlan {
    fun schema(): KQuerySchema
    fun execute(): Sequence<RecordBatch>
    fun children(): List<PhysicalPlan>

    fun pretty(): String {
        return format(this)
    }
}

private fun format(plan: PhysicalPlan, indent: Int = 0): String {
    val b = StringBuilder()
    0.until(indent).forEach { b.append("\t") }
    b.append(plan.toString()).append("\n")
    plan.children().forEach { b.append(format(it, indent + 1)) }
    return b.toString()
}