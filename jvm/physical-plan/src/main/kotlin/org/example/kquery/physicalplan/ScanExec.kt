package org.example.kquery.physicalplan

import org.example.kquery.datasource.DataSource
import org.example.kquery.datatypes.KQuerySchema
import org.example.kquery.datatypes.RecordBatch

/**
 * Scan execution plan simply delegates to a date source.
 */
class ScanExec(val ds: DataSource, val projection: List<String>) : PhysicalPlan {
    override fun schema(): KQuerySchema {
        return ds.schema().select(projection)
    }

    override fun execute(): Sequence<RecordBatch> {
        return ds.scan(projection)
    }

    override fun children(): List<PhysicalPlan> {
        // scan is a leaf node and has no child plans.
        return listOf()
    }

    override fun toString(): String {
        return "ScanExec: schema=${schema()}, projection=$projection"
    }
}