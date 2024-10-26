package org.example.kquery.physicalplan

import org.example.kquery.datatypes.KQuerySchema
import org.example.kquery.datatypes.RecordBatch

interface PhysicalPlan {
    fun schema(): KQuerySchema
    fun execute(): Sequence<RecordBatch>
    fun children(): List<PhysicalPlan>
}