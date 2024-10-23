package org.example.kquery.datasource

import org.example.kquery.datatypes.RecordBatch
import org.example.kquery.datatypes.KQuerySchema

class InMemoryDataSource(val schema: KQuerySchema, val data: List<RecordBatch>) : DataSource {
    override fun schema(): KQuerySchema {
        return schema
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        val projectionIndices = projection.map { name -> schema.fields.indexOfFirst { it.name == name }  }

        return data.asSequence().map { batch ->
            RecordBatch(schema, projectionIndices.map {i -> batch.field(i) })
        }
    }
}