package org.example.kquery.logicalplan

import org.example.kquery.datasource.DataSource
import org.example.kquery.datatypes.Schema

/** Scan logical plan represents fetching data from a Data source.
 * It does not have another logical plan as an input.
 * It is a leaf node in query tree.
 */
class Scan(
    val path: String,
    val dataSource: DataSource,
    val projection: List<String>
): LogicalPlan {
    val schema = deriveSchema()

    override fun schema(): Schema {
        return schema
    }

    private fun deriveSchema(): Schema {
        val schema = dataSource.schema()
        if (projection.isEmpty()) {
            return schema
        } else {
            return schema.select(projection)
        }
    }

    override fun children(): List<LogicalPlan> {
        return listOf()
    }

    override fun toString(): String {
        return if (projection.isEmpty()) {
            "Scan: $path; projection=None"
        } else {
            "Scan: $path; projection=$projection"
        }
    }
}