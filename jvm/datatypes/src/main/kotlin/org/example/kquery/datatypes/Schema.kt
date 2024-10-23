package org.example.kquery.datatypes

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.pojo.Schema

object KQuerySchemaConverter {
    fun fromArrow(arrowSchema: Schema) : KQuerySchema {
        val fields = arrowSchema.fields.map {  KQueryField(it.name, it.fieldType.type) }
        return KQuerySchema(fields)
    }
}

data class KQuerySchema(val fields: List<KQueryField>) {
    fun toArrow(): Schema {
        return Schema(fields.map { it.toArrow() })
    }

    fun select(names: List<String>) : KQuerySchema {
        val f = mutableListOf<KQueryField>()
        names.forEach { name ->
            val m = fields.filter { it.name == name }
            if (m.size == 1) {
                f.add(m[0])
            } else {
                throw IllegalArgumentException()
            }
        }
        return KQuerySchema(f)
    }
}

data class KQueryField(val name: String, val dataType: ArrowType) {
    fun toArrow(): Field {
        val fieldType = FieldType(true, dataType, null)
        return Field(name, fieldType, listOf())
    }
}
