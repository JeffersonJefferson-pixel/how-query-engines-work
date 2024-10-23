package org.example.kquery.datasource

import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ParquetDataSourceTest {
    val dir = "../testdata"

    @Test
    fun `read parquet schema`() {
        val parquet = ParquetDataSource(File(dir, "alltypes_plain.parquet").absolutePath)
        assertEquals(
            "KQuerySchema(fields=[KQueryField(name=id, dataType=Int(32, true)), KQueryField(name=bool_col, dataType=Bool), KQueryField(name=tinyint_col, dataType=Int(32, true)), KQueryField(name=smallint_col, dataType=Int(32, true)), KQueryField(name=int_col, dataType=Int(32, true)), KQueryField(name=bigint_col, dataType=Int(64, true)), KQueryField(name=float_col, dataType=FloatingPoint(SINGLE)), KQueryField(name=double_col, dataType=FloatingPoint(DOUBLE)), KQueryField(name=date_string_col, dataType=Binary), KQueryField(name=string_col, dataType=Binary), KQueryField(name=timestamp_col, dataType=Binary)])",
            parquet.schema().toString())
    }

    @Test
    fun `read parquet file`() {
        val parquet = ParquetDataSource(File(dir, "alltypes_plain.parquet").absolutePath)
        val it = parquet.scan(listOf("id", "bool_col", "string_col")).iterator()
        assertTrue(it.hasNext())

        val batch = it.next()
        assertEquals(3, batch.schema.fields.size)
        assertEquals(8, batch.field(0).size())

        val id = batch.field(0)
        val values = (0..id.size()).map { id.getValue(it) ?: "null" }
        assertEquals("4,5,6,7,2,3,0,1,null", values.joinToString(","))

        val boolCol = batch.field(1)
        val boolValues = (0..boolCol.size()).map { boolCol.getValue(it) ?: "null" }
        assertEquals("true,false,true,false,true,false,true,false,null", boolValues.joinToString(","))

        val stringCol = batch.field(2)
        val stringValues = (0..boolCol.size()).map { stringCol.getValue(it) ?: "null" }
        assertEquals("0,1,0,1,0,1,0,1,null", stringValues.joinToString(","))


        assertFalse(it.hasNext())
    }
}