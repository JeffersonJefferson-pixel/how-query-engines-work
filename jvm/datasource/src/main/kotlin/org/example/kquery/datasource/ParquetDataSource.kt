package org.example.kquery.datasource

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.*
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.arrow.schema.SchemaConverter
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.RecordReader
import org.example.kquery.datatypes.ArrowFieldVector
import org.example.kquery.datatypes.KQuerySchema
import org.example.kquery.datatypes.KQuerySchemaConverter
import org.example.kquery.datatypes.RecordBatch


class ParquetDataSource(private val filename: String) : DataSource {
    override fun schema(): KQuerySchema {
        return ParquetScan(filename, listOf()).use {
            val arrowSchema = SchemaConverter().fromParquet(it.schema).arrowSchema
            KQuerySchemaConverter.fromArrow(arrowSchema)
        }
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        return ParquetScan(filename, projection)
    }

}

class ParquetScan(
    filename: String,
    private val columns: List<String>
) : AutoCloseable, Sequence<RecordBatch> {
    private val reader = ParquetFileReader.open(HadoopInputFile.fromPath(Path(filename), Configuration()))
    val schema = reader.footer.fileMetaData.schema

    override fun iterator(): Iterator<RecordBatch> {
        return ParquetIterator(reader, columns)
    }

    override fun close() {
        reader.close()
    }
}

class ParquetIterator(
    private val reader: ParquetFileReader,
    private val projectedColumns: List<String>
) : Iterator<RecordBatch> {
    val schema = reader.footer.fileMetaData.schema

    val arrowSchema = SchemaConverter().fromParquet(schema).arrowSchema

    val projectedArrowSchema = Schema(projectedColumns.map { name -> arrowSchema.fields.find { it.name == name } })

    var batch: RecordBatch? = null

    override fun hasNext(): Boolean {
        batch = nextBatch()
        return batch != null
    }

    override fun next(): RecordBatch {
        val next = batch
        batch = null
        return next!!
    }

    private fun nextBatch(): RecordBatch? {
        val pages = reader.readNextRowGroup() ?: return null

        if (pages.rowCount > Integer.MAX_VALUE) {
            throw IllegalStateException()
        }

        val rows = pages.rowCount.toInt()
        println("Reading $rows rows")

        val groups = ArrayList<Group>(rows)
        val columnIO = ColumnIOFactory().getColumnIO(schema)
        val recordReader = columnIO.getRecordReader(pages, GroupRecordConverter(schema))

        for (i in 0 until rows) {
            groups.add(recordReader.read())
        }

        val kQuerySchema = KQuerySchemaConverter.fromArrow(projectedArrowSchema)

        val root = VectorSchemaRoot.create(projectedArrowSchema, RootAllocator(Long.MAX_VALUE))
        root.allocateNew()
        root.rowCount = groups.size

        batch = RecordBatch(kQuerySchema, root.fieldVectors.map { ArrowFieldVector(it) })

        root.fieldVectors.withIndex().forEach { field ->
            val vector = field.value
            when (vector) {
                is TinyIntVector ->
                    groups.withIndex().forEach { row ->
                        val valueStr = row.value.getInteger(vector.name, 0)
                        vector.set(row.index, valueStr.toByte())
                    }
                is SmallIntVector ->
                    groups.withIndex().forEach { row ->
                        val value = row.value.getInteger(vector.name, 0)
                        vector.set(row.index, value.toShort())
                    }
                is IntVector ->
                    groups.withIndex().forEach { row ->
                        val value = row.value.getInteger(vector.name, 0)
                        vector.set(row.index, value)
                    }
                is BigIntVector ->
                    groups.withIndex().forEach { row ->
                        val value = row.value.getLong(vector.name, 0)
                        vector.set(row.index, value)
                    }
                is Float4Vector ->
                    groups.withIndex().forEach { row ->
                        val value = row.value.getFloat(vector.name, 0)
                        vector.set(row.index, value.toFloat())
                    }
                is Float8Vector ->
                    groups.withIndex().forEach { row ->
                        val value = row.value.getDouble(vector.name, 0)
                        vector.set(row.index, value.toDouble())
                    }
                is VarCharVector ->
                    groups.withIndex().forEach { row ->
                        val value = row.value.getString(vector.name, 0)
                        vector.setSafe(row.index, value.toByteArray())
                    }
                else ->
                    throw IllegalStateException("No support for reading CSV columns with data type $vector")
            }
            vector.valueCount = groups.size
        }

        return batch
    }
}