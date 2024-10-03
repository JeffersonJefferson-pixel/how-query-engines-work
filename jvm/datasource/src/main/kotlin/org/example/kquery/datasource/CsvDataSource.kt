package org.example.kquery.datasource

import com.univocity.parsers.common.record.Record
import com.univocity.parsers.csv.CsvParser
import com.univocity.parsers.csv.CsvParserSettings
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.BigIntVector
import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.Float8Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.SmallIntVector
import org.apache.arrow.vector.TinyIntVector
import org.apache.arrow.vector.VarCharVector
import org.apache.arrow.vector.VectorSchemaRoot
import org.example.kquery.datatypes.*
import java.io.File
import java.io.FileNotFoundException

class CsvDataSource(
    val filename: String,
    val schema: Schema?,
    private val hasHeaders: Boolean,
    private val batchSize: Int
) : DataSource {
    private val finalSchema: Schema by lazy { schema ?: inferSchema() }

    override fun schema(): Schema {
        return finalSchema
    }

    override fun scan(projection: List<String>): Sequence<RecordBatch> {
        val file = File(filename)
        if (!file.exists()) {
            throw FileNotFoundException(file.absolutePath)
        }

        val readSchema =
            if (projection.isNotEmpty()) {
                finalSchema.select(projection)
            } else {
                finalSchema
            }

        val settings = defaultSettings()
        if (projection.isNotEmpty()) {
            settings.selectFields(*projection.toTypedArray())
        }
        settings.isHeaderExtractionEnabled = hasHeaders
        if (!hasHeaders) {
            settings.setHeaders(*readSchema.fields.map { it.name }.toTypedArray())
        }

        val parser = buildParser(settings)
        parser.beginParsing(file.inputStream().reader())
        parser.detectedFormat

        return ReaderAsSequence(readSchema, parser, batchSize)
    }

    private fun inferSchema(): Schema {
        val file = File(filename)
        if (!file.exists()) {
            throw FileNotFoundException(file.absolutePath)
        }

        val parser = buildParser(defaultSettings())
        return file.inputStream().use {
            parser.beginParsing(it.reader())
            parser.detectedFormat
            parser.parseNext()

            val headers = parser.context.parsedHeaders().filterNotNull()

            val schema =
                if (hasHeaders) {
                    Schema(headers.map { colName -> Field(colName, ArrowTypes.StringType) })
                } else {
                    Schema(List(headers.size) { i -> Field("field_${i + 1}", ArrowTypes.StringType) })
                }

            parser.stopParsing()
            schema
        }
    }


    private fun defaultSettings(): CsvParserSettings {
        return CsvParserSettings().apply {
            isDelimiterDetectionEnabled = true
            isLineSeparatorDetectionEnabled = true
            skipEmptyLines = true
            isAutoClosingEnabled = true
        }
    }

    private fun buildParser(settings: CsvParserSettings): CsvParser {
        return CsvParser(settings);
    }

    class ReaderAsSequence(
        private val schema: Schema, private val parser: CsvParser, private val batchSize: Int
    ) : Sequence<RecordBatch> {
        override fun iterator(): Iterator<RecordBatch> {
            return ReaderIterator(schema, parser, batchSize)
        }
    }

    class ReaderIterator(
        private val schema: Schema, private val parser: CsvParser, private val batchSize: Int
    ) : Iterator<RecordBatch> {
        private var next: RecordBatch? = null
        private var started: Boolean = false

        override fun hasNext(): Boolean {
            if (!started) {
                started = true

                next = nextBatch()
            }

            return next != null
        }

        override fun next(): RecordBatch {
            if (!started) {
                hasNext()
            }

            val out = next

            next = nextBatch()

            if (out == null) {
                throw NoSuchElementException("Cannot read past the end of ${ReaderIterator::class.simpleName}")
            }

            return out
        }

        private fun nextBatch(): RecordBatch? {
            val rows = ArrayList<Record>(batchSize)

            do {
                val line = parser.parseNextRecord()
                if (line != null) rows.add(line)
            } while (line != null && rows.size < batchSize)

            if (rows.isEmpty()) {
                return null
            }

            return createBatch(rows)
        }

        private fun createBatch(rows: ArrayList<Record>): RecordBatch {
            val root = VectorSchemaRoot.create(schema.toArrow(), RootAllocator(Long.MAX_VALUE))
            root.fieldVectors.forEach { it.setInitialCapacity(rows.size) }
            root.allocateNew()

            root.fieldVectors.withIndex().forEach { field ->
                val vector = field.value
                when (vector) {
                    is TinyIntVector ->
                        rows.withIndex().forEach { row ->
                            val valueStr = row.value.getValue(vector.name, "").trim()
                            if (valueStr.isEmpty()) {
                                vector.setNull(row.index)
                            } else {
                                vector.set(row.index, valueStr.toByte())
                            }
                        }
                    is SmallIntVector ->
                        rows.withIndex().forEach { row ->
                            val valueStr = row.value.getValue(vector.name, "").trim()
                            if (valueStr.isEmpty()) {
                                vector.setNull(row.index)
                            } else {
                                vector.set(row.index, valueStr.toShort())
                            }
                        }
                    is IntVector ->
                        rows.withIndex().forEach { row ->
                            val valueStr = row.value.getValue(vector.name, "").trim()
                            if (valueStr.isEmpty()) {
                                vector.setNull(row.index)
                            } else {
                                vector.set(row.index, valueStr.toInt())
                            }
                        }
                    is BigIntVector ->
                        rows.withIndex().forEach { row ->
                            val valueStr = row.value.getValue(vector.name, "")
                            if (valueStr.isEmpty()) {
                                vector.setNull(row.index)
                            } else {
                                vector.set(row.index, valueStr.toLong())
                            }
                        }
                    is Float4Vector ->
                        rows.withIndex().forEach { row ->
                            val valueStr = row.value.getValue(vector.name, "")
                            if (valueStr.isEmpty()) {
                                vector.setNull(row.index)
                            } else {
                                vector.set(row.index, valueStr.toFloat())
                            }
                        }
                    is Float8Vector ->
                        rows.withIndex().forEach { row ->
                            val valueStr = row.value.getValue(vector.name, "")
                            if (valueStr.isEmpty()) {
                                vector.setNull(row.index)
                            } else {
                                vector.set(row.index, valueStr.toDouble())
                            }
                        }
                    is VarCharVector ->
                        rows.withIndex().forEach { row ->
                            val valueStr = row.value.getValue(vector.name, "")
                            vector.setSafe(row.index, valueStr.toByteArray())
                        }
                    else ->
                        throw IllegalStateException("No support for reading CSV columns with data type $vector")
                }
                vector.valueCount = rows.size
            }

            return RecordBatch(schema, root.fieldVectors.map { ArrowFieldVector(it) })
        }
    }
}