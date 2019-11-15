package etl.loader

import com.google.api.services.bigquery.model.TableReference
import com.google.api.services.bigquery.model.TableRow
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.transforms.Filter
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ProcessFunction
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors

typealias FileLine = String
typealias SpitedFileLine = List<String>

object PipelineSteps {
    private const val headers = """"No","year","month","day","hour","PM2.5","PM10","SO2","NO2","CO","O3","TEMP","PRES","DEWP","RAIN","wd","WSPM","station""""

    fun filterHeaderRow(): Filter<String> = Filter.by(ProcessFunction<String, Boolean> { !it.startsWith(headers) })

    fun writeToBigQuery(resultTableSpec: TableReference): BigQueryIO.Write<TableRow> {
        return BigQueryIO.writeTableRows()
                .to(resultTableSpec)
                .withSchema(RecordSchema.schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
    }

    fun splitCsvLine(): MapElements<FileLine, SpitedFileLine> {
        return MapElements.into(
                TypeDescriptors.lists(
                        TypeDescriptor.of(String::class.java)
                )
        )
                .via(ProcessFunction<FileLine, SpitedFileLine> { it.split(",") })
    }

    fun mapEntityToTableRow(): MapElements<Record, TableRow> {
        return MapElements.into(TypeDescriptor.of(TableRow::class.java))
                .via(ProcessFunction { RecordSchema.toTableRow(it) })
    }

    fun mapElementsToEntity(): MapElements<SpitedFileLine, Record> {
        return MapElements.into(TypeDescriptor.of(Record::class.java))
                .via(ProcessFunction<SpitedFileLine, Record> { r ->
                    Record(timestamp = Utils.csvToLocalDateTime(r.subList(1, 5)),
                            pm25 = r.getDoubleOrNull(5),
                            pm10 = r.getDoubleOrNull(6),
                            so2 = r.getDoubleOrNull(7),
                            no2 = r.getDoubleOrNull(8),
                            co = r.getDoubleOrNull(9),
                            o3 = r.getDoubleOrNull(10),
                            temp = r.getDoubleOrNull(11),
                            press = r.getDoubleOrNull(12),
                            dewp = r.getDoubleOrNull(13),
                            rain = r.getDoubleOrNull(14),
                            wd = r.getOrNull(15),
                            wspm = r.getDoubleOrNull(16),
                            station = r.getOrNull(17)
                    )
                })
    }
}

private fun List<String>.getDoubleOrNull(i: Int) = getOrNull(i)?.toDoubleOrNull()
