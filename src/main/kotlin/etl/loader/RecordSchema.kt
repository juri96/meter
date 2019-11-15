package etl.loader

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema

object RecordSchema {

    val schema = TableSchema().also {
        it.fields = listOf(
                TableFieldSchema().setColumn("station", "STRING", "NULLABLE"),
                TableFieldSchema().setColumn("timestamp", "TIMESTAMP", "NULLABLE"),
                TableFieldSchema().setColumn("pm25", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("pm10", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("so2", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("no2", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("co", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("o3", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("temp", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("press", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("dewp", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("rain", "FLOAT64", "NULLABLE"),
                TableFieldSchema().setColumn("wd", "STRING", "NULLABLE"),
                TableFieldSchema().setColumn("wspm", "FLOAT64", "NULLABLE")
        )
    }

    fun toTableRow(record: Record) = TableRow().also {
        it.set("station", record.station)
        it.set("timestamp", Utils.toTimeStampBQFormat(record.timestamp))
        it.set("pm25", record.pm25)
        it.set("pm10", record.pm10)
        it.set("so2", record.so2)
        it.set("no2", record.no2)
        it.set("co", record.co)
        it.set("o3", record.o3)
        it.set("temp", record.temp)
        it.set("press", record.press)
        it.set("dewp", record.dewp)
        it.set("rain", record.rain)
        it.set("wd", record.wd)
        it.set("wspm", record.wspm)
    }
}

private fun TableFieldSchema.setColumn(fieldName: String, fieldType: String, fieldMode: String) = TableFieldSchema().apply {
    name = fieldName
    type = fieldType
    mode = fieldMode
}
