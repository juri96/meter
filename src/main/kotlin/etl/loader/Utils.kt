package etl.loader

import java.time.DateTimeException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Utils {

    private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

    fun toTimeStampBQFormat(timestamp: LocalDateTime?): String? {
        return try {
            timestamp?.format(dateTimeFormatter)
        } catch (e: DateTimeException) {
            null
        }
    }

    fun csvToLocalDateTime(r: List<String>): LocalDateTime? {
        return try {
            val anyTimestampPartIsNull = (1..3).any { r.getOrNull(it) == null }
            if (anyTimestampPartIsNull) {
                return null
            }
            LocalDateTime.of(r[0].toInt(), r[1].toInt(), r[2].toInt(), r[3].toInt(), 0)
        } catch (e: DateTimeException) {
            null
        }
    }
}
