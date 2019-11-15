package etl.loader

import java.io.Serializable
import java.time.LocalDateTime

data class Record(val station: String?,
                  val timestamp: LocalDateTime?,
                  val pm25: Double?,
                  val pm10: Double?,
                  val so2: Double?,
                  val no2: Double?,
                  val co: Double?,
                  val o3: Double?,
                  val temp: Double?,
                  val press: Double?,
                  val dewp: Double?,
                  val rain: Double?,
                  val wd: String?,
                  val wspm: Double?) : Serializable
