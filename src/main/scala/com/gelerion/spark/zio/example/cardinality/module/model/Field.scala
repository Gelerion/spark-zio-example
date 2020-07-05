package com.gelerion.spark.zio.example.cardinality.module.model

import org.apache.spark.sql.DataFrame
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

case class Field(name: String, replacement: String, partitionBy: Seq[String])

case class FieldData(field: Field, loadDate: LoadDate, data: DataFrame)

case class LoadDate(dt: String, pattern: String = "yyyy-MM-dd") {
  private val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)
  def asMillis(): Long = DateTime.parse(dt, formatter).withZone(UTC).getMillis
}

case class GrayList(field: Field, data: DataFrame)

case class LoadRangeSpec(basePath: String, startDate: ProcessDate, timeRange: Int = 0) {
  def dates: Seq[LoadDate] = startDate.minusDays(timeRange)
}

case class ProcessDate(dt: String, pattern: String = "yyyy-MM-dd") {
  private val formatter: DateTimeFormatter = DateTimeFormat.forPattern(pattern)

  def minusDays(timeRange : Int): Seq[LoadDate] = {
    (0 until timeRange).map(DateTime.parse(dt, formatter).minusDays).map(date => LoadDate(date.toString(formatter)))
  }
}

case class LoadFieldSpec(field: Field, loadDate: LoadDate, basePath: String) {
  def fullPath: String = basePath + s"/${loadDate.dt}/${field.name})"
}