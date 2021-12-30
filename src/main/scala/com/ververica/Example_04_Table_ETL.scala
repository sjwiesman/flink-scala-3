package com.ververica

import org.apache.flink.table.api.{
  EnvironmentSettings,
  JsonOnNull,
  TableEnvironment
}
import org.apache.flink.table.api.Expressions.*
import org.apache.flink.table.expressions.TimePointUnit

import java.time.LocalDate

/** Use built-in functions to perform streaming ETL i.e. convert records into
  * JSON.
  */
@main def example4 =
  val env = TableEnvironment.create(EnvironmentSettings.inStreamingMode)
  env
    .fromValues(
      row(12L, "Alice", LocalDate.of(1984, 3, 12)),
      row(32L, "Bob", LocalDate.of(1990, 10, 14)),
      row(7L, "Kyle", LocalDate.of(1979, 2, 23))
    )
    .as("c_id", "c_name", "c_birthday")
    .select(
      jsonObject(
        JsonOnNull.NULL,
        "name",
        $("c_name"),
        "age",
        timestampDiff(TimePointUnit.YEAR, $("c_birthday"), currentDate())
      )
    )
    .execute
    .print()
