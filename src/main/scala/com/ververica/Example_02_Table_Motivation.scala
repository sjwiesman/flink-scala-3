package com.ververica

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

import java.time.LocalDate
import org.apache.flink.table.api.Expressions.row

/** Basic example of generating data and printing it. */
@main def example2 =
  val env = TableEnvironment.create(EnvironmentSettings.inStreamingMode())

  env
    .fromValues(
      row(12L, "Alice", LocalDate.of(1984, 2, 12)),
      row(32L, "Bob", LocalDate.of(1990, 10, 14)),
      row(7L, "Kyle", LocalDate.of(1979, 2, 23))
    )
    .as("c_id", "c_name", "c_birthday")
    .execute()
    .print()
