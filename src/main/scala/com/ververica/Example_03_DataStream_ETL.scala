package com.ververica

import com.ververica.data.ExampleData
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import java.time.{LocalDate, Period}

/** Use arbitrary libraries to perform streaming ETL i.e. convert records into
  * JSON.
  */
@main def example3 =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env
    .fromElements(ExampleData.customers: _*)
    .map(customer =>
      val age = Period
        .between(customer.c_birthday, LocalDate.now())
        .getYears

      s"""
         |{
         |  "name": ${customer.c_name},
         |  "age": $age
         |}
         |""".stripMargin
    )
    .executeAndCollect()
    .forEachRemaining(println);
