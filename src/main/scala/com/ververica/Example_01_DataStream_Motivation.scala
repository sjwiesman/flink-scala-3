package com.ververica

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.ververica.data.ExampleData

/** Basic example of generating data and printing it. */
@main def example1 =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env
    .fromElements(ExampleData.customers: _*)
    .executeAndCollect
    .forEachRemaining(println)
