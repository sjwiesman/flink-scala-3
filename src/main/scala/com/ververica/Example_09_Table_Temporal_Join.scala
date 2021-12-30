package com.ververica

import com.ververica.data.ExampleData
import com.ververica.models.{Transaction, TransactionDeserializer}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.Row

import java.time.ZoneId

/** Perform the materialized view maintenance smarter by using time-versioned
  * joins.
  */
@main def example9 =
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1) // due to little data

  val tableEnv = StreamTableEnvironment.create(env)
  val config = tableEnv.getConfig
  config.setLocalTimeZone(ZoneId.of("UTC"))
  // read transactions
  val transactionSource = KafkaSource
    .builder[Transaction]
    .setBootstrapServers("localhost:9092")
    .setTopics("transactions")
    .setStartingOffsets(OffsetsInitializer.earliest)
    .setValueOnlyDeserializer(new TransactionDeserializer)
    .build

  val transactionStream = env.fromSource(
    transactionSource,
    WatermarkStrategy.noWatermarks,
    "Transactions"
  )
  tableEnv.createTemporaryView(
    "Transactions",
    transactionStream,
    Schema.newBuilder
      .columnByExpression("t_rowtime", "CAST(t_time AS TIMESTAMP_LTZ(3))")
      .watermark("t_rowtime", "t_rowtime - INTERVAL '10' SECONDS")
      .build
  )

  val deduplicateTransactions = tableEnv.sqlQuery(
    """
        |SELECT t_id, t_rowtime, t_customer_id, t_amount
        |FROM (
        |   SELECT *
        |     ROW_NUMBER() OVER (PARTITION BY t_id ORDER BY t_rowtime) AS row_num
        |   FROM Transactions)
        |WHERE row_num = 1
        |""".stripMargin
  )
  tableEnv.createTemporaryView(
    "DeduplicateTransactions",
    deduplicateTransactions
  )
  // use a customer changelog with timestamps
  val customerStream = env
    .fromElements(ExampleData.customers_with_temporal_updates: _*)
    .returns(
      Types.ROW_NAMED(
        Array("c_update_time", "c_id", "c_name", "c_birthday"),
        Types.INSTANT,
        Types.LONG,
        Types.STRING,
        Types.LOCAL_DATE
      )
    )
  // make it a temporal view
  tableEnv.createTemporaryView(
    "Customers",
    tableEnv.fromChangelogStream(
      customerStream,
      Schema.newBuilder
        .columnByExpression(
          "c_rowtime",
          "CAST(c_update_time AS TIMESTAMP_LTZ(3))"
        )
        .primaryKey("c_id")
        .watermark("c_rowtime", "c_rowtime - INTERVAL '10' SECONDS")
        .build,
      ChangelogMode.upsert
    )
  )
  tableEnv
    .executeSql("""
          |SELECT t_rowtime, c_rowtime, t_id, c_name, t_amount
          |FROM DeduplicateTransactions
          |LEFT JOIN Customers FOR SYSTEM_TIME AS OF t_rowtime
          |   ON c_id = t_customer_id
          |""".stripMargin)
    .print()
