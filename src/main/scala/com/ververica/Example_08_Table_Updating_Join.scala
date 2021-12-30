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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.Row

/** Maintain a materialized view. */
@main def example8 =
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv = StreamTableEnvironment.create(env)
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
  tableEnv.createTemporaryView("Transactions", transactionStream)
  // use a customer changelog
  val customerStream = env
    .fromElements(ExampleData.customers_with_updates: _*)
    .returns(
      Types.ROW_NAMED(
        Array("c_id", "c_name", "c_birthday"),
        Types.LONG,
        Types.STRING,
        Types.LOCAL_DATE
      )
    )
  // make it an updating view
  tableEnv.createTemporaryView(
    "Customers",
    tableEnv.fromChangelogStream(
      customerStream,
      Schema.newBuilder.primaryKey("c_id").build,
      ChangelogMode.upsert
    )
  )
  // query the changelog backed view
  // and thus perform materialized view maintenance
  tableEnv
    .executeSql(
      """
        |SELECT c_name, CAST(t_amount AS DECIMAL(5, 2))
        |FROM Customers
        |JOIN (SELECT DISTINCT * FROM Transactions) ON c_id = t_customer_id
        |""".stripMargin
    )
    .print()
