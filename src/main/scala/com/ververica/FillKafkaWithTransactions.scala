package com.ververica

import com.ververica.data.ExampleData
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Schema, TableDescriptor}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

/** Utility for writing exmple transaction data into a Kafka topic
 */
@main def fillKafkaWithTransactions =
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv = StreamTableEnvironment.create(env)
  val transactionStream = env.fromElements(ExampleData.transaction: _*)
  tableEnv
    .fromDataStream(transactionStream)
    .executeInsert(
      TableDescriptor
        .forConnector("kafka")
        .schema(
          Schema
            .newBuilder()
            .column("t_time", DataTypes.TIMESTAMP_LTZ(3))
            .column("t_id", DataTypes.BIGINT().notNull())
            .column("t_customer_id", DataTypes.BIGINT().notNull())
            .column("t_amount", DataTypes.DECIMAL(5, 2))
            .watermark("t_time", "t_time - INTERVAL '10' SECONDS")
            .build()
        )
        .format("json")
        .option("json.timestamp-format.standard", "ISO-8601")
        .option("topic", "transactions")
        .option("scan.startup.mode", "earliest-offset")
        .option("properties.bootstrap.servers", "localhost:9092")
        .build()
    )
