package com.ververica

import org.apache.flink.configuration.CoreOptions
import org.apache.flink.table.api.{
  DataTypes,
  EnvironmentSettings,
  Schema,
  Table,
  TableConfig,
  TableDescriptor,
  TableEnvironment
}

import java.time.ZoneId

/** Table API end-to-end example with time-versioned joins. */
@main def example10 =
  val tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode)
  val config = tableEnv.getConfig
  config.getConfiguration.set(
    CoreOptions.DEFAULT_PARALLELISM,
    1
  ) // due to little data

  config.setLocalTimeZone(ZoneId.of("UTC"))

  // use descriptor API to use dedicated table connectors
  tableEnv.createTemporaryTable(
    "Customers",
    TableDescriptor
      .forConnector("upsert-kafka")
      .schema(
        Schema
          .newBuilder()
          .column("c_rowtime", DataTypes.TIMESTAMP_LTZ(3))
          .column("c_id", DataTypes.BIGINT().notNull())
          .column("c_name", DataTypes.STRING())
          .column("c_birthday", DataTypes.DATE())
          .primaryKey("c_id")
          .watermark("c_rowtime", "c_rowtime - INTERVAL '10' SECONDS")
          .build()
      )
      .option("key.format", "json")
      .option("value.format", "json")
      .option("topic", "customers")
      .option("properties.bootstrap.servers", "localhost:9092")
      .build()
  )

  tableEnv.createTemporaryTable(
    "Transactions",
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
  tableEnv
    .executeSql("""
                  |SELECT t_rowtime, c_rowtime, t_id, c_name, t_amount
                  |FROM DeduplicateTransactions
                  |LEFT JOIN Customers FOR SYSTEM_TIME AS OF t_rowtime
                  |   ON c_id = t_customer_id
                  |""".stripMargin)
    .print()
