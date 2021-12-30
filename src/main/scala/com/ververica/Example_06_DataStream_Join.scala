package com.ververica

import com.ververica.data.ExampleData
import com.ververica.models.{Customer, Transaction, TransactionDeserializer}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.{
  ListState,
  ListStateDescriptor,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import scala.jdk.CollectionConverters._

/** Use Flink's state to perform efficient record joining based on business
  * requirements.
  */
@main def example6 =
  val env = StreamExecutionEnvironment.getExecutionEnvironment()

  // switch to batch mode on demand
  // env.setRuntimeMode(RuntimeExecutionMode.BATCH)

  // read transactions
  val transactionSource = KafkaSource
    .builder[Transaction]
    .setBootstrapServers("localhost:9092")
    .setTopics("transactions")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new TransactionDeserializer)
    // .setBounded(OffsetsInitializer.latest())
    .build()

  val transactionStream =
    env.fromSource(
      transactionSource,
      WatermarkStrategy.noWatermarks(),
      "Transactions"
    )

  // Deduplicate using the function
  // defined in example 5
  val deduplicatedStream =
    transactionStream
      .keyBy((t: Transaction) => t.t_id)
      .process(new DataStreamDeduplicate)

  // join transactions and customers
  env
    .fromElements(ExampleData.customers: _*)
    .connect(deduplicatedStream)
    .keyBy((c: Customer) => c.c_id, (t: Transaction) => t.t_customer_id)
    .process(new JoinCustomersWithTransaction)
    .executeAndCollect
    .forEachRemaining(println)

class JoinCustomersWithTransaction
    extends KeyedCoProcessFunction[Long, Customer, Transaction, String]:

  var customer: ValueState[Customer] = _
  var transactions: ListState[Transaction] = _

  override def open(parameters: Configuration): Unit =
    customer = getRuntimeContext.getState(
      new ValueStateDescriptor("customer", classOf[Customer])
    )
    transactions = getRuntimeContext.getListState(
      new ListStateDescriptor("transactions", classOf[Transaction])
    )

  override def processElement1(
      in1: Customer,
      context: KeyedCoProcessFunction[
        Long,
        Customer,
        Transaction,
        String
      ]#Context,
      collector: Collector[String]
  ): Unit =
    customer.update(in1)
    val txs = transactions.get().asScala.to(LazyList)

    if !txs.isEmpty then join(collector, in1, txs)

  override def processElement2(
      in2: Transaction,
      context: KeyedCoProcessFunction[
        Long,
        Customer,
        Transaction,
        String
      ]#Context,
      collector: Collector[String]
  ): Unit =
    transactions.add(in2)
    val c = customer.value

    if c != null then
      join(collector, c, transactions.get().asScala.to(LazyList))

  private def join(
      out: Collector[String],
      c: Customer,
      txs: LazyList[Transaction]
  ) =
    txs.foreach(t => out.collect(s"${c.c_name} ${t.t_amount}"))
