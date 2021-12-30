package com.ververica

import com.ververica.models.{Transaction, TransactionDeserializer}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.time.Duration

/** Use Flink's state to perform efficient record deduplication. */
@main def example5 =
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // set up a Kafka source
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

  transactionStream
    .keyBy((t: Transaction) => t.t_id)
    .process(new DataStreamDeduplicate)
    .executeAndCollect
    .forEachRemaining(println)

class DataStreamDeduplicate
    extends KeyedProcessFunction[Long, Transaction, Transaction]:
  // use Flink's managed keyed state
  var seen: ValueState[Transaction] = _

  override def open(parameters: Configuration): Unit =
    seen = getRuntimeContext.getState(
      new ValueStateDescriptor("seen", classOf[Transaction])
    )

  @throws[Exception]
  override def processElement(
      transaction: Transaction,
      context: KeyedProcessFunction[Long, Transaction, Transaction]#Context,
      out: Collector[Transaction]
  ): Unit =
    if (seen.value == null) {
      seen.update(transaction)
      // use timers to clean up state
      context.timerService.registerProcessingTimeTimer(
        context.timerService.currentProcessingTime + Duration
          .ofHours(1)
          .toMillis
      )
      out.collect(transaction)
    }

  override def onTimer(
      timestamp: Long,
      ctx: KeyedProcessFunction[Long, Transaction, Transaction]#OnTimerContext,
      out: Collector[Transaction]
  ): Unit =
    seen.clear()
