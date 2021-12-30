package com.ververica.models

import little.json.*
import little.json.Implicits.{*, given}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.time.Instant
import java.math.BigDecimal
import scala.language.implicitConversions

class Transaction(
    var t_time: Instant,
    var t_id: Long,
    var t_customer_id: Long,
    var t_amount: BigDecimal
):
  def this() =
    this(null, 0L, 0L, null)

  override def toString: String =
    s"Transaction($t_time, $t_id, $t_customer_id, $t_amount)"

given jsonToInstant: JsonInput[Instant] with
  def apply(json: JsonValue) = Instant.parse(json.as[String])

given jsonToBigDecimal: JsonInput[BigDecimal] with
  def apply(json: JsonValue) = new BigDecimal(json.as[String])

given jsonToTransaction: JsonInput[Transaction] with
  def apply(json: JsonValue) =
    val t = new Transaction()
    t.t_time = json("t_time")
    t.t_id = json("t_id")
    t.t_customer_id = json("t_customer_id")
    t.t_amount = json("t_amount")
    t

class TransactionDeserializer extends DeserializationSchema[Transaction]:
  override def isEndOfStream(customer: Transaction): Boolean = false

  override def getProducedType: TypeInformation[Transaction] =
    TypeInformation.of(classOf[Transaction])

  override def deserialize(bytes: Array[Byte]): Transaction =
    Json.parse(bytes).as[Transaction]
