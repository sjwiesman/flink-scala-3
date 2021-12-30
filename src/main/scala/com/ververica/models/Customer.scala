package com.ververica.models

import little.json.*
import little.json.Implicits.{*, given}
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation

import java.time.LocalDate
import java.util.Objects
import scala.language.implicitConversions

class Customer(var c_id: Long, var c_name: String, var c_birthday: LocalDate):
  def this() =
    this(0L, "", null)

  override def toString: String = s"Customer($c_id, $c_name, $c_birthday)"

given jsonToLocalDate: JsonInput[LocalDate] with
  def apply(json: JsonValue) = LocalDate.parse(json.as[String])

given jsonToCustomer: JsonInput[Customer] with
  def apply(json: JsonValue) =
    val c = new Customer()
    c.c_id = json("c_id")
    c.c_name = json("c_name")
    c.c_birthday = json("c_birthday")
    c

class CustomerDeserializer extends DeserializationSchema[Customer]:
  override def isEndOfStream(customer: Customer): Boolean = false

  override def getProducedType: TypeInformation[Customer] =
    TypeInformation.of(classOf[Customer])

  override def deserialize(bytes: Array[Byte]): Customer =
    Json.parse(bytes).as[Customer]
