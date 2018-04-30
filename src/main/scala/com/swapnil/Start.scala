package com.swapnil

import java.util.Properties

import com.swapnil.producer.{FakePayloadProducer, FakeSchemaProducer}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig

object Start extends App {
  val config: Properties = {
    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "StudentAdmission")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Bytes().getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Bytes().getClass)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    props
  }

  private val payloadProducer = new FakePayloadProducer(config)
  payloadProducer.sendMessage("ogg-payload")
  new FakeSchemaProducer(config).sendMessage("ogg-schema")
}
