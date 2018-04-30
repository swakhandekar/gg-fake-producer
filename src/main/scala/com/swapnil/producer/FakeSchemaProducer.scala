package com.swapnil.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

class FakeSchemaProducer(val config: Properties) {
  private var producer: KafkaProducer[String, String] = _

  def sendMessage(topic: String) = {
    openProducer()

    val studentSchema = Source.fromURI(getClass.getClassLoader.getResource("avro/student.avsc").toURI).mkString
    val studentDepartmentSchema = Source.fromURI(getClass.getClassLoader.getResource("avro/student-department.avsc").toURI).mkString
    producer.send(new ProducerRecord[String, String](topic, "", studentSchema))
    producer.send(new ProducerRecord[String, String](topic, "", studentDepartmentSchema))

    close()
  }

  private def openProducer(): Unit = {
    config.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producer = new KafkaProducer[String, String](config)
  }

  private def close(): Unit = {
    producer.close()
  }
}
