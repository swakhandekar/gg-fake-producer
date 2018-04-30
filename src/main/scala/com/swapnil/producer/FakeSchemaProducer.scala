package com.swapnil.producer

import java.util.Properties

import com.sksamuel.avro4s.AvroSchema
import com.swapnil.model.{Student, StudentDepartment}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class FakeSchemaProducer(val config: Properties) {
  private var producer: KafkaProducer[String, String] = _

  def sendMessage(topic: String) = {
    openProducer()

    val studentSchema = AvroSchema[Student].toString(false)
    val studentDepartmentSchema = AvroSchema[StudentDepartment].toString(false)
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
