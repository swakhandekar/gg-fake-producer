package com.swapnil.producer

import java.io.ByteArrayOutputStream
import java.util.{Calendar, Properties}

import com.sksamuel.avro4s._
import com.swapnil.model.{GenericWrapper, Student, StudentDepartment}
import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.reflect.ClassTag

class FakePayloadProducer(val config: Properties) {
  private val genericWrapperSchema: Schema = AvroSchema[GenericWrapper]

  private var producer: KafkaProducer[String, Array[Byte]] = _
  private val COMP_DEPT = 2L

  private def newAdmission(): (Student, StudentDepartment) = {
    val student = Student(1, "Swapnil", 23, Calendar.getInstance().getTime().toString)
    val studentDepartmentMapping = StudentDepartment(student.id, COMP_DEPT)

    (student, studentDepartmentMapping)
  }

  private def toByteArray[T: ClassTag : SchemaFor : ToRecord : FromRecord](obj: T, schema: Schema): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val output = AvroOutputStream.binary[T](byteArrayOutputStream)

    output.write(obj)
    output.close()

    byteArrayOutputStream.toByteArray
  }

  private def wrapInGenericWrapper[T: ClassTag : SchemaFor : ToRecord : FromRecord](obj: T, schema: Schema): Array[Byte] = {
    val objBytes = toByteArray(obj, schema)
    val studentSchemaFingerprint = SchemaNormalization.parsingFingerprint64(schema)

    val wrapper = GenericWrapper("Student", studentSchemaFingerprint, objBytes)
    toByteArray(wrapper, genericWrapperSchema)
  }

  private def formStudentMessage(student: Student): Array[Byte] = {
    val studentSchema = AvroSchema[Student]
    wrapInGenericWrapper(student, studentSchema)
  }

  private def formStudentDepartmentMessage(studentDept: StudentDepartment): Array[Byte] = {
    val studentDepartmentSchema = AvroSchema[StudentDepartment]
    wrapInGenericWrapper(studentDept, studentDepartmentSchema)
  }

  private def combineMessages(delimiter: String): Array[Byte] = {
    val combinedBuffer = new ByteArrayOutputStream()
    val (student, studentDept) = newAdmission()


    combinedBuffer.write(formStudentMessage(student))
    combinedBuffer.write(delimiter.getBytes())
    combinedBuffer.write(formStudentDepartmentMessage(studentDept))
    combinedBuffer.close()

    combinedBuffer.toByteArray
  }

  def sendMessage(topic: String) = {
    openProducer()

    producer.send(new ProducerRecord[String, Array[Byte]](topic, System.currentTimeMillis().toString, combineMessages("^")))

    close()
  }

  private def openProducer(): Unit = {
    producer = new KafkaProducer[String, Array[Byte]](config)
  }

  private def close(): Unit = {
    producer.close()
  }
}
