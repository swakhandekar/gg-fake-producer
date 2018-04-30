package com.swapnil.model

case class GenericWrapper(table_name: String, schema_fingerprint: Long, payload: Array[Byte]) {}
