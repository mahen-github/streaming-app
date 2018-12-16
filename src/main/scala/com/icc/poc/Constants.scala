package com.icc.poc

object Constants {
  final val ZK_CONN_STRING = "localhost:2181"
  //Kafka Konstants
  final val KAFKA_CH_ROOT = "/kafka"
  final val ZK_PATH = "/config/topics"
  final val BOOTSTRAP_SERVERS = "localhost:9092"
  final val ACKS = "all"
  final val BATCH_SIZE = 16384
  final val LINGER_MS = 100
  final val BUFFER_MEMORY = 3554432
  final val DATA_SERIALIZER = "com.icc.poc.DataDeserializer"
  final val KAFKA_RESET = "earliest"
  final val KAFKA_MAX_FETCH_BYTES = "16000"
}