package com.icc.poc
/**
 * @author Mahendran Ponnusamy
 */
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer

import com.fasterxml.jackson.databind.ObjectMapper

case class Data(customer_id: Long, acct_num: Long, customer_profile: String, trans_num: Long, trans_date: String, trans_time: String, unix_time: Long, category: String, amt: Double, is_fraud: Short) {
  override def toString(): String = {
    s"""$customer_id,$acct_num,$customer_profile,$trans_num,$trans_date,$trans_time,$unix_time,$category,$amt,$is_fraud"""
  }
}

class DataSerializer extends Serializer[Data] {

  override def close() {}

  override def configure(configs: java.util.Map[String, _], arg1: Boolean) {}

  override def serialize(arg0: String, data: Data): Array[Byte] = {
//     val objectMapper = new ObjectMapper();
//     objectMapper.writeValueAsString(data).getBytes
    data.toString().getBytes
  }
}

class DataDeserializer extends Deserializer[Data] {

  override def close() {}

  override def configure(configs: java.util.Map[String, _], arg1: Boolean) {}

  override def deserialize(arg0: String, data: Array[Byte]): Data = {
    val objectMapper = new ObjectMapper()
    objectMapper.readValue(data, classOf[Data]);
  }
}