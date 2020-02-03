package com.song.flink.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._

object KafkaConnectorConsumerApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.137.10:9092")
    properties.setProperty("group.id", "test")

    val data = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties))
    data.print()

    env.execute("KafkaConnectorConsumerApp")
  }
}
