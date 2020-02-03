package com.song.flink.course08

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object KafkaConnectorSinkApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //从socket接收数据，通过flink将数据Sink到kafka
    val data = env.socketTextStream("localhost",9999)

    val topic = "test"
    val properties = new Properties()

    properties.setProperty("bootstrap.servers", "192.168.137.10:9092")
    val kafkaSink = new FlinkKafkaProducer[String](topic,new SimpleStringSchema(),properties)

    data.addSink(kafkaSink)
    env.execute("KafkaConnectorSinkApp")
  }
}
