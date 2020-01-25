package com.song.flink.course07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * 滚动窗口
  **/
object WindowsTumbApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = env.socketTextStream("localhost",9999)

    input
      .flatMap(_.split("\\,"))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("WindowsApp")
  }
}
