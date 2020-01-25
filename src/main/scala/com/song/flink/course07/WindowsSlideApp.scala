package com.song.flink.course07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

/**
  * 滑动窗口
  * */
object WindowsSlideApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)

    text.flatMap((_.split("\\,")))
      .map((_, 1))
      .keyBy(0)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1)
      .print()
      .setParallelism(1)

    env.execute("WindowsSlideApp")
  }
}
