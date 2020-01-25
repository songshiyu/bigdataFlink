package com.song.flink.course07

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

/**
  * ReduceFunction
  **/
object WindowsReduceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)

    //原来传递进来的数据书字符串，此处我们使用数值类型，通过数值类型来演示增量效果
    text.flatMap(_.split("\\,"))
      .map(x => (1, x.toInt))
      .keyBy(0)  //因为key都是1，所以所有的元素都到一个task来执行
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce((v1,v2) => {  //不是等待窗口所有的数据进行一次性处理，而是数据两两处理
        println(v1 + "..." + v2)
        (v1._1,v1._2 + v2._2)
      })
      .print()
      .setParallelism(1)

    env.execute("WindowsReduceApp")
  }

}
