package com.song.flink.course4

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

/**
  * @author songshiyu
  */
object BroadCastApp {

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = ExecutionEnvironment.getExecutionEnvironment

    val toBroadcast = env.fromElements(1, 2, 3)

    val data = env.fromElements("a", "b")

    data.map(new RichMapFunction[String,String] {

      var broadcastSet: Traversable[Int] = null

      override def open(parameters: Configuration): Unit = {
        import scala.collection.JavaConversions._
        broadcastSet = getRuntimeContext().getBroadcastVariable[Int]("broadcastSetName")
      }

      override def map(value: String): String = {
        for (ele <- broadcastSet){
          println("广播变量:" + ele)
        }
        value
      }
    }).withBroadcastSet(toBroadcast,"broadcastSetName").print()

  }
}
