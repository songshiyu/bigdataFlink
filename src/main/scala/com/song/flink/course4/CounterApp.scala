package com.song.flink.course4

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @author songshiyu
  *
  *  定义计数器三部曲
  *  step1:定义计数器
  *  step2:注册计数器
  *  step3:获取计数器
  */
object CounterApp {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("hadoop", "spark", "flink", "java", "hive", "hbase")

    val info = data.map(new RichMapFunction[String, String] {

      //定义计数器
      val counter = new LongCounter();

      override def open(parameters: Configuration): Unit = {
        //注册计数器
        getRuntimeContext.addAccumulator("ele-counts-scala", counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })

    val filePath = "file:///D:/home/flink/counter/scala_counter"
    info.writeAsText(filePath,WriteMode.OVERWRITE)

    val jobResult = env.execute("CounterApp")
    val num = jobResult.getAccumulatorResult[Long]("ele-counts-scala")
    print("num: " + num)

  }
}
