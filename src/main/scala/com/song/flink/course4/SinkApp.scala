package com.song.flink.course4

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode


/**
  * @author songshiyu
  */
object SinkApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment;
    writeAsTextFunction(env)
    env.execute("SinkApp")
  }

  /**
    * sink到本地文件
    * */
  def writeAsTextFunction(env:ExecutionEnvironment): Unit ={
    import org.apache.flink.api.scala._
    val data = 1 to 10
    val text = env.fromCollection(data)
    val filePath = "file:///D:/home/flink/outPut";
    text.writeAsText(filePath,WriteMode.OVERWRITE).setParallelism(2);
  }
}
