package com.song.flink.course4

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

/**
  * @author songshiyu
  */
object DistributedCacheApp {

  def main(args: Array[String]): Unit = {

    import org.apache.flink.api.scala._

    val env = ExecutionEnvironment.getExecutionEnvironment;

    val filePath = "file:///D:/Git/flink-train/data/04/text/hello.txt";

    //step1:注册一个本地文件/HDFS文件
    env.registerCachedFile(filePath,"scala-dc");

    val data = env.fromElements("hadoop", "spark", "flink", "java", "hive", "hbase")

    data.map(new RichMapFunction[String,String] {

      override def open(parameters: Configuration): Unit = {
        //step2:在open方法中获取到分布式缓存的内容
        val file = getRuntimeContext.getDistributedCache.getFile("scala-dc")

        val lines = FileUtils.readLines(file)  //返回的是一个java的集合List<String>

        //将java集合转换为scala的集合
        import scala.collection.JavaConversions._
        for (ele <- lines){
          println(ele)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).print()
  }

}
