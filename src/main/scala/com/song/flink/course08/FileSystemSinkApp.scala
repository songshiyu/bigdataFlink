package com.song.flink.course08

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
  * socket数据落地到文件系统
  *  这种方式仅供参考  小文件太多
  * */
object FileSystemSinkApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = env.socketTextStream("localhost",9999)

    data.print().setParallelism(1)


    val filePath = "file:///E:/data/flink/sink/hdfssink";
    val sink = new BucketingSink[String](filePath);
    sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd -- HHmm"))  //添加时间戳
    sink.setWriter(new StringWriter[String]())

    sink.setBatchRolloverInterval(2000)

    data.addSink(sink)
    env.execute("FileSystemSinkApp")
  }
}
