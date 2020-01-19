package com.song.flink.course05

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import util.{CustomNonParallelSourceFunction, CustomParallelSourceFunction, CustomRichSourceFunction}
import org.apache.flink.api.scala._

/**
  * @author songshiyu
  */
object DataStreamSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //socketFunction(env)
    nonParallelSourceFunction(env)
    env.execute("DataStreamSourceApp")
  }

  def nonParallelSourceFunction(env:StreamExecutionEnvironment): Unit ={
    //不支持将setParallelism(1)设置为1以外的值
    //val data = env.addSource(new CustomNonParallelSourceFunction).setParallelism(1)
    //val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
    val data = env.addSource(new CustomRichSourceFunction).setParallelism(2)
    data.print().setParallelism(2)
  }

  def socketFunction(env:StreamExecutionEnvironment): Unit ={
    val data = env.socketTextStream("localhost",9999)
    data.print().setParallelism(2)
  }
}
