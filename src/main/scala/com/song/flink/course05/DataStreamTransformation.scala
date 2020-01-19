package com.song.flink.course05

import java.{lang, util}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import _root_.util.CustomNonParallelSourceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector

/**
  * @author songshiyu
  */
object DataStreamTransformation {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //filterFunction(env)
    //unionFunction(env)
    splitAndSelectFunction(env)
    env.execute("DataStreamTransformation")
  }

  /**
    * map和filter的综合使用
    * */
  def filterFunction(env:StreamExecutionEnvironment): Unit ={
    val data = env.addSource(new CustomNonParallelSourceFunction)

    data.map(x => {
      println("接受到的数据为：" + x)
      x
    }).filter(_ % 2 ==0).print().setParallelism(1)
  }

  def unionFunction(env:StreamExecutionEnvironment): Unit ={
    val data1 = env.addSource(new CustomNonParallelSourceFunction)
    val data2 = env.addSource(new CustomNonParallelSourceFunction)

    data1.union(data2).print().setParallelism(1)
  }

  def splitAndSelectFunction(env:StreamExecutionEnvironment): Unit ={
    val data = env.addSource(new CustomNonParallelSourceFunction)

    data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]();
        if(value % 2 == 0){
          list.add("even")
        }else{
          list.add("odd")
        }
        list
      }
    }).select("odd").print().setParallelism(1)
  }
}
