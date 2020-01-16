package com.song.flink.course4

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import util.DBUtils

import scala.collection.mutable.ListBuffer

/**
  * @author songshiyu
  */
object DataSetTransformationApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //MapFunction(env)
    //filterFunction(env)
    //mapPartitionFunction(env)
    //firstFunction(env)
    //flatMapFunction(env)
    //distinctFunction(env)
    //joinFunction(env)
    //leftOuterJoinFunction(env)
    //rightOuterJoinFunction(env)
    //fullOuterJoinFunction(env)
    crossFunction(env)
  }

  /**
    * map 操作
    **/
  def MapFunction(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    //data.map((x:Int) => x + 1).print()
    data.map(_ + 1).print()
  }

  /**
    * filter操作
    **/
  def filterFunction(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    data.map(_ + 1).filter(x => x > 5).print()
  }

  /**
    * MapPartition
    **/
  def mapPartitionFunction(env: ExecutionEnvironment): Unit = {
    val students = new ListBuffer[String];
    for (i <- 1 to 100) {
      students.append("student: " + i)
    }

    /*env.fromCollection(students)
        .map(x =>{
          //每一个元素存储到数据库，需要去获取到一个collection
          val connection = DBUtils getConnection();
          System.out.println("获取到数据库链接：" + connection)
          DBUtils.returnConnection(connection)
          x
        })
      .print()*/
    env.fromCollection(students).setParallelism(5)
      .mapPartition(x => {
        val connection = DBUtils getConnection();
        System.out.println("获取到数据库链接：" + connection)
        DBUtils.returnConnection(connection)
        x
      }).print()
  }

  /**
    * first 返回前几个元素
    **/
  def firstFunction(env: ExecutionEnvironment): Unit = {

    val info = new ListBuffer[(Int, String)];
    info.append((1, "hadoop"))
    info.append((1, "spark"))
    info.append((1, "flink"))
    info.append((2, "java"))
    info.append((2, "php"))
    info.append((2, "scala"))
    info.append((2, "js"))
    info.append((3, "vue"))
    info.append((3, "jquery"))
    info.append((3, "receat"))


    //所有取三条
    /*env.fromCollection(info)
      .first(3).print()*/

    //分组取两条
    /*env.fromCollection(info).groupBy(0)
        .first(2).print()*/

    //分组排序取两条
    env.fromCollection(info)
      .groupBy(0)
      .sortGroup(1,Order.DESCENDING)
      .first(2).print()
  }

  /**
    * FlatMap
    * */
  def flatMapFunction(env: ExecutionEnvironment): Unit ={
    val info = new ListBuffer[String];

    info.append("hadoop,spark")
    info.append("flink,java")
    info.append("vue,java")
    info.append("hadoop,spark")

    env.fromCollection(info).flatMap(_.split("\\,")).map((_,1)).groupBy(0).sum(1).print()
  }

  /**
    * distinct
    * */
  def distinctFunction(env: ExecutionEnvironment): Unit ={
    val info = new ListBuffer[String];

    info.append("hadoop,spark")
    info.append("flink,java")
    info.append("vue,java")
    info.append("hadoop,spark")

    env.fromCollection(info).flatMap(_.split("\\,")).distinct().print()
  }

  /**
    * join
    * */
  def joinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = new ListBuffer[(Int, String)];  //编号  名字
    info1.append((1, "hadoop"))
    info1.append((2, "spark"))
    info1.append((3, "flink"))
    info1.append((5, "java"))

    val info2 = new ListBuffer[(Int, String)];  //编号  城市
    info2.append((1, "北京"))
    info2.append((2, "上海"))
    info2.append((3, "石家庄"))
    info2.append((4, "新乐"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.join(data2).where(0).equalTo(0).apply((first,second) => {
      (first._1,first._2,second._2)
    }).print()
  }

  /**
    * leftOuterJoin
    * */
  def leftOuterJoinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = new ListBuffer[(Int, String)];  //编号  名字
    info1.append((1, "hadoop"))
    info1.append((2, "spark"))
    info1.append((3, "flink"))
    info1.append((5, "java"))

    val info2 = new ListBuffer[(Int, String)];  //编号  城市
    info2.append((1, "北京"))
    info2.append((2, "上海"))
    info2.append((3, "石家庄"))
    info2.append((4, "新乐"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first,second) => {
      if (second == null){
        (first._1,first._2,"-")
      }else{
        (first._1,first._2,second._2)
      }
    }).print()
  }

  /**
    * rightOuterJoin
    * */
  def rightOuterJoinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = new ListBuffer[(Int, String)];  //编号  名字
    info1.append((1, "hadoop"))
    info1.append((2, "spark"))
    info1.append((3, "flink"))
    info1.append((5, "java"))

    val info2 = new ListBuffer[(Int, String)];  //编号  城市
    info2.append((1, "北京"))
    info2.append((2, "上海"))
    info2.append((3, "石家庄"))
    info2.append((4, "新乐"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first,second) => {
      if (first == null){
        (second._1,"-",second._2)
      }else{
        (first._1,first._2,second._2)
      }
    }).print()
  }

  /**
    * fullOuterJoin
    * */
  def fullOuterJoinFunction(env: ExecutionEnvironment): Unit ={
    val info1 = new ListBuffer[(Int, String)];  //编号  名字
    info1.append((1, "hadoop"))
    info1.append((2, "spark"))
    info1.append((3, "flink"))
    info1.append((5, "java"))

    val info2 = new ListBuffer[(Int, String)];  //编号  城市
    info2.append((1, "北京"))
    info2.append((2, "上海"))
    info2.append((3, "石家庄"))
    info2.append((4, "新乐"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first,second) => {
      if (first == null){
        (second._1,"-",second._2)
      }else if (second == null){
        (first._1,first._2,"-")
      }else{
        (first._1,first._2,second._2)
      }
    }).print()
  }

  /**
    * 笛卡尔积
    * */
  def crossFunction(env: ExecutionEnvironment): Unit ={
    val info1 = List("曼联","曼城")
    val info2 = List(3,1,0)

    val data1 = env.fromCollection(info1);
    val data2 = env.fromCollection(info2);

    data1.cross(data2).print()

  }
}
