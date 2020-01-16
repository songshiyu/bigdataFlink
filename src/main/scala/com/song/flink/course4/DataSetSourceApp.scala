package com.song.flink.course4

import com.song.flink.pojo.People
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
  * @author songshiyu
  */
object DataSetSourceApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.createCollectionsEnvironment
    //fromCollection(env)
    readRecursiveFiles(env)
  }

  def fromCollection(env:ExecutionEnvironment): Unit ={
    import org.apache.flink.api.scala._
     val data = 1 to 10
    env.fromCollection(data).print()
  }

  def fromTextFile(env:ExecutionEnvironment): Unit ={
    val filePath = "file:///D:/Git/flink-train/data/04/text";
    env.readTextFile(filePath).print()
  }

  def fromCsv(env:ExecutionEnvironment): Unit ={
    import org.apache.flink.api.scala._
    val filePath = "file:///D:/Git/flink-train/data/04/csv/people.csv";

    //first
    env.readCsvFile[(String,Int,String)](filePath,ignoreFirstLine = true).print()
    System.out.println("-------first--------")

    env.readCsvFile[(Int,String)](filePath,ignoreFirstLine = true,includedFields = Array(1,2)).print()
    System.out.println("-------second--------")

    case class myCaseClass(name:String,age:Int)
    env.readCsvFile[myCaseClass](filePath,ignoreFirstLine = true,includedFields = Array(0,1)).print()
    System.out.println("-------third--------")

    env.readCsvFile[People](filePath,ignoreFirstLine = true,pojoFields = Array("name","age","work")).print()
    System.out.println("-------forth--------")
  }
  def readRecursiveFiles(env:ExecutionEnvironment): Unit ={

      val filePath = "file:///D:/Git/flink-train/data/04/recursive";
      env.readTextFile(filePath).print()
      System.out.println("-------not correct--------")

    val configuration = new Configuration
    configuration.setBoolean("recursive.file.enumeration",true)
    env.readTextFile(filePath).withParameters(configuration).print()

  }
}
