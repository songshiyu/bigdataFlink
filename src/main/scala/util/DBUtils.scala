package util

import scala.util.Random

/**
  * @author songshiyu
  */
object DBUtils {

  def getConnection():String ={
    return new Random().nextInt(100) + ""
  }

  def returnConnection(connection:String): Unit ={
    System.out.println("connection:" + connection  + "被还回去了")
  }
}
