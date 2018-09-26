package com.reni;
import  com.reni.constants.Constants._

object TimeApp {

  def main(args:Array[String])={

    println("Started processing TIME-APP Change")


    val FILE_LIST=Array(PROCESS_MANAGER_OUT_FILE,PROCESS_MANAGER_OUT_SCOUT_FILE)
    val event_list=Array(ORDER_PROCESS_FILE,CRITICAL_DEAL_OUT,SCOUT_PROCESS_OUT)
    val parse=new Parser()

    val orderTimeSet= event_list.flatMap(file=>{
      val time_set=parse.parser(file)
      parse.reduceByKey(time_set)
    }).toSeq.sortBy(x=>x._2)



    val fileList=FILE_LIST.map(path=>{
      println(s"FInished Processing File ${path}")
      parse.parser(path)
    })
    println; println("WorkFLowTimeSheet :") ;println

    for(i<-0 until orderTimeSet.size-1){
      parse.getTime(orderTimeSet(i)._1,orderTimeSet(i)._2,orderTimeSet(i)._3,orderTimeSet(i+1)._2,fileList)
    }

    parse.getTime(orderTimeSet(orderTimeSet.size-1)._1,orderTimeSet(orderTimeSet.size-1)._2,orderTimeSet(orderTimeSet.size-1)._3,orderTimeSet(orderTimeSet.size-1)._3+100,fileList)

    println("Completed Processing TIME-APP")
  }

}
