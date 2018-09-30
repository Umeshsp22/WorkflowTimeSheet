package com.reni

import com.reni.constants.Constants._
import scala.io.Source
import java.text.SimpleDateFormat

/* ccreated by Umesh 2018-09-21*/

class Parser 
  /* Process the Files using parallel colletcion */
  def parser(path:String)={

    val iterator = Source.fromFile(path).getLines.grouped(CHUNK_SIZE)
    iterator.flatMap { lines =>
      lines.par.filter(line=>line.contains(":: Start ::") || line.contains(":: End ::") ).map { line => process(line) }
    }.toArray

  }

/* Extract only the Start and End Time of the Workflows */
  def process(line:String)={

    val dateFormat=new SimpleDateFormat("yy/MM/dd HH:mm:ss")
    val row=line.split("::")
    val date=row(0).split(" ")
    val timeStamp=dateFormat.parse(date(0)+" "+date(1)).getTime
    (row(3).split(" ")(4),timeStamp)
  }

  /* group it and find maxium and minium time of the workflow */
  def reduceByKey[K](collection:Array[Tuple2[String,Long]])={
    collection
      .groupBy(_._1)
      .map { case (group: String, traversable) => {
        traversable.tail.foldLeft(traversable.head._1,traversable.head._2,traversable.head._2)(
          (a,b) => {
            var min=a._2
            var max=a._3
            if(min > b._2) min=b._2 ; if(b._2>max) max=b._2
            (group,min,max)
          })
      } }
  }


  def getTime(group:String,min:Long,mt:Long,limit:Long,file_list:Array[Array[Tuple2[String,Long]]])={
    var max=mt
    file_list.foreach(list=>{
      val getMaxlist=list.filter(set=>set._2>min && set._2<limit )
      if(getMaxlist.size>0) {val maxTime=getMaxlist.maxBy(max=>max._2)._2; if(maxTime>max) max=maxTime }
    })
    println(s"Time Taken to Complete ${group} ----> "+(max-min)/60000.0) //timeSet will be in milliSeconds so divide by 60000
  }

}
