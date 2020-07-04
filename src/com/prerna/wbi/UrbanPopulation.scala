package com.prerna.wbi

import org.apache.spark.sql.SparkSession
import java.lang.Long

object UrbanPopulation {
  
  def main(args: Array[String]) = {
    
    if(args.length < 2 ){
      System.err.println("Usage: UrbanPopulation <Input-File> <Output-File> ");
      System.exit(1);
    }
    
    val spark = SparkSession
      .builder
      .appName("UrbanPopulation")
      .getOrCreate()
      
   val data= spark.read.csv(args(0)).rdd
   
   val result = data.map( line =>{
     //val uPopulation = line.getString(10).replaceAll(","," ")
     val uPopulation = Option(line.getString(10)).getOrElse("")
     var uPopNum = 0L
      
     if(uPopulation.length() > 0)
       uPopNum = Long.parseLong(uPopulation)
       
       (uPopNum, line.getString(0))
   })
   .sortByKey(false)
   .first()
   
   spark.sparkContext.parallelize(Seq(result)).saveAsTextFile(args(1))
   
   spark.stop
    
  }
}