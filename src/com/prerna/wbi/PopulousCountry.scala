package com.prerna.wbi

import org.apache.spark.sql.SparkSession
import java.lang.Long

object PopulousCountry {
  def main(args: Array[String]) = {
    
    if(args.length < 2 ){
      System.err.println("Usage: UrbanPopulation <Input-File> <Output-File> ");
      System.exit(1);
    }
    
    val spark = SparkSession
      .builder
      .appName("PopulousCountry")
      .getOrCreate()
      
   val data= spark.read.csv(args(0)).rdd
   
  // val data = spark.read.csv("..\\home\\prerna\\data\\World_Bank_Indicators.csv").rdd
   
//   val result = data.map( line =>{
//    // val uPopulation = line.getString(10).replaceAll(","," ")
//     val uPopulation = (Option(line.getString(9)).getOrElse("")).replaceAll(",", "")
//     var uPopNum = 0L
//      
//     if(uPopulation.length() > 0)
//       uPopNum = Long.parseLong(uPopulation.trim())
//       
//       (uPopNum, line.getString(0))
//   })
//   .groupBy()
//   .map(rec => {
//			  (rec._1, rec._2.max)
//			})
//			.sortBy(rec => (rec._2), false)
//			.take(10)
   
			val result = data.map { line => {
			  val population =(Option(line.getString(9)).getOrElse("")).replaceAll(",", "")
			  var popNum = 0L
			  if (population.length() > 0)
			    popNum = Long.parseLong(population)
			  
			  (line.getString(0), popNum)
			}}
			.groupByKey()
//			India  [3018294, 45304958, 7238472, 102938102.....]
			.map(rec => {
			  (rec._1, rec._2.max)
			})
//			India    1298723948723
			.sortBy(rec => (rec._2), false)
			.take(10)
			
		result.foreach{ println }	
   spark.sparkContext.parallelize(result.toSeq, 1).saveAsTextFile(args(1))
   
   spark.stop
    
  }
}