package com.prerna.wbi

import org.apache.spark.sql.SparkSession
import java.lang.Long

object PopulationGrowth {
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
   
 
   
			val result = data.map { line => {
			  val population = line.getString(9).replaceAll(",", "")
			  var popNum = 0D
			  if (population.length() > 0)
			    popNum = Long.parseLong(population)
			  
			  (line.getString(0), popNum)
			}}
			.groupByKey()
//			India  [3018294, 45304958, 7238472, 102938102.....]
			.map(rec => {
			  val minPop = (rec._2.min)
			  val maxPop = (rec._2.max)
			  println(maxPop)
			  var perGrowth = 0D
			    perGrowth=  ((maxPop -minPop)/minPop) * 100
			    (rec._1, perGrowth)
			})
//			India    1298723948723
			.sortBy(rec => (rec._2), false)
			.take(1)
		
			result.foreach{ println }
		spark.sparkContext.parallelize(result.toSeq, 1).saveAsTextFile(args(1))
   
   spark.stop
}
}