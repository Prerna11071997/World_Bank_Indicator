package com.prerna.wbi

import org.apache.spark.sql.SparkSession
import java.lang.Long

object Youngest {
  def main(args: Array[String]) = {
      
     if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <Input-File> <Output-File>");
            System.exit(1);
     }
			val spark = SparkSession
				.builder
				.appName("Youngest country")
				.master("local")
				.getOrCreate()
				
  		
			val data= spark.read.csv(args(0)).rdd
			
			val result = data.map { line => {
			var currYoungPop = 0L
			
			if(line.isNullAt(16)){
			   currYoungPop = 0
				}
		  else{
			    val YoungPop = line.getString(16).replaceAll(",", "")  
			    if (YoungPop.length() > 0)
			    currYoungPop = Long.parseLong(YoungPop)
			  }
	
			  (line.getString(0), currYoungPop)
			}}
			.groupByKey()
//			India  [3018294, 45304958, 7238472, 102938102.....]
			.map(rec => {
			  val minPop = (rec._2.min)
			  val maxPop = (rec._2.max)
			  println(rec._2)
			 
			  var perGrowth = 0L
			    perGrowth=  maxPop
			    (rec._1, perGrowth)
			})
//			India    1298723948723
			.sortBy(rec => (rec._2), false)
			.take(1)
		
			//result.foreach{ println }
			
	    spark.sparkContext.parallelize(result.toSeq, 1).saveAsTextFile(args(1))
			
			spark.stop
			
} 
}