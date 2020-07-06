package com.prerna.wbi

import org.apache.spark.sql.SparkSession
import java.lang.Long

object Internet_Use {
  def main(args: Array[String]) = {
 
     if (args.length < 2) {
            System.err.println("Usage: JavaWordCount <Input-File> <Output-File>");
            System.exit(1);
          }

			val spark = SparkSession
				.builder
				.appName("Internet users highest growth")
				.master("local")
				.getOrCreate()
				
  		
			 val data= spark.read.csv(args(0)).rdd
			val result = data.map { line => {
			var currIntUsers = 0L
			
			if(line.isNullAt(5)){
			   currIntUsers = 0
				}
		  else{
			    val Intusers = line.getString(5).replaceAll(",", "")  
			    if (Intusers.length() > 0)
			    currIntUsers = Long.parseLong(Intusers)
			  }
	
			  (line.getString(0), currIntUsers)
			}}
			.groupByKey()
//			India  [3018294, 45304958, 7238472, 102938102.....]
			.map(rec => {
			  val minUsers = (rec._2.min)
			  val maxUsers = (rec._2.max)
			  println(minUsers)
			  println(maxUsers)
			  var perGrowth = 0L
			    perGrowth=  (maxUsers -minUsers)
			    (rec._1, perGrowth)
			})
//			India    1298723948723
			.sortBy(rec => (rec._2), false)
			.take(1)
		
			spark.sparkContext.parallelize(result.toSeq, 1).saveAsTextFile(args(1))
			
			spark.stop
			
} 
 
}