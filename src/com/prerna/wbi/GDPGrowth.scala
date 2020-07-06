package com.prerna.wbi

import org.apache.spark.sql.SparkSession
import java.lang.Long

object GDPGrowth {
  def main(args: Array[String]) = {
    		
  	if(args.length < 2 ){
      System.err.println("Usage: UrbanPopulation <Input-File> <Output-File> ");
      System.exit(1);
    }
  	
			val spark = SparkSession
				.builder
				.appName("GDP")
				.master("local")
				.getOrCreate()
      
   val data= spark.read.csv(args(0)).rdd
	
		val result = data.map { line => {
			var currGDP = 0L
			 
			if(line.isNullAt(18)){
			   currGDP = 0
		//	   println(currGDP)
			}
		  else{
			    val GDP = line.getString(18).replaceAll(",", "")  
			    if (GDP.length() > 0)
			    currGDP = Long.parseLong(GDP)
			  }
	//		 println(currGDP)
			  (line.getString(0), currGDP)
							  
			}}
			.groupByKey()
//			India  [3018294, 45304958, 7238472, 102938102.....]
			.map(rec => {
			  val values = (rec._2.takeRight(2))
			  val GDP_09 = values.min
			//  println(GDP_09)
			  val GDP_10 = values.max
			//  println(GDP_10)
			  var perGrowth = 0L
			    perGrowth=  (GDP_10 - GDP_09)
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