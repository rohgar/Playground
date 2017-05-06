package com.test.spark.examples

import com.test.spark.SSParent
import org.apache.spark.sql.functions.udf

object UDFSimple extends SSParent {

  import sqlContext.implicits._

  // column names
  val INTCOL = "INTCOL"
  val STRCOL = "STRCOL"
  
  // Create the dataframe
  val sequence = Seq((0, "hello"), (1, "world"))
  val dataframe = sequence.toDF(INTCOL, STRCOL)
  dataframe.show()
  
  // define a function and convert it to a UDF
  def funcUpperCase = (str: String) => str.toUpperCase()
  def upperUDF = udf(funcUpperCase)

  // Apply the UDF to the dataset
  val newDataFrame = dataframe.withColumn("NEWSTRCOL", upperUDF('STRCOL))
  newDataFrame.show()

}