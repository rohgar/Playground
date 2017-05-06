package com.test.spark.examples

import com.test.spark.SSParent
import org.apache.spark.sql.functions.{ lit, to_date, date_format, current_timestamp }

object SQLCustomColumnsAndFilter extends SSParent {

  import sqlContext.implicits._

  /* Create the dataframe */
  val sequence = Seq(("1950-1-1", 31), ("1951-1-1", 23), ("1952-1-1", 32), ("1953-1-1", 31), ("1954-1-1", 43), ("1955-1-1", 38), ("1956-1-1", 43), ("1957-1-1", 32), ("1958-1-1", 13))
  val dataframe = sequence.toDF("STRCOL", "INTCOL")
  println("Original dataframe")
  dataframe.show()

  // change the type of the column to the desired type and create a new dataframe
  val typeDataframe = dataframe.withColumn("DATECOL", to_date($"STRCOL"))
  println("Dataframe with STRCOL changed from string to date type in new Col DATECOL_DATETYPE")
  typeDataframe.show()

  // Some filter
  val resultDataFrame = typeDataframe.filter("STRCOL LIKE '1952-1-%'")
  println("Dataframe with a like filter on STRCOL")
  resultDataFrame.show(false)

}