package com.test.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

class SSParent extends App {

  val sqlContext = SSParent.getSqlContext()
  val sparkSession = SSParent.getSparkSession()

  /**
   * Returns the specified csv file with its absolute path as a dataframe
   * @param filepathWithFilename
   * @param dropmalformed
   * @return Dataframe
   */
  def csvToDataframe(filepathWithFilename: String, dropmalformed: Boolean = false) = {
    sparkSession.read.format("org.apache.spark.csv")
      .option("header", "true") //reading the headers
      .option("mode", dropmalformed)
      .csv(filepathWithFilename)
  }

  /**
   * Returns the specified csb file in the src/main/resources folder as a dataframe
   * @param filename
   * @param dropmalformed
   * @return Dataframe
   */
  def resourceCsvToDataframe(filename: String, dropmalformed: Boolean = false) = {
    val resourceFolderPath = "src/main/resources/"
    csvToDataframe(resourceFolderPath + filename)
  }

}

object SSParent {

  private val APPNAME = "scala-spark-playground"
  private val MASTER = "local[*]"

  private def getSqlContext() = {
    // Create the Spark Context
    val sc: SparkContext = new SparkContext(master = MASTER, appName = APPNAME, new SparkConf)
    sc.setLogLevel("ERROR")
    // Create the Sql context
    new org.apache.spark.sql.SQLContext(sc)
  }

  private def getSparkSession() = {
    // Create the Spark Session
    org.apache.spark.sql.SparkSession.builder.master(MASTER).appName(APPNAME).getOrCreate
  }

}