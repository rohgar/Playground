package com.test.spark.examples

import com.test.spark.SSParent

object JoinSimple extends SSParent {

  import sqlContext.implicits._
  
  val a = List("a", "b", "c").toDF
  val b = List("a", "b", "d").toDF
  
  a.show
  b.show
  
  val joinExpr = Array("value")
    .zip(Array("value"))
    .map { case (c1, c2) => a(c1) === b(c2) }
    .reduce(_ && _)
    
  (a.join(b, joinExpr, "INNER")).show
  (a.join(b, joinExpr, "LEFT_OUTER")).show
  (a.join(b, joinExpr, "RIGHT_OUTER")).show
  (a.join(b, joinExpr, "FULL_OUTER")).show
  
}