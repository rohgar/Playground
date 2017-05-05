package com.test.spark.examples

import com.test.spark.SSParent

object JoinCSV extends SSParent {

  val parent = resourceCsvToDataframe("optim_customers_adj.csv")
  val child = resourceCsvToDataframe("optim_orders_adj.csv")

  //  parent.show
  //  child.show

  val joinExprs = Array("CUST_ID_CHAR")
    .zip(Array("CUST_ID_CHAR"))
    .map { case (c1, c2) => parent(c1) === child(c2) }
    .reduce(_ && _)

  val joinedInner = parent.join(child, joinExprs, "INNER")
  val joinedparent_outer = parent.join(child, joinExprs, "LEFT_OUTER")
  val joinedchild_outer = parent.join(child, joinExprs, "RIGHT_OUTER")
  val joinedFull_outer = parent.join(child, joinExprs, "FULL_OUTER")

  println("joinedInner count = " + joinedInner.count)
  println("joinedparent_outer count = " + joinedparent_outer.count)
  println("joinedchild_outer count = " + joinedchild_outer.count)
  println("joinedFull_outer count = " + joinedFull_outer.count)
  
}