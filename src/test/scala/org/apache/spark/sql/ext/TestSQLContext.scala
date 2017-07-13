package org.apache.spark.sql.ext

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
 * Created by kevin on 16/09/15.
 */
object TestSQLContext {
  val sqlContext = new SQLContext(new SparkContext("local[2]", "test-sql-context", new SparkConf()))
}
