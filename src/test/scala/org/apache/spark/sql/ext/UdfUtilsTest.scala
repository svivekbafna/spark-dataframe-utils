package org.apache.spark.sql.ext

import org.apache.spark.sql.ext.UdfUtils._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

/**
 * Created by jianshuang on 3/19/15.
 */
class UdfUtilsTest extends FunSuite {

  type DMap = Map[String, Double]

  import TestSQLContext.sqlContext._
  import TestSQLContext.sqlContext.implicits._

  test("dummy makeUdf") {
    val udf = makeUdf(UdfUtilsTest.mergeStringToDoubleMap _)
    assert(udf.dataType === MapType(StringType, DoubleType, valueContainsNull = true))
  }

  test("using UDF created b makeUdf") {
    val ip = read.parquet("data/ip_sample")
    val vid = read.parquet("data/vid_sample")
    val merge_map_d = makeUdf(UdfUtilsTest.mergeStringToDoubleMap _)
    val res =
      ip.join(vid, ip("mid") === vid("mid") and ip("source") === vid("source"), "left_outer")
        .select(merge_map_d(ip("nvar"), vid("nvar")) as "nvar")

    assert(res.count() > 0)
    println(res.count())
    assert(res.collect().forall(r => r(0).asInstanceOf[Map[String, Double]].size > 0))
  }

}

object UdfUtilsTest {

  // NOTE: java.long.Double will be inferred as nullable = true
  def mergeStringToDoubleMap(m1: Map[String, java.lang.Double], m2: Map[String, java.lang.Double]): Map[String, java.lang.Double] = {
    (m1, m2) match {
      case (null, _) => m2
      case (_, null) => m1
      case _ => m1 ++ m2
    }
  }

}
