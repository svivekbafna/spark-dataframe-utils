package org.apache.spark.sql.ext

import org.apache.spark.sql.ext.UdfUtils._

/**
 * Created by jianshuang on 4/21/15.
 */
object Udfs {

  // NOTE: java.long.Double will be inferred as nullable = true
  private def mergeStringToDoubleMap(m1: Map[String, java.lang.Double], m2: Map[String, java.lang.Double]): Map[String, java.lang.Double] = {
    (m1, m2) match {
      case (null, _) => m2
      case (_, null) => m1
      case _ => m1 ++ m2
    }
  }

  private def mergeStringToFloatMap(m1: Map[String, java.lang.Float], m2: Map[String, java.lang.Float]): Map[String, java.lang.Float] = {
    (m1, m2) match {
      case (null, _) => m2
      case (_, null) => m1
      case _ => m1 ++ m2
    }
  }

  private def mergeStringToIntegerMap(m1: Map[String, java.lang.Integer], m2: Map[String, java.lang.Integer]): Map[String, java.lang.Integer] = {
    (m1, m2) match {
      case (null, _) => m2
      case (_, null) => m1
      case _ => m1 ++ m2
    }
  }

  private def mergeStringToLongMap(m1: Map[String, java.lang.Long], m2: Map[String, java.lang.Long]): Map[String, java.lang.Long] = {
    (m1, m2) match {
      case (null, _) => m2
      case (_, null) => m1
      case _ => m1 ++ m2
    }
  }

  private def mergeStringToStringMap(m1: Map[String, String], m2: Map[String, String]): Map[String, String] = {
    (m1, m2) match {
      case (null, _) => m2
      case (_, null) => m1
      case _ => m1 ++ m2
    }
  }


  val merge_string_to_double_map = makeUdf(mergeStringToDoubleMap _)
  val merge_string_to_float_map = makeUdf(mergeStringToFloatMap _)
  val merge_string_to_integer_map = makeUdf(mergeStringToIntegerMap _)
  val merge_string_to_long_map = makeUdf(mergeStringToLongMap _)
  val merge_string_to_string_map = makeUdf(mergeStringToStringMap _)

}
