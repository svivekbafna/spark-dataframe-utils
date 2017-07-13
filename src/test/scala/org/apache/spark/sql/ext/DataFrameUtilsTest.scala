package org.apache.spark.sql.ext

import org.apache.spark.sql.ext.{TestSQLContext, DataFrameUtils}
import org.apache.spark.sql.ext.dataframe.Implicits._
import org.apache.spark.sql.types.StructType
import org.scalatest.{Tag, FunSuite}

/**
 * Created by jianshuang on 3/19/15.
 */
class DataFrameUtilsTest extends FunSuite {

  import TestSQLContext.sqlContext._
  import TestSQLContext.sqlContext.implicits._

  test("yanlzhang test for 1.5 version") {
    val df = read.parquet("data/ip_sample_part")
    val partitions = df.getPartitions
    println(partitions)
    df.printSchema()
  }

  test("Can get partition info from Parquet-based DataFrame", Tag("getPartitions")) {
    val df = read.parquet("data/ip_sample_part", "parquet")
    val partitions = df.getPartitions
    assert(partitions.nonEmpty)
    assert(partitions.head.partitionColumns.map(_.name) === Seq("source", "date"))
  }

  test("For DataFrame doesn't support partitions, return empty Seq", Tag("getPartitions")) {
    val df = Seq((1,2,3)).toDF("a", "b", "c")
    assert(df.getPartitions.isEmpty)
  }

  test("allKeysInMapColumn works in normal cases", Tag("allKeysInMapColumn")) {
    val df = read.parquet("data/ip_sample_part/source=live/date=2015-03-19")
    val keys = df.allKeysInMapColumn("meta")
    val re = df.select("mid").take(3)
    re.foreach(println)
    println(re.length)
    assert(keys === Seq("account_number", "time_event_published"))
  }

  test("allKeysInMapColumn returns empty sequence for empty dataframe", Tag("allKeysInMapColumn")) {
    val df = Seq((Map[String, String](), 1)).toDF("m", "n")
    assert(df.allKeysInMapColumn("m").isEmpty)
  }

  test("allKeysInMapColumn returns sorted keys -- sample data", Tag("allKeysInMapColumn")) {
    val df = load("data/ip_sample_part", "parquet")
    val keys = df.allKeysInMapColumn("nvar")
    assert(keys === keys.sorted)
  }

  test("allKeysInMapColumn returns sorted keys -- test data", Tag("allKeysInMapColumn")) {
    val df = Seq((Map[String, String]("b" -> "1", "a" -> "2"), 1)).toDF("m", "n")
    val keys = df.allKeysInMapColumn("m")
    println(keys)
    assert(keys === keys.sorted)
  }

  test("mergeMapColumns works for sample data", Tag("mergeMapColumns")) {
    val df1 = load("data/ip_sample", "parquet")
    val df2 = load("data/vid_sample", "parquet")

    val df1varCount = df1.select("nvar").take(1)(0).getMap[String, Double](0).size
    val df2varCount = df2.select("nvar").take(1)(0).getMap[String, Double](0).size

    val res = DataFrameUtils.mergeMapColumns(df1, df2, df1("mid") === df2("mid"))
    val vars = res.select("nvar").take(1)(0).getMap[String, Double](0)

    res.printSchema()
    assert(res.columns === Seq("mid", "event_date", "source", "meta", "nvar"))
    assert(vars.size === df1varCount + df2varCount)
  }

  // TODO: wait for SPARK-6432 to be fixed
//  test("mergeMapColumns works for sample data with partitions", Tag("mergeMapColumns")) {
//    val df1 = load("data/ip_sample_part", "parquet")
//    val df2 = load("data/vid_sample_part", "parquet")
//
//    val df1varCount = df1.select("nvar").take(1)(0).getMap[String, Double](0).size
//    val df2varCount = df2.select("nvar").take(1)(0).getMap[String, Double](0).size
//
//    val res = DataFrameUtils.mergeMapColumns(df1, df2, df1("mid") === df2("mid"))
//    val vars = res.select("nvar").take(1)(0).getMap[String, Double](0)
//
//    res.printSchema()
//    assert(res.columns === Seq("mid", "event_date", "source", "meta", "nvar"))
//    assert(vars.size === df1varCount + df2varCount)
//  }

  test("mergeMapColumns works for sample data by specifying columns to merge", Tag("mergeMapColumns")) {
    val df1 = load("data/ip_sample", "parquet")
    val df2 = load("data/vid_sample", "parquet")

    val df1varCount = df1.select("nvar").take(1)(0).getMap[String, Double](0).size
    val df2varCount = df2.select("nvar").take(1)(0).getMap[String, Double](0).size
    val df1metaCount = df1.select("meta").take(1)(0).getMap[String, String](0).size

    val res = DataFrameUtils.mergeMapColumns(df1, df2, df1("mid") === df2("mid"), Seq("nvar", "meta"))
    val vars = res.select("nvar").take(1)(0).getMap[String, Double](0)
    val meta = res.select("meta").take(1)(0).getMap[String, String](0)

    res.printSchema()
    assert(res.columns === Seq("mid", "event_date", "source", "nvar", "meta"))
    assert(vars.size === df1varCount + df2varCount)
    assert(meta.size === df1metaCount)
  }

  test("mergeMapColumns works for sample data, only merges meta column", Tag("mergeMapColumns")) {
    val df1 = load("data/ip_sample", "parquet")
    val df2 = load("data/vid_sample", "parquet")

    val df1varCount = df1.select("nvar").take(1)(0).getMap[String, Double](0).size
    val df1metaCount = df1.select("meta").take(1)(0).getMap[String, String](0).size

    val res = DataFrameUtils.mergeMapColumns(df1, df2, df1("mid") === df2("mid"), Seq("meta"))
    val vars = res.select("nvar").take(1)(0).getMap[String, Double](0)
    val meta = res.select("meta").take(1)(0).getMap[String, String](0)

    res.printSchema()
    assert(res.columns === Seq("mid", "event_date", "source", "nvar", "meta"))
    assert(vars.size === df1varCount)
    assert(meta.size === df1metaCount)
  }

  test("mergeMapColumns works for sample data, do not merge any column", Tag("mergeMapColumns")) {
    val df1 = load("data/ip_sample", "parquet")
    val df2 = load("data/vid_sample", "parquet")

    val df1varCount = df1.select("nvar").take(1)(0).getMap[String, Double](0).size
    val df1metaCount = df1.select("meta").take(1)(0).getMap[String, String](0).size

    val res = DataFrameUtils.mergeMapColumns(df1, df2, df1("mid") === df2("mid"), Seq())
    val vars = res.select("nvar").take(1)(0).getMap[String, Double](0)
    val meta = res.select("meta").take(1)(0).getMap[String, String](0)

    res.printSchema()
    assert(res.columns === Seq("mid", "event_date", "source", "meta", "nvar"))
    assert(vars.size === df1varCount)
    assert(meta.size === df1metaCount)
  }

  test("Flatten without reducing level works for sample data", Tag("flatten")) {
    val df1 = load("data/ip_sample", "parquet").flatten()
    df1.printSchema()
    assert(df1.schema.fields.find(_.name == "nvar").get.dataType.isInstanceOf[StructType])
  }

  test("Flatten with reducing level works for sample data", Tag("flatten")) {
    val df1 = load("data/ip_sample", "parquet").flatten(reduceOneLevel = true)
    df1.printSchema()
    assert(!df1.schema.fields.exists(_.name == "nvar"))
  }

}
