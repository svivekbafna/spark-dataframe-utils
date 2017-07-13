package org.apache.spark.sql.ext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetRelation
import org.apache.spark.sql.execution.datasources.PartitionSpec
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, Column, DataFrame}

import scala.collection.mutable

/**
 * Created by jianshuang on 3/19/15.
 */

/**
 * Utilities for DataFrame that's not in official Spark (yet)
 */
object DataFrameUtils {

  /**
   * Returns a list of partition spec objects
   */
  def getPartitions(dataframe: DataFrame): Seq[PartitionSpec] = {
    dataframe.queryExecution.logical.collect {
      case p: LogicalRelation =>
        p.relation match {
          case r: ParquetRelation => r.partitionSpec
        }
    }
  }

  /**
   * Return a RDD of Map[String, Any] containing column names -> values
   *
   * @param columns (columns to extract, default is all columns)
   */
  def getColumnAsMap(dataframe: DataFrame, columns: Seq[String] = Seq.empty): RDD[Map[String, Any]] = {

    val projected = columns match {
      case null | Seq() => dataframe
      case Seq(x, xs@_*) => dataframe.select(x, xs: _*)
    }

    val cols = projected.columns

    projected.map { r =>
      cols.zip(r.toSeq).toMap
    }
  }

  def rowToMap(row: Row, fieldNames: Seq[(String, Int)]): Map[String, Any] = {
    fieldNames.map {
      case (n, i) => (n, row(i))
    }.toMap
  }

  /**
   * Return a RDD of Map[String, Any] containing all column names -> values
   */
  def toMap(dataframe: DataFrame): RDD[Map[String, Any]] = {


    val fieldNames = dataframe.schema.fields.map(_.name).zipWithIndex

    dataframe.map(rowToMap(_, fieldNames))
  }

  /**
   * Same as [[toMap]]
   */
  def toMapRDD = toMap _

  /**
   * Return a new DataFrame where all Map columns are flattened to Struct columns
   */
  def flatten(dataframe: DataFrame, reduceOneLevel: Boolean = false): DataFrame = {
    val df = MapRDD.mapToDataFrame(toMap(dataframe))(dataframe.sqlContext)

    if (reduceOneLevel)
      furtherFlatten(df)
    else
      df
  }

  private[this] def furtherFlatten(dfStruct: DataFrame): DataFrame = {
    val projectionClause = dfStruct.schema.flatMap { sf =>
      sf.dataType match {
        case x: StructType =>
          x.map(f => dfStruct(s"${sf.name}.${f.name}").as(f.name))
        case _ =>
          Seq(dfStruct(sf.name))
      }
    }

    dfStruct.select(projectionClause: _*)
  }

  /**
   * Return a sequence of keys: String, where it's the union of all keys in each row of the column
   */
  def allKeysInMapColumn(dataFrame: DataFrame, column: String): Seq[String] = {
    assert(dataFrame.select(column).schema.fields(0).dataType.isInstanceOf[MapType])

    val keySets: RDD[mutable.SortedSet[String]] = dataFrame.select(column).mapPartitions { rows =>
      val keys: mutable.SortedSet[String] = new mutable.TreeSet()
      rows.map(_(0).asInstanceOf[Map[String, _]].keys).foreach { ks =>
        ks.foreach(keys.add)
      }
      Seq(keys).toIterator
    }

    val allKeys = keySets.reduce((x, y) => x.union(y))

    allKeys.toSeq
  }


  /**
   * Return a new DataFrame where it merges Map columns (specified by user) from both dataframes.
   *
   * The type of Map columns to be merged must match. E.g. both are Map[String, String]
   *
   * '''Merge''' here means the second Map will append to the first Map and will override any value of the first Map for common keys
   *
   * The rest of columns of first input dataframe will also be included in the result (placed before the merged Map columns)
   *
   * The pseudo-SQL example looks like this:
   *
   * {{{
   * SELECT df1.mid, df1.source, df1.date, ..., merge_string_to_string_map(df1.cvar, df2.cvar) as cvar,  merge_string_to_double_map(df1.nvar, df2.nvar) as nvar
   * FROM df1
   * JOIN df2 ON df1.mid = df2.mid
   * }}}
   *
   * @param joinExpr SparkSQL DSL expression. e.g. df1("mid") === df2("mid")
   */
  def mergeMapColumns(df1: DataFrame, df2: DataFrame, joinExpr: Column, mapColumnsToMerge: Seq[String]): DataFrame = {
    import Udfs._

    // NOTE: need to dedup
    val df1Columns = df1.columns.diff(mapColumnsToMerge).distinct

    val newColumns = df1Columns.map(df1(_)) ++ mapColumnsToMerge.map { c =>
      val df1sf = df1.schema.find(_.name == c).orNull
      val df2sf = df2.schema.find(_.name == c).orNull

      assert(df1sf.dataType.isInstanceOf[MapType])
      assert(df1sf.dataType == df2sf.dataType)

      df1sf.dataType match {
        case MapType(StringType, StringType, _) => merge_string_to_string_map(df1(c), df2(c)).as(c)
        case MapType(StringType, DoubleType, _) => merge_string_to_double_map(df1(c), df2(c)).as(c)
        case MapType(StringType, FloatType, _) => merge_string_to_float_map(df1(c), df2(c)).as(c)
        case MapType(StringType, IntegerType, _) => merge_string_to_integer_map(df1(c), df2(c)).as(c)
        case MapType(StringType, LongType, _) => merge_string_to_long_map(df1(c), df2(c)).as(c)
      }
    }

    df1.join(df2, joinExpr).select(newColumns: _*)
  }

  /**
   * Return a new DataFrame where it merges Map columns from both dataframes that:
   * 1) the column names are the same
   * 2) the column type matches (e.g. both are Map[String, String])
   *
   * '''Merge''' here means the second Map will append to the first Map and will override any value of the first Map for common keys
   *
   * The rest of columns of first input dataframe will also be included in the result (placed before the merged Map columns)
   *
   * @see [[mergeMapColumns]]
   */
  def mergeMapColumns(df1: DataFrame, df2: DataFrame, joinExpr: Column): DataFrame = {
    val df1MapCols = df1.schema.filter(_.dataType.isInstanceOf[MapType])
    val df2MapCols = df2.schema.filter(_.dataType.isInstanceOf[MapType])
    val commonMapCols = df1MapCols.intersect(df2MapCols).map(_.name)

    mergeMapColumns(df1, df2, joinExpr, commonMapCols)
  }

  /**
   * Reduce the top level Struct typed columns to flattened columns
   * e.g. (nvar.v1, nvar.v2) => (v1, v2)
   */
  def reduceOneLevel(dfStruct: DataFrame): DataFrame = {

    val projectionClause = dfStruct.schema.flatMap { sf =>
      sf.dataType match {
        case x: StructType =>
          x.map(f => dfStruct(s"${sf.name}.${f.name}").as(f.name))
        case _ =>
          Seq(dfStruct(sf.name))
      }
    }

    dfStruct.select(projectionClause: _*)
  }

  /**
   * Get all fields belong to a Struct typed column
   */
  def getStructFields(df: DataFrame, structColumn: String): Seq[StructField] = {
    df.select(structColumn).schema.fields match {
      case Array() => Array[StructField]()
      case x => x.head.dataType.asInstanceOf[StructType].fields
    }
  }
}
