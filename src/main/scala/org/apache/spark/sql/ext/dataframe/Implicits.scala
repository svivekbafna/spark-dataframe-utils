package org.apache.spark.sql.ext.dataframe

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.ext.DataFrameUtils

/**
 * Created by jianshuang on 3/19/15.
 */

/**
 * Implicit class for calling functions from DataFrameUtils
 */
object Implicits {

  /**
   * Usage:
   * {{{
   * import org.apache.spark.sql.ext.ExtendedDataFrame
   * val d = sqlContext.load("...", "parquet")
   * val partitions = d.getPartitions
   * ...
   * }}}
   *
   */
  implicit class ExtendedDataFrame(df: DataFrame) {


    /**
     * @see [[DataFrameUtils.getPartitions]]
     */
    def getPartitions = DataFrameUtils.getPartitions(df)

    /**
     * @see [[DataFrameUtils.getColumnAsMap]]
     */
    def getColumnsAsMap(columns: Seq[String] = Seq.empty) = DataFrameUtils.getColumnAsMap(df, columns)

    /**
     * @see [[DataFrameUtils.toMap]]
     */
    def toMap = DataFrameUtils.toMap(df)

    /**
     * Same as [[toMap]]
     * @see [[DataFrameUtils.toMapRDD]]
     */
    def toMapRDD = DataFrameUtils.toMapRDD(df)

    /**
     * @see [[DataFrameUtils.flatten]]
     */
    def flatten(reduceOneLevel: Boolean = false) = DataFrameUtils.flatten(df, reduceOneLevel)

    /**
     * @see [[DataFrameUtils.allKeysInMapColumn]]
     */
    def allKeysInMapColumn(column: String) = DataFrameUtils.allKeysInMapColumn(df, column)

    /**
     * @see [[DataFrameUtils.mergeMapColumns]]
     */
    def mergeMapColumns(df2: DataFrame, joinExpr: Column, mapColumnsToMerge: Seq[String]) = DataFrameUtils.mergeMapColumns(df, df2, joinExpr, mapColumnsToMerge)

    /**
     * @see [[DataFrameUtils.mergeMapColumns]]
     */
    def mergeMapColumns(df2: DataFrame, joinExpr: Column) = DataFrameUtils.mergeMapColumns(df, df2, joinExpr)

    /**
     * @see [[DataFrameUtils.reduceOneLevel]]
     */
    def reduceOneLevel(): DataFrame = DataFrameUtils.reduceOneLevel(df)

    /**
     * @see [[DataFrameUtils.getStructFields]]
     */
    def getStructFields(structColumn: String): Seq[StructField] = DataFrameUtils.getStructFields(df, structColumn)
  }

}
