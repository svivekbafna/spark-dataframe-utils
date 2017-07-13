package org.apache.spark.sql.hive.ext

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.hive.HiveMetastoreTypes

/**
 * Utilities related to Hive
 */
object HiveUtils {

  def toHiveType(dataType: DataType): String = {
    HiveMetastoreTypes.toMetastoreType(dataType)
  }
}
