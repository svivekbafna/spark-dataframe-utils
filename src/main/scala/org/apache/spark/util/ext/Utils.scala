package org.apache.spark.util.ext

import org.apache.spark.SparkContext
/**
 * Created by kevin on 16/09/15.
 */
object Utils {

  def withDummyCallSite[T](sc: SparkContext)(body: => T): T = {
    org.apache.spark.util.Utils.withDummyCallSite(sc)(body)
  }

}
