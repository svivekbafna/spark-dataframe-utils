package org.apache.spark.sql.ext

import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import scala.reflect.runtime.universe.TypeTag

/**
 * Created by jianshuang on 3/19/15.
 */
// scalastyle:off line.size.limit
object UdfUtils {

  /**
   * Turn a Scala closure of 0 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag](func: Function0[RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 1 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag](func: Function1[A1, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 2 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag](func: Function2[A1, A2, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 3 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](func: (A1, A2, A3) => RT): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 4 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 5 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 6 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 7 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 8 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 9 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 10 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 11 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag](func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 12 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag](func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 13 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag](func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 14 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag](func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 15 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag](func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 16 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag](func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 17 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag](func: Function17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 18 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag](func: Function18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 19 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag](func: Function19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 20 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag](func: Function20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 21 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag](func: Function21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  /**
   * Turn a Scala closure of 22 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
   */
  def makeUdf[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag, A22: TypeTag](func: Function22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    UserDefinedFunction(func, dataType)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Turn a user-defined function with 1 arguments.
   */
  def makeUdf(f: UDF1[_, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF1[Any, Any]].call(_: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 2 arguments.
   */
  def makeUdf(f: UDF2[_, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF2[Any, Any, Any]].call(_: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 3 arguments.
   */
  def makeUdf(f: UDF3[_, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF3[Any, Any, Any, Any]].call(_: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 4 arguments.
   */
  def makeUdf(f: UDF4[_, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF4[Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 5 arguments.
   */
  def makeUdf(f: UDF5[_, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 6 arguments.
   */
  def makeUdf(f: UDF6[_, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF6[Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 7 arguments.
   */
  def makeUdf(f: UDF7[_, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF7[Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 8 arguments.
   */
  def makeUdf(f: UDF8[_, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 9 arguments.
   */
  def makeUdf(f: UDF9[_, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 10 arguments.
   */
  def makeUdf(f: UDF10[_, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 11 arguments.
   */
  def makeUdf(f: UDF11[_, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 12 arguments.
   */
  def makeUdf(f: UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 13 arguments.
   */
  def makeUdf(f: UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 14 arguments.
   */
  def makeUdf(f: UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 15 arguments.
   */
  def makeUdf(f: UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 16 arguments.
   */
  def makeUdf(f: UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 17 arguments.
   */
  def makeUdf(f: UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 18 arguments.
   */
  def makeUdf(f: UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 19 arguments.
   */
  def makeUdf(f: UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 20 arguments.
   */
  def makeUdf(f: UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 21 arguments.
   */
  def makeUdf(f: UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

  /**
   * Turn a user-defined function with 22 arguments.
   */
  def makeUdf(f: UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    (e: Seq[Expression]) => ScalaUDF(f.asInstanceOf[UDF22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e)
  }

}
// scalastyle:on line.size.limit
