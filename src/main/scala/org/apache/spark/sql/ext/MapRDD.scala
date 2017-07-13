package org.apache.spark.sql.ext

import java.sql.{Date, Timestamp}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.joda.time.{DateTime, LocalDate, LocalDateTime}

import scala.collection.convert.Wrappers.{JListWrapper, JMapWrapper}
import scala.collection.immutable.Map
import scala.math.BigDecimal

/**
 * Convert RDD[Map[String, Any]] to DataFrame while keys in the map are turned into columns, nested maps turned into
 * Structs and types got inferred. All columns are nullable by default.
 *
 * Usage:
 * {{{
 *   val rdd: RDD[Map[String, Any]] = ...
 *   val df = MapRDD.mapToDataFrame(rdd)
 *   // or
 *   val rdd: RDD[T] = ...
 *   val wrapper: T => Map[String, Any] = ...
 *   val df = MapRDD.toDataFrame(rdd)(wrapper)
 * }}}
 */
object MapRDD {

  @inline
  private def toByte(value: Any): Byte = {
    value match {
      case value: Byte => value
      case value: Short => value.toByte
      case value: Int => value.toByte
    }
  }

  @inline
  private def toInt(value: Any): Int = {
    value match {
      case value: Int => value
      case value: Byte => value.toInt
      case value: Short => value.toInt
      case value: Long => value.toInt
      case value: Double => value.toInt
      case value: Float => value.toInt
    }
  }

  @inline
  private def toLong(value: Any): Long = {
    value match {
      case value: Long => value
      case value: Int => value.toLong
    }
  }

  @inline
  private def toFloat(value: Any): Float = {
    value match {
      case value: Float => value
      case value: Double => value.toFloat
      case value: Int => value.toFloat
      case value: Long => value.toFloat
    }
  }

  @inline
  private def toDouble(value: Any): Double = {
    value match {
      case value: Double => value
      case value: Float => value.toDouble
      case value: Int => value.toDouble
      case value: Long => value.toDouble
    }
  }

  @inline
  private def toDecimal(value: Any): Decimal = {
    value match {
      case value: Int => Decimal(value)
      case value: Long => Decimal(value)
      case value: BigInt => Decimal(BigDecimal(value))
      case value: Double => Decimal(value)
      case value: Float => Decimal(value)
      case value: BigDecimal => Decimal(value)
      case value: java.math.BigInteger => Decimal(BigDecimal(value))
      case value: java.math.BigDecimal => Decimal(BigDecimal(value))
    }
  }

  @inline
  private def toBoolean(value: Any): Boolean = {
    value match {
      case value: Boolean => value
    }
  }

  @inline
  private def toBinary(value: Any): Array[Byte] = {
    value match {
      case value: Array[Byte] => value
      case value: Byte => Array(value)
    }
  }

  @inline
  private def toDate(value: Any): Date = {
    value match {
      case v: LocalDate => new Date(v.toDateTimeAtStartOfDay.getMillis)
      case v: DateTime => new Date(v.getMillis)
      case v: Date => v
    }
  }

  @inline
  private def toTimestamp(value: Any): Timestamp = {
    value match {
      case v: DateTime => new Timestamp(v.getMillis)
      case v: LocalDateTime => new Timestamp(v.toDateTime.getMillis)
      case v: Timestamp => v
    }
  }

  private def asArraySyntax(seq: Seq[Any]): String = {
    s"""[${seq.mkString(", ")}]"""
  }

  private def asMapSyntax(map: Map[Any, Any]): String = {
    s"""{${map.map(m => s""""${m._1}": ${
      m._2 match {
        case v: String => s""""$v""""
        case v: Any => v.toString
      }
    }""").mkString(", ")}}"""
  }

  private def toString(value: Any): String = {
    value match {
      case value: Map[Any, _] => asMapSyntax(value)
      case value: Seq[_] => asArraySyntax(value)
      case value => Option(value).map(_.toString).orNull
    }
  }

  private def enforceCorrectType(value: Any, desiredType: DataType): Any = {
    if (value == null) {
      null
    } else {
      desiredType match {
        case StringType => toString(value)
        case IntegerType => toInt(value)
        case LongType => toLong(value)
        case DoubleType => toDouble(value)
        case FloatType => toFloat(value)
        case DecimalType() => toDecimal(value)
        case BooleanType => toBoolean(value)
        case ByteType => toByte(value)
        case ShortType => toInt(value)
        case BinaryType => toBinary(value)
        case NullType => null
        case ArrayType(elementType, _) => value.asInstanceOf[Seq[Any]].map(enforceCorrectType(_, elementType))
        case struct: StructType => asRow(value.asInstanceOf[Map[String, Any]], struct)
        case DateType => toDate(value)
        case TimestampType => toTimestamp(value)
      }
    }
  }

  def asRow(data: Map[String, Any], schema: StructType): Row = {
    // TODO: Reuse the row instead of creating a new one for every record.
    Row(schema.fields.zipWithIndex.map {
      case (StructField(name, dataType, _, _), i) =>
        data.get(name).flatMap(v => Option(v)).map(
          enforceCorrectType(_, dataType)).orNull
    })
  }

  private def createSchema(allKeys: Set[(String, DataType)]): StructType = {
    // Resolve type conflicts
    val resolved = allKeys.groupBy {
      case (key, dataType) => key
    }.map {
      // Now, keys and types are organized in the format of
      // key -> Set(type1, type2, ...).
      case (key, typeSet) => {
        val fieldName = key.substring(1, key.length - 1).split("`.`").toSeq
        val dataType = typeSet.map {
          case (_, dataType) => dataType
        }.reduce((type1: DataType, type2: DataType) => compatibleType(type1, type2))

        (fieldName, dataType)
      }
    }

    def makeStruct(values: Seq[Seq[String]], prefix: Seq[String]): StructType = {
      val (topLevel, structLike) = values.partition(_.size == 1)

      val topLevelFields = topLevel.filter {
        name => resolved.get(prefix ++ name).get match {
          case ArrayType(elementType, _) => {
            def hasInnerStruct(t: DataType): Boolean = t match {
              case s: StructType => true
              case ArrayType(t1, _) => hasInnerStruct(t1)
              case o => false
            }

            // Check if this array has inner struct.
            !hasInnerStruct(elementType)
          }
          case struct: StructType => false
          case _ => true
        }
      }.map {
        a => StructField(a.head, resolved.get(prefix ++ a).get, nullable = true)
      }
      val topLevelFieldNameSet = topLevelFields.map(_.name)

      val structFields: Seq[StructField] = structLike.groupBy(_(0)).filter {
        case (name, _) => !topLevelFieldNameSet.contains(name)
      }.map {
        case (name, fields) => {
          val nestedFields = fields.map(_.tail)
          val structType = makeStruct(nestedFields, prefix :+ name)
          val dataType = resolved.get(prefix :+ name).get
          dataType match {
            case array: ArrayType =>
              // The pattern of this array is ArrayType(...(ArrayType(StructType))).
              // Since the inner struct of array is a placeholder (StructType(Nil)),
              // we need to replace this placeholder with the actual StructType (structType).
              def getActualArrayType(innerStruct: StructType, currentArray: ArrayType): ArrayType = currentArray match {
                case ArrayType(s: StructType, containsNull) =>
                  ArrayType(innerStruct, containsNull)
                case ArrayType(a: ArrayType, containsNull) =>
                  ArrayType(getActualArrayType(innerStruct, a), containsNull)
              }
              Some(StructField(name, getActualArrayType(structType, array), nullable = true))
            case struct: StructType => Some(StructField(name, structType, nullable = true))
            // dataType is StringType means that we have resolved type conflicts involving
            // primitive types and complex types. So, the type of name has been relaxed to
            // StringType. Also, this field should have already been put in topLevelFields.
            case StringType => None
          }
        }
      }.flatMap(field => field).toSeq

      StructType((topLevelFields ++ structFields).sortBy(_.name))
    }

    makeStruct(resolved.keySet.toSeq, Nil)
  }

  def nullTypeToStringType(struct: StructType): StructType = {
    val fields = struct.fields.map {
      case StructField(fieldName, dataType, nullable, _) => {
        val newType = dataType match {
          case NullType => StringType
          case ArrayType(NullType, containsNull) => ArrayType(StringType, containsNull)
          case ArrayType(struct: StructType, containsNull) =>
            ArrayType(nullTypeToStringType(struct), containsNull)
          case struct: StructType => nullTypeToStringType(struct)
          case other: DataType => other
        }
        StructField(fieldName, newType, nullable)
      }
    }

    StructType(fields)
  }

  /**
   * Returns the most general data type for two given data types.
   */
  private def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonTypeOfTwo(t1, t2) match {
      case Some(commonType) => commonType
      case None =>
        // t1 or t2 is a StructType, ArrayType, or an unexpected type.
        (t1, t2) match {
          case (other: DataType, NullType) => other
          case (NullType, other: DataType) => other
          case (StructType(fields1), StructType(fields2)) => {
            val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
              case (name, fieldTypes) => {
                val dataType = fieldTypes.map(field => field.dataType).reduce(
                  (type1: DataType, type2: DataType) => compatibleType(type1, type2))
                StructField(name, dataType, true)
              }
            }
            StructType(newFields.toSeq.sortBy(_.name))
          }
          case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
            ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)
          case (MapType(keyType1, valueType1, containsNull1), MapType(keyType2, valueType2, containsNull2)) =>
            MapType(compatibleType(keyType1, keyType2), compatibleType(valueType1, valueType2), containsNull1 || containsNull2)
          // FIXME:
          case (_, _) => StringType
        }
    }
  }

  private def typeOfPrimitiveValue: PartialFunction[Any, DataType] = {
    ScalaReflection.typeOfObject orElse {
      // Since we do not have a data type backed by BigInteger,
      // when we see a Java BigInteger, we use DecimalType.
      case value: java.math.BigInteger => DecimalType.Unlimited
      // DecimalType's JVMType is scala BigDecimal.
      case value: java.math.BigDecimal => DecimalType.Unlimited
      case value: BigInt => DecimalType.Unlimited
      // joda DateTime support
      case value: LocalDate => DateType
      case value: DateTime => TimestampType
      case value: LocalDateTime => TimestampType
      // Unexpected data type.
      case _ => StringType
    }
  }

  /**
   * Returns the element type of an Array. We go through all elements of this array
   * to detect any possible type conflict. We use [[compatibleType]] to resolve
   * type conflicts.
   */
  private def typeOfArray(l: Seq[Any]): ArrayType = {
    val containsNull = l.contains(null)
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType, containsNull)
    } else {
      val elementType = elements.map {
        case map: Map[_, _] => StructType(Nil)
        // We have an array of arrays. If those element arrays do not have the same
        // element types, we will return ArrayType[StringType].
        case seq: Seq[_] => typeOfArray(seq)
        case value => typeOfPrimitiveValue(value)
      }.reduce((type1: DataType, type2: DataType) => compatibleType(type1, type2))

      ArrayType(elementType, containsNull)
    }
  }

  def allKeysWithValueTypes(m: Map[String, Any]): Set[(String, DataType)] = {
    val keyValuePairs = m.map {
      // Quote the key with backticks to handle cases which have dots
      // in the field name.
      case (key, value) => (s"`$key`", value)
    }.toSet
    keyValuePairs.flatMap {
      case (key: String, struct: Map[_, _]) => {
        // The value associated with the key is an Struct object.
        allKeysWithValueTypes(struct.asInstanceOf[Map[String, Any]]).map {
          case (k, dataType) => (s"$key.${k.toLowerCase}", dataType)
        } ++ Set((key, StructType(Nil)))
      }
      case (key: String, array: Seq[_]) => {
        // The value associated with the key is an array.
        // Handle inner structs of an array.
        def buildKeyPathForInnerStructs(v: Any, t: DataType): Seq[(String, DataType)] = t match {
          case ArrayType(e: StructType, _) => {
            // The elements of this arrays are structs.
            v.asInstanceOf[Seq[Map[String, Any]]].flatMap(Option(_)).flatMap {
              element => allKeysWithValueTypes(element)
            }.map {
              case (k, t) => (s"$key.$k", t)
            }
          }
          case ArrayType(t1, containsNull) =>
            v.asInstanceOf[Seq[Any]].flatMap(Option(_)).flatMap {
              element => buildKeyPathForInnerStructs(element, t1)
            }
          case other => Nil
        }
        val elementType = typeOfArray(array)
        buildKeyPathForInnerStructs(array, elementType) :+(key, elementType)
      }
      case (key: String, value) => (key, typeOfPrimitiveValue(value)) :: Nil
    }
  }

  /**
   * Converts a Java Map/List to a Scala Map/Seq.
   * We do not use Jackson's scala module at here because
   * DefaultScalaModule in jackson-module-scala will make
   * the parsing very slow.
   */
  private def scalafy(obj: Any): Any = obj match {
    case map: java.util.Map[_, _] =>
      // .map(identity) is used as a workaround of non-serializable Map
      // generated by .mapValues.
      // This issue is documented at https://issues.scala-lang.org/browse/SI-7005
      JMapWrapper(map).mapValues(scalafy).map(identity)
    case list: java.util.List[_] =>
      JListWrapper(list).map(scalafy)
    case atom => atom
  }

  def inferSchema(data: RDD[Map[String, Any]], samplingRatio: Double) = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
    val schemaData = if (samplingRatio > 0.99) data else data.sample(withReplacement = false, samplingRatio, 1)
    val allKeys =
      schemaData.map(allKeysWithValueTypes).reduce(_ ++ _)
    createSchema(allKeys)
  }

  /**
   * :: Experimental ::
   * Loads an RDD[Any] with implicit MapBuilder
   * returning the result as a DataFrame.
   *
   */
  def toDataFrame[T](rdd: RDD[T], schema: StructType)(implicit mapBuilder: T => Map[String, Any], sqlc: SQLContext): DataFrame = {
    val mapRdd = rdd.map(mapBuilder)
    val appliedSchema =
      Option(schema).getOrElse(
        nullTypeToStringType(inferSchema(mapRdd, 1.0)))
    val rowRDD = mapRdd.map(asRow(_, appliedSchema))
    sqlc.createDataFrame(rowRDD, appliedSchema)
  }

  def toDataFrame[T](rdd: RDD[T], samplingRatio: Double = 1.0)(implicit mapBuilder: T => Map[String, Any], sqlc: SQLContext): DataFrame = {
    val mapRdd = rdd.map(mapBuilder)
    val appliedSchema = nullTypeToStringType(inferSchema(mapRdd, samplingRatio))
    val rowRDD = mapRdd.map(asRow(_, appliedSchema))
    sqlc.createDataFrame(rowRDD, appliedSchema)
  }

  // Convenient function for converting Map[String, Any]
  def mapToDataFrame(rdd: RDD[Map[String, Any]], samplingRatio: Double = 1.0)(implicit sqlc: SQLContext): DataFrame = {
    def id[T](x: T) = x
    toDataFrame(rdd, samplingRatio)(id, sqlc)
  }

  def mapToDataFrame(rdd: RDD[Map[String, Any]], schema: StructType)(implicit sqlc: SQLContext): DataFrame = {
    def id[T](x: T) = x
    toDataFrame(rdd, schema)(id, sqlc)
  }

}
