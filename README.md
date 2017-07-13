# Extension and Utilities for Spark Dataframe

## Usage

### SBT

Make sure you have PayPal nexus repo include in `resolvers` in your project setting.

``` scala
resolvers ++= Seq(
    ...
    "PayPal Nexus releases" at "http://nexus.paypal.com/nexus/content/repositories/releases",
    "PayPal Nexus snapshots" at "http://nexus.paypal.com/nexus/content/repositories/snapshots"
    )
```

Now you can include the library as a dependency.

``` scala
libraryDependencies += "com.paypal.risk.grs" %% "spark-dataframe-utils" % "0.1.2-SNAPSHOT"
```

### API usage

#### DataFrame Extensions

The extensions are mainly for exposing DataFrame internal information, such as partition specs.

To enable dataframe extensions, simply import the implicit class like this:

``` scala
import org.apache.spark.sql.ext.dataframe.Implicits._
```

*Implemented APIs*:

- `df.getPartitions` (Returns a list of partition spec objects)

- `df.getColumnAsMap(columns: Seq[String])` (Return a RDD of `Map[String, Any]` containing column names -> values)

- `df.toMap/toMapRDD` (Return a RDD of `Map[String, Any]` containing all column names -> values)

- `df.flatten` (Return a new DataFrame where all Map columns are flattened to Struct columns)

- `df.allKeysInMapColumn(column: String)` (Return a sequence of keys: String, where it's the union of all keys in each row of the column)

TODO: usage examples

#### MapRDD

MapRDD can infer directly from Scala/Java collections supporting Map interface (`Map[String, Any]`).

Usage:
``` scala
val rdd: RDD[Map[String, Any]] = ...
val df = MapRDD.mapToDataFrame(rdd)
// or
val rdd: RDD[T] = ...
val wrapper: T => Map[String, Any] = ...
val df = MapRDD.toDataFrame(rdd)(wrapper)
```

#### UDF utilities

- makeUdf(func) (Make a UDF from a scala function)

It's useful for controlling the scope of the UDF, as currently sqlc.udf.register can only register a UDF with global scope.

The downside is that it can only be used in the SparkSQL DSL, but since DataFrame is officially proposed as new common base,
DSL is preferred than SQL for developers.

Example:
``` scala
val ip = parquetFile("data/ip_sample")
val vid = parquetFile("data/vid_sample")
val merge_map_d = makeUdf(UdfUtilsTest.mergeStringToDoubleMap _)
    val res =
      ip.join(vid, ip("mid") === vid("mid") and ip("source") === vid("source"), "left_outer")
        .select(merge_map_d(ip("nvar"), vid("nvar")) as "nvar")
```

## Author

- Jianshi Huang (jianshuang@paypal.com)
