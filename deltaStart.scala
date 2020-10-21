/**DeltaTest > deltaStart.scala  (IntelliJ 2020.2.3)  Spark 3.0.1 & Scala 2.12.10
 * Test out delta lake modifying the QuickStart example on github
 * this involves reading/writing/modifying  a spark.range(n) dataframe
 * to  parquet format and then writing that to a 'delta table
 * This doc illustrates using the  Delta functions to  keep track of all the
 * reads and writes so they can be revisited as desired ( time-travel)
 * 1. SBT file nuances
 *Special build,sbt file. note resolver for searches other than
 * the mvnrepository. also, see the function defined right inside the build.sbt
 * file...
 *  2020-10-10/ 18 rr
 * Below is a copy of a few parquet files and associated log files that Delta keeps
 * 3.  (Part of) Parquet files as a result of code below
 * part-00000-1d4c732d-b717-4d94-badb-8017e7aafe6c-c000.snappy.parquet
part-00000-28e7e6d1-1495-41e6-9f69-1c89ae702f86-c000.snappy.parquet
part-00000-50c66add-f496-40a4-b8b3-aa196da52824-c000.snappy.parquet
part-00000-47686cc9-3862-4ca2-8b79-691473250df0-c000.snappy.parquet
 * part-00000-1d4c732d-b717-4d94-badb-8017e7aafe6c-c000.snappy.parquet
  (Part of) DELTA LOG
00000000000000000004.json
00000000000000000003.json
00000000000000000002.json
00000000000000000001.json
00000000000000000000.json
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.commons.io.FileUtils
import java.io.File

import cats.data          // a strictly functional library
import dataclass.data
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLImplicits, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import scala.reflect.internal.util.FileUtils

object deltaStart  {
def main(args: Array[String]): Unit = {
Logger.getLogger("org").setLevel(Level.OFF)
println(s"Delta Test ${new java.util.Date()} ") // seamless integration with Java
type S = String;
type I = Integer;
type D = Double;
type B = Boolean // type synonyms
val spark = SparkSession
.builder()
.appName("DeltaTest ")
.master("local[*]")
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
.getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "5")
import spark.implicits._
println(s"Set up a local file for a parquet dataset, /Users/rob/tmp/delta_table")
// the FileUtils was ambiguous so I used the complete path
val file = new File("/Users/rob/tmp/delta_table")
if (file.exists()) org.apache.commons.io.FileUtils.deleteDirectory(file)

println("get the (canonical) file path and then Create a table from spark.range(0,5)")
val path = file.getCanonicalPath
println(s" Path to new file.getCanonicalPath  is $path")
println(s"  'VAR' write out a range(0,5).toDF(nr) to the path as a delta file ,I made the col = nr  ")
var df1 = spark.range(0, 5).toDF("nr")
val delta1 = df1.write.format("delta").save(path)
println(s" df1 -> delta1  and show path to the delta file ")
println(path)

println(s" Now read that df=range(0,5).toDF(nr) -> delta file back in and show it as a DF")
val df = spark.read.format("delta").load(path)
df.show()
println(s" print the schema for the range(0,5).toDF")
df.printSchema()
println(s" See io.delta docs ")
println(s" New operation 'Upsert (merge)' new data  ")
println("Upsert 'newData', spark.range(0, 20).toDF(nr) with oldData range(0,5).toDF(nr)")
println(s"alias the old table as 'oldData' , alias the new table as 'newData' ")
val newData = spark.range(0, 20).toDF("nr")
val deltaTable = DeltaTable.forPath(path)
deltaTable.as("oldData")
.merge(
newData.as("newData"),
"oldData.nr = newData.nr")
.whenMatched
.update(Map("nr" -> col("newData.nr")))
.whenNotMatched
.insert(Map("nr" -> col("newData.nr")))
.execute()
println(s" Now show the merged table")
deltaTable.toDF.show()
println(s" Now Overwrite the current table, with data = spark.range(5, 10 ), to path $path ")
// Update table data, why no 'val' here ?
df1 = spark.range(5, 10).toDF("nr")
df1.write.format("delta").mode("overwrite").save(path)

println(s" Now show overwritten table ")
deltaTable.toDF.show()

// Update every even value by adding 100 to it
println("Update to the table (add 100 to every even value)")
deltaTable.update(
condition = expr("nr % 2 == 0"),
set = Map("nr" -> expr("nr + 100")))
deltaTable.toDF.show()
println(s" Now delete every even value and show")
// Delete every even value
deltaTable.delete(condition = expr("nr % 2 == 0"))
deltaTable.toDF.show()
println(s" Now read old data using 'time travel' ")
// Read old version of the data using time travel
print("Read old data using time travel   version 0?")
val df2 = spark.read.format("delta")
                .option("versionAsOf", 0).load(path)
df2.show()


 println(
 s""" if you want to see what was written,
   | don't deleteDirectory, however, if you run again
   | it will error off """.stripMargin)
println(s" using FileUtils, delete the directory ")
org.apache.commons.io.FileUtils.deleteDirectory(file)
println(s" spark.stop() ")
spark.stop()

}// end main
}//end object
