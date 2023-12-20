import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

val redwine_notcleaned = spark.read.option("header", true).csv("finalproj/redwine.csv")
val whitewine_notcleaned = spark.read.option("header", true).csv("finalproj/whitewine.csv")

val redwineCount_notcleaned = redwine_notcleaned.count()
println(s"Total number of records for redwine before cleaning: $redwineCount_notcleaned")
val whitewineCount_notcleaned = whitewine_notcleaned.count()
println(s"Total number of records for whitewine before cleaning: $whitewineCount_notcleaned")

val redwine = spark.read.option("header", true).csv("finalproj/redwinecleaned.csv")
val whitewine = spark.read.option("header", true).csv("finalproj/whitewinecleaned.csv")

val redwineCount = redwine.count()
println(s"Total number of records for redwine after cleaning: $redwineCount")
val whitewineCount = whitewine.count()
println(s"Total number of records for whitewine after cleaning: $whitewineCount")

def mapAndCount(df: DataFrame): RDD[(String, Int)] = {
  val lastColumnIndex = df.columns.length - 1
  val keyValuePairRDD = df.rdd.map { row =>
    val key = row.getString(lastColumnIndex)
    (key, 1)
  }
  val countByKeyRDD = keyValuePairRDD.reduceByKey(_ + _)
  countByKeyRDD
}

val redwineresultRDD = mapAndCount(redwine)
val whitewineresultRDD = mapAndCount(whitewine)

redwineresultRDD.collect().foreach { case (key, count) =>
  println(s"Key(redwine_quality): $key, Count: $count")
}

whitewineresultRDD.collect().foreach { case (key, count) =>
  println(s"Key(whitewine_quality): $key, Count: $count")
}

def maxDecimalDigits(df: DataFrame): Unit = {
  df.columns.foreach { column =>
    // Define a UDF to compute the length after the decimal point for each string
    val decimalLength = udf((s: String) => {
      s.split("\\.") match {
        case Array(_, decimalPart) => decimalPart.length
        case _ => 0
      }
    })

    // Apply the UDF to each value in the column and find the maximum
    val maxLength = df.withColumn("decimalLength", decimalLength(col(column)))
      .agg(max("decimalLength"))
      .collect()(0)(0)

    println(s"Column: $column, Max decimal digits: $maxLength")
  }
}

maxDecimalDigits(redwine)
