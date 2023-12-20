import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
val redwine = spark.read.option("header",true).csv("finalproj/redwine.csv")
val whitewine = spark.read.option("header",true).csv("finalproj/whitewine.csv")

def cleanData(df: DataFrame): DataFrame = {
  // Drop rows with density greater than 1 in column 8 (adjust column index as needed)
  val cleanedDF = df.filter(row => row.getString(7).toDouble <= 1)
  // You can add more data cleaning steps here if needed
  cleanedDF
}

val cleanedredwine = cleanData(redwine)
val cleanedwhitewine = cleanData(whitewine)

val roundStringFloatColumn = udf((value: String, digits: Int) => {
  try {
    BigDecimal(value.toDouble).setScale(digits, BigDecimal.RoundingMode.HALF_UP).toString
  } catch {
    case _: NumberFormatException => value // If parsing fails, keep the original value
  }
})

val redwineTransformed = cleanedredwine
  .withColumn("density", roundStringFloatColumn(col("density"), lit(4)))
  .withColumn("alcohol", roundStringFloatColumn(col("alcohol"), lit(1)))

val whitewineTransformed = cleanedwhitewine
  .withColumn("density", roundStringFloatColumn(col("density"), lit(4)))
  .withColumn("alcohol", roundStringFloatColumn(col("alcohol"), lit(1)))

redwineTransformed.write.option("header", "true").csv("finalproj/output/redwine")
whitewineTransformed.write.option("header", "true").csv("finalproj/output/whitewine")