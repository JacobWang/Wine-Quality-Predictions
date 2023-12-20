import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// Function to convert columns to double and compute statistics
def computeStatistics(data: DataFrame, wineType: String): Unit = {
  val numericData = data.columns.foldLeft(data) { (df, columnName) =>
    if (columnName != "quality") df.withColumn(columnName, col(columnName).cast("double"))
    else df
  }

  // Iterate over each column to compute statistics
  for (columnName <- data.columns.filter(_ != "quality")) {
    val stats = numericData.groupBy("quality").agg(
      mean(columnName).alias("mean"),
      stddev(columnName).alias("stddev"),
      max(columnName).alias("max"),
      min(columnName).alias("min")
    )

    stats.collect().foreach { row =>
      println(s"${wineType} wine quality group ${row.getAs[String]("quality")}, " +
        s"$columnName mean is ${row.getAs[Double]("mean")}, " +
        s"sd is ${row.getAs[Double]("stddev")}, max value is ${row.getAs[Double]("max")}, " +
        s"min value is ${row.getAs[Double]("min")}")
    }
  }
}

// Read datasets
val redwine = spark.read.option("header", true).csv("finalproj/redwinecleaned.csv")
val whitewine = spark.read.option("header", true).csv("finalproj/whitewinecleaned.csv")
val combinedWineData = redwine.unionByName(whitewine)

// Compute statistics for each dataset
computeStatistics(redwine, "Red")
computeStatistics(whitewine, "White")
// Compute statistics for the combined dataset
computeStatistics(combinedWineData, "Combined")