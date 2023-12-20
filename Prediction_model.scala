import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator

// Initialize Spark session
val spark = SparkSession.builder.appName("RandomForestExample").getOrCreate()

def preprocessTrainAndPredictLR(data: DataFrame): Double = {
  // Convert string columns to double
  val dataConverted = data.columns.foldLeft(data) { (df, columnName) =>
    df.withColumn(columnName, col(columnName).cast("double"))
  }

  // Prepare data
  val featureColumns = dataConverted.columns.slice(0, dataConverted.columns.length - 1)
  val assembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
  val output = assembler.transform(dataConverted)

  // Identify and convert label column
  val labelIndexer = new StringIndexer().setInputCol("quality").setOutputCol("label").fit(output)
  val finalData = labelIndexer.transform(output)

  // Split data into training and test sets
  val Array(trainingData, testData) = finalData.randomSplit(Array(0.8, 0.2))

  // Build Linear Regression model
  val lr = new LinearRegression().setLabelCol("label").setFeaturesCol("features")
  val model = lr.fit(trainingData)

  // Make predictions
  val predictions = model.transform(testData)

  // Evaluate the model using R-squared
  val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2")
  evaluator.evaluate(predictions)
}

// Function to preprocess data, train model, and make predictions
def preprocessTrainAndPredictRF(data: DataFrame): Double = {
  // Convert string columns to double
  val dataConverted = data.columns.foldLeft(data) { (df, columnName) =>
    df.withColumn(columnName, col(columnName).cast("double"))
  }

  // Prepare data
  val featureColumns = dataConverted.columns.slice(0, dataConverted.columns.length - 1)
  val assembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
  val output = assembler.transform(dataConverted)

  // Identify and convert label column
  val labelIndexer = new StringIndexer().setInputCol("quality").setOutputCol("label").fit(output)
  val finalData = labelIndexer.transform(output)

  // Split data into training and test sets
  val Array(trainingData, testData) = finalData.randomSplit(Array(0.8, 0.2))

  // Build Random Forest model
  val rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")
  val model = rf.fit(trainingData)

  // Make predictions
  val predictions = model.transform(testData)

  // Evaluate the model
  val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
  evaluator.evaluate(predictions)
}


// Read datasets
val redwine = spark.read.option("header", true).csv("finalproj/redwinecleaned.csv")
val whitewine = spark.read.option("header", true).csv("finalproj/whitewinecleaned.csv")

val redwineR2 = preprocessTrainAndPredictLR(redwine)
val whitewineR2 = preprocessTrainAndPredictLR(whitewine)

// Apply the function to both datasets
val RFredwineAccuracy = preprocessTrainAndPredictRF(redwine)
val RFwhitewineAccuracy = preprocessTrainAndPredictRF(whitewine)


println(s"Red Wine R-squared For LinaerRegression = $redwineR2")
println(s"White Wine R-squared For LinaerRegression = $whitewineR2")

println(s"Red Wine Accuracy Using RandomForest = $RFredwineAccuracy")
println(s"White Wine Accuracy Using RandomForest = $RFwhitewineAccuracy")

def manualKFoldCrossValidation(data: DataFrame, numFolds: Int): Array[Double] = {
  val foldAccuracies = new ArrayBuffer[Double]()

  // Convert all feature columns to double
  val dataConverted = data.columns.foldLeft(data) { (df, columnName) =>
    if (columnName != "quality") // Replace "quality" with your label column name if different
      df.withColumn(columnName, col(columnName).cast("double"))
    else
      df
  }

  // Define feature columns
  val featureColumns = dataConverted.columns.filter(_ != "quality") // assuming "quality" is the label

  // Generate k random splits of the data
  val folds = dataConverted.randomSplit(Array.fill(numFolds)(1.0 / numFolds))

  for (testIndex <- 0 until numFolds) {
    // Combine (k-1) folds for training and leave one for testing
    val trainingDataUnassembled = folds.zipWithIndex.filter(_._2 != testIndex).map(_._1).reduce(_ union _)
    val testDataUnassembled = folds(testIndex)

    // Assemble features
    val assembler = new VectorAssembler().setInputCols(featureColumns).setOutputCol("features")
    val trainingData = assembler.transform(trainingDataUnassembled)
    val testData = assembler.transform(testDataUnassembled)

    // Apply StringIndexer to the label column if necessary
    val labelIndexer = new StringIndexer().setInputCol("quality").setOutputCol("label")
    val trainingDataFinal = labelIndexer.fit(trainingData).transform(trainingData)
    val testDataFinal = labelIndexer.fit(testData).transform(testData)

    // Train the Random Forest model on the training data
    val rf = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")
    val model = rf.fit(trainingDataFinal)

    // Make predictions on the test data
    val predictions = model.transform(testDataFinal)

    // Evaluate the model
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    // Store the accuracy
    foldAccuracies += accuracy
  }

  foldAccuracies.toArray
}

val RFredwineFoldAccuracies = manualKFoldCrossValidation(redwine, 10)
RFredwineFoldAccuracies.zipWithIndex.foreach { case (accuracy, index) =>
  println(s"Red wine Fold ${index + 1} Accuracy: $accuracy")
}

val RFwhitewineFoldAccuracies = manualKFoldCrossValidation(whitewine, 10)
RFwhitewineFoldAccuracies.zipWithIndex.foreach { case (accuracy, index) =>
  println(s"White wine Fold ${index + 1} Accuracy: $accuracy")
}
