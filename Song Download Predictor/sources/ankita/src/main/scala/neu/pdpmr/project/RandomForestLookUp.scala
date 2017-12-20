package neu.pdpmr.project

import neu.pdpmr.project.RunContext.cleanUp
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, Word2Vec}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

class RandomForestLookUp {

  def predict(sc : SparkContext , spark : SparkSession, input : String, path: String): Integer = {


    val lookUpData = spark.read.parquet(path+"/inputData/")

    val pipelineModel = PipelineModel.read.load(path+"/TrainData-2/")

    /// Test Data extraction

    val testInput = new Driver()

    val predictionInput= testInput.readInput(sc,spark, input)

    val testData = predictionInput.withColumn("artist",lower(trim(predictionInput("artist"))))
      .withColumn("title",lower(trim(predictionInput("title"))))
      .withColumnRenamed("artist","artistName")
      .withColumnRenamed("title","songTitle")

    var extractFeatures = testData.join(lookUpData).where (lookUpData("artist").startsWith(testData("artistName"))
      && lookUpData("title").startsWith(testData("songTitle"))).drop("confidence","downloads","songTitle","artistName").dropDuplicates()

    if(extractFeatures.count() == 0) {
      extractFeatures = testData.join(lookUpData).where (lookUpData("artist").startsWith(testData("artistName"))
        || lookUpData("title").startsWith(testData("songTitle"))).drop("confidence","downloads","songTitle","artistName").dropDuplicates()
    }
    if(extractFeatures.count() == 0) {
      return 4175
    }

    // Predict downloads

    // make feature cols
    val testFeatureCols = Array("price", "ConfidenceId", "artistFamiliarity", "artistHotness", "artistCol", "titleCol")

    val testAssembler = new VectorAssembler().setInputCols(testFeatureCols).setOutputCol("features")

    val testPredict = testAssembler.transform(extractFeatures)

    val predictions = pipelineModel.transform(testPredict)

    val result = predictions.select("prediction").head().getDouble(0).toInt

    result
  }

}
