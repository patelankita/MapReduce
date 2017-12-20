import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, Word2Vec}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object trainModel {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Predict Songs")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark sql start")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val trainData = spark.read.parquet("Data/inputData/")

    val convertToString = udf((value: String) => value.split(" "))

    // convert artistName to a vector.

    val dataSet = trainData.withColumn("artistName", convertToString(trainData("artist")))

    val word2Vec = new Word2Vec()
      .setInputCol("artistName")
      .setOutputCol("artistCol")
      .setVectorSize(10)
      .setMinCount(0)

    val model = word2Vec.fit(dataSet)

    val result = model.transform(dataSet)
    //
    //    //    result.show()
    //
    //    // convert song title to a vector.
    //
    val titleData = result.withColumn("songTitle", convertToString(trainData("title")))

    val word2VectorTitle = new Word2Vec()
      .setInputCol("songTitle")
      .setOutputCol("titleCol")
      .setVectorSize(10)
      .setMinCount(0)

    val modelTitle = word2VectorTitle.fit(titleData)

    val resultTitle = modelTitle.transform(titleData)
    //

    // convert release to vector

    val releaseData = resultTitle.withColumn("releaseVec", convertToString(resultTitle("release")))

    val word2VectorRelease = new Word2Vec()
      .setInputCol("releaseVec")
      .setOutputCol("releaseVector")
      .setVectorSize(10)
      .setMinCount(0)

    val modelRelease = word2VectorRelease.fit(releaseData)

    val resultData = modelRelease.transform(releaseData)

    //RF MODEL

    val subDataset = resultData.drop("confidence", "songTitle", "artistFamiliarity", "artistHotness")
      .withColumnRenamed("artistHotness_imputed", "artistHotness")
      .withColumnRenamed("artistFamiliarity_imputed", "artistFamiliarity")
      .toDF()

    //make feature cols
    val featureCols = Array("price", "ConfidenceId", "artistFamiliarity", "artistHotness", "artistCol", "titleCol")

    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")

    val temp = assembler.transform(subDataset)


    //  split the dataframe into training and test data**
    val splitSeed = 5043

    //    trainingData.show()
    val trainingData = temp.select("features", "downloads")

    val Array(trdData, tstData) = trainingData.randomSplit(Array(0.7, 0.3), splitSeed)


    val rf = new RandomForestRegressor()
      .setLabelCol("downloads")
      .setFeaturesCol("features")
      .setMaxDepth(17)
      .setNumTrees(30)
      .setFeatureSubsetStrategy("all")
      .setImpurity("variance")

    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(rf))

    // Train model. This also runs the indexer.
    val modal = pipeline.fit(trdData)
    modal.save("data/TrainData-2/")


    val predictions = modal.transform(tstData)

    // Select example rows to display.
    //        predictions.select("features", "downloads")
    //        predictions.show(15)


    val evaluator = new RegressionEvaluator()
      .setLabelCol("downloads")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)

    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
  }


}
