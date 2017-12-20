//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.feature.VectorAssembler
//import org.apache.spark.ml.regression.RandomForestRegressor
//
//object RandomForestStep2 {
//
//
//  val featureColStep2 = Array("artistCol","titleCol","ConfidenceId","price")
//
//  val assemblerSTep2 = new VectorAssembler().setInputCols(featureColStep2).setOutputCol("featureColStep2")
//
////  val tempStep2 =assemblerSTep2.transform(subDataset)
//
//  // Train a RandomForest model.
//  val rfStep2 = new RandomForestRegressor()
//    .setLabelCol("price")
//    .setFeaturesCol("featureColStep2")
//    .setMaxDepth(5)
//    .setNumTrees(30)
//    .setFeatureSubsetStrategy("all")
//
//  //
//  System.out.print("Initialized2")
//  //
//  // Chain indexer and forest in a Pipeline.
//  val pipelineStep2 = new Pipeline().setStages(Array(rfStep2))
//  //
//  System.out.print("set pipeline 2")
//
//  // Train model. This also runs the indexer.
//  val modalStep2 = pipelineStep2.fit(trainingData)
//  modalStep2.save("data/outputBigData/")
//
//
//  testData.show()
//
//  // Make predictions.
//  val predictionsStep2 = modalStep2.transform(newTestData)
//  //
//  System.out.print("Predict now")
//
//  // Select example rows to display.
//  predictionsStep2.select("features", "price")
//  predictionsStep2.show(15)
//
//  //
//  //    val evaluatorStep2 = new RegressionEvaluator()
//  //      .setLabelCol("price")
//  //      .setPredictionCol("prediction")
//  //      .setMetricName("rmse")
//  //
//  //    val rmseStep2 = evaluatorStep2.evaluate(predictionsStep2)
//  //
//  //    println("Root Mean Squared Error (RMSE) on test data = " + rmseStep2)
//
//}
