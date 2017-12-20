package neu.pdpmr.project


import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

class Driver {

  def readInput(sc: SparkContext,spark : SparkSession, input: String): org.apache.spark.sql.DataFrame = {

    // Reading the clean data

    import spark.implicits._

    val data = sc.parallelize(List(input)).map(_.split(";")).map(a => (a(0), a(1))).toDF("artist", "title")

    val toString = udf((value: String) => value.split(" "))

    // convert artistName to a vector.

    val dataSet = data.withColumn("artistName", toString(data("artist")))

    val word2Vec = new Word2Vec()
      .setInputCol("artistName")
      .setOutputCol("artistCol")
      .setVectorSize(10)
      .setMinCount(0)

    val model = word2Vec.fit(dataSet)

    val result = model.transform(dataSet)

    // convert song title to a vector.

    val titleData = result.withColumn("songTitle", toString(data("title")))

    val word2VectorTitle = new Word2Vec()
      .setInputCol("songTitle")
      .setOutputCol("titleCol")
      .setVectorSize(10)
      .setMinCount(0)

    val modelTitle = word2VectorTitle.fit(titleData).transform(titleData).drop("artistName","songTitle")

    modelTitle


  }
}
