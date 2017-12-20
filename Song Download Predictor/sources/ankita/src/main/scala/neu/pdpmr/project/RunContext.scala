package neu.pdpmr.project

import org.apache.spark.ml.feature.Imputer
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.{SparkConf, SparkContext}


object RunContext {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Predict Songs")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    def removeSpclChar = udf(($columnName : String) => cleanUp($columnName))

    //Download table

    val download_location = "input/downloads.csv"

    val customDownloadsSchema = StructType(Array(
      StructField("artist", StringType, true),
      StructField("title", StringType, true),
      StructField("price", DoubleType, true),
      StructField("downloads", IntegerType, true),
      StructField("confidence", StringType, true)))

    val downloads = spark.read
      .format("csv")
      .schema(customDownloadsSchema)
      .option("header", "true")
      .option("sep",";")
      .option("inferSchema","true")
      .load(download_location)

    def category = udf(($confidence : String) => if($confidence == "terrible") 1 else if ($confidence == "bad") 2 else if
    ($confidence == "average") 3 else if ($confidence == "good") 4 else if ($confidence == "very good") 5 else if ($confidence == "excellent") 6 else 0)

    val newDownloads = downloads.withColumn("ConfidenceId", category(downloads("confidence")))

//remove NA/null values
    val filterDownloads =  newDownloads.filter(r => nullChecker(r(0)) && nullChecker(r(1)))

    val cleanDownloads = filterDownloads.withColumn("songTitle",removeSpclChar(trim(col("title"))))
      .withColumn("artistName",removeSpclChar(trim(col("artist")))).drop("artist","title")
      .filter(r => nullChecker(r(4)) && nullChecker(r(5)))

// Song Info table
   val songInfo_location = "input/song_info.csv"

   val songInfo = spark.read.option("header", "true").option("delimiter", ";").csv(songInfo_location)

    val requiredSongInfo = songInfo.drop("track_id","audio_md5", "start_of_fade_out", "end_of_fade_in", "analysis_sample_rate",
      "key", "key_confidence", "mode", "mode_confidence", "time_signature", "danceability", "energy", "artist_location",
      "genre", "song_hotttnesss", "artist_id", "song_id","time_signature_confidence",
      "duration", "loudness","track_id", "tempo", "year").withColumnRenamed("artist_name","artistName").withColumnRenamed("title","songTitle")


   val filterSongInfo =  requiredSongInfo.filter(r => nullChecker(r(0)) && nullChecker(r(4)))

    val  songInfo_df = filterSongInfo
        .withColumn("title",removeSpclChar(trim(col("songTitle"))))
        .withColumn("artist",removeSpclChar(trim(col("artistName")))).filter(r => nullChecker(r(5)) && nullChecker(r(6)))


    val downloads_songInfo = cleanDownloads.join(songInfo_df,cleanDownloads.col("artistName")  === songInfo_df.col("artist")
      && songInfo_df.col("title").startsWith(cleanDownloads.col("songTitle"))).drop("artistName","songTitle").dropDuplicates()

    val downloadInfoSchema = downloads_songInfo.withColumn("artistFamiliarity",downloads_songInfo("artist_familiarity").cast(DoubleType))
      .withColumn("artistHotness",downloads_songInfo("artist_hotttnesss").cast(DoubleType))
      .drop("artist_familiarity","artist_hotttnesss")

    val tuple = downloadInfoSchema.select("artistFamiliarity","artistHotness").columns

//    downloadInfoSchema.show()

    val imputer = new Imputer()
      .setInputCols(tuple)
      .setOutputCols(tuple.map(c => s"${c}_imputed"))
      .setStrategy("median")

    val imputedDF = imputer.fit(downloadInfoSchema).transform(downloadInfoSchema)

    System.out.print(imputedDF.stat.corr("artistFamiliarity","downloads"))

    System.out.print(imputedDF.stat.corr("artistHotness","downloads"))

  }

  def cleanUp(s : String): String= {
    s.toLowerCase.replaceAll("[^a-zA-Z0-9\\s\\']","")
  }

  def nullChecker(s : Any): Boolean= {
    if (s != null && s.toString != "" && s.toString !="NA")
      return true
    return false
  }

}
