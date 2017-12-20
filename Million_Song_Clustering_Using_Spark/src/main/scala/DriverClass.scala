import Classification.runKmeans
import HAC.runHAC
import DriverClass.kCluster
import org.apache.spark.rdd.RDD
import HAC.runHAC2D
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Ankita
  * */

object DriverClass {

  def kCluster = 3

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Million Classification")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val input = sc.textFile("input/song_info.csv")
    // to run the program on big Dataset
//    val input = sc.textFile("bigInput/song_info.csv")

    val songInfos = input.mapPartitionsWithIndex { (idx, iterate) => if (idx == 0) iterate.drop(1) else iterate }.map(line => new SongInfo(line))
    val centroids = new Array[SongInfo](kCluster)

    //generate random intial centroids
    for (i <- 0 until kCluster) {
      centroids(i) = songInfos.takeSample(false, 1)(0)
      if (!centroids(i).isValid('fuzzyLoudness)) centroids(i).LOUDNESS = (0.123 * i + 0.01).toString
      if (!centroids(i).isValid('fuzzyLength)) centroids(i).DURATION = (0.123 * i + 0.01).toString
      if (!centroids(i).isValid('fuzzyTempo)) centroids(i).TEMPO = (0.123 * i + 0.01).toString
      if (!centroids(i).isValid('fuzzyHotness))
        centroids(i).SONG_HOTNESS = (0.123 * i + 0.01).toString
      if (!centroids(i).isValid('combinedHotness)) {
        centroids(i).SONG_HOTNESS = (0.123 * i + 0.01).toString
        centroids(i).SONG_HOTNESS = (0.321 * i + 0.01).toString
      }
    }

      runKmeans(songInfos, centroids, 'fuzzyLoudness)
      runKmeans(songInfos, centroids, 'fuzzyLength)
      runKmeans(songInfos, centroids, 'fuzzyTempo)
      runKmeans(songInfos, centroids, 'fuzzyHotness)
      runKmeans(songInfos, centroids, 'combinedHotness)

      runHAC(sc, songInfos,'fuzzyLoudness)
      runHAC(sc, songInfos, 'fuzzyLength)
      runHAC(sc, songInfos, 'fuzzyTempo)
      runHAC(sc, songInfos, 'fuzzyHotness)
      runHAC2D(sc, songInfos, 'combinedHotness)

    }

}
