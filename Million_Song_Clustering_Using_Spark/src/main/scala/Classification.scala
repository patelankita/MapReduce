import java.io.{File, PrintWriter}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Random, Try}

/**
  * @author Ankita
  */

object Classification {

  def epsilon = 0.001

  def runKmeans(songInfos : RDD[SongInfo],centroids: Seq[SongInfo], symbol: Symbol): Unit = {
    val filteredSI = songInfos.filter(si => si.isValid(symbol))
    val newCentroids = kMeans(filteredSI,centroids,symbol)
    val fuzzyHotnessClusters = getClusterByCentroids(filteredSI,newCentroids, symbol)
    symbol match{
      case 'fuzzyLoudness => print(fuzzyHotnessClusters,"output/kmeansLoudness.csv",symbol)
      case 'fuzzyLength => print(fuzzyHotnessClusters,"output/kmeansLength.csv",symbol)
      case 'fuzzyTempo => print(fuzzyHotnessClusters,"output/kmeansTempo.csv",symbol)
      case 'fuzzyHotness => print(fuzzyHotnessClusters,"output/kmeansHotness.csv",symbol)
      case 'combinedHotness => print(fuzzyHotnessClusters,"output/kmeansCombinedHotness.csv",symbol)
    }
  }


  def kMeans(songInfos : RDD[SongInfo],intitCentroids: Seq[SongInfo], symbol: Symbol): Seq[SongInfo] = {
    var centroids = intitCentroids
    for(i <- 0 to 9){
      // calculate cluster by input centroids
      var clusters = getClusterByCentroids(songInfos,centroids, symbol)
      // recalculate centroids
      centroids = getCentroids(clusters, symbol)
    }
    return centroids
  }


  def getClusterByCentroids(songInfos :RDD[SongInfo],centroids: Seq[SongInfo],symbol: Symbol ) = {
    songInfos.groupBy(song => {
      centroids.reduceLeft((a, b) =>
        if ((song.calculateDistance(a, symbol) ) < (song.calculateDistance(b, symbol))) a
        else b).getSymbol(symbol)})
  }


  def getCentroids(clusters : RDD[(String, Iterable[SongInfo])], symbol: Symbol ) : Seq[SongInfo]= {
    symbol match {
      case 'combinedHotness => get2DimensionCentroids(clusters, symbol)
      case _ => get1DimensionCentroids(clusters, symbol)
    }
  }

  // calculate centroids for 1D data
  def get1DimensionCentroids(clusters : RDD[(String, Iterable[SongInfo])], symbol: Symbol ) : Seq[SongInfo]= {
    val centroids = clusters.map(key => {
      var sum = 0.0
      var it = key._2
      for (i <- it){
        sum = i.getSymbol(symbol).toDouble + sum
      }
      new SongInfo(sum/it.size, symbol)
    }).collect().toList
    return centroids
  }

  // calculate centroids for 2D data
  def get2DimensionCentroids(clusters : RDD[(String, Iterable[SongInfo])], symbol: Symbol ) : Seq[SongInfo]= {
    val centroids = clusters.map(key => {
      var songSum = 0.0
      var artistSum = 0.0
      var it = key._2
      for (i <- it){
        songSum = i.SONG_HOTNESS.toDouble + songSum
        artistSum = i.ARTIST_HOT.toDouble + artistSum
      }
      new SongInfo(songSum/it.size, artistSum/it.size, symbol)
    }).collect().toList
    return centroids
  }

  def print(result: RDD[(String, Iterable[SongInfo])], path: String, symbol: Symbol): Unit = {
    val pw = new PrintWriter(new File(path))
    result.collect().foreach(x => x._2.foreach(
      y => pw.println(x._1 + "," +y.getSymbol(symbol))))
    pw.close()
  }

}

