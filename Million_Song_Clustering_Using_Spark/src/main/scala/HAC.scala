import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author Ankita
  * */

object HAC {

  def kCluster = 3

  def runHAC(sc: SparkContext, songInfos: RDD[SongInfo], symbol: Symbol): Unit = {

    val filteredSongs = songInfos.filter(si => si.isValid(symbol))

    // Sort the value in ascending order assign index to each Datapoint.
    var clusterDataPoints = filteredSongs.sortBy(x => x.getCoordinate(symbol), ascending = true).map(x => List(x.getCoordinate(symbol))).collect().zipWithIndex.map(_.swap)

    // run till number of Cluster is equal to kCluster.
    while (clusterDataPoints.length != kCluster) {

      // Get pairs with minimum distance between.
      val minDistPair = clusterDataPoints.zip(clusterDataPoints.tail).minBy { case ((index1: Int, value1: List[Double]), (index2: Int, value2: List[Double]))
      => get1DimensionDistance(value2.head, value1.head)
      }

      // Merge pair
      clusterDataPoints = clusterDataPoints.filter(_ != minDistPair._2).map({ case (index: Int, value: List[Double]) =>
        if (index == minDistPair._1._1)
          (index, minDistPair._1._2 ::: minDistPair._2._2)
        else (index, value)
      })

    }
    val cluster = clusterDataPoints.map(_._2).zipWithIndex.map(_.swap)
    sc.parallelize(cluster.flatMap(a => a._2.map(b => a._1 + "," + b))).saveAsTextFile("outputHAC/"+symbol)
  }

  // HAC done for 2D dataset.
  def runHAC2D(sc: SparkContext, songInfos: RDD[SongInfo], symbol: Symbol): Unit = {

    val filteredSongs = songInfos.filter(si => si.isValid(symbol))

    var clusterDataPoints = filteredSongs.sortBy(x => get2DimensionDistance(x.SONG_HOTNESS.toDouble, (x.ARTIST_HOT.toDouble)), ascending = true)
      .map(x => List( (x.SONG_HOTNESS.toDouble,x.ARTIST_HOT.toDouble) )).collect().zipWithIndex.map(_.swap)

    while (clusterDataPoints.length != kCluster) {
      val minPair = clusterDataPoints.zip(clusterDataPoints.tail)
        .minBy { case ((index1: Int, value1: List[(Double, Double)]), (index2: Int, value2: List[(Double, Double)]))
        => get2DimensionDistance((value2.head._1 - value1.head._1),(value2.head._2 - value1.head._2))
        }
      clusterDataPoints = clusterDataPoints.filter(_ != minPair._2).map({ case (index: Int, value: List[(Double, Double)]) =>
          if (index == minPair._1._1) (index, minPair._1._2 ::: minPair._2._2) else (index, value)
        })

    }
    val cluster = clusterDataPoints.map(_._2).zipWithIndex.map(_.swap)
    sc.parallelize(cluster.flatMap(x => x._2.map(y => x._1 + "," + y._1 + ", " + y._2))).saveAsTextFile("outputHAC/"+symbol)
  }

  def get1DimensionDistance(p1: Double, p2: Double) = math.abs(p1 - p2)

  def get2DimensionDistance(p1 : Double, p2:Double) = math.sqrt(math.pow(p1, 2) + math.pow(p2, 2))


}






