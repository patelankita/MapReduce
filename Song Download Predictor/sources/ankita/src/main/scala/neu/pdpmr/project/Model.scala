package neu.pdpmr.project
import scala.util.control.Breaks._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import scala.sys.process._
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.File
import org.apache.commons.io.FileUtils

object Model {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Predict Songs")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val tempDirPath = util.Properties.envOrElse("TEMP_DIR_PATH","temp")
    val s = tempDirPath

    val dataJarPath = getClass.getResource("/input.tar.bz2")
    val dataOutPath = new File(s+"/input.tar.bz2")
    

    val modelJarPath = getClass.getResource("/model.tar.bz2")
    val modelOutPath = new File(s+"/model.tar.bz2")

    FileUtils.copyURLToFile(dataJarPath, dataOutPath)
    FileUtils.copyURLToFile(modelJarPath, modelOutPath)

    Process("tar -xjvf "+ s + "/input.tar.bz2 -C "+s).run

    Process("rm "+ s + "/input.tar.bz2").run

    Process("tar -xjvf "+ s + "/model.tar.bz2 -C "+s).run

    Process("rm "+ s + "/model.tar.bz2").run

    val spark = SparkSession
      .builder()
      .appName("Spark sql start")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

     val input = new RandomForestLookUp()
    var inputExists = false
    while(!inputExists) {
      val ln = scala.io.StdIn.readLine()
      if (!(ln == null || ln.equals(""))){
        val result = input.predict(sc, spark, ln, s)
        System.out.println(result)
      }
      else{
        inputExists = true
      }
      
    }
  }

}
