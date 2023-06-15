import DataSetOperations.{checkAnyMissingValue, preprocessArtificial, readDataset}
import Methods.windowsBased
import org.apache.spark.sql.SparkSession

object Driver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("ChangePointDetection").config("spark.master", "local[*]").getOrCreate()
    val sc = spark.sparkContext.setLogLevel("ERROR")
    val path = "1000_1_4_points.txt"
    val df =  preprocessArtificial(readDataset(path,spark))
    if (false != checkAnyMissingValue(df)){
      println("Exist missing value, terminating !!")
      return
    }
    println("No missing value for the dataset!!")
    println(df.count())
    val points = windowsBased(df,"L2",100)
    points.foreach(f => print(f + "\n"))
  }
}
