import DataSetOperations.{checkAnyMissingValue, preprocessBorcelik, readDataset}
import Methods.binSeg
import org.apache.spark.sql.SparkSession

object Driver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("AnomalyDetection").config("spark.master", "local[*]").getOrCreate()
    val sc = spark.sparkContext.setLogLevel("ERROR")
    val path = "borçelik_data.csv"
    //val path = "datasets2/g.csv"
    val df =  preprocessBorcelik(readDataset(path,spark))
    if (false != checkAnyMissingValue(df)){
      println("Exist missing value, terminating !!")
      return
    }
    println("No missing value for the dataset!!")
    //df.show()
    //calculateMetrics(lSVM(df))
    //println(meanOfSignal(df))
    //println(costPoission(df))
    //println(costL1(df))
    //println(costL2(df))
    //windowsBased(df,"L2",10000)
    //binSeg(df,"L1",3)
    //val points = windowsBased(df,"L2",10000)
    val points = binSeg(df,"L2",10)
    points.foreach(f => print(f + "\n"))
  }
}
