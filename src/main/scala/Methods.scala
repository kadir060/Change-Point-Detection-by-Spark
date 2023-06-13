import costFunctions.{costL1, costL2, costPoission}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}
import scala.collection.mutable.ArrayBuffer

object Methods {

  def calculateMetrics(dataFrame: DataFrame): Unit = {
    if (!(dataFrame.columns.contains("label") && dataFrame.columns.contains("prediction"))) {
      println("calculateMetrics function requires a dataset having prediction and label columns")
      return false
    }
    val helperDataset = dataFrame.select(col("prediction").cast("float").as("prediction"),
      col("value").cast("float").as("value"))

    val predictionAndLabels = helperDataset.rdd.map(f => Tuple2(f.get(0), f.get(1)))

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.accuracy)

  }

  def windowsBased(dataFrame: DataFrame, costFunctionType: String, windowsSize: Int): Array[Int] = {
    if (true != dataFrame.columns.contains("value")) {
      println("windowsBased requires dataframe having value column")
      return Array(-1)
    }
    //val newDf = dataFrame.select("value").withColumn("id",monotonically_increasing_id())
    var newDf = dataFrame.withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
    val size = newDf.count()
    var t = 0
    var scores = ArrayBuffer[(Double, Int)]()
    for (t <- windowsSize to size.toInt - windowsSize) {
      val p = newDf.filter(col("id").between(t - windowsSize, t)).toDF()
      val q = newDf.filter(col("id").between(t, t + windowsSize)).toDF()
      val r = newDf.filter(col("id").between(t - windowsSize, t + windowsSize)).toDF()
      //print("p=" + p.count() + " q=" + q.count() + " r=" + r.count() +"\n")
      print(t - windowsSize + "/" + (size - windowsSize) + "\n")
      val score = costFunctionType match {
        case "Poission" => costPoission(r) - costPoission(p) - costPoission(q)
        case "L1" => costL1(r) - costL1(p) - costL1(q)
        case "L2" => costL2(r) - costL2(p) - costL2(q)
        case _ => -23
      }
      if (-23 == score) {
        println("Invalid cost function !")
        return Array(-1)
      }
      scores += Tuple2(score, t)
    }
    scores = scores.sortBy(f => f._1).reverse
    return scores.map(f => f._2).toArray
  }

  def binSeg(dataFrame: DataFrame, costFunctionType: String, pointCount: Int ): Array[Int] = {
    if (true != dataFrame.columns.contains("value")) {
      println("binSeg requires dataframe having value column")
      return Array(-1)
    }
    var newDf = dataFrame.withColumn("id", row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
    newDf = newDf.filter(col("id").between(0,400))
    val iter = 0
    val size = newDf.count()
    var points = ArrayBuffer[Int]()
    for (iter <- 1 to pointCount){
      print("iter=" + iter + "\n")
      val k = points.size
      var scores = ArrayBuffer[(Double, Int)]()
      if (k == 0) {
        var t = 0
        for (t <- 0 to size.toInt-2) {
          val p = newDf.filter(col("id").between(0, t)).toDF()
          val q = newDf.filter(col("id").between(t + 1, size-1)).toDF()
          //print("p=" + p.count() + " q=" + q.count() + "\n")
          val score = costFunctionType match {
            case "Poission" => costPoission(p) + costPoission(q)
            case "L1" => costL1(p) + costL1(q)
            case "L2" => costL2(p) + costL2(q)
            case _ => -23
          }
          //print(costL2(p) + " " + costL2(q) + "\n")
          if (-23 == score) {
            println("Invalid cost function !")
            return Array(-1)
          }
          scores += Tuple2(score, t)
        }
        val best = scores.toArray.maxBy(_._1)
        points += best._2
      }
      else{
        var t = 0
        for(t <-0 to points.size){
          var b = 0
          var f = 0
          if(t == 0){
            b = 0
            f = points(0)
          }
          else if (t == points.size){
            b = points(t-1) + 1
            f = size.toInt-1
          }
          else {
            b = points(t-1)+1
            f = points(t)
          }
          if(b < f){
            var newDFH = newDf.filter(col("id").between(b, f))
            val currentCost = costFunctionType match {
              case "Poission" => costPoission(newDFH)
              case "L1" => costL1(newDFH)
              case "L2" => costL2(newDFH)
              case _ => -23
            }

            var h = b
            for (h <- b to f) {
              val p = newDFH.filter(col("id").between(b, h))
              val q = newDFH.filter(col("id").between(h, f))
              val cost = costFunctionType match {
                case "Poission" => currentCost - costPoission(p) + costPoission(q)
                case "L1" => currentCost - costL1(p) + costL1(q)
                case "L2" => currentCost - costL2(p) + costL2(q)
                case _ => -23
              }
              scores += Tuple2(cost, h)
            }
          }

        }
        val best = scores.toArray.maxBy(_._1)
        points += best._2
        points = points.sortBy(f => f)
        //points.foreach( f=> println(f))
      }
    }
    return points.toArray
  }

}

