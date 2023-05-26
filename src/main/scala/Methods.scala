import costFunctions.{costL1, costL2, costPoission}
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}

import scala.collection.mutable.ArrayBuffer

object Methods {

  def calculateMetrics(dataFrame: DataFrame): Unit = {
    if(!(dataFrame.columns.contains("label") && dataFrame.columns.contains("prediction")) ){
      println("calculateMetrics function requires a dataset having prediction and label columns")
      return false
    }
    val helperDataset = dataFrame.select(col("prediction").cast("float").as("prediction"),
      col("value").cast("float").as("value"))

    val predictionAndLabels = helperDataset.rdd.map(f=> Tuple2(f.get(0),f.get(1)))

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.accuracy)


  }

  def lSVM(dataFrame: DataFrame): DataFrame = {
    println("Linear Support Vector Machine")
    val vecAssemb = new VectorAssembler().setInputCols(Array("value")).setOutputCol("features")

    val newDF = vecAssemb.transform(dataFrame)

    val lsvc = new LinearSVC().setMaxIter(10).setRegParam(0.1)

    val lsvcModel = lsvc.fit(newDF)

    //println(s"Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")

    return lsvcModel.transform(newDF)

  }

  def windowsBased(dataFrame: DataFrame, costFunctionType: String, windowsSize: Int): Unit = {
    if (true != dataFrame.columns.contains("value")) {
      println("windowsBased requires dataframe having value column")
      return -1
    }
    //val newDf = dataFrame.select("value").withColumn("id",monotonically_increasing_id())
    val newDf = dataFrame.withColumn("id",row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)

    val size = newDf.count()
    var t = 0
    var scores = ArrayBuffer[(Double,Int)]()
    for (t <- windowsSize to size.toInt - windowsSize){
      val p = newDf.filter(col("id").between(t-windowsSize,t)).toDF()
      val q = newDf.filter(col("id").between(t,t+windowsSize)).toDF()
      val r = newDf.filter(col("id").between(t-windowsSize,t+windowsSize)).toDF()
      //print("p=" + p.count() + " q=" + q.count() + " r=" + r.count() +"\n")
      print(t-windowsSize +  "/" + (size - windowsSize) + "\n")
      val score = costFunctionType match {
        case "Poission" => costPoission(r)-costPoission(p)-costPoission(q)
        case "L1" => costL1(r)-costL1(p)-costL1(q)
        case "L2" => costL2(r)-costL2(p)-costL2(q)
        case _ => -23
      }
      if (-23 == score ){
        println("Invalid cost function !")
        return
      }
      scores += Tuple2(score,t)
    }
    scores.toArray.sortBy(f => f._1).reverse.foreach(f=> println(f._1,f._2))
  }
}
