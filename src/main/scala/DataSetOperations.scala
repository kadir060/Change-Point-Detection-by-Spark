import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSetOperations {
  def readDataset(path: String, sparkSession: SparkSession): DataFrame = {
    println("Dataset is being read !!")
    val df = sparkSession.read.option("header","true").csv(path)
    println("Dataset has been read !!")
    return df
  }

  def preprocessBorcelik(dataFrame: DataFrame): DataFrame = {
    println("Preprocessing Borçelik Dataset")
    return dataFrame.select(col("timestamp").cast("long").as("timestamp"),
      col("value").cast("double").as("value"))
  }

  def preprocessLabelled(dataFrame: DataFrame): DataFrame = {
    println("Preprocessing Labelled Dataset")
    return dataFrame.select(col("timestamp").cast("long").as("timestamp"),
      col("value").cast("double").as("value"),
      col("label").cast("integer").as("label"))
  }

  def preprocessArtificial(dataFrame: DataFrame): DataFrame = {
    println("Preprocessing Artificial Dataset")
    return dataFrame.select(col("value").cast("double").as("value"))
  }

  def checkAnyMissingValue(dataFrame: DataFrame): Boolean ={
    println("Checking if any missing value")
    val newDf = dataFrame.select(dataFrame.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*)
    if (0 != newDf.first().get(0)) {
      println("Missing timestamp !!")
      return true
    }

    if (newDf.columns.contains("timestamp") == false){ // For artificial
      return false
    }

    if (0 != newDf.first().get(1)){
      println("Missing value !!")
      return true
    }

    if (newDf.columns.contains("label") && 0 != newDf.first().get(2) ){
      println("Missing label !!")
      return true
    }

    return false
  }


}
