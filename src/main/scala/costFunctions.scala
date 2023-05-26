import org.apache.spark.sql.DataFrame
import scala.math.log
import scala.math.pow

object costFunctions {
  def tupleSum(t1: (Double, Int), t2: (Double, Int)): (Double, Int) = {

    return (t1._1 + t2._1, t1._2 + t2._2)
  }
  def meanOfSignal(dataFrame: DataFrame): Double = {

    val helper = dataFrame.select("value").rdd.map(f => (f.get(0).asInstanceOf[Double],1)).reduce(tupleSum)

    return helper._1/helper._2
  }

  def varOfSignal(dataFrame: DataFrame): Double = {

    val mean = meanOfSignal(dataFrame)
    val helper = dataFrame.select("value").rdd.map(f => (pow(f.get(0).asInstanceOf[Double]-mean,2), 1)).reduce(tupleSum)

    return helper._1 / helper._2
  }

  def costPoission(dataFrame: DataFrame): Double = {

    val mean = meanOfSignal(dataFrame)

    return -1*(dataFrame.count())*mean*log(mean)
  }

  def costL1(dataFrame: DataFrame): Double = {

    val mean = meanOfSignal(dataFrame)

    return dataFrame.select("value").rdd.map(f => (f.get(0).asInstanceOf[Double] - mean)).sum()
  }

  def costL2(dataFrame: DataFrame): Double = {

    val variance = varOfSignal(dataFrame)

    return variance*(dataFrame.count())
  }
}
