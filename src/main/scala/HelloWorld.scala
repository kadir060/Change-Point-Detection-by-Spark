import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Spark SQL").config("spark.master", "local[*]").getOrCreate()
    val sc = spark.sparkContext
    val words = Array("one","two","two","four","five","six","six","eight","nine","ten")
    words.foreach(println)
    val deneme = words.union(words)
    deneme.foreach(println)
    val te = words.map(f => (f, 1)).toMap

    var number : Int = 0
    var number1 : Int = 0
    if (te.contains("one") == true){
      number = te("one")
    }
    if (te.contains("onee") == true){
      number1 = te("onee")
    }

    println(number,te("two"),number1)
    //      println("Hello World")
    //      val file = new FileWriter("deneme.txt")
    //      print(5.toString() + "  " + 6.toString() + " dene")
    //      file.write("Selam %d naber nasılsın\n")
    //      file.write(5.toString() + "iyiyim %d sen nasılsın")
    //      file.close()
  }
}
