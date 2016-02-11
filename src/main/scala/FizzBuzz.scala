import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ganeshchand on 2/10/16.
  *
  * To run this on your local machine
  *   `$ bin/spark-submit --class FizzBuzz /path-to/fizzbuzz-1.0-SNAPSHOT.jar`
  *
  */
object FizzBuzz {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Fizz Buzz").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val numbers = sc.parallelize(1 to 15)

    def transform(n: Int): String = {
      if (n % 3 == 0 && n % 5 == 0) "Fizz Buzz"
      else if (n % 3 == 0) "Fizz"
      else if (n % 5 == 0) "Buzz"
      else n.toString
    }
    val fizzBuzzKV = numbers.map(n => {
      (transform(n), 1)
    })

    fizzBuzzKV.collect().foreach(println)

    val fizzBuzzCount = fizzBuzzKV.reduceByKey(_ + _)

    // print
    fizzBuzzCount.sortByKey(numPartitions = 1).foreach(t => println(s"${t._1}  - ${t._2}"))

    sc.stop()
  }

}
