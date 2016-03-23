package co.id.lsa.proc

import co.id.lsa.mapper.ProductWeb
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by harji on 3/8/16.
  */
object MainTestScala {

//  private val sparkConf = new SparkConf().setAppName("HbaseProcessor").setMaster("local").set("spark.executor.memory", "1g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//  private val jsc = new SparkContext(sparkConf)


  def main(args: Array[String]) {
    val numbers = List(1, 2, 3, 4, 5,6,6)

    numbers.reverse.foreach {
      n: Int => println("Number: " + n)
    }
//    numbers.foreach(n -> { System.out.println("Numbers "+n);});
    val fewNum = numbers.drop(2)
    val fewNum2 = numbers.take(3)



    print("drop num: "+fewNum)
    print("\ntake num:"+fewNum2.map(x=>x*4).sum)

//    val words = Array("one", "two", "two", "three", "three", "three")
//    val wordPairsRDD = jsc.parallelize(words).map(word => (word, 1))
//
//    val wordCountsWithReduce = wordPairsRDD
//      .reduceByKey(_ + _)
//      .collect()
//
//    val wordCountsWithGroup = wordPairsRDD
//      .groupByKey()
//      .map(t => (t._1, t._2.sum))
//      .collect()
//
//    val prw = new ProductWeb()

    var arr = Array(2,3,1,0,-1)
    arr = sort(arr)

    var a = sumInts(1,5)
    println("\na:"+a)


    var b =sum(x=>x)
    println("\nb:"+b)
    arr.foreach(println)
//    println("\nid cust: "+customer.myFirstMethod())

    System.exit(0)
  }

  def sort(xs: Array[Int]): Array[Int] = {
    if (xs.length <= 1) xs
    else {
      val pivot = xs(xs.length / 2)
      Array.concat(sort(xs.filter(pivot >)), xs.filter (pivot ==), sort(xs.filter(pivot <)))
    }
  }

  def sumInts(a: Int, b: Int): Int = if (a > b) 0 else a + sumInts(a + 1, b)

  def sum(f: Int => Int): (Int, Int) => Int = {
    def sumF(a: Int, b: Int): Int =
      if (a > b) 0 else f(a) + sumF(a + 1, b)
    sumF
  }



}
