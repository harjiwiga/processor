package co.id.lsa.proc

import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.spark.{SparkConf, SparkContext}
import unicredit.spark.hbase.HBaseConfig
import unicredit.spark.hbase._
import scala.collection.mutable.ArrayBuffer


/**
  * Created by harji on 3/7/16.
  */
object MainHbaseProcess {


   private val sparkConf = new SparkConf().setAppName("HbaseProcessor").setMaster("local").set("spark.executor.memory", "1g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
   private val jsc = new SparkContext(sparkConf)

   def main(args: Array[String]) {

      implicit val config = HBaseConfig()
      val hbasecon = config.get
      hbasecon.addResource("processor-conf.xml")
      val tablename2 = hbasecon.get("hbase.tablename")
      println("hbase.rootdir:" + hbasecon.get("hbase.rootdir"))
      val hbaseAdmin = new HBaseAdmin(hbasecon)

      val table = "test-table"
      val families = Set("cf1", "cf2")

      if (hbaseAdmin.isTableAvailable(tablename2)) {

         val families = Set("f", "prod", "comm_t")
         val columns = Map("prod" -> Set("desc", "sn")
         )
         val tableRdd2 = jsc.hbaseTS[String](tablename2, families)
         //      val rddpart = tableRdd2.partitions


         val rddfiltered = tableRdd2.filter(x => x._2.get("prod").exists(_.get("sn").nonEmpty))

         var smalest = 1000
         val rddconverted = rddfiltered.map { eachVal => {
            val sn = eachVal._2.get("prod").get("sn")._1.filter(v => !v.equals(""))
            val cat = eachVal._2.get("prod").get("cat")._1
            val newVal = sn.replace("rb", "00").replace(",", "")
            val prot = eachVal._2.get("f").get("prot")._1
            val price = eachVal._2.get("prod").get("prc")._1.replace("Rp", "").trim.replace(".", "")
            val kLength = eachVal._1.trim.length

            var lengthArr = ArrayBuffer[Int]()
            lengthArr += kLength

            Map("rk" -> eachVal._1, "cat" -> cat, "sn" -> newVal, "prot" -> prot, "pr" -> price)
         }
         }

         rddfiltered.foreach(i => println("filtered_data:" + (i._2.get("prod").get("sn")._1)))
         rddconverted.foreach(i => println("convertedData: " + i))

         var arrKeys = ArrayBuffer[String]()

//         val maxKey = rddconverted.
         println("VALARRAY: "+arrKeys)
//
//         val rddlenghtkey = rddconverted.reduce((k,v) => )
         val rddGrouped = rddconverted.groupBy(x => {
            //need improve to get substring index by the shortest key
            var key = x.get("rk").mkString; key.substring(0,80)
         })

//          val rddMaped = rddGrouped.map(x => x._2.reduce(_++:_))// need improvement

         rddGrouped.map(x => x._2.reduce(_++:_))
         rddGrouped.foreach(x => println("rddgrouped: "+x._2))

//         rddMaped.foreach(m => println("mp: "+m));


         println("valuess:" + rddfiltered.count())
         println("class rdd: " + tableRdd2.getClass)
      }

      hbaseAdmin.close();
      jsc.stop();
      System.exit(0);
   }

   def convert(sn: String): String = {
      sn.replace(",", "").replace("rb", "00")
   }


   /**
     * This method merge map within list
     *
     * @param ms
     * @param f
     * @tparam A
     * @tparam B
     * @return
     * ref: http://stackoverflow.com/questions/1262741/scala-how-to-merge-a-collection-of-maps
     * by Walter Chang
     */
   def mergeMap[A, B](ms: List[Map[A, B]])(f: (B, B) => B): Map[A, B] =
      (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
         a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
      }
}
