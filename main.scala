package MMAS

import java.util.Date

import MMAS.common._
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by root on 2016/3/6.
 */
object main {
  def main(args: Array[String]) {
    //System.setProperty("spark.driver.host","192.168.1.52")
    val conf = new SparkConf().setAppName("WTA").setMaster("local[2]")
    //.setJars(List("D:\\IdeaWorkspace\\out\\artifacts\\SparkLocal_jar\\SparkLocal.jar"))
    //"D:\\IdeaWorkspace\\out\\artifacts\\SparkLocal_jar\\spark-assembly-1.4.1-hadoop2.6.0.jar"))
    //conf.set("spark.executor.memory", "1024m")
    //conf.set("spark.driver.host","192.168.1.52")
    val sc = new SparkContext(conf)
    //read data
    val (rawAVS, rawDSAK, rawSANG) = ReadData(sc)
    val starttime = new Date().getTime
    val (dsak_j_RDD, avs_RDD, sang_RDD) = deal_Jup(sc, rawAVS, rawDSAK, rawSANG)
    dsak_j_RDD.cache()
    avs_RDD.cache()
    sang_RDD.cache()
    val J_max = dsak_j_RDD.map(dsak_j => dsak_j.Jup).max
    //global pher and probability
    val g_Pher: Array[Array[Double]] = Array.ofDim(U, J_max + 1)
    val g_Prob: Array[Array[Double]] = Array.ofDim(U, J_max + 1)
    //init pher = pherMax
    for (i <- 0 until U)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = pher_max
      }
    //init an ant, add in bestants
    val bestants = scala.collection.mutable.ArrayBuffer[Ant](
      new Ant(g_Pher, U, J_max, dsak_j_RDD, avs_RDD, sang_RDD)
    )
    //run
    AntOneIteration(bestants, J_max, dsak_j_RDD, avs_RDD, sang_RDD, sc)
    val stoptime = new Date().getTime
    printf("the whole run time：%d ms",stoptime-starttime)
    val results = sc.parallelize(Output(bestants).sortWith(_.ant.Fobj>_.ant.Fobj))
    results.saveAsTextFile("hdfs://192.168.120.133:9000/WTA/data/output/result")
    sc.stop()
  }

}
