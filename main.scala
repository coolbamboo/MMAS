package MMAS

import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2016/3/6.
 */
object main {
  def main(args: Array[String]) {
    //System.setProperty("spark.driver.host","192.168.1.52")
    val conf = new SparkConf().setAppName("WTA").setMaster("local")
    //.setJars(List("D:\\IdeaWorkspace\\out\\artifacts\\SparkLocal_jar\\SparkLocal.jar"))
    //"D:\\IdeaWorkspace\\out\\artifacts\\SparkLocal_jar\\spark-assembly-1.4.1-hadoop2.6.0.jar"))
    //conf.set("spark.executor.memory", "1024m")
    //conf.set("spark.driver.host","192.168.1.52")
    val sc = new SparkContext(conf)
    val starttime = new Date().getTime
    //先读取数据并处理
    deal_U(sc)
    //运行
    new Tsp().dealflow()
    val stoptime = new Date().getTime
    printf("运行时间：%d 毫秒",stoptime-starttime)
    val results = sc.parallelize(Output.results.sortWith(_.ant.Fobj>_.ant.Fobj).toSeq)
    results.saveAsTextFile("hdfs://192.168.120.133:9000/WTA/data/output/result")
    sc.stop()
  }

}
