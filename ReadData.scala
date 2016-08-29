package MMAS

import org.apache.spark._
import org.apache.spark.rdd.RDD
import common._
/**
 * Created by root on 2016/3/5.
 */
case class AVS(s:Int, v:Int, Gs:Int)
//Jup is up bound of J
case class DSAK_Jup(num:Int,d:Int, s:Int, a:Int, Kdsa:Double, Jup:Int = 0)
case class SANG(s:Int, a:Int, nsa:Int, gsa:Double)

object ReadData{

  def parseAVS(line: String) = {
    val pieces = line.split(',')
    val s = pieces(0).toInt
    val v = pieces(1).toInt
    val Gs = pieces(2).toInt
    AVS(s, v, Gs)
  }

  def parseDSAK(line: String) = {
    val pieces = line.split(',')
    val num = pieces(0).toInt
    val d = pieces(1).toInt
    val s = pieces(2).toInt
    val a = pieces(3).toInt
    val Kdsa = pieces(4).toDouble
    DSAK_Jup(num, d, s, a, Kdsa)
  }

  def parseSANG(line: String) = {
    val pieces = line.split(',')
    val s = pieces(0).toInt
    val a = pieces(1).toInt
    val nsa = pieces(2).toInt
    val gsa = pieces(3).toDouble
    SANG(s, a, nsa, gsa)
  }

  def apply(sc : SparkContext) = {
    (dealAVS(sc) , dealDSAK(sc),  dealSANG(sc))
  }

  def dealAVS(sc : SparkContext) = {
    //1, svg
    val rawAVS = sc.textFile("hdfs://192.168.120.133:9000/WTA/data/input/1.csv")
    val pasedAVS = rawAVS.map(line => parseAVS(line))
    pasedAVS
  }

  def dealDSAK(sc:SparkContext) = {
    //2, dsak
    val rawDSAK = sc.textFile("hdfs://192.168.120.133:9000/WTA/data/input/2.csv")
    val pasedDSAK = rawDSAK.map(line => parseDSAK(line))
    pasedDSAK
  }

  def dealSANG(sc: SparkContext) = {
    //3, sang
    val rawSANG = sc.textFile("hdfs://192.168.120.133:9000/WTA/data/input/3.csv")
    val pasedSANG = rawSANG.map(line => parseSANG(line))
    pasedSANG
  }
}

object deal_Jup{
//return DSAK_Jup's RDD
  def apply(sc:SparkContext, rawAVS:RDD[AVS], rawDSAK:RDD[DSAK_Jup], rawSANG:RDD[SANG])= {
  //get value,not iter a RDD using another RDD
    val avss = rawAVS.collect()//
    val dsaks = rawDSAK.collect()
    val Vdsak_j = dsaks.map{_ match {
        case DSAK_Jup(num,d,s,a,kdsa,j)=>{
          //Gs
          var gs = avss.filter(x=>x.s==s).map(x=>x.Gs)
          val jup = Array(
            B(d-1),gs(0)/t(d-1),Cmax/c(d-1),M(d-1)/m(d-1)
          ).min
          if(jup<0) throw new Exception("jup is wrong！")
          DSAK_Jup(num,d,s,a,kdsa,jup)
        }
      }
    }
    (sc.parallelize(Vdsak_j),rawAVS,rawSANG)
  }
}

