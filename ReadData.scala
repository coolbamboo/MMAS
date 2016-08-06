package MMAS

import org.apache.spark._
import org.apache.spark.rdd.RDD
import common._
/**
 * Created by root on 2016/3/5.
 */
case class AVS(s:Int, v:Int, Gs:Int)
//读完数据后要处理一下，获取出J的上限
//DSAK有最全的U
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
    val rawAVS = sc.textFile("hdfs://192.168.120.133:9000/WTA/data/input/svg.csv")
    val pasedAVS = rawAVS.map(line => parseAVS(line))
    pasedAVS.cache()
    pasedAVS
  }

  def dealDSAK(sc:SparkContext) = {
    val rawDSAK = sc.textFile("hdfs://192.168.120.133:9000/WTA/data/input/dsak.csv")
    val pasedDSAK = rawDSAK.map(line => parseDSAK(line))
    pasedDSAK.cache()
    pasedDSAK
  }

  def dealSANG(sc: SparkContext) = {
    val rawSANG = sc.textFile("hdfs://192.168.120.133:9000/WTA/data/input/sang.csv")
    val pasedSANG = rawSANG.map(line => parseSANG(line))
    pasedSANG.cache()
    pasedSANG
  }
}

object deal_Jup{
//返回DSAK_Jup的RDD
  def apply(sc:SparkContext)= {
    val (rawAVS, rawDSAK, rawSANG) = ReadData(sc)
  //遍历一个RDD的同时不能遍历其他的RDD
    val avss = rawAVS.collect()//取出RDD的值，好在值不多
    val dsaks = rawDSAK.collect()
    val Vdsak_j = dsaks.map{_ match {
        case DSAK_Jup(num,d,s,a,kdsa,j)=>{
          //求Gs
          var gs = avss.filter(x=>x.s==s).map(x=>x.Gs)
          val jup = Jupmin(Array(
            B(d-1),gs(0)/t(d-1),Cmax/c(d-1),M(d-1)/m(d-1)
          ))
          if(jup<0) throw new Exception("jup变量计算有误！")
          DSAK_Jup(num,d,s,a,kdsa,jup)
        }
      }
    }
    (Vdsak_j,rawAVS,rawSANG)
  }

  def Jupmin(x:Array[Int]):Int ={
    x.min
  }
}

object deal_U{
  var Vdsak_j:Array[DSAK_Jup] = null
  var rawAVS :RDD[AVS] = null
  var rawSANG:RDD[SANG] = null
  var Jmax = 0
  def apply(sc:SparkContext) ={
    if(Vdsak_j==null){
      val (dsakj1, avs1, sang1) = deal_Jup(sc)
      Vdsak_j = dsakj1
      rawAVS = avs1
      rawSANG = sang1
    }
    Vdsak_j
  }

  def getMaxJ() ={
    if(Jmax==0)
      Jmax = Vdsak_j.map(dsak_j => dsak_j.Jup).max
    Jmax
  }
}

