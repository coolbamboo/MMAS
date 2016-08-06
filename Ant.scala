package MMAS

import MMAS.common._
import Tsp._
import org.apache.spark.rdd.RDD
import math.pow

import scala.util.Random

/**
 * Created by root on 2016/3/5.
 * 在初始化deal_U后才能使用
 * 使用ant的步骤：
 * 初始化（获取全局最优的pher值）
 * 搜索一遍，求出决策变量
 * 计算目标函数
 *
 * 要在spark运算需序列化
 */
class Ant(U:Int, Jmax:Int, vdsak_j:Array[DSAK_Jup], rawAVS:RDD[AVS], rawSANG:RDD[SANG])
extends Serializable {

  val avss = rawAVS.collect()
  val sangs = rawSANG.collect()
  val dsaks = vdsak_j

  var pathlength = 0//有值的变量长度
  val local_step = 1//优化步长

  var prob:Array[Array[Double]] = Array.ofDim(U, Jmax + 1)
  var pher:Array[Array[Double]] = _
  //决策变量
  val Xdsa :Array[Int] = new Array(U)
  //目标函数
  var Fobj = 0.0
  //部署的三种防御武器的总量
  var b_s: Array[Double] = new Array(D)
  //占地
  var g_s:Array[Double] = new Array(S)
  //成本
  var c_s:Array[Double] = new Array(D)
  //电力
  var m_s:Array[Double] = new Array(D)

  def init(): Unit ={

    pher = g_Pher.clone()
  }

  //搜索一次
  def search(): Unit = {
    dsaks.map {
      _ match {
        case DSAK_Jup(num,d,s,a,kdsa,jup)=>{
          //处理每一个stage
          //先考虑选出哪个j
          val l = constraints(num)//j的上限
          if(l==0){//如果上限已经为0，则其后所有的变量不会被取到，转移概率都是0
            pathlength = num
            //不需要再计算了
            return
          }else{
            //从l中取哪个？
            val probl:Array[Double] = new Array[Double](l+1)

            //此时选出的结果就是随机变量xdsa
            Xdsa(num-1) = computeProb_l(num,l,probl)
          }
          pathlength = num//蚂蚁走过的路径长度
        }
      }
    }
  }
  //对于某个stagei，判断其之前是否满足约束，满足返回最大的j
  private def constraints(row:Int): Int={
    //计算约束
    val B_total:Array[Int] = new Array(B.length)
    val G_total:Array[Int] = new Array(S)
    var C_total = 0
    var M_total:Array[Int] =new Array(M.length)
    val cst_dsak_j = dsaks.filter(x=>x.num<row).map(dsak=>{
      val d=dsak.d
      val s=dsak.s
      B_total(d-1) = B_total(d-1) + Xdsa(dsak.num-1)
      G_total(s-1) = G_total(s-1) + Xdsa(dsak.num-1)*t(d-1)
      C_total = C_total + Xdsa(dsak.num-1)*c(d-1)
      M_total(d-1) = M_total(d-1)+Xdsa(dsak.num-1)*m(d-1)
    }
    )
    //前row-1个变量已算好，接下来算第row个变量最大是几
    var l:Int = 0
    dsaks.filter(x=>x.num==row).map{dsak=>{
      val d=dsak.d
      val s=dsak.s
      var gs:Array[Int] = avss.filter(x=>x.s==s).map(x=>x.Gs)
      l = Array(B(d-1)-B_total(d-1),
        (gs(0)-G_total(s-1))/t(d-1),
        (Cmax-C_total)/c(d-1),
        (M(d-1)-M_total(d-1))/m(d-1)
      ).min
    }
    }
    if(l<0) throw new Exception("l变量计算有误！")
    l
  }

  private def computeProb_l(num: Int, l : Int, probl : Array[Double]): Int ={
    //依次算从0到l的转移概率
    //先总信息素
    var total_g_pher = 0.0
    for(i<-0 to l) {
      total_g_pher = total_g_pher + pher(num - 1)(i)
    }
    var selectedL = 0
    //求转移概率（有信息素的情况下）
    if(total_g_pher > 0) {
      for (j <- 0 to l) {
        probl(j) = pher(num - 1)(j) / total_g_pher
        prob(num - 1)(j) = probl(j)
      }
      //轮盘选择
      var temp: Double = 0.0
      //总信息素大于0，其实是等于1的
      temp = (new Random).nextDouble() //0~1.0之间
      import scala.util.control.Breaks._
      breakable {
        for (i <- 0 to l) {
          temp = temp - probl(i)
          if (temp < 0.0) {
            selectedL = i
            break()
          }
        }
      }
    }else{//无信息素信息，选第一个，也就是0
      selectedL = 0
    }
    selectedL
  }

  //计算对于整个Xdsa的约束
  private def matchconstraints_all_xdsa(pathL : Int):Boolean={
    //计算约束
    val B_total:Array[Int] = new Array(B.length)
    val G_total:Array[Int] = new Array(S)
    var C_total = 0
    var M_total:Array[Int] =new Array(M.length)
    val cst_dsak_j = dsaks.filter(x=>x.num <= pathL).map(dsak=>{
      val d=dsak.d
      val s=dsak.s
      B_total(d-1) = B_total(d-1) + Xdsa(dsak.num-1)
      G_total(s-1) = G_total(s-1) + Xdsa(dsak.num-1)*t(d-1)
      C_total = C_total + Xdsa(dsak.num-1)*c(d-1)
      M_total(d-1) = M_total(d-1)+Xdsa(dsak.num-1)*m(d-1)
    }
    )
    //是否满足约束
    val gs:Array[Int] = avss.map(x=>x.Gs)
    if(!Util.morethan(B_total, B) && !Util.morethan(M_total, M)
    && C_total<= Cmax && !Util.morethan(G_total, gs)){
      true//满足约束
    }else{
      false
    }
  }

  def local_optimization():Unit = {
    //对每一个Xdsa，试着加一个step，看是否满足约束，如果满足，则以优化后的Xdsa为准
    for(i <- 0 until pathlength){
      Xdsa(i) = Xdsa(i) + local_step
      if(!matchconstraints_all_xdsa(pathlength)) Xdsa(i) = Xdsa(i) - local_step
    }
  }

  def computeObjectFunction(): Unit ={

    var f = 0.0
    avss.map(avs=>{
      val s = avs.s
      val vs = avs.v
      //根据公式，攻击方的连乘
      var attack:Double = 1.0
      sangs.filter(x=>x.s==s).map(sang=>{
        val a = sang.a
        val nsa = sang.nsa
        val gsa = sang.gsa
        //部署方（防御方）（1-kdsa）xdsa/nsa的连乘
        var defense:Double = 1.0
        dsaks.filter(dsak=>dsak.s==s && dsak.a==a).map{dsak1=>{
          val num = dsak1.num
          val d = dsak1.d
          val kdsa = dsak1.Kdsa
          defense = defense*pow(1.0-kdsa,Xdsa(num-1)/nsa.toDouble)//此处不转换为double会当成整数
        }
        }
        attack = attack*pow(1.0-defense*gsa,nsa)
      })
      f=f+vs*attack
    })
    Fobj = f / avss.map(_.v).reduce(_+_)//求百分比
  }

  def constraintShow(): Unit ={
    //部署数
    for(i <- 0 until b_s.length){
      b_s(i) = dsaks.filter(_.d==i+1).map(dsak=>Xdsa(dsak.num-1)).sum
    }
    //成本
    for(i <- 0 until c_s.length){
      c_s(i) = dsaks.filter(_.d==i+1).map(dsak=>c(i)*Xdsa(dsak.num-1)).sum
    }
    //电力
    for(i <- 0 until m_s.length){
      m_s(i) = dsaks.filter(_.d==i+1).map(dsak=>m(i)*Xdsa(dsak.num-1)).sum
    }
    //占地
    for(i <- 0 until g_s.length){
      g_s(i) = dsaks.filter(_.s==i+1).map(dsak=>t(dsak.d-1)*Xdsa(dsak.num-1)).sum
    }
  }

  def dealflow(): Unit ={
    //初始化
    init()
    //搜索
    search()
    /*
    //局部优化
    local_optimization()
    */
    //计算目标函数
    computeObjectFunction()
    //显示内容
    constraintShow()
  }

}

object Ant{

  var bestAnts = Vector[Ant]()
  val vdsak_j = deal_U.Vdsak_j
  val rawAVS = deal_U.rawAVS
  val rawSANG = deal_U.rawSANG

  def apply(): Ant ={
    val myant = new Ant(U,deal_U.getMaxJ(),vdsak_j,rawAVS,rawSANG)
    myant.dealflow()//计算，否则无法得出Fobj
    if(bestAnts.length < up){
      bestAnts = myant +: bestAnts
    }
    else{//要比较是不是最好
      var min_index = 0
      var minobj = Double.MaxValue
      for(i <- 0 until bestAnts.length){
        if(bestAnts(i).Fobj < minobj){
          min_index = i
          minobj = bestAnts(i).Fobj
        }
      }
      if(myant.Fobj > minobj){//替换
        bestAnts = bestAnts.updated(min_index, myant)
      }
    }
    myant
  }

}