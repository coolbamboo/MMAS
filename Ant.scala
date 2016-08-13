package MMAS

import MMAS.common._
import org.apache.spark.rdd.RDD

import scala.math.pow
import scala.util.Random

/**
 * Created by root on 2016/3/5.
 * ant used after deal_U
 * use ant：
 * search for decision variable
 * compute object fun
 *
 */
class Ant(g_Pher:Array[Array[Double]], U:Int, Jmax:Int, vdsak_j:RDD[DSAK_Jup], rawAVS:RDD[AVS], rawSANG:RDD[SANG])
extends Serializable {

  val avss = rawAVS.collect()
  val sangs = rawSANG.collect()
  val dsaks = vdsak_j.collect()

  var pathlength = 0//variable in stage length
  val local_step = 1//optimal step

  val prob:Array[Array[Double]] = Array.ofDim(U, Jmax + 1)
  //init（catch global pher）
  val pher:Array[Array[Double]] = g_Pher.clone()
  //decision variable
  val Xdsa :Array[Int] = new Array(U)
  //object fun
  var Fobj = 0.0
  //D(defend weapon amount)
  var b_s: Array[Double] = new Array(D)
  //Ground
  var g_s:Array[Double] = new Array(S)
  //C
  var c_s:Array[Double] = new Array(D)
  //Manpower
  var m_s:Array[Double] = new Array(D)



  //search once
  def search(): Unit = {
    dsaks.map {
      _ match {
        case DSAK_Jup(num,d,s,a,kdsa,jup)=>{
          //deal every stage
          //choose which j?
          val l = constraints(num)//j's up bound
          if(l == 0){//if l is 0, after l 's variable are all 0
            pathlength = num
            //no use to compute
            return
          }else{
            //choose which l?
            val probl:Array[Double] = new Array[Double](l+1)
            //this row:num to random variable xdsa
            Xdsa(num-1) = computeProb_l(num,l,probl)
          }
          pathlength = num//row num
        }
      }
    }
  }
  //to some stage i, judge if match constraint before i, if true, return max j
  private def constraints(row:Int): Int={
    //compute constraint
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
    //befor row-1 variable is done, compute this row 's max j
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
    if(l<0) throw new Exception("l compute wrong！")
    l
  }

  private def computeProb_l(num: Int, l : Int, probl : Array[Double]): Int ={
    //from 0 to l's trans prob
    var total_g_pher = 0.0
    for(i<-0 to l) {
      total_g_pher = total_g_pher + pher(num - 1)(i)
    }
    var selectedL = 0
    //transform prob
    if(total_g_pher > 0) {
      for (j <- 0 to l) {
        probl(j) = pher(num - 1)(j) / total_g_pher
        prob(num - 1)(j) = probl(j)
      }
      //choose
      var temp: Double = 0.0

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
    }else{//choose first, is 0
      selectedL = 0
    }
    selectedL
  }

  //if the whole decision variable Xdsa match constraints
  private def matchconstraints_all_xdsa(pathL : Int):Boolean={
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
    //match constraints?
    val gs:Array[Int] = avss.map(x=>x.Gs)
    if(!Util.morethan(B_total, B) && !Util.morethan(M_total, M)
    && C_total<= Cmax && !Util.morethan(G_total, gs)){
      true//满足约束
    }else{
      false
    }
  }

  private def local_optimization():Unit = {
    //to every decision variable Xdsa, add step,if match constraint,success
    for(i <- 0 until pathlength){
      Xdsa(i) = Xdsa(i) + local_step
      if(!matchconstraints_all_xdsa(pathlength)) Xdsa(i) = Xdsa(i) - local_step
    }
  }

  private def computeObjectFunction(): Unit ={

    var f = 0.0
    avss.map(avs=>{
      val s = avs.s
      val vs = avs.v
      //attacker 's TT
      var attack:Double = 1.0
      sangs.filter(x=>x.s==s).map(sang=>{
        val a = sang.a
        val nsa = sang.nsa
        val gsa = sang.gsa
        //defense:（1-kdsa）xdsa/nsa TT
        var defense:Double = 1.0
        dsaks.filter(dsak=>dsak.s==s && dsak.a==a).map{dsak1=>{
          val num = dsak1.num
          val d = dsak1.d
          val kdsa = dsak1.Kdsa
          defense = defense*pow(1.0-kdsa,Xdsa(num-1)/nsa.toDouble)
        }
        }
        attack = attack*pow(1.0-defense*gsa,nsa)
      })
      f=f+vs*attack
    })
    Fobj = f / avss.map(_.v).reduce(_+_)//%
  }

  private def constraintShow(): Unit ={
    //D
    for(i <- 0 until b_s.length){
      b_s(i) = dsaks.filter(_.d==i+1).map(dsak=>Xdsa(dsak.num-1)).sum
    }
    //C
    for(i <- 0 until c_s.length){
      c_s(i) = dsaks.filter(_.d==i+1).map(dsak=>c(i)*Xdsa(dsak.num-1)).sum
    }
    //M
    for(i <- 0 until m_s.length){
      m_s(i) = dsaks.filter(_.d==i+1).map(dsak=>m(i)*Xdsa(dsak.num-1)).sum
    }
    //G
    for(i <- 0 until g_s.length){
      g_s(i) = dsaks.filter(_.s==i+1).map(dsak=>t(dsak.d-1)*Xdsa(dsak.num-1)).sum
    }
  }

  def dealflow(): Unit ={
    //
    search()
    /*
    //局部优化
    local_optimization()
    */
    //
    computeObjectFunction()
    //
    constraintShow()
  }

}