package MMAS

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import common._

import scala.collection.mutable.ArrayBuffer
/**
 * Created by root on 2016/3/6.
 *
 */
class AntOneIteration(val bestAnts: ArrayBuffer[T_Ant], J_max:Int,
                      dsak_j_RDD:RDD[DSAK_Jup], avs_RDD:RDD[AVS],
                      sang_RDD:RDD[SANG], val i_iter: Long) extends Serializable {

  val local_antGroup = scala.collection.mutable.ArrayBuffer[T_Ant]() //every iter' ants

  def geneAllAntsOneIter(sc: SparkContext, par: Int) :RDD[T_Ant] = {
    val bestant: T_Ant = Util.getBestAnt(bestAnts)
    //empty local ant group
    local_antGroup.clear()
    //gene ants
    for (i <- 1 to ANT_NUM) {
      local_antGroup.append(
        new Ant(bestant.pher, U, J_max, dsak_j_RDD, avs_RDD, sang_RDD)
      )
    }
    val ants: RDD[T_Ant] = sc.parallelize(local_antGroup, par)
    val ants_deal: RDD[T_Ant] = ants.map(myant => {
      myant.dealflow() //compute object fun
      myant
    }
    ).sortBy(_.Fobj,false)
    //action!
    val bestlocalant = ants_deal.take(1)(0)
    Util.addInBestants(bestAnts, bestlocalant)
    local_antGroup.update(0,bestlocalant)
    ants_deal.cache()
  }

  def geneAllAntsOneIter() = {
    val bestant: T_Ant = Util.getBestAnt(bestAnts)
    //empty local ant group
    local_antGroup.clear()
    //gene ants
    for (i <- 1 to ANT_NUM) {
      local_antGroup.append(
        new Ant(bestant.pher, U, J_max, dsak_j_RDD, avs_RDD, sang_RDD)
      )
    }
    local_antGroup.map(myant => {
      myant.dealflow()
      myant
    }).map(myant => {
      Util.addInBestants(bestAnts, myant)
      myant
    })
  }

  //update pher using the best ant
  def global_updatePher(): Unit = {
    val bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher //best global pher
    //pher minus
    for (i <- 0 until U)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }

    val faverage = local_antGroup.map(ant => ant.Fobj).sum / local_antGroup.length
    val added = pher0 * (bestAnt.Fobj / faverage)
    //best ant add pher
    for (i <- 0 until bestAnt.pathlength - 1 if bestAnt.pathlength < U) {
      //update
      g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
      //reset
      /*if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
        g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset*/
    }
    if (bestAnt.pathlength == U) {
      for (i <- 0 until bestAnt.pathlength) {
        g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
        //reset
        /*if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
          g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset*/
      }
    }
    //check max-min scope
    maxmincheck(g_Pher)
  }

  private def maxmincheck(g_Pher:Array[Array[Double]]): Unit = {
    for (i <- 0 until U)
      for (j <- 0 to J_max) {
        if (g_Pher(i)(j) > pher_max)
          g_Pher(i)(j) = pher_max
        if (g_Pher(i)(j) < pher_min)
          g_Pher(i)(j) = pher_min
      }
  }

  def local_updatePher() = {
    //local best ant
    val bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher
    val best_local_ant = Util.getBestAnt(local_antGroup)

    for (i <- 0 until U)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }

    val faverage = local_antGroup.map(ant => ant.Fobj).sum / local_antGroup.length
    val added = pher0 * (best_local_ant.Fobj / faverage)

    for (i <- 0 until best_local_ant.pathlength - 1 if best_local_ant.pathlength < U) {
      //update
      g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
    }
    if (best_local_ant.pathlength == U) {
      for (i <- 0 until best_local_ant.pathlength) {
        g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
      }
    }
    maxmincheck(g_Pher)
  }

  //update pher using the best ant with RDD
  def global_updatePher(ants : RDD[T_Ant]): Unit = {
    //action!
    val antsFogj = ants.map(ant => ant.Fobj)
    val faverage = antsFogj.reduce(_+_) / antsFogj.count()

    val bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher //best global pher
    //pher minus
    for (i <- 0 until U)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }
    val added = pher0 * (bestAnt.Fobj / faverage)
    //best ant add pher
    for (i <- 0 until bestAnt.pathlength - 1 if bestAnt.pathlength < U) {
      //update
      g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
      //reset
      /*if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
        g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset*/
    }
    if (bestAnt.pathlength == U) {
      for (i <- 0 until bestAnt.pathlength) {
        g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
        //reset
        /*if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
          g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset*/
      }
    }
    //check max-min scope
    maxmincheck(g_Pher)
  }

  //no need local_antGroup
  def local_updatePher(ants : RDD[T_Ant]) = {
    //local best ant
    val best_local_ant = local_antGroup(0)
    val antsFobj = ants.map(ant => ant.Fobj)
    //action!
    val faverage = antsFobj.reduce(_+_) / antsFobj.count()

    val bestAnt = Util.getBestAnt(bestAnts)
    val g_Pher = bestAnt.pher
    for (i <- 0 until U)
      for (j <- 0 to J_max) {
        g_Pher(i)(j) = (1 - rou) * g_Pher(i)(j)
      }
    val added = pher0 * (best_local_ant.Fobj / faverage)

    for (i <- 0 until best_local_ant.pathlength - 1 if best_local_ant.pathlength < U) {
      //update
      g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
    }
    if (best_local_ant.pathlength == U) {
      for (i <- 0 until best_local_ant.pathlength) {
        g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
      }
    }
    maxmincheck(g_Pher)
  }
}

object AntOneIteration {

  def apply(bestants : ArrayBuffer[T_Ant], J_max:Int,
            dsak_j_RDD:RDD[DSAK_Jup], avs_RDD:RDD[AVS],
            sang_RDD:RDD[SANG], sc : SparkContext, par: Int, mode : String = ""){
    for (i <- 1 to iter){
      val antoneiter = new AntOneIteration(bestants ,J_max, dsak_j_RDD, avs_RDD, sang_RDD, i)
      val starttime = new Date().getTime
      mode match {
        case "RDD" => {
          val ants = antoneiter.geneAllAntsOneIter(sc,par)
          //update pher
          if (antoneiter.i_iter % l_g_ratio == 0) {
            antoneiter.global_updatePher(ants)
          } else {
            antoneiter.local_updatePher(ants)
          }
        }
        case _ => {
          antoneiter.geneAllAntsOneIter()
          //update pher
          if (antoneiter.i_iter % l_g_ratio == 0) {
            antoneiter.global_updatePher()
          } else {
            antoneiter.local_updatePher()
          }
        }
      }
      val stoptime = new Date().getTime
      printf("every iteration run time：%d ms ",stoptime-starttime)
    }
  }
}