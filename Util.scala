package MMAS

import scala.collection.mutable.ArrayBuffer
import common._
import org.apache.spark.rdd.RDD

/**
 * Created by root on 2016/4/6.
 */
object Util {
  def morethan(first:Array[Int], second:Array[Int]):Boolean={
    if(first.length != second.length) throw new Exception("compared array length not equal")
    var temp = false
    for(i <- 0 until first.length){
      //one more than , true
      if(first(i) > second(i)) {
        temp = true
        return temp
      }
    }
    temp
  }

  def getBestAnt(bestants : ArrayBuffer[Ant]) : Ant = {
    bestants.max(new Ordering[Ant] {
      def compare(a: Ant, b: Ant) = a.Fobj compare b.Fobj
    })
  }

  def getBestAnt(bestants : RDD[Ant]) : Ant = {
    bestants.collect().max(new Ordering[Ant] {
      def compare(a: Ant, b: Ant) = a.Fobj compare b.Fobj
    })
  }

  def addInBestants(bestAnts:ArrayBuffer[Ant], myant:Ant) :Unit = {
    if (bestAnts.length < up) {
      bestAnts.append(myant)
    }
    else {
      //if better than bestAnts,update
      var min_index = 0
      var minobj = Double.MaxValue
      for (i <- 0 until bestAnts.length) {
        if (bestAnts(i).Fobj < minobj) {
          min_index = i
          minobj = bestAnts(i).Fobj
        }
      }
      if (myant.Fobj > minobj) {
        bestAnts.update(min_index, myant)
      }
    }
  }
}
