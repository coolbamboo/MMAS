package MMAS
import Tsp._
import common._
/**
 * Created by root on 2016/3/6.
 *
 * 初始化pher
 * 所有蚂蚁遍历一遍，找出其中最好的蚂蚁，将它转移概率和pher变成全局
 * 继续下一次迭代
 */
class Tsp {
  var local_antGroup:Array[Ant] = Array.empty//存每运行(一次迭代)的N只蚂蚁
  //初始化信息素
  //开始时信息素均为pherMAX
  for(i<-0 until U)
    for(j<-0 to Jmax){
      g_Pher(i)(j) = pher_max
    }
  //更新信息素(全局，用最好的蚂蚁)，只在每次遍历后，取出最好的结果遍历
  def global_updatePher(): Unit ={
    /**
     * 更新信息素的方法，是总是找出最好的蚂蚁（而不是一次迭代中的）
     * 有利全局
     */
    val bestAnt = Ant.bestAnts.sortWith(_.Fobj>_.Fobj)(0)
    g_Prob = bestAnt.prob.clone()//最好蚂蚁的选择概率变成全局概率
    //首先，所有的信息素全部衰减
    for(i<-0 until U)
      for(j<-0 to Jmax){
        g_Pher(i)(j) = (1-rou)*g_Pher(i)(j)
      }
    //均值是第t次迭代出的结果的均值
    val faverage = local_antGroup.map(ant => ant.Fobj).sum / local_antGroup.length
    val added = pher0 * (bestAnt.Fobj / faverage)
    //对于最优蚂蚁的信息素加成
    for(i<-0 until bestAnt.pathlength-1 if bestAnt.pathlength < U) {
      //更新信息素
      g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
      //重置信息素
      if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
        g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset
    }
    if (bestAnt.pathlength == U){
      for(i<-0 until bestAnt.pathlength) {
        g_Pher(i)(bestAnt.Xdsa(i)) = g_Pher(i)(bestAnt.Xdsa(i)) + added
        //重置信息素
        if(g_Pher(i)(bestAnt.Xdsa(i)) < pher_reset)
          g_Pher(i)(bestAnt.Xdsa(i)) = pher_reset
      }
    }
    //检查是否在最大最小范围内
    for(i<-0 until U)
      for(j<-0 to Jmax){
        if(g_Pher(i)(j) > pher_max)
          g_Pher(i)(j) = pher_max
        if(g_Pher(i)(j) < pher_min)
          g_Pher(i)(j) = pher_min
      }
  }

  def local_updatePher() = {
    //求出局部最优的蚂蚁
    val best_local_ant = local_antGroup.max(new Ordering[Ant] { def compare(a: Ant, b: Ant) = a.Fobj compare b.Fobj })
    g_Prob = best_local_ant.prob.clone()//最好蚂蚁的选择概率变成全局概率
    //首先，所有的信息素全部衰减
    for(i<-0 until U)
      for(j<-0 to Jmax){
        g_Pher(i)(j) = (1-rou)*g_Pher(i)(j)
      }
    //均值是第t次迭代出的结果的均值
    val faverage = local_antGroup.map(ant => ant.Fobj).sum / local_antGroup.length
    val added = pher0 * (best_local_ant.Fobj / faverage)
    //对于最优蚂蚁的信息素加成
    for(i<-0 until best_local_ant.pathlength-1 if best_local_ant.pathlength < U) {
      //更新信息素
      g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
    }
    if (best_local_ant.pathlength == U){
      for(i<-0 until best_local_ant.pathlength) {
        g_Pher(i)(best_local_ant.Xdsa(i)) = g_Pher(i)(best_local_ant.Xdsa(i)) + added
      }
    }
    //检查是否在最大最小范围内
    for(i<-0 until U)
      for(j<-0 to Jmax){
        if(g_Pher(i)(j) > pher_max)
          g_Pher(i)(j) = pher_max
        if(g_Pher(i)(j) < pher_min)
          g_Pher(i)(j) = pher_min
      }
  }

  def dealflow(): Unit ={
    for(i<-1 to iter){
      //先清空蚂蚁序列
      local_antGroup = Array.empty
      //生成蚂蚁
      for(ant_index <- 0 until ANT_NUM){
        val myant = Ant()
        local_antGroup = local_antGroup :+ myant
      }
      /*
      //全局优化（对最好的蚂蚁做一次优化，因为会根据它更新信息素）
      val bestAnt = Ant.bestAnts.sortWith(_.Fobj>_.Fobj)(0)
      bestAnt.local_optimization()
      bestAnt.computeObjectFunction()
      bestAnt.constraintShow()
      */
      //更新信息素
      if(i%l_g_ratio == 0) {
        global_updatePher()
      }else{
        local_updatePher()
      }
    }
    //最好结果保留
    Output()
  }
}

object Tsp{
  import common._
  //信息素
  val pher0 = 1
  val pher_min = 0.01*pher0
  val pher_max = 10 * pher0
  val rou = 0.01
  val ANT_NUM = 40
  val iter = 1000 //遍历1000次

  val l_g_ratio = 3 //局部更新信息素和全局更新信息素为4:1
  val pher_reset = pher0//最好路径的信息素重置
  val Jmax = deal_U.getMaxJ()
  var g_Pher:Array[Array[Double]] = Array.ofDim(U, Jmax + 1)
  var g_Prob:Array[Array[Double]] = Array.ofDim(U, Jmax + 1)
}