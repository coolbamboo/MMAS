package MMAS

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 2016/3/7.
 * 结果类
 */
class Output(val ant:T_Ant) extends Serializable{

  override def toString(): String ={
    val sb = new StringBuilder("\n")
    sb.append(ant.Fobj).append("\n")
    sb.append(ant.b_s.toSeq).append(":总数\n")
    sb.append(ant.g_s.toSeq).append(":占地\n")
    sb.append(ant.c_s.sum).append(":成本\n")
    sb.append(ant.m_s.toSeq).append(":电力\n")
    for(i <- 0 until ant.Xdsa.length){
      if(ant.Xdsa(i) >0)
        sb.append("(").append(i+1).append(",").append(ant.Xdsa(i)).append(")")
    }
    sb.toString()
  }

}

object Output{

  def apply(bestAnts:ArrayBuffer[T_Ant]): Vector[Output] = {
    bestAnts.map(x=> new Output(x)).toVector
  }
}
