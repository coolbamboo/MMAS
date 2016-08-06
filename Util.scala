package MMAS

/**
 * Created by root on 2016/4/6.
 */
object Util {
  def morethan(first:Array[Int], second:Array[Int]):Boolean={
    if(first.length != second.length) throw new Exception("compared array length not equal")
    for(i <- 0 until first.length){
      if(first(i) > second(i)) return true
    }
    false
  }
}
