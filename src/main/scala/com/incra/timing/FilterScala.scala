package com.incra.timing

import scala.collection.mutable

/**
 * Created by jeff on 10/24/15.
 */
object FilterScala {

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    //println("start")

    var i = 0
    while (i < 10000) {
      val states = Array("NY", "CA", "NJ", "OH", "OK", "MA", "TX", "MN", "ORE", "FL", "CT", "PA", "WA", "VA", "ME", "VT", "NH", "NV")

      //val validStates = states.filter { state => state.size == 2 }
      //println(valueStates.size)

      val validStates = mutable.MutableList[String]()

      if (false) {
        var j = 0
        while (j < states.size) {
          val state = states(j)
          //if (state.size == 2) validStates += state
          j = j + 1
        }
      }
      //states.foreach { state =>
      //  if (state.size == 2) validStates += state
      //}
      i = i + 1
    }
    val elapsed = System.currentTimeMillis() - start
    println(elapsed)
  }
}