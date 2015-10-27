package com.incra.timing

import scala.collection.mutable

/**
 * Created by jeff on 10/24/15.
 */
object FilterScala {

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()

    var x = new ScalaFilterExample()
    var y = new ScalaFilterExample()
    var z = new ScalaFilterExample()
    var t = new ScalaFilterExample()
    var w = new ScalaFilterExample()
    var a = new ScalaFilterExample()
    var b = new ScalaFilterExample()

    val elapsed = System.currentTimeMillis() - start
    println("Total " + elapsed)
  }
}

class ScalaFilterExample {
  val start = System.currentTimeMillis()

  for (i <- Range(0, 100)) {
    val states = List("NY", "CA", "NJ", "OH", "OK", "MA", "TX", "MN", "ORE", "FL", "CT", "PA", "WA", "VA", "ME", "VT", "NH", "NV")

    val validStates = states.withFilter { state => state.size == 2 }
    val hawaii = validStates.withFilter { state => state == "HI" }
    hawaii.toString()
    //println(hawaii.size)
  }

  val elapsed = System.currentTimeMillis() - start
  println(elapsed)
}