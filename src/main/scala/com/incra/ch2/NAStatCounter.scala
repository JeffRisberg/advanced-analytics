package com.incra.ch2

/**
 * Created by jeff on 7/7/15.
 */
import org.apache.spark.util.StatCounter

class NAStatCounter extends Serializable { val stats: StatCounter = new StatCounter()

  var missing: Long = 0

  def add(x: Double): NAStatCounter = { if (java.lang.Double.isNaN(x)) {
    missing += 1 }else{
    stats.merge(x)
  }
    this
  }
  def merge(other: NAStatCounter): NAStatCounter = { stats.merge(other.stats)
    missing += other.missing
    this
  }
  override def toString = {
    "stats: " + stats.toString + " NaN: " + missing
  } }

object NAStatCounter extends Serializable {
  def apply(x: Double) = new NAStatCounter().add(x)
}