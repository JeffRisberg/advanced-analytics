package com.incra.search

import java.lang.Double.isNaN

import org.apache.commons.math3.distribution.{NormalDistribution, MultivariateNormalDistribution}
import org.apache.commons.math3.random.MersenneTwister

//import com.incra.ch2.MatchData
//import com.incra.ch2.NAStatCounter

import org.apache.spark.SparkContext
import org.apache.spark.util.StatCounter
import org.apache.spark.rdd.RDD

/**
 * Created by jeff on 9/7/15.
 */

case class Vehicle(var x: Double,
                   var y: Double,
                   var dX: Double,
                   var dY: Double) {
  def step(dt: Double): Unit = {
    x = x + dX
    y = y + dY
  }
}

object RunSim extends Serializable {

  def main(args: Array[String]): Unit = {
    /* init the sparkContext */
    val sc = new SparkContext("local", "SearchSim", System.getenv("SPARK_HOME"))

    val numTimesteps = 1000
    val numTrials = 10000 // should be ten million!
    val parallelism = 1000
    val baseSeed = 1001L

    println("start the trials!")
    // This is done in parallel
    val endpoints = computeTrialReturns(sc, numTimesteps, baseSeed, numTrials, parallelism)
    endpoints.cache()
    println("trials are done!")

    // This is done in parallel
    val endCells = endpoints.map(endpoint => {
      val ix = (endpoint.x / 1000.0).toInt
      val iy = (endpoint.y / 1000.0).toInt

      ((ix, iy), 1)
    })

    // This is done in parallel
    val counts = endCells.reduceByKey { case (x, y) => x + y }

    val grid = Array.fill(15, 15)(0)

    // We call collect to convert RDD to cell counts in master process
    counts.collect.foreach { case ((x, y), count) =>
      if (x >= 0 && x < 15 && y >= 0 && y < 15)
        grid(x)(y) = count;
    }

    grid foreach {
      row => {
        row foreach { count => print(f" ${count}%4d") }
        println
      }
    }
  }

  def computeTrialReturns(sc: SparkContext,
                          numTimesteps: Int,
                          baseSeed: Long,
                          numTrials: Int,
                          parallelism: Int): RDD[Vehicle] = {

    // Generate different seeds so that our simulations don't all end up with the same results
    val seeds = (baseSeed until baseSeed + parallelism)
    val seedRdd = sc.parallelize(seeds, parallelism)

    // Main computation: run simulations and compute aggregate return for each
    seedRdd.flatMap(
      trialResults(_, numTimesteps, numTrials / parallelism))
  }

  def trialResults(seed: Long, numTimesteps: Int, numTrials: Int): Seq[Vehicle] = {

    val rand = new MersenneTwister(seed)
    val trialReturns = new Array[Vehicle](numTrials)

    val positionDistribution = new NormalDistribution(rand, 50.0, 1.0, 0.0)
    val speedDistribution = new NormalDistribution(rand, 6.0, 1.0, 0.0)

    for (i <- 0 until numTrials) {
      val x = positionDistribution.sample()
      val y = positionDistribution.sample()
      val dX = speedDistribution.sample()
      val dY = speedDistribution.sample()

      val vehicle = Vehicle(x, y, dX, dY)
      for (t <- 1 until numTimesteps) {
        vehicle.step(1.0)
      }

      trialReturns(i) = vehicle
    }
    trialReturns
  }
}
