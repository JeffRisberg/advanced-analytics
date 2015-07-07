package com.incra.ch2

import java.lang.Double.isNaN
import org.apache.spark.SparkContext
import org.apache.spark.util.StatCounter
import org.apache.spark.rdd.RDD

/**
 * Created by jeff on 7/7/15.
 */

case class MatchData(id1: Int,
                     id2: Int,
                     scores: Array[Double],
                     matched: Boolean)

object RecordLinkage extends Serializable {

  def main(args: Array[String]): Unit = {
    /* init the sparkContext */
    val sc = new SparkContext("local", "RecordLinkage", System.getenv("SPARK_HOME"))

    /* load the data set from a textFile, into an RDD */
    val rawblocks = sc.textFile("../../advanced-analytics/linkage/data")

    /* show the count (> 5Million records */
    println(rawblocks.count())

    /* learn a few conv functions on RDDs */
    val head = rawblocks.take(10)

    head.foreach(println)

    println(head.filter(!isHeader(_)).length)

    val noheader = rawblocks.filter(x => !isHeader(x))

    println(noheader.first)

    /* start parsing the CSV string into useful content for analysis */
    val mds = head.filter(x => !isHeader(x)).map(x => parse(x))

    println(mds)

    /* parse all data for analysis */
    val parsed = noheader.map(line => parse(line))

    println(parsed)

    /* storage type discussion is covered here */
    parsed.cache()

    val grouped = mds.groupBy(md => md.matched)

    /* find the most effective propty for matching */
    val matchCounts = parsed.map(md => md.matched).countByValue()

    val matchCountsSeq = matchCounts.toSeq // make them sortable

    matchCountsSeq.sortBy(_._2).reverse.foreach(println)

    println(parsed.map(md => md.scores(0)).filter(!isNaN(_)).stats())

    val statsMatched = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsNotMatched = statsWithMissing(parsed.filter(!_.matched).map(_.scores))

    /* print the effectiveness measures for the properties */
    statsMatched.zip(statsNotMatched).map {
      case(m, n) => (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }.foreach(println)

    /* check */
    def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d

    case class Scored(md: MatchData, score: Double)

    val ct = parsed.map(md => {
      val score = Array(2, 5, 6, 7, 8).map(i => naz(md.scores(i))).sum
      Scored(md, score)
    })

    println(ct.filter(s => s.score >= 4.0).map(s => s.md.matched).countByValue())
    println(ct.filter(s => s.score >= 2.0).map(s => s.md.matched).countByValue())
  }

  def isHeader(line: String): Boolean = {
    line.contains("id_1")
  }

  def toDouble(s: String) = {
    if ("?".equals(s)) Double.NaN else s.toDouble
  }

  def parse(line: String) = {
    val pieces = line.split(',')

    val id1 = pieces(0).toInt
    val id2 = pieces(1).toInt

    val scores = pieces.slice(2, 11).map(toDouble)

    val matched = pieces(11).toBoolean

    MatchData(id1, id2, scores, matched)
  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions((iter: Iterator[Array[Double]]) => {
      val nas: Array[NAStatCounter] = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    }) }
}
