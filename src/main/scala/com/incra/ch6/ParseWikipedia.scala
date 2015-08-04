/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.incra.ch6

//import com.cloudera.datascience.common.XmlInputFormat

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}

//import edu.umd.cloud9.collection.wikipedia.WikipediaPage
//import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage

import java.io.{FileOutputStream, PrintStream}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

object ParseWikipedia {
  /**
   * Returns a term-document matrix where each element is the TF-IDF of the row's document and
   * the column's term.
   */
  def termDocumentMatrix(docs: RDD[(String, Seq[String])], stopWords: Set[String], numTerms: Int,
                         sc: SparkContext): (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
    val docTermFreqs = docs.mapValues(terms => {
      val termFreqsInDoc = terms.foldLeft(new HashMap[String, Int]()) {
        (map, term) => map += term -> (map.getOrElse(term, 0) + 1)
      }
      termFreqsInDoc
    })

    docTermFreqs.cache()
    val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()

    val docFreqs = documentFrequenciesDistributed(docTermFreqs.map(_._2), numTerms)
    println("Number of terms: " + docFreqs.size)
    saveDocFreqs("docfreqs.tsv", docFreqs)

    val numDocs = docIds.size

    val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

    // Maps terms to their indices in the vector
    val termToId = idfs.keys.zipWithIndex.toMap

    val bIdfs = sc.broadcast(idfs).value
    val bTermToId = sc.broadcast(termToId).value

    val vecs = docTermFreqs.map(_._2).map(termFreqs => {
      val docTotalTerms = termFreqs.values().sum
      val termScores = termFreqs.filter {
        case (term, freq) => bTermToId.containsKey(term)
      }.map {
        case (term, freq) => (bTermToId(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(bTermToId.size, termScores)
    })
    (vecs, termToId.map(_.swap), docIds, idfs)
  }

  def documentFrequencies(docTermFreqs: RDD[HashMap[String, Int]]): HashMap[String, Int] = {
    val zero = new HashMap[String, Int]()
    def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int])
    : HashMap[String, Int] = {
      tfs.keySet.foreach { term =>
        dfs += term -> (dfs.getOrElse(term, 0) + 1)
      }
      dfs
    }
    def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int])
    : HashMap[String, Int] = {
      for ((term, count) <- dfs2) {
        dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
      }
      dfs1
    }
    docTermFreqs.aggregate(zero)(merge, comb)
  }

  def documentFrequenciesDistributed(docTermFreqs: RDD[HashMap[String, Int]], numTerms: Int)
  : Array[(String, Int)] = {
    val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _, 15)
    val ordering = Ordering.by[(String, Int), Int](_._2)
    docFreqs.top(numTerms)(ordering)
  }

  def trimLeastFrequent(freqs: Map[String, Int], numToKeep: Int): Map[String, Int] = {
    freqs.toArray.sortBy(_._2).take(math.min(numToKeep, freqs.size)).toMap
  }

  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Int)
  : Map[String, Double] = {
    docFreqs.map { case (term, count) => (term, math.log(numDocs.toDouble / count)) }.toMap
  }

  def readFile(path: String, sc: SparkContext): RDD[String] = {
    /*
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<page>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</page>")
    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
      classOf[Text], conf)
    rawXmls.map(p => p._2.toString)
    */
    val data = Array(
      "Doc1::Data Science is the extraction of knowledge from large volumes of data that are structured or unstructured which is a continuation of the field data mining and predictive analytics, also known as knowledge discovery and data mining (KDD). Unstructured data can include emails, videos, photos, social media, and other user-generated content. Data science often requires sorting through a great amount of information and writing algorithms to extract insights from this data",
      "Doc2::Application of Data Science does not only stop at clinical trials, it was also applied to learning the proteins and DNA sequences in Genomics. This field, because of the tools of the data scientist, the work for analyzing, and studying DNA structures, viruses and other biological pathogens. Handling of data is around before but using data science will make it easier for handling vast amount of data in Genomics and make the procedures repeatable",
      "Doc3::Security data science is focused on advancing information security through practical applications of exploratory data analysis, statistics, machine learning and data visualization. Although the tools and techniques are no different from those used in data science in any data domain this group has a micro-focus on reducing risk, identifying fraud or malicious insiders using data science. The information security and fraud prevention industry have been evolving security data science in order to tackle the challenges of managing and gaining insights from huge streams of log data, discover insider threats and prevent fraud",
      "Doc4::Data scientists, that peculiar mix of software engineer and statistician, are notoriously difficult to interview. One approach that I’ve used over the years is to pose a problem that requires some mixture of algorithm design and probability theory in order to come up with an answer Here’s an example of this type of question that has been popular in Silicon Valley for a number of years",
      "Doc5::Among the 34 states in January 1861, seven Southern slave states individually declared their secession from the United States and formed the Confederate States of America. The Confederacy, often simply called the South, grew to include eleven states, and although they claimed thirteen states and additional western territories, the Confederacy was never diplomatically recognized by a foreign country",
      "Doc6::Slavery was the central source of escalating political tension in the 1850s. The Republican Party was determined to prevent any spread of slavery, and many Southern leaders had threatened secession if the Republican candidate, Lincoln, won the 1860 election",
      "Doc7::The earlier political party structure failed to make accommodation among sectional differences. Disagreements over slavery caused the Whig and Know-Nothing parties to collapse. In 1860, the last national political party, the Democratic Party, split along sectional lines. Anti-slavery Northerners mobilized in 1860 behind moderate Abraham Lincoln because he was most likely to carry the doubtful western states. In 1857, the Supreme Court's Dred Scott decision ended the Congressional compromise for Popular Sovereignty in Kansas. According to the court, slavery in the territories was a property right of any settler, regardless of the majority there. Chief Justice Taney's decision said that slaves were \"... so far inferior that they had no rights which the white man was bound to respect.\" The decision overturned the Missouri Compromise, which banned slavery in territory north of the 36°30' parallel",
      "Doc8::The use of slaves declined in the border states and could barely survive in cities and industrial areas (it was fading out in cities such as Baltimore, Louisville, and St. Louis), so a South based on slavery was rural and non-industrial. On the other hand, as the demand for cotton grew, the price of slaves who picked it soared. Historians have debated whether economic differences between the industrial Northeast and the agricultural South helped cause the war. ",
      "Doc9::the rain in spain is mainly on the plain",
      "Doc10::only one life to live for my country",
      "Doc11::all good people turn their heads each so satisifed",
      "Doc12::this is the place this is really the place")
    sc.parallelize(data)
  }

  /**
   * Returns a (title, content) pair
   */
  // changed to return Seq[String] for the second value in the tuple
  def wikiXmlToPlainText(pageXml: String, stopWords: Set[String]): Option[(String, Seq[String])] = {
    val splitPage = pageXml.split("::")

    val title = splitPage(0)
    val content = splitPage(1)
    var words = content.split(" ").toSeq
    var longWords = words.filter(_.length > 2).filter( !stopWords.contains(_) )
    Some(title, longWords.map(word => word.toLowerCase)) // content.split(" ").toSeq is a crude lemmatizer
    /*
    val page = new EnglishWikipediaPage()
    WikipediaPage.readPage(page, pageXml)
    if (page.isEmpty || !page.isArticle || page.isRedirect ||
      page.getTitle.contains("(disambiguation)")) {
      None
    } else {
      Some((page.getTitle, page.getContent))
    }
    */
  }

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
  : Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

  def isOnlyLetters(str: String): Boolean = {
    // While loop for high performance
    var i = 0
    while (i < str.length) {
      if (!Character.isLetter(str.charAt(i))) {
        return false
      }
      i += 1
    }
    true
  }

  def loadStopWords(path: String) = scala.io.Source.fromFile(path).getLines().toSet

  def saveDocFreqs(path: String, docFreqs: Array[(String, Int)]) {
    val ps = new PrintStream(new FileOutputStream(path))
    for ((doc, freq) <- docFreqs) {
      ps.println(s"$doc\t$freq")
    }
    ps.close()
  }
}

