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

  def readFile(sc: SparkContext): RDD[String] = {
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
      "Doc2::Application of Data Science does not only stop at clinical trials it was also applied to learning the proteins and DNA sequences in Genomics. This field because of the tools of the data scientist, the work for analyzing, and studying DNA structures, viruses and other biological pathogens. Handling of data is around before but using data science will make it easier for handling vast amount of data in Genomics and make the procedures repeatable",
      "Doc3::Security data science is focused on advancing information security through practical applications of exploratory data analysis, statistics, machine learning and data visualization. Although the tools and techniques are no different from those used in data science in any data domain this group has a micro-focus on reducing risk, identifying fraud or malicious insiders using data science. The information security and fraud prevention industry have been evolving security data science in order to tackle the challenges of managing and gaining insights from huge streams of log data, discover insider threats and prevent fraud",
      "Doc4::Data scientists having that peculiar mix of software engineer and statistician are notoriously difficult to interview. One approach that I’ve used over the years is to pose a problem that requires some mixture of algorithm design and probability theory in order to come up with an answer Here’s an example of this type of question that has been popular in Silicon Valley for a number of years",
      "Doc5::Spark revolves around the concept of a resilient distributed dataset RDD), which is a fault-tolerant collection of elements that can be operated on in parallel. There are currently two types of RDDs : parallelized collections, which take an existing Scala collection and run functions on it in parallel, and Hadoop datasets, which run functions on each record of a file in Hadoop distributed file system or any other storage system supported by Hadoop. Both types of RDDs can be operated on through the same methods",
      "Doc6::I have seen many companies try to narrow their recruiting by searching for only candidates who have a Phd in mathematics, but in truth, a good data scientist could come from a variety of backgrounds — and may not necessarily have an advanced degree in any of them. business savvy.  If a candidate does not have much business experience, the company must compensate by pairing him or her with someone who does. analytical. A good data scientist must be naturally analytical and have a strong ability to spot patterns. good at visual communications. Anyone can make a chart or graph; it takes someone who understands visual communications to create a representation of data that tells the story the audience needs to hear. versed in computer science. Professionals who are familiar with Hadoop, Java, Python, etc. are in high demand. If your candidate is not expert in these tools, he or she should be paired with a data engineer who is. creative. Creativity is vital for a data scientist, who needs to be able to look beyond a particular set of numbers, beyond even the company’s data sets to discover answers to questions — and perhaps even pose new questions. able to add significant value to data. If someone only presents the data, he or she is a statistician, not a data scientist. Data scientists offer great additional value over data through insights and analysis. a storyteller. In the end, data is useless without context. It is the data scientist’s job to provide that context, to tell a story with the data that provides value to the company. If you can find a candidate with all of these traits — or most of them with the ability and desire to grow — then you’ve found someone who can deliver incredible value to your company, your systems, and your field. But skimp on any of these traits, and you run the risk of hiring an imposter, someone just hoping to ride the data sciences bubble until it bursts.",
      "Doc7::Have you noticed how many people are suddenly calling themselves data scientists? Your neighbour, that gal you met at a cocktail party — even your accountant has had his business cards changed! Second is that the role of a data scientist is often ill-defined within the field and even within a single company.  People throw the term around to mean everything from a data engineer (the person responsible for creating the software 'plumbing' that collects and stores the data) to statisticians who merely crunch the numbers.",

      "Doc8::Among the 34 states in January 1861, seven Southern slave states individually declared their secession from the United States and formed the Confederate States of America. The Confederacy, often simply called the South, grew to include eleven states, and although they claimed thirteen states and additional western territories, the Confederacy was never diplomatically recognized by a foreign country.",
      "Doc9::Slavery was the central source of escalating political tension in the 1850s. The Republican Party was determined to prevent any spread of slavery, and many Southern leaders had threatened secession if the Republican candidate Lincoln won the 1860 election",
      "Doc10::The earlier political party structure failed to make accommodation among sectional differences. Disagreements over slavery caused the Whig and Know-Nothing parties to collapse. In 1860, the last national political party, the Democratic Party split along sectional lines. Anti-slavery Northerners mobilized in 1860 behind moderate Abraham Lincoln because he was most likely to carry the doubtful western states. In 1857, the Supreme Court 's Dred Scott decision ended the Congressional compromise for Popular Sovereignty in Kansas. According to the court, slavery in the territories was a property right of any settler, regardless of the majority there. Chief Justice decision said that slaves were so far inferior that they had no rights which the white man was bound to respect. The decision overturned the Missouri Compromise, which banned slavery in territory north of the parallel",
      "Doc11::The use of slaves declined in the border states and could barely survive in cities and industrial areas (it was fading out in cities such as Baltimore Louisville and St. Louis so a South based on slavery was rural and non-industrial. On the other hand, as the demand for cotton grew, the price of slaves who picked it soared. Historians have debated whether economic differences between the industrial Northeast and the agricultural South helped cause the war.",
      "Doc12::Virginia's Confederate government fielded about 150,000 troops in the American Civil War. They came from all economic and social levels, including some Unionists and former Unionists. However, at least 30,000 of these men were actually from other states. Most of these non-Virginians were from Maryland, whose government was controlled by Unionists during the war. Another 20,000 of these troops were from what would become the State of West Virginia in August 1863. Important Confederates from Virginia included General Robert E. Lee, commander of the Army of Northern Virginia, General Stonewall Jackson and General J.E.B. Stuart.",

      "Doc13::George Washington: Widely admired for his strong leadership qualities, Washington was unanimously elected President in the first two national elections. He oversaw the creation of a strong, well-financed national government that maintained neutrality in the French Revolutionary Wars, suppressed the Whiskey Rebellion, and won acceptance among Americans of all types.[5] Washington's incumbency established many precedents, still in use today, such as the cabinet system, the inaugural address, and the title Mr. President.[6][7] His retirement from office after two terms established a tradition which was unbroken until 1940. Born into the provincial gentry of Colonial Virginia, his family were wealthy planters who owned tobacco plantations and slaves which he inherited; he owned hundreds of slaves throughout his lifetime, but his views on slavery evolved. In his youth he became a senior British officer in the colonial militia during the first stages of the French and Indian War. In 1775, the Second Continental Congress commissioned Washington as commander-in-chief of the Continental Army in the American Revolution. In that command, Washington forced the British out of Boston in 1776, but was defeated and nearly captured later that year when he lost New York City. After crossing the Delaware River in the middle of winter, he defeated the British in two battles, retook New Jersey and restored momentum to the Patriot cause. This is known as the Battle of Trenton.",
      "Doc14::Nathan Hale was a soldier for the Continental Army during the American Revolutionary War. He volunteered for an intelligence-gathering mission in New York City but was captured by the British and executed. He is probably best remembered for his purported last words before being hanged: 'I only regret that I have but one life to give for my country.' Hale has long been considered an American hero and, in 1985, he was officially designated the state hero of Connecticu",
      "Doc15::Patrick Henry was an American attorney, planter and politician who became known as an orator during the movement for independence in Virginia in the 1770s. A Founding Father, he served as the first and sixth post-colonial Governor of Virginia, from 1776 to 1779 and from 1784 to 1786.  Henry led the opposition to the Stamp Act 1765 and is remembered for his 'Give me liberty, or give me death!' speech. Along with Samuel Adams and Thomas Paine, he is regarded as one of the most influential champions of Republicanism and an invested promoter of the American Revolution and its fight for independence",
      "Doc16::John Hancock was a merchant, smuggler, statesman, and prominent Patriot of the American Revolution. He served as president of the Second Continental Congress and was the first and third Governor of the Commonwealth of Massachusetts. He is remembered for his large and stylish signature on the United States Declaration of Independence, so much so that the term \"John Hancock\" has become, in the United States, a synonym for a signature.",
      "Doc17::Thomas Jefferson was an American Founding Father, the principal author of the Declaration of Independence (1776), and the third President of the United States (1801–1809). He was an ardent proponent of democracy and embraced the principles of republicanism and the rights of the individual with worldwide influence. At the beginning of the American Revolution, he served in the Continental Congress, representing Virginia, and then served as a wartime Governor of Virginia (1779–1781). In May 1785, he became the United States Minister to France and later the first United States Secretary of State (1790–1793) serving under President George Washington. In opposition to Alexander Hamilton's Federalism, Jefferson and his close friend, James Madison, organized the Democratic-Republican Party, and")
    sc.parallelize(data)
  }

  /**
   * Returns a (title, content) pair
   */
  def wikiXmlToPlainText(pageXml: String, stopWords: Set[String]): Option[(String, String)] = {
    val splitPage = pageXml.split("::")

    val title = splitPage(0)
    val content = splitPage(1)

    Some(title, content)
  }

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP): Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 3 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
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

