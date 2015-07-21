package com.incra.ch4

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest, DecisionTree}
import org.apache.spark.mllib.tree.model.{Node, DecisionTreeModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RunRDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDF")
      .set("spark.executor.memory", "6g")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val base = "../../advanced-analytics/forestCover/"
    val rawData = sc.textFile(base + "covtype.data")

    val data = rawData.map { line =>
      val values = line.split(',').map(_.toDouble)
      val featureVector = Vectors.dense(values.init)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }

    // Split into 80% train, 10% cross validation, 10% test
    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    simpleDecisionTree(trainData, cvData)
    //randomClassifier(trainData, cvData)
    //evaluate(trainData, cvData, testData)
    //evaluateCategorical(rawData)
    //evaluateForest(rawData)

    trainData.unpersist()
    cvData.unpersist()
    testData.unpersist()
  }

  def simpleDecisionTree(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {
    // Build a simple default DecisionTreeModel
    val model = DecisionTree.trainClassifier(trainData, 7, Map[Int, Int](), "gini", 5, 100)

    val metrics = getMetrics(model, cvData)

    println(metrics.confusionMatrix)
    println(metrics.precision)

    (0 until 7).map(
      category => (metrics.precision(category), metrics.recall(category))
    ).foreach(println)

    val input1 = "2596,51,3,258,0,510,221,232,148,6279,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0"
    println("should be 5")
    predict(model, input1)

    val input2 = "2500,74,11,190,9,930,233,219,116,5279,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
    println("should be 5")
    predict(model, input2)

    val input3 = "2952,107,11,42,7,5845,239,226,116,3509,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0"
    println("should be 2")
    predict(model, input3)

    val input4 = "2805,44,17,170,32,1902,222,200,108,2436,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"
    println("should be 2")
    predict(model, input4)

    println("Dumping the tree")
    val topNode = model.topNode
    showNode(topNode)
  }

  def showNode(node: Node): Unit = {
    println(node)

    val leftNode = node.leftNode
    val rightNode = node.rightNode

    if (leftNode.isDefined) {
      showNode(leftNode.get)
    }
    if (rightNode.isDefined) {
      showNode(rightNode.get)
    }
  }
  def predict(model: DecisionTreeModel, line: String): Unit = {
    val values = line.split(',').map(_.toDouble)
    val featureVector = Vectors.dense(values)

    println(model.predict(featureVector)+1)
  }

  def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics = {
    val predictionsAndLabels = data.map(example =>
      (model.predict(example.features), example.label)
    )
    new MulticlassMetrics(predictionsAndLabels)
  }

  def randomClassifier(trainData: RDD[LabeledPoint], cvData: RDD[LabeledPoint]): Unit = {
    val trainPriorProbabilities = classProbabilities(trainData)
    val cvPriorProbabilities = classProbabilities(cvData)
    val accuracy = trainPriorProbabilities.zip(cvPriorProbabilities).map {
      case (trainProb, cvProb) => trainProb * cvProb
    }.sum
    println(accuracy)
  }

  def classProbabilities(data: RDD[LabeledPoint]): Array[Double] = {
    // Count (category,count) in data
    val countsByCategory = data.map(_.label).countByValue()
    // order counts by category and extract counts
    val counts = countsByCategory.toArray.sortBy(_._1).map(_._2)
    counts.map(_.toDouble / counts.sum)
  }

  def evaluate(
                trainData: RDD[LabeledPoint],
                cvData: RDD[LabeledPoint],
                testData: RDD[LabeledPoint]): Unit = {

    val evaluations =
      for (impurity <- Array("gini", "entropy");
           depth <- Array(1, 20);
           bins <- Array(10, 300))
        yield {
          val model = DecisionTree.trainClassifier(
            trainData, 7, Map[Int, Int](), impurity, depth, bins)
          val accuracy = getMetrics(model, cvData).precision
          ((impurity, depth, bins), accuracy)
        }

    evaluations.sortBy(_._2).reverse.foreach(println)

    val model = DecisionTree.trainClassifier(
      trainData.union(cvData), 7, Map[Int, Int](), "entropy", 20, 300)
    println(getMetrics(model, testData).precision)
    println(getMetrics(model, trainData.union(cvData)).precision)
  }

  def unencodeOneHot(rawData: RDD[String]): RDD[LabeledPoint] = {
    rawData.map { line =>
      val values = line.split(',').map(_.toDouble)
      // Which of 4 "wilderness" features is 1
      val wilderness = values.slice(10, 14).indexOf(1.0).toDouble
      // Similarly for following 40 "soil" features
      val soil = values.slice(14, 54).indexOf(1.0).toDouble
      // Add derived features back to first 10
      val featureVector = Vectors.dense(values.slice(0, 10) :+ wilderness :+ soil)
      val label = values.last - 1
      LabeledPoint(label, featureVector)
    }
  }

  def evaluateCategorical(rawData: RDD[String]): Unit = {

    val data = unencodeOneHot(rawData)

    val Array(trainData, cvData, testData) = data.randomSplit(Array(0.8, 0.1, 0.1))
    trainData.cache()
    cvData.cache()
    testData.cache()

    val evaluations =
      for (impurity <- Array("gini", "entropy");
           depth <- Array(10, 20, 30);
           bins <- Array(40, 300))
        yield {
          // Specify value count for categorical features 10, 11
          val model = DecisionTree.trainClassifier(
            trainData, 7, Map(10 -> 4, 11 -> 40), impurity, depth, bins)
          val trainAccuracy = getMetrics(model, trainData).precision
          val cvAccuracy = getMetrics(model, cvData).precision
          // Return train and CV accuracy
          ((impurity, depth, bins), (trainAccuracy, cvAccuracy))
        }

    evaluations.sortBy(_._2._2).reverse.foreach(println)

    val model = DecisionTree.trainClassifier(
      trainData.union(cvData), 7, Map(10 -> 4, 11 -> 40), "entropy", 30, 300)
    println(getMetrics(model, testData).precision)

    trainData.unpersist()
    cvData.unpersist()
    testData.unpersist()
  }

  def evaluateForest(rawData: RDD[String]): Unit = {
    val data = unencodeOneHot(rawData)

    val Array(trainData, cvData) = data.randomSplit(Array(0.9, 0.1))
    trainData.cache()
    cvData.cache()

    val forest = RandomForest.trainClassifier(
      trainData, 7, Map(10 -> 4, 11 -> 40), 20, "auto", "entropy", 30, 300)

    val predictionsAndLabels = cvData.map(example =>
      (forest.predict(example.features), example.label)
    )
    println(new MulticlassMetrics(predictionsAndLabels).precision)

    val input = "2709,125,28,67,23,3224,253,207,61,6094,0,29"
    val vector = Vectors.dense(input.split(',').map(_.toDouble))
    println(forest.predict(vector))
  }
}