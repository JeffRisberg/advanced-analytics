package com.incra.prosper

import au.com.bytecode.opencsv.CSVParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Using data from Prosper bank, train a LogisticRegression model to predict defaults on loans.
 *
 * Data set is located at
 * https://docs.google.com/document/d/1qEcwltBMlRYZT-l699-71TzInWfk4W9q5rTCSvDVMpc/pub
 *
 * @author Brandon Risberg and Jeffrey Risberg
 * @since September 2015
 */
object RunLoanDefaultPredictor {

  var loanStatusIndex = 0
  var isBorrowerHomeownerIndex = 0
  var isEmployedIndex = 0
  var employmentStatusDurationIndex = 0
  var loanOriginalAmountIndex = 0
  var amountDelinquentIndex = 0
  var delinquenciesLast7YearsIndex = 0
  var creditScoreRangeLowerIndex = 0
  var statedMonthlyIncomeIndex = 0
  var debtToIncomeRatioIndex = 0

  object RegType extends Enumeration {
    type RegType = Value
    val L1, L2 = Value
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RunPredictor")
      .set("spark.executor.memory", "6g")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val base = "../../advanced-analytics/prosper/"
    val rawData = sc.textFile(base + "prosperLoanData.csv")

    val headerAndRows = rawData.map { line => mLine(line) }
    val header = headerAndRows.first
    header foreach println

    loanStatusIndex = header.indexOf("LoanStatus")
    isEmployedIndex = header.indexOf("EmploymentStatus")
    employmentStatusDurationIndex = header.indexOf("EmploymentStatusDuration")
    isBorrowerHomeownerIndex = header.indexOf("IsBorrowerHomeowner")
    loanOriginalAmountIndex = header.indexOf("LoanOriginalAmount")
    amountDelinquentIndex = header.indexOf("AmountDelinquent")
    delinquenciesLast7YearsIndex = header.indexOf("DelinquenciesLast7Years")
    creditScoreRangeLowerIndex = header.indexOf("CreditScoreRangeLower")
    statedMonthlyIncomeIndex = header.indexOf("StatedMonthlyIncome")
    debtToIncomeRatioIndex = header.indexOf("DebtToIncomeRatio")

    val data = headerAndRows.filter(_(0) != header(0))

    val safeParse = safe(parse)
    val labeledPoints = data.map(safeParse)

    labeledPoints.cache()

    val labeledPointsBad = labeledPoints.collect({
      case t if t.isRight => t.right.get
    })

    val labeledPointsGood = labeledPoints.collect({
      case t if t.isLeft => t.left.get
    })
    labeledPointsGood.cache()

    println("Number of valid input records " + labeledPointsGood.count)
    println("Typical input record:")
    println("  Good/Bad status " + (if (labeledPointsGood.first.label == 1.0) "Good" else "Bad"))
    println("  IsBorrowerHomeowner " + labeledPointsGood.first.features(0))
    println("  IsEmployed " + labeledPointsGood.first.features(1))
    println("  employmentStatusDuration " + labeledPointsGood.first.features(2))
    println("  LoanOriginalAmount " + labeledPointsGood.first.features(3))
    println("  AmountDelinquent " + labeledPointsGood.first.features(4))
    println("  DelinquenciesLast7YearsIndex " + labeledPointsGood.first.features(5))
    println("  CreditScoreRangeLower " + labeledPointsGood.first.features(6))
    println("  StatedMonthlyIncome " + labeledPointsGood.first.features(7))
    println("  IncomeToAmountRatio " + labeledPointsGood.first.features(8))
    println("  DebtToIncomeRatio " + labeledPointsGood.first.features(9))

    val numIterations = 100
    val regParam = 0.1
    val regType = RegType.L2

    val splits = labeledPointsGood.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    labeledPointsGood.unpersist(blocking = false)

    val updater = regType match {
      case RegType.L1 => new L1Updater()
      case RegType.L2 => new SquaredL2Updater()
    }

    val algorithm = new LogisticRegressionWithLBFGS()
    algorithm.optimizer
      .setNumIterations(numIterations)
      .setUpdater(updater)
      .setRegParam(regParam)

    val model = algorithm.run(training).clearThreshold()

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")

    println("Model Weights:")
    model.weights.toArray.foreach { weight => println("  " + weight) }
    // the highest weight is given to the debt-income ratio

    sc.stop()
  }

  def mLine(line: String) = {
    val parser = new CSVParser(',')
    parser.parseLine(line)
  }

  def safe[S, T](f: S => T): S => Either[T, Exception] = {
    new Function[S, Either[T, Exception]] with Serializable {
      def apply(s: S): Either[T, Exception] = {
        try {
          Left(f(s))
        } catch {
          case e: Exception => Right(e)
        }
      }
    }
  }

  def parse(data: Array[String]): LabeledPoint = {
    val loanStatus = data(loanStatusIndex)
    val loanGood = List("Current", "Completed", "FinalPaymentInProgress", "Cancelled").contains(loanStatus)

    val isBorrowerHomeowner = if (data(isBorrowerHomeownerIndex) == "True") 1.0 else 0.0
    val isEmployed = if (data(isEmployedIndex) == "Employed") 1.0 else 0.0
    var employmentStatusDuration = data(employmentStatusDurationIndex).toDouble
    val originalAmount = data(loanOriginalAmountIndex).toDouble
    val amountDelinquent = data(amountDelinquentIndex).toDouble
    val delinquenciesLast7Years = data(delinquenciesLast7YearsIndex).toDouble
    val creditScoreRangeLower = data(creditScoreRangeLowerIndex).toDouble
    val statedMonthlyIncome = data(statedMonthlyIncomeIndex).toDouble
    val incomeToAmountRatio = statedMonthlyIncome / originalAmount
    val debtToIncomeRatio = data(debtToIncomeRatioIndex).toDouble

    val label = if (loanGood) 1.0 else 0.0
    val numFeatures = 10
    val indices = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val values = Array(isBorrowerHomeowner, isEmployed, employmentStatusDuration,
      originalAmount, amountDelinquent,
      delinquenciesLast7Years, creditScoreRangeLower,
      statedMonthlyIncome, incomeToAmountRatio, debtToIncomeRatio)

    LabeledPoint(label, Vectors.sparse(numFeatures, indices, values))
  }
}
