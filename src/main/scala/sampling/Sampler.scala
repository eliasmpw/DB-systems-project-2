package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.SizeEstimator

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    lineitem.show

    val rows = lineitem.rdd
    val n = rows.count()

    val priceIndex = 5
    var z: Double = 2.8
    if (ci <= 0.99) {
      z = 2.58
    } else if (ci <= 0.98) {
      z = 2.33
    } else if (ci <= 0.95) {
      z = 1.96
    } else if (ci <= 0.9) {
      z = 1.64
    } else if (ci <= 0.8) {
      z = 1.28
    }

    val samplesList: List[List[Int]] = List(List(4, 6, 10), List(8, 9, 10), List(10, 11, 12, 14), List(4, 13, 14), List(2, 4))
    lazy val nhSh1 = rows.groupBy(x => (x(4), x(6), x(10)))
      .map(x => (x._1, x._2.size, x._2.map(y => y(priceIndex).asInstanceOf[java.math.BigDecimal].doubleValue()).toList))
      .map(x => (x._1, x._2, x._3.sum / x._3.length.toDouble, x._3))
      .map(x => (x._1, x._2, x._4.map(y => Math.pow(y - x._3, 2) / x._2.toDouble).sum))
    lazy val nhSh2 = rows.groupBy(x => (x(8), x(9), x(10)))
      .map(x => (x._1, x._2.size, x._2.map(y => y(priceIndex).asInstanceOf[java.math.BigDecimal].doubleValue()).toList))
      .map(x => (x._1, x._2, x._3.sum / x._3.length.toDouble, x._3))
      .map(x => (x._1, x._2, x._4.map(y => Math.pow(y - x._3, 2) / x._2.toDouble).sum))
    lazy val nhSh3 = rows.groupBy(x => (x(10), x(11), x(12), x(14)))
      .map(x => (x._1, x._2.size, x._2.map(y => y(priceIndex).asInstanceOf[java.math.BigDecimal].doubleValue()).toList))
      .map(x => (x._1, x._2, x._3.sum / x._3.length.toDouble, x._3))
      .map(x => (x._1, x._2, x._4.map(y => Math.pow(y - x._3, 2) / x._2.toDouble).sum))
    lazy val nhSh4 = rows.groupBy(x => (x(4), x(13), x(14)))
      .map(x => (x._1, x._2.size, x._2.map(y => y(priceIndex).asInstanceOf[java.math.BigDecimal].doubleValue()).toList))
      .map(x => (x._1, x._2, x._3.sum / x._3.length.toDouble, x._3))
      .map(x => (x._1, x._2, x._4.map(y => Math.pow(y - x._3, 2) / x._2.toDouble).sum))
    lazy val nhSh5 = rows.groupBy(x => (x(2), x(4)))
      .map(x => (x._1, x._2.size, x._2.map(y => y(priceIndex).asInstanceOf[java.math.BigDecimal].doubleValue()).toList))
      .map(x => (x._1, x._2, x._3.sum / x._3.length.toDouble, x._3))
      .map(x => (x._1, x._2, x._4.map(y => Math.pow(y - x._3, 2) / x._2.toDouble).sum))
    val nhShList = List(nhSh1, nhSh2, nhSh3, nhSh4, nhSh5)

    def estimateSize(input: RDD[_]): Long = {
      return SizeEstimator.estimate(input.collect())
    }

    def sampler(indexes: List[Int], nhShSampling: RDD[_], start: Double = 0.03, end: Double = 0.8, step: Double = 0.03): RDD[_] = {
      var finalSample: (RDD[_], Double) = (null, 0)
      var probability: Double = start
      var sampleData: RDD[Row] = null
      var nhSh2Sample: RDD[(Any, Int)] = null

      while (finalSample._1 == null && probability < end) {
        indexes.length match {
          case 1 =>
            val fractions = rows.map(x => x(indexes(0)))
              .distinct()
              .map(x => (x, probability))
              .collectAsMap()
            sampleData = rows.keyBy(x => x(indexes(0)))
              .sampleByKeyExact(false, fractions)
              .map(x => x._2)
            nhSh2Sample = sampleData.groupBy(x => x(indexes(0)))
              .map(x => (x._1, x._2.size))
          case 2 =>
            val fractions = rows.map(x => (x(indexes(0)), x(indexes(1))))
              .distinct()
              .map(x => (x, probability))
              .collectAsMap()
            sampleData = rows.keyBy(x => (x(indexes(0)), x(indexes(1))))
              .sampleByKeyExact(false, fractions)
              .map(x => x._2)
            nhSh2Sample = sampleData.groupBy(x => (x(indexes(0)), x(indexes(1))))
              .map(x => (x._1, x._2.size))
          case 3 =>
            val fractions = rows.map(x => (x(indexes(0)), x(indexes(1)), x(indexes(2))))
              .distinct()
              .map(x => (x, probability))
              .collectAsMap()
            sampleData = rows.keyBy(x => (x(indexes(0)), x(indexes(1)), x(indexes(2))))
              .sampleByKeyExact(false, fractions)
              .map(x => x._2)
            nhSh2Sample = sampleData.groupBy(x => (x(indexes(0)), x(indexes(1)), x(indexes(2))))
              .map(x => (x._1, x._2.size))
          case 4 =>
            val fractions = rows.map(x => (x(indexes(0)), x(indexes(1)), x(indexes(2)), x(indexes(3))))
              .distinct()
              .map(x => (x, probability))
              .collectAsMap()
            sampleData = rows.keyBy(x => (x(indexes(0)), x(indexes(1)), x(indexes(2)), x(indexes(3))))
              .sampleByKeyExact(false, fractions)
              .map(x => x._2)
            nhSh2Sample = sampleData.groupBy(x => (x(indexes(0)), x(indexes(1)), x(indexes(2)), x(indexes(3))))
              .map(x => (x._1, x._2.size))
        }

        // Get nh and sh
        val mergedRdd = nhSh2Sample.keyBy(x => x._1)
          .join(nhShSampling.asInstanceOf[RDD[(Any, Int, Double)]].keyBy(x => x._1))
          .map(x => (x._2._1._2, x._2._2._2, x._2._2._3))

        val nSample = n

        // Calculate relative error
        val errorValue = mergedRdd.map(x => (nSample / x._1) * Math.pow(x._2, 2) * x._3)
          .collect()
          .sum / nSample

        // Calculate true error
        val trueError = z * Math.pow(errorValue, 0.5)

        val meanSample: List[Double] = sampleData.map(x => x(priceIndex))
          .collect()
          .map(x => x.asInstanceOf[java.math.BigDecimal].doubleValue())
          .toList

        val meanValue: Double = meanSample.foldLeft(0.0)(_ + _)

        val totalError: Double = trueError / meanValue

        // Check if value is inbounds
        if (totalError <= e && totalError > 0) {
          finalSample = (sampleData, totalError)
        } else if (totalError == 0 && probability > 0.3) {
          finalSample = (null, totalError)
        } else {
          finalSample = (null, totalError)
        }

        println("Probability")
        println(probability)
        println(finalSample._2)

        probability = probability + step
      }
      finalSample._1
    }

    var returnVal = samplesList.zip(nhShList)
      .map(x => (sampler(x._1, x._2, 0.15, 0.8, 0.1), x._1))
      .map(x => (x._1, estimateSize(x._1), x._2))

    var sizeTot: Long = 0
    var i: Int = 0

    while (i < returnVal.length) {
      sizeTot = sizeTot + returnVal(i)._2

      if (sizeTot > storageBudgetBytes) {
        sizeTot = sizeTot - returnVal(i)._2
        returnVal = returnVal.drop(i)
      } else {
        i = i + 1
      }
    }

    (returnVal.map(x => x._1), returnVal.map(x => x._3))
  }
}
