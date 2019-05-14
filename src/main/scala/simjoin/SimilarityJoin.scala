package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")

  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {

    def compareAll(part: Iterator[(String, (String, Boolean))]): Iterator[(String, String)] = {
      val strings = part.map(_._2).toSeq
      val nativeStrings = strings.filter(_._2).map(_._1).toIndexedSeq
      val otherStrings = strings.filterNot(_._2).map(_._1).toIndexedSeq

      val homePairs = for {
        i <- 0 to (nativeStrings.size - 1)
        j <- i + 1 to (nativeStrings.size - 1)
        if levenshtein(nativeStrings(i), nativeStrings(j)) <= distThreshold
      } yield (nativeStrings(i), nativeStrings(j))

      val outsiderPairs = for {
        i <- 0 to (otherStrings.size - 1)
        j <- 0 to (nativeStrings.size - 1)
        if levenshtein(otherStrings(i), nativeStrings(j)) <= distThreshold
      } yield (otherStrings(i), nativeStrings(j))

      (homePairs ++ outsiderPairs).toIterator

    }

    // Extract values.
    val rdd: RDD[String] = dataset.getRDD().map(_.getString(attrIndex))

    // Take a sample of correct size
    val anchors: Array[String] = rdd.takeSample(false, numAnchors)

    rdd.flatMap(assign(anchors))
      .partitionBy(new SimilarityJoinPartitioner(anchors))
      .mapPartitions(compareAll, true)

  }

  // Assign each element of the RDD to it's anchor point, and to the anchor
  // point's of which it is in the outer region, but marking it as not
  // originating from that region.
  def assign(anchors: Array[String])(v: String): Array[(String, (String, Boolean))] = {

    val dists = anchors.map(k => levenshtein(k, v))
    val anchorDist = dists.min
    val outers = dists.zipWithIndex.filter{ case (d, i) => d != anchorDist && d <= anchorDist + 2 * distThreshold }

    outers.map{ case (d, i) => (anchors(i), (v, false))} :+ (anchors(dists.indices.minBy(dists)), (v, true))

  }

  // Compute the Levenshtein distance between two strings.
  def levenshtein(k: String, v: String): Int = {
    val m = k.size
    val n = v.size

    var distanceMatrix: Array[Array[Int]] = Array.ofDim(m + 1, n + 1)

    def min3(a: Int, b: Int, c: Int): Int = {
      if (a < b) {
        if (a < c) a else c
      } else {
        if (b < c) b else c
      }
    }

    for(i <- 0 to m) {
      distanceMatrix(i)(0) = i
    }
    for(j <- 0 to n) {
      distanceMatrix(0)(j) = j
    }

    for(j <- 1 to n) {
      for (i <- 1 to m) {
        if (k(i-1) == v(j-1)) {
          distanceMatrix(i)(j) = distanceMatrix(i-1)(j-1)
        } else {
          distanceMatrix(i)(j) = min3(distanceMatrix(i-1)(j) + 1,   // deletion
                                      distanceMatrix(i)(j-1) + 1,   // insertion
                                      distanceMatrix(i-1)(j-1) + 1) // substitution
        }
      }
    }
    distanceMatrix(m)(n)
  }
}

class SimilarityJoinPartitioner(anchors: Array[String]) extends org.apache.spark.Partitioner {

  def getPartition(key: Any): Int = anchors.indexOf(key)

  def numPartitions(): Int = anchors.size

}
