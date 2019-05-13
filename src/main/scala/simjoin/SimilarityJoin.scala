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

    val rdd: RDD[String] = dataset.getRDD().map(_.getString(attrIndex))

    // Should we use `sample` wich returns an RDD ?
    val anchors: Array[String] = rdd.takeSample(false, numAnchors)

    def compareAll(part: Iterator[(String, String)]): Iterator[(String, String)] = {
      // FIXME: Ensure that comparisons are done only once.
      val strings = part.map(_._2).toIndexedSeq
      val ret = for {
        i <- 0 to (strings.size - 1)
        j <- i + 1 to (strings.size - 1)
        if levenshtein(strings(i), strings(j)) <= distThreshold
      } yield (strings(i), strings(j))
      ret.toIterator
    }

    rdd.flatMap(assign(anchors))
      .partitionBy(new SimilarityJoinPartitioner(anchors))
      .mapPartitions(compareAll, true)

  }

  // Assign each element of the RDD to it's anchor point
  def assign(anchors: Array[String])(v: String): List[(String, String)] = {
    val dists = anchors.map(k => levenshtein(k, v))
    List((anchors(dists.indices.minBy(dists)), v))
  }

  // Compute the Levenshtein distance between two strings.
  def levenshtein(k: String, v: String): Int = {
    val m = k.size
    val n = v.size
    var distanceMatrix: Array[Array[Int]] = Array.ofDim(m + 1, n + 1)

    def min3(a: Int, b: Int, c: Int): Int = { if (a < b) { if (a < c) (a) else c } else { if (b < c) b else c } }

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
