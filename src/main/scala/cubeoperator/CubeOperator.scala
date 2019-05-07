package cubeoperator

import org.apache.spark.rdd.RDD

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    println(index)
    val aggIndex = schema.indexOf(aggAttribute)

    // Task 1
    val groupingRange = index.indices
    var permutations = Range(0, index.length).flatMap(i => groupingRange.combinations(i).toSet)
    permutations = permutations :+ groupingRange.toVector

    // First Step
    var mrsSpread: RDD[(List[Any], (Double, Double))] = null
    agg match {
      case "COUNT" => mrsSpread = rdd.map(row => (index.map(colIndex => row.get(colIndex)), 1.0)).groupBy(_._1).mapValues(aggVal => (aggVal.map(_._2).sum, 0.0))
      case "SUM" => mrsSpread = rdd.map(row => (index.map(colIndex => row.get(colIndex)), row.get(aggIndex).asInstanceOf[Int].toDouble)).groupBy(_._1).mapValues(aggVal => (aggVal.map(_._2).sum, 0.0))
      case "MIN" => mrsSpread = rdd.map(row => (index.map(colIndex => row.get(colIndex)), row.get(aggIndex).asInstanceOf[Int].toDouble)).groupBy(_._1).mapValues(aggVal => (aggVal.map(_._2).min, 0.0))
      case "MAX" => mrsSpread = rdd.map(row => (index.map(colIndex => row.get(colIndex)), row.get(aggIndex).asInstanceOf[Int].toDouble)).groupBy(_._1).mapValues(aggVal => (aggVal.map(_._2).max, 0.0))
      case "AVG" => mrsSpread = rdd.map(row => (index.map(colIndex => row.get(colIndex)), row.get(aggIndex).asInstanceOf[Int].toDouble)).groupBy(_._1).mapValues(aggVal => (aggVal.map(_._2).sum, aggVal.map(_._2).size))
    }

    val partialCells = mrsSpread.map(groupedRow => permutations.map(perm => (groupedRow._1.zipWithIndex.map{case(cols, i) => if(perm contains i) Some(cols) else None}, groupedRow._2))).flatMap(x => x)

    // Second Step
    var cuboids: RDD[(List[Option[Any]], Double)] = null
    agg match {
      case "COUNT" => cuboids = partialCells.groupBy(_._1).mapValues(_.map(_._2._1).sum)
      case "SUM" => cuboids = partialCells.groupBy(_._1).mapValues(_.map(_._2._1).sum)
      case "MIN" => cuboids = partialCells.groupBy(_._1).mapValues(_.map(_._2._1).min)
      case "MAX" => cuboids = partialCells.groupBy(_._1).mapValues(_.map(_._2._1).max)
      case "AVG" => cuboids = partialCells.groupBy(_._1).mapValues(aggVal => aggVal.map(_._2._1).sum / aggVal.map(_._2._2).sum)
    }

    val formatted = cuboids.map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))

    formatted
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation
    null
  }

}
