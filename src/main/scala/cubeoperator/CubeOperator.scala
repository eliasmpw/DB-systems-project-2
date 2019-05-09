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
    // First Step
    var partialCellsSingle: RDD[(List[Any], Double)] = null
    var partialCellsDouble: RDD[(List[Any], (Double, Double))] = null

    // Group by - bottom cell
    agg match {
      case "COUNT" => partialCellsSingle = rdd.map(row => (index.map(colIndex => row.get(colIndex)), 1.0)).reduceByKey((accum, current) => accum + current)
      case "SUM" => partialCellsSingle = rdd.map(row => (index.map(colIndex => row.get(colIndex)), row.get(aggIndex).asInstanceOf[Int].toDouble)).reduceByKey((accum, current) => accum + current)
      case "MIN" => partialCellsSingle = rdd.map(row => (index.map(colIndex => row.get(colIndex)), row.get(aggIndex).asInstanceOf[Int].toDouble)).reduceByKey((accum, current) => Math.min(accum, current))
      case "MAX" => partialCellsSingle = rdd.map(row => (index.map(colIndex => row.get(colIndex)), row.get(aggIndex).asInstanceOf[Int].toDouble)).reduceByKey((accum, current) => Math.max(accum, current))
      case "AVG" => partialCellsDouble = rdd.map(row => (index.map(colIndex => row.get(colIndex)), (row.get(aggIndex).asInstanceOf[Int].toDouble, 1.0))).reduceByKey((accum, current) => (accum._1 + current._1, accum._2 + current._2))
    }

    // Calculate permutations
    var permutations = index.indices.flatMap(i => index.indices.combinations(i).toSet)
    permutations = permutations :+ index.indices.toVector

    if (agg == "AVG") {
      partialCellsDouble = partialCellsDouble.map(groupedRow => permutations.map(perm => (groupedRow._1.zipWithIndex.map { case (cols, i) => if (perm contains i) Some(cols) else None }, groupedRow._2))).flatMap(x => x)
    }
    else {
      partialCellsSingle = partialCellsSingle.map(groupedRow => permutations.map(perm => (groupedRow._1.zipWithIndex.map { case (cols, i) => if (perm contains i) Some(cols) else None }, groupedRow._2))).flatMap(x => x)
    }

    // Second Step - shuffle and reduce partial results
    var cuboids: RDD[(List[Any], Double)] = null
    agg match {
      case "COUNT" => cuboids = partialCellsSingle.reduceByKey((accum, current) => accum + current)
      case "SUM" => cuboids = partialCellsSingle.reduceByKey((accum, current) => accum + current)
      case "MIN" => cuboids = partialCellsSingle.reduceByKey((accum, current) => Math.min(accum, current))
      case "MAX" => cuboids = partialCellsSingle.reduceByKey((accum, current) => Math.max(accum, current))
      case "AVG" => cuboids = partialCellsDouble.reduceByKey((accum, current) => (accum._1 + current._1, accum._2 + current._2)).map { case (cols, values) => (cols, values._1 / values._2) }
    }

    val finalCube = cuboids.map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))

    finalCube
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    println(index)
    val aggIndex = schema.indexOf(aggAttribute)

    // Task 1
    // First Step
    var partialCellsSingle: RDD[(List[Any], Double)] = null
    var partialCellsDouble: RDD[(List[Any], (Double, Double))] = null
    var permutations = index.indices.flatMap(i => index.indices.combinations(i).toSet)
    permutations = permutations :+ index.indices.toVector

    // Naive - create all permutations without grouping first
    if (agg == "AVG") {
      partialCellsDouble = rdd.map(row => (index.map(colIndex => row.get(colIndex)), (row.get(aggIndex).asInstanceOf[Int].toDouble, 1.0)))
      partialCellsDouble = partialCellsDouble.map(groupedRow => permutations.map(perm => (groupedRow._1.zipWithIndex.map { case (cols, i) => if (perm contains i) Some(cols) else None }, groupedRow._2))).flatMap(x => x)
    }
    else {
      if (agg == "COUNT") {
        partialCellsSingle = rdd.map(row => (index.map(colIndex => row.get(colIndex)), 1.0))
      } else {
        partialCellsSingle = rdd.map(row => (index.map(colIndex => row.get(colIndex)), row.get(aggIndex).asInstanceOf[Int].toDouble))
      }
      partialCellsSingle = partialCellsSingle.map(groupedRow => permutations.map(perm => (groupedRow._1.zipWithIndex.map { case (cols, i) => if (perm contains i) Some(cols) else None }, groupedRow._2))).flatMap(x => x)
    }

    // Shuffle and reduce all
    var cuboids: RDD[(List[Any], Double)] = null
    agg match {
      case "COUNT" => cuboids = partialCellsSingle.reduceByKey((accum, current) => accum + current)
      case "SUM" => cuboids = partialCellsSingle.reduceByKey((accum, current) => accum + current)
      case "MIN" => cuboids = partialCellsSingle.reduceByKey((accum, current) => Math.min(accum, current))
      case "MAX" => cuboids = partialCellsSingle.reduceByKey((accum, current) => Math.max(accum, current))
      case "AVG" => cuboids = partialCellsDouble.reduceByKey((accum, current) => (accum._1 + current._1, accum._2 + current._2)).map { case (cols, values) => (cols, values._1 / values._2) }
    }

    val finalCube = cuboids.map(x => (x._1.mkString(", ").replace("Some(", "").replace(")", ""), x._2))

    finalCube
  }

}
