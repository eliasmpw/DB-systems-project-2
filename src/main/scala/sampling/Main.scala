package sampling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Main {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val session = SparkSession.builder().getOrCreate();

    val rdd = RandomRDDs.uniformRDD(sc, 100000)
    val rdd2 = rdd.map(f => Row.fromSeq(Seq(f * 2, (f*10).toInt)))

    val table = session.createDataFrame(rdd2, StructType(
      StructField("A1", DoubleType, false) ::
      StructField("A2", IntegerType, false) ::
      Nil
    ))

    var desc = new Description
    desc.lineitem = table
    desc.e = 0.1
    desc.ci = 0.95

    val tmp = Sampler.sample(desc.lineitem, 1000000, desc.e, desc.ci)
    desc.samples = tmp._1
    desc.sampleDescription = tmp._2

    // check storage usage for samples

    // Execute first query
    Executor.execute_Q1(desc, session, List("3"))
  }     
}
