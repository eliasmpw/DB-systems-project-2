package sampling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import java.io._

object Main {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)
    val session = SparkSession.builder().getOrCreate()

//    val lineorderFile= "hdfs:/user/cs422-group3/test/lineorder_small.tbl"
//    val lineorderFile= "../lineorder_small.tbl"
//    val lineorder = new File(getClass.getResource(lineorderFile).getFile).getPath
//    val lineorderDf = session.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .option("delimiter", "|")
//      .load(lineorder)

    val rdd = RandomRDDs.uniformRDD(sc, 100000)
    val rdd2 = rdd.map(f => Row.fromSeq(Seq(f * 2, (f*10).toInt)))

    val table = session.createDataFrame(rdd2, StructType(
      StructField("A1", DoubleType, false) ::
      StructField("A2", IntegerType, false) ::
      Nil
    ))

    val desc = new Description
    desc.lineitem = table
//    desc.lineitem = lineorderDf
    desc.e = 0.1
    desc.ci = 0.95

//    val pathPrefix = "/cs422-data/tpch/sf100/parquet/"
    val pathPrefix = "./tpch_parquet_sf1/"
    desc.customer = session.read.parquet(pathPrefix + "customer.parquet")
    desc.lineitem = session.read.parquet(pathPrefix + "lineitem.parquet")
    desc.nation = session.read.parquet(pathPrefix + "nation.parquet")
    desc.orders = session.read.parquet(pathPrefix + "order.parquet")
    desc.part = session.read.parquet(pathPrefix + "part.parquet")
    desc.partsupp = session.read.parquet(pathPrefix + "partsupp.parquet")
    desc.region = session.read.parquet(pathPrefix + "region.parquet")
    desc.supplier = session.read.parquet(pathPrefix + "supplier.parquet")

    val tmp = Sampler.sample(desc.lineitem, 1000000, desc.e, desc.ci)
    desc.samples = tmp._1
    desc.sampleDescription = tmp._2

    // check storage usage for samples

    // Execute first query
    Executor.execute_Q1(desc, session, List("3"))
    //Executor.execute_Q3(desc, session, List("AUTOMOBILE","1996-03-13"))
    //Executor.execute_Q5(desc, session, List("ASIA", "1996-03-13"))
  }     
}
