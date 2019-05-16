package sampling

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long, e: Double, ci: Double): (List[RDD[_]], _) = {
    lineitem.show
    lineitem.groupBy()
    null
  }
}
