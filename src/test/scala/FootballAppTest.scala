import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import FootballApp._

class FootballAppTest extends FlatSpec with Matchers {
  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  "createCsvDf" should "return the number of row of the csv DataFrame(442) filtered" in {
    // Given
    //val df = List(1d, 3d, 4d, 1d).toDF("double-col")
    // When
    val count = FootballApp.createCsvDf(spark).count()
    // Then
    count shouldBe 442
  }
  def hasColumn(df: DataFrame, colName: String) = df.columns.contains(colName)
  "x4X6NotPresent" should "return true if X4 and X6 column are not present" in {
    // Given
    //val df = List(1d, 3d, 4d, 1d).toDF("double-col")
    // When
    val x4X6NotPresent = !hasColumn(FootballApp.createCsvDf(spark), "X4") && !hasColumn(FootballApp.createCsvDf(spark), "X6")
    // Then
    x4X6NotPresent shouldBe true
  }
  "allColumnPresent" should "return true if all column are present" in {
    // Given
    //val df = List(1d, 3d, 4d, 1d).toDF("double-col")
    // When
    val allColumnPresent = hasColumn(FootballApp.createCsvDf(spark), "match") &&
      hasColumn(FootballApp.createCsvDf(spark), "competition") &&
      hasColumn(FootballApp.createCsvDf(spark), "adversaire") &&
      hasColumn(FootballApp.createCsvDf(spark), "score_france") &&
      hasColumn(FootballApp.createCsvDf(spark), "score_adversaire") &&
      hasColumn(FootballApp.createCsvDf(spark), "penalty_france") &&
      hasColumn(FootballApp.createCsvDf(spark), "penalty_adversaire") &&
      hasColumn(FootballApp.createCsvDf(spark), "date")
    // Then
    allColumnPresent shouldBe true
  }
}
