import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import FootballApp._

class FootballAppTest extends FlatSpec with Matchers {
  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._
  val dfCsv = FootballApp.createCsvDf(spark)
  "countDfCsv" should "return the number of row of the csv DataFrame(442) filtered" in {
    // Given
    //val df = List(1d, 3d, 4d, 1d).toDF("double-col")
    // When
    val countDfCsv = dfCsv.count()
    // Then
    countDfCsv shouldBe 442
  }
  def hasColumn(df: DataFrame, colName: String) = df.columns.contains(colName)
  "x4X6NotPresent" should "return true if X4 and X6 column are not present" in {
    // Given
    //val df = List(1d, 3d, 4d, 1d).toDF("double-col")
    // When
    val x4X6NotPresent = !hasColumn(dfCsv, "X4") && !hasColumn(dfCsv, "X6")
    // Then
    x4X6NotPresent shouldBe true
  }
  "allColumnPresent" should "return true if all column are present" in {
    // Given
    //val df = List(1d, 3d, 4d, 1d).toDF("double-col")
    // When
    val allColumnPresent = hasColumn(dfCsv, "match") &&
      hasColumn(dfCsv, "competition") &&
      hasColumn(dfCsv, "adversaire") &&
      hasColumn(dfCsv, "score_france") &&
      hasColumn(dfCsv, "score_adversaire") &&
      hasColumn(dfCsv, "penalty_france") &&
      hasColumn(dfCsv, "penalty_adversaire") &&
      hasColumn(dfCsv, "date")
    // Then
    allColumnPresent shouldBe true
  }
}
