import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import FootballApp._

class FootballAppTest extends FlatSpec with Matchers {
  val spark: SparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  val dfCsv = FootballApp.createCsvDf(spark)
  val dfStats = crateDfStats(dfCsv)
  val dfJoinStats = createDfJoinStats(dfCsv, dfStats)
  "countDfCsv" should "return the number of row of the csv DataFrame(442) filtered" in {
    // When
    val countDfCsv = dfCsv.count()
    // Then
    countDfCsv shouldBe 442
  }
  def hasColumn(df: DataFrame, colName: String) = df.columns.contains(colName)
  "x4X6NotPresent" should "return true if X4 and X6 column are not present" in {
    // When
    val x4X6NotPresent = !hasColumn(dfCsv, "X4") && !hasColumn(dfCsv, "X6")
    // Then
    x4X6NotPresent shouldBe true
  }
  "allColumnPresent" should "return true if all column are present in the first data frame" in {
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

  "allDfStatsColumnPresent" should "return true if all column are present" in {
    // When
    val allDfStatsColumnPresent = hasColumn(dfStats, "nbPtsFranceAvg") &&
      hasColumn(dfStats, "nbPtsAdversaireAvg") &&
      hasColumn(dfStats, "nbMatch") &&
      hasColumn(dfStats, "percentageDomicileFrance") &&
      hasColumn(dfStats, "nbMatchCdm") &&
      hasColumn(dfStats, "maxPenaltyFrance") &&
      hasColumn(dfStats, "PenaltyFranceMinusPenaltyAdversaire")
    // Then
    allDfStatsColumnPresent shouldBe true
  }

  "allDJoinStatsColumnPresent" should "return true if all column are present in the join data frame" in {
    // When
    val allDJoinStatsColumnPresent = hasColumn(dfJoinStats, "match") &&
      hasColumn(dfJoinStats, "competition") &&
      hasColumn(dfJoinStats, "adversaire") &&
      hasColumn(dfJoinStats, "score_france") &&
      hasColumn(dfJoinStats, "score_adversaire") &&
      hasColumn(dfJoinStats, "penalty_france") &&
      hasColumn(dfJoinStats, "penalty_adversaire") &&
      hasColumn(dfJoinStats, "date") &&
      hasColumn(dfJoinStats, "nbPtsFranceAvg") &&
      hasColumn(dfJoinStats, "nbPtsAdversaireAvg") &&
      hasColumn(dfJoinStats, "nbMatch") &&
      hasColumn(dfJoinStats, "percentageDomicileFrance") &&
      hasColumn(dfJoinStats, "nbMatchCdm") &&
      hasColumn(dfJoinStats, "maxPenaltyFrance") &&
      hasColumn(dfJoinStats, "PenaltyFranceMinusPenaltyAdversaire")
    // Then
    allDJoinStatsColumnPresent shouldBe true
  }
}


