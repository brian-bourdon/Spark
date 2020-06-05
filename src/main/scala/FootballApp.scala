import java.io.FileNotFoundException

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, IntegerType}
import org.apache.spark.sql.functions.{avg, col, count, max, month, sum, udf, when, year}

object FootballApp {
  // UDF that add a column domicile
  val is_domicile = (value:String) => (value.split("-")(0).trim() == "France")
  val is_domicile_udf = udf(is_domicile)

  // Create dataframe with the CSV data, rename the columns X4, X6, filter and format some data
  def createCsvDf(spark: SparkSession): DataFrame = {
    import spark.implicits._
      spark.read.option("header", "true").option("sep", ",").csv(System.getProperty("user.dir")+"\\src\\main\\ressources\\df_matches.csv")
        .withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")
        .select($"match", $"competition", $"adversaire", $"score_france".cast(IntegerType), $"score_adversaire".cast(IntegerType), $"penalty_france".cast(IntegerType),
          $"penalty_adversaire".cast(IntegerType), $"date".cast(DateType))
        .withColumn("penalty_france", when($"penalty_france".isNull, 0))
        .withColumn("penalty_adversaire", when($"penalty_adversaire".isNull, 0))
        .filter(year($"date") >= 1980)
        .withColumn("Domicile", is_domicile_udf(col("match"))) // add boolean if france play at home
        .cache()
  }

  // UDF that check if the a match is a CDM match
  val cdm_count = (value:String) => (value.contains("Coupe du monde"))
  val cdm_count_udf = udf(cdm_count)

  /*
  J'étais parti sur une window function mais vu que dans l'énoncé il est indiqué qu'il faut faire une jointure, j'ai changé pour un group_by
  Ducoup cette fonction n'est pas utilisé mais elle fonctionne
  */
  def windowDfMatch(dfCsv: DataFrame): DataFrame = {
    val window = Window.partitionBy(col("adversaire"))
    // Nombre de point moyen marqué par la France par match
    val nbPtsFranceAvg = avg(dfCsv.col("score_france")).over(window)
    // Nombre de point moyen marqué par l'adversaire par match
    val nbPtsAdversaireAvg = avg(dfCsv.col("score_adversaire")).over(window)
    // Nombre de match joué total
    val nbMatch = count("*").over(window)
    // Pourcentage de match joué à domicile pour la France
    val percentageDomicileFrance =  sum(col("domicile").cast(IntegerType)).over(window) / nbMatch * 100
    // Nombre de match joué en coupe du monde
    val nbMatchCdm = sum(cdm_count_udf(col("competition")).cast(IntegerType)).over(window)

    val dfWithAvg = dfCsv.withColumn("nbMatchCdm", nbMatchCdm)
    return dfWithAvg
  }

  // Calcul/add all the stats and return a dataframe
  def crateDfStats(dfCsv: DataFrame): DataFrame = {
    dfCsv.groupBy(dfCsv("adversaire")).agg(
      // Nombre de point moyen marqué par la France par match
      avg(dfCsv.col("score_france")).alias("nbPtsFranceAvg"),
      // Nombre de point moyen marqué par l'adversaire par match
      avg(dfCsv.col("score_adversaire")).alias("nbPtsAdversaireAvg"),
      // Nombre de match joué total
      count("*").alias("nbMatch"),
      // Pourcentage de match joué à domicile pour la France
      (sum(col("domicile").cast(IntegerType)) / count("*") * 100).alias("percentageDomicileFrance"),
      // Nombre de match joué en coupe du monde
      sum(cdm_count_udf(col("competition")).cast(IntegerType)).alias("nbMatchCdm"),
      // Pénalité max de la france
      max(col("penalty_france")).alias("maxPenaltyFrance"),
      // Nombre de pénalité total reçu par la France moins nombre de pénalité total reçu parl’adversaire
      (sum(col("penalty_france")) - sum(col("penalty_adversaire"))).alias("PenaltyFranceMinusPenaltyAdversaire")
    )
  }

  // Join the machs data with those stats and return a dataframe
  def createDfJoinStats(dfCsv: DataFrame, dfStats: DataFrame): DataFrame = {
    dfCsv.join(
      dfStats,
      (dfCsv("adversaire") === dfStats("adversaire")),
      "inner"
    ).drop(dfStats.col("adversaire"))
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    val dfCsv = createCsvDf(spark)
    val dfStats = crateDfStats(dfCsv)

    // Create the stats parquet file
    dfStats.write.parquet(".\\parquet\\stats.parquet")

    // Create the result parquet file
    val dfJoinStats = createDfJoinStats(dfCsv, dfStats)
    // Add the column year and month for the partition of the parquet file
    val dfAllStatsForParquet = dfJoinStats
      .withColumn("Year", year(col("date")))
      .withColumn("Month", month(col("date")))
    // Create the final result parquet file partitioned by year then month
    dfAllStatsForParquet.write.partitionBy("year", "month").parquet(".\\parquet\\result.parquet")

    dfJoinStats.show()
    dfJoinStats.printSchema()
    spark.stop()
  }
}