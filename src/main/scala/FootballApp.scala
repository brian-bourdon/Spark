import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, udf, when, year, avg, count, sum, max}

object FootballApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    // Import implicits
    import spark.implicits._

    // UDF that add a column domicile
    val is_domicile = (value:String) => (value.split("-")(0).trim() == "France")

    val is_domicile_udf = udf(is_domicile)

    val cdm_count = (value:String) => (value.contains("Coupe du monde"))

    val cdm_count_udf = udf(cdm_count)

    // Create dataframe with the CSV data and rename the columns X4, X6
    val dfCsv = spark.read.option("header", "true").option("sep", ",").csv("C:\\Users\\brian\\IdeaProjects\\Spark\\df_matches.csv")
                .withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")
                .select($"match", $"competition", $"adversaire", $"score_france".cast(IntegerType), $"score_adversaire".cast(IntegerType), $"penalty_france".cast(IntegerType),
                  $"penalty_adversaire".cast(IntegerType), $"date".cast(DateType))
                .withColumn("penalty_france", when($"penalty_france".isNull, 0))
                .withColumn("penalty_adversaire", when($"penalty_adversaire".isNull, 0))
                .filter(year($"date") >= 1980)
                .withColumn("Domicile", is_domicile_udf(col("match")))
   // j'étais parti sur une window function mais vu que dans l'énoncé il faut faire une jointure, j'ai changé pour un group_by
    /*val window = Window.partitionBy(col("adversaire"))
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

    val dfWithAvg = dfCsv.withColumn("nbMatchCdm", nbMatchCdm)*/

    val statsMatch = dfCsv.groupBy(dfCsv("adversaire")).agg(
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

    // Create the parquet file
    statsMatch.write.parquet("stats.parquet")

    statsMatch.show()
    statsMatch.printSchema()
    spark.stop()
  }
}