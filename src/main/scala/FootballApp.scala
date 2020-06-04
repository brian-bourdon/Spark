import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, udf, when, year, avg, count, sum}

object FootballApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    // Import implicits
    import spark.implicits._

    // UDF that add a column domicile
    val is_domicile = (value:String) => (value.split("-")(0).trim() == "France")

    val is_domicile_udf = udf(is_domicile)

    // Create dataframe with the CSV data and rename the columns X4, X6
    val dfCsv = spark.read.option("header", "true").option("sep", ",").csv("C:\\Users\\brian\\IdeaProjects\\Spark\\df_matches.csv")
                .withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")
                .select($"match", $"competition", $"adversaire", $"score_france".cast(IntegerType), $"score_adversaire".cast(IntegerType), $"penalty_france".cast(IntegerType),
                  $"penalty_adversaire".cast(IntegerType), $"date".cast(DateType))
                .withColumn("penalty_france", when($"penalty_france".isNull, 0))
                .withColumn("penalty_adversaire", when($"penalty_adversaire".isNull, 0))
                .filter(year($"date") >= 1980)
                .withColumn("Domicile", is_domicile_udf(col("match")))

    val window = Window.partitionBy(col("adversaire"))
    // Nombre de point moyen marqué par la France par match
    val nbPtsFranceAvg = avg(dfCsv.col("score_france")).over(window)
    // Nombre de point moyen marqué par l'adversaire par match
    val nbPtsAdversaireAvg = avg(dfCsv.col("score_adversaire")).over(window)
    // Nombre de match joué total
    val nbMatch = count("*").over(window)
    // Pourcentage de match joué à domicile pour la France
    val percentageDomicileFrance =  sum(col("domicile").cast(IntegerType)).over(window) / nbMatch * 100

    val dfWithAvg = dfCsv.withColumn("percentageDomicileFrance", percentageDomicileFrance)


    dfWithAvg.show()
    dfWithAvg.printSchema()
    spark.stop()
  }
}