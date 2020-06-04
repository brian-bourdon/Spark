import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType};
import org.apache.spark.sql.functions.{udf, col, when, year}

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

      val test = dfCsv.withColumn("Domicile", is_domicile_udf(col("match")))

    test.show
    test.printSchema()
    spark.stop()
  }
}