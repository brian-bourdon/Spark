import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DateType};
import org.apache.spark.sql.functions.{udf, col, when, year}

object FootballApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    // Import implicits
    import spark.implicits._

    // UDF that return 0 if the value == null
    val nullToZeroUdf = (value:String) => if(value == "NA") {"0"}
                                          else {value}

    val nullToZero = udf(nullToZeroUdf)

    // Create dataframe with the CSV data and rename the columns X4, X6
    val dfCsv = spark.read.option("header", "true").option("sep", ",").csv("C:\\Users\\brian\\IdeaProjects\\Spark\\df_matches.csv")
                .withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")
                .select($"match", $"competition", $"adversaire", $"score_france".cast(IntegerType), $"score_adversaire".cast(IntegerType), $"penalty_france".cast(IntegerType),
                  $"penalty_adversaire".cast(IntegerType), $"date".cast(DateType))
                .withColumn("penalty_france", when($"penalty_france".isNull, 0))
                .withColumn("penalty_adversaire", when($"penalty_adversaire".isNull, 0))
                .filter(year($"date") >= 1980)

    dfCsv.show
    dfCsv.printSchema()
    spark.stop()
  }
}