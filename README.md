# Spark FootballApp
## To run:
- Clone the repo
- Launch a terminal at the root of the project and run "sbt package" to generate the jar
- Then run "spark-submit --master local[4] --class FootballApp <YOUR_PROJECT_ROOT>\target\scala-2.11\spark_2.11-0.1.jar"
- And finally run "sbt test" to run the test

(The parquet files generated are in the root of the project, in the folder "parquet")
