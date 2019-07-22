import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j._


object Fifa5qs {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils");
    //Setting up the Spark Session and Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("Fifa")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Fifa 5 queries part-3")
      .config(conf =conf)
      .getOrCreate()


    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // We are using all 3 Fifa dataset taken from Kaggle Repository
    //Import the dataset and create df and print Schema

    val wc_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Lenovo\\IdeaProjects\\M2_Lab2_2\\WorldCups.csv")

    val wcplayers_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Lenovo\\IdeaProjects\\M2_Lab2_2\\WorldCupPlayers.csv")


    val wcmatches_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load("C:\\Users\\Lenovo\\IdeaProjects\\M2_Lab2_2\\WorldCupMatches.csv")


    // Printing the Schema

    wc_df.printSchema()

    wcmatches_df.printSchema()

    wcplayers_df.printSchema()

    //First of all create three Temp View

    wc_df.createOrReplaceTempView("WorldCup")

    wcmatches_df.createOrReplaceTempView("wcMatches")

    wcplayers_df.createOrReplaceTempView("wcPlayers")

    //Perform any 5 queries in Spark RDD’s and Spark Data Frames.

    // To Solve this Problem we first create the rdd as we already have Dataframe wc_df created above

    // RDD creation

    val csv = sc.textFile("C:\\Users\\Lenovo\\IdeaProjects\\M2_Lab2_2\\WorldCups.csv")

    val header = csv.first()

    val data = csv.filter(line => line != header)

    val rdd = data.map(line=>line.split(",")).collect()

    //Q1. Find Highest Number of Goals

    //RDD
    val rddgoals = data.filter(line => line.split(",")(6) != "NULL").map(line => (line.split(",")(1),
      (line.split(",")(6)))).takeOrdered(10)
    rddgoals.foreach(println)

    // Dataframe
    wc_df.select("Country","GoalsScored").orderBy("GoalsScored").show(10)

    // Dataframe SQL
    val dfGoals = spark.sql("select Country,GoalsScored FROM WorldCup order by GoalsScored Desc Limit 10").show()

    //Q2. Retrieve all the hosting countries who are winning countries along with the year.

    //RDD
    val rddvenue = data.filter(line => line.split(",")(1)==line.split(",")(2))
      .map(line => (line.split(",")(0),line.split(",")(1), line.split(",")(2)))
      .collect()

    rddvenue.foreach(println)

    //Dataframe
    wc_df.select("Year","Country","Winner").filter("Country==Winner").show(10)

    //Spark SQL
    val venueDF = spark.sql("select Year,Country,Winner from WorldCup where Country = Winner order by Year").show()

    //Q3. Details of years ending in ZERO

    // RDD
    var years = Array("1930","1950","1970","1990","2010")

    val rddwinY = data.filter(line => (line.split(",")(0)=="1950" ))
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddwinY.foreach(println)

    //DataFrame
    wc_df.select("Year","Runners-Up","Winner").filter("Year='1950' or Year='1930' or " +
      "Year='1990' or Year='1970' or Year='2010'").show(10)

    // SQL
    val winYDF = spark.sql("SELECT * FROM WorldCup  WHERE " +
      " Year IN ('1950','1930','1990','1970','2010') ").show()

    //Q4. Retrieve all the details of the World Cup match organised in 2006.

    //RDD

    val rddStat = data.filter(line=>line.split(",")(0)=="2006")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddStat.foreach(println)

    //Dataframe
    wc_df.filter("Year=2006").show()

    //Sql
    spark.sql(" Select * from WorldCup where Year == 2006 ").show()

    //Q5. Maximum Matches Played

    //RDD

    val rddMax = data.filter(line=>line.split(",")(8) == "64")
      .map(line=> (line.split(",")(0),line.split(",")(2),line.split(",")(3))).collect()

    rddMax.foreach(println)

    // DataFrame
    wc_df.filter("MatchesPlayed == 64").show()

    // Spark SQL

    spark.sql(" Select * from WorldCup where MatchesPlayed in " +
      "(Select Max(MatchesPlayed) from WorldCup )" ).show()






  }
}
