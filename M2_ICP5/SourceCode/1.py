from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import col, concat, lit
import os

os.environ["SPARK_HOME"] = "C:\\spark-2.3.1-bin-hadoop2.7"

# Create spark session
spark = SparkSession.builder.appName("ICP5").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Load the csv data and create graph
# 6. Create vertices
input_path = "C:\\Users\\Lenovo\\Documents\\UMKC_SEMS\\UMKC_summer_sem\\M2_ICP5"
trip_data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "\\201508_trip_data.csv")
station_data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "\\201508_station_data.csv")
# Create vertex and edge dataframes
v = station_data_df.select(col("name").alias("id"), "lat", "long")
e = trip_data_df.select(col("Start Station").alias("src"), col("End Station").alias("dst"), col("Subscriber Type").alias("relationship"))
# Create graph
g = GraphFrame(v, e)

# 2. Concatenate chunks into list & convert to DataFrame
station_data_df.select(concat(col("lat"), lit(" "), col("long")).alias("loc")).show(10, False)

# 3. Remove duplicates
station_data_df.select("dockcount").distinct().show()

# 4. Name columns
print(station_data_df.columns)

# 5. Output dataframe
station_data_df.write.parquet(input_path + "\\data_parquet")

# 7. Show some vertics
g.vertices.show(10, False)

# 8. Show some edges
g.edges.show(10, False)

# 9. Vertex in-degree
inDeg1=g.inDegrees
g.inDegrees.show(10, False)

# 10. Vertex out-degree
outDeg1=g.outDegrees
g.outDegrees.show(10, False)

# 11. Apply the motif findings.
g.find("(a)-[e]->(b); (b)-[e2]->(a)").distinct().show(10, False)

# Bonus 1
g.degrees.show(10, False)

# Bonus 2
topTrips = g.edges.groupBy("src", "dst").count().orderBy("count",ascending=False).show(10,False)

# Bonus 3
degreeRatio = inDeg1.join(outDeg1, inDeg1.id == outDeg1.id).drop(outDeg1.id).selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
degreeRatio.orderBy("degreeRatio",ascending=False).show(10,False)

# Bonus 4
g.vertices.write.parquet(input_path + "\\vertices")
g.edges.write.parquet(input_path + "\\edges")