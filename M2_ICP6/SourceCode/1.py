from pyspark.sql import SparkSession
from graphframes import *
from pyspark.sql.functions import col, concat, lit
from IPython.display import display

# Create spark session
spark = SparkSession.builder.appName("ICP6").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Load the csv data and create graph
input_path = "C:\\Users\\Lenovo\\Documents\\UMKC_SEMS\\UMKC_summer_sem\\M2_ICP6"
trip_data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "\\201508_trip_data.csv")
station_data_df = spark.read.format("csv").option("header", True).option("inferSchema", True).load(input_path + "\\201508_station_data.csv")
# Create vertex and edge dataframes
v = station_data_df.select(col("name").alias("id"), "lat", "long","dockcount")
e = trip_data_df.select(col("Start Station").alias("src"), col("End Station").alias("dst"), col("Subscriber Type").alias("relationship"))
# Create graph
g = GraphFrame(v, e)

# 2. Triangle count
results = g.triangleCount()
results.select("id", "count").show(10, False)

# 3. Find shortest path
results = g.shortestPaths(landmarks=["2nd at Folsom", "California Ave Caltrain Station","2nd at Townsend"])
results.select("id", "distances").show(10, False)

# 4. Apply page rank
results = g.pageRank(resetProbability=0.15, tol=0.01)
results.vertices.select("id", "pagerank").show(10, False)
results.edges.select("src", "dst", "weight").distinct().show(10, False)
#results.vertices.show(10,False)
#results.edges.distinct().show(10,False)

# 5. Save graph
g.vertices.write.parquet(input_path + "\\vertices")
g.edges.write.parquet(input_path + "\\edges")

# Bonus 1: LPA
tts = g.labelPropagation(maxIter=5)
tts.persist().show(10, False)

# Bonus 2: BFS
paths = g.bfs(fromExpr="id='St James Park'",toExpr ="dockcount < 15",edgeFilter="relationship!='Subscriber'",maxPathLength=3).show(1,False)