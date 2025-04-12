# tarea3_peliculas.py
# Diana Marcela Castiblanco Sanchez
# Grupo 202016911_17

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, to_date, year
from pyspark.sql.types import DoubleType


# Se inicializa  sesión de Spark
spark = SparkSession.builder \
    .appName("Tarea3") \
    .getOrCreate()

# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/top_rated_movies.csv'


# Lectura del archivo  dataset del archivo top_rated_movies.csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

#imprimimos el esquema
df.printSchema()

# Muestra las primeras filas del DataFrame
df.show()

# Se realiza la limpieza y trasnformacion de  datos
df = df.dropna(subset=["Release_Date", "Popularity", "Vote_Average", "Vote_Count"])
df = df.withColumn("Release_Date", to_date(col("Release_Date"), "yyyy-MM-dd"))
df = df.withColumn("Release_Year", year(col("Release_Date")))
df = df.withColumn("Popularity", col("Popularity").cast(DoubleType()))

# 1. Películas con mayor popularidad y su fecha de lanzamiento
print("Películas con mayor popularidad y su fecha de lanzamiento:\n")
popular_movies = df.select("Original_Title", "Popularity", "Release_Date") \
    .orderBy(desc("Popularity"))
popular_movies.show(10, False)

# 2. Películas con mayor promedio y recuento de votos
print("Películas con mayor promedio y recuento de votos:\n")
best_rated_movies = df.select("Original_Title", "Vote_Average", "Vote_Count") \
    .orderBy(desc("Vote_Average"), desc("Vote_Count"))
best_rated_movies.show(10, False)

# 3. Años con mayor popularidad promedio y películas más populares en esos años
print("Años con mayor popularidad promedio y películas destacadas:\n")
popularity_by_year = df.groupBy("Release_Year") \
    .avg("Popularity") \
    .orderBy(desc("avg(Popularity)"))
popularity_by_year.show(10, False)

# Almacenar los resultados procesados
popular_movies.write.mode("overwrite").parquet("file:///home/vboxuser/resultados/popular_movies")
best_rated_movies.write.mode("overwrite").parquet("file:///home/vboxuser/resultados/best_rated_movies")
popularity_by_year.write.mode("overwrite").parquet("file:///home/vboxuser/resultados/popularity_by_year")

