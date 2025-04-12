# spark_streaming_consumer_peliculas.py
# Diana Marcela Castiblanco Sanchez
# Grupo 202016911_17

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# se realiza la invocacion del  Spark para configuraciones de log a WARN 
spark = SparkSession.builder \
    .appName("KafkaPeliculasStreaming") \
    .master("local[*]") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Se permite conectar el  Conectamos con el oráculo Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "peliculas_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Definimos el esquema de datoos de la entreda de las peliculas 
schema_peliculas = StructType() \
    .add("title", StringType()) \
    .add("popularity", IntegerType()) \
    .add("vote_average", DoubleType()) \
    .add("release_date", StringType())

#Realziacion del parsear los datos del  mensaje JSON
peliculas_df = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_peliculas).alias("data")) \
    .select("data.*")

#Mostrar películas con popularidad mayor a 95 de nuestro dataset 
peliculas_populares = peliculas_df.filter(col("popularity") > 95)

#visualizaremos  en consola las películas populares en tiempo real
query = peliculas_populares.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
