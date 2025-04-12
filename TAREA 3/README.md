Análisis de Películas Mejor Valoradas – TMDB
Diana Marcela Castiblanco Sánchez
Grupo: 202016911_17

📌 Descripción del Proyecto
Este proyecto tiene como objetivo analizar películas altamente valoradas en la base de datos de TMDB, 
aplicando tanto procesamiento por lotes como procesamiento en tiempo real. Se hace uso de herramientas 
del ecosistema Apache Spark y Apache Kafka.

Etapas del Proyecto
1. Procesamiento por Lotes (Batch Processing)
Se realiza una carga y tratamiento inicial de los datos históricos para su análisis:

 Carga de datos desde la fuente original.

Limpieza y transformación del dataset utilizando RDDs o DataFrames.

Análisis exploratorio de datos (EDA).

Almacenamiento de los resultados procesados para su posterior consulta.

Implementado en: tarea3_peliculas_limpieza.py

2. Procesamiento en Tiempo Real (Streaming con Kafka + Spark)
Se simula la llegada continua de nuevos datos y se procesan en tiempo real mediante Spark Streaming:

Configuración de un topic en Kafka para simular eventos en tiempo real.

Generación de datos en tiempo real usando un productor Kafka.

Implementación de una aplicación Spark Structured Streaming que:

Consume los datos del topic.

Realiza análisis en tiempo real (ej. conteo de eventos, cálculos estadísticos).

Presenta los resultados procesados.

Componentes implementados:

kafka_producer_peliculas.py – Simula el flujo de datos hacia Kafka.

spark_streaming_consumer_peliculas.py – Consume y analiza el flujo en tiempo real desde Kafka.

Archivos del Proyecto
Archivo	Descripción
tarea3_peliculas_limpieza.py	        Limpieza y análisis batch del dataset TMDB.
kafka_producer_peliculas.py	            Envío de datos simulados a Kafka.
spark_streaming_consumer_peliculas.py	Consumo y procesamiento en tiempo real desde Kafka.