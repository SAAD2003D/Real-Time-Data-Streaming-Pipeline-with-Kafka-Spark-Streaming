from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_unixtime, to_timestamp, when
import threading
import os
import queue

# Initialize Flask App
app = Flask(__name__)
CORS(app)

# Thread-safe queue for processed data
data_queue = queue.Queue()

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaStreamingConsumerWithAlerts") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
LOCATION_TOPIC = "location_data"
MOTORCYCLE_TOPIC = "motorcycle_data"
WEATHER_TOPIC = "weather_data"

# Flask endpoint to serve processed data
@app.route('/data', methods=['GET'])
def get_data():
    """Serve the processed data as JSON"""
    data = []
    while not data_queue.empty():
        data.append(data_queue.get())  # Fetch all data from the queue
    return jsonify(data)

# Root URL route
@app.route('/', methods=['GET'])
def index():
    return jsonify({"message": "Welcome to the API"})

# Favicon route
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico')

# Define Kafka Data Streams (Location, Motorcycle, Weather)
location_data_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", LOCATION_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr(
        "from_json(value, 'STRUCT<id STRING, latitude DOUBLE, longitude DOUBLE, timestamp DOUBLE>') AS data"
    ) \
    .select(
        col("data.id").alias("location_id"),
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        to_timestamp(from_unixtime(col("data.timestamp"))).alias("location_timestamp")
    ) \
    .withWatermark("location_timestamp", "10 minutes")

motorcycle_data_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", MOTORCYCLE_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr(
        "from_json(value, 'STRUCT<id STRING, speed DOUBLE, temperature DOUBLE, fuel_level DOUBLE, engine_status STRING, location_id STRING, timestamp DOUBLE>') AS data"
    ) \
    .select(
        col("data.id").alias("motorcycle_id"),
        col("data.speed"),
        col("data.temperature").alias("motorcycle_temperature"),
        col("data.fuel_level"),
        col("data.engine_status"),
        col("data.location_id"),
        to_timestamp(from_unixtime(col("data.timestamp"))).alias("motorcycle_timestamp")
    ) \
    .withWatermark("motorcycle_timestamp", "10 minutes")

weather_data_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", WEATHER_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr(
        "from_json(value, 'STRUCT<id STRING, location_id STRING, temperature DOUBLE, humidity DOUBLE, condition STRING, wind_speed DOUBLE, timestamp DOUBLE>') AS data"
    ) \
    .select(
        col("data.id").alias("weather_id"),
        col("data.temperature").alias("weather_temperature"),
        col("data.humidity"),
        col("data.condition"),
        col("data.wind_speed"),
        col("data.location_id"),
        to_timestamp(from_unixtime(col("data.timestamp"))).alias("weather_timestamp")
    ) \
    .withWatermark("weather_timestamp", "10 minutes")

# Join and Enrich Data
enriched_df = motorcycle_data_df.alias("motorcycle") \
    .join(
        location_data_df.alias("location"),
        [
            motorcycle_data_df.location_id == location_data_df.location_id,
            (motorcycle_data_df.motorcycle_timestamp >= location_data_df.location_timestamp - expr("INTERVAL 2 MINUTES")) &
            (motorcycle_data_df.motorcycle_timestamp <= location_data_df.location_timestamp + expr("INTERVAL 2 MINUTES"))
        ],
        "inner"
    ) \
    .join(
        weather_data_df.alias("weather"),
        [
            motorcycle_data_df.location_id == weather_data_df.location_id,
            (motorcycle_data_df.motorcycle_timestamp >= weather_data_df.weather_timestamp - expr("INTERVAL 2 MINUTES")) &
            (motorcycle_data_df.motorcycle_timestamp <= weather_data_df.weather_timestamp + expr("INTERVAL 2 MINUTES"))
        ],
        "inner"
    ) \
    .select(
        "motorcycle.*",
        "location.latitude",
        "location.longitude",
        "weather.weather_temperature",
        "weather.humidity",
        "weather.condition",
        "weather.wind_speed"
    ) \
    .withColumn("high_speed_alert", col("speed") > 100) \
    .withColumn("high_temperature_alert", col("motorcycle_temperature") > 40) \
    .withColumn(
        "extreme_weather_alert",
        (col("weather_temperature") < 0) | (col("weather_temperature") > 35)
    ) \
    .withColumn("bad_weather_alert", col("condition").isin("Rainy", "Snowy"))

# Write to Flask via foreachBatch
def update_data(batch_df, batch_id):
    """Push processed data to the Flask API"""
    global data_queue
    batch = batch_df.toPandas()
    for record in batch.to_dict(orient="records"):
        data_queue.put(record)

query = enriched_df.writeStream \
    .foreachBatch(update_data) \
    .outputMode("append") \
    .start()

# Run Flask App
if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=5000, debug=False)).start()
    query.awaitTermination()
