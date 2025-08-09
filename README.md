
# Real-Time-Data-Streaming-Pipeline-with-Kafka-Spark-Streaming
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)


![image](https://github.com/user-attachments/assets/1fdb1065-9953-4d80-a7c3-665dcd727e47)


This project demonstrates a real-time data streaming pipeline using Kafka for message queuing, Spark Streaming for data processing, and Flask for API serving.  It simulates a motorcycle telemetry system, processing location, motorcycle performance, and weather data to generate real-time insights and alerts.

## Architecture

The system is composed of the following key components, orchestrated using Docker Compose:

*   **Kafka (Broker):**  A distributed streaming platform used for publishing and subscribing to streams of records.
*   **Zookeeper:** Manages and coordinates the Kafka broker.
*   **Spark Master:**  The coordinator node of the Spark cluster.
*   **Spark Workers:**  Worker nodes in the Spark cluster responsible for executing data processing tasks.
*   **Data Producer (Python):**  A Python application that generates simulated data (location, motorcycle, weather) and publishes it to Kafka topics.
*   **Data Consumer (Spark Streaming & Flask):** A Spark Streaming application that consumes data from Kafka, performs real-time processing and enrichment, and exposes the results through a Flask API.

## Components details :

-The data producer simulates generating data for 3 topics location,motorcycle, weather with main.py file.
-The data consumer consumes the generated data from the data producer and makes data processing and provides a flask api.

## Data Flow

1.  The `data-producer` generates simulated motorcycle, location, and weather data.
2.  The data is published to separate Kafka topics: `location_data`, `motorcycle_data`, and `weather_data`.
3.  The `data-consumer` (Spark Streaming application) subscribes to these Kafka topics.
4.  Spark Streaming processes the data, joining data from different topics based on location and timestamp.
5.  Enriched data, including calculated alerts (speed, temperature, weather conditions), is streamed to the Flask application
6.  The Flask API serves this processed data as JSON.

## Prerequisites

*   Docker
*   Docker Compose

## Setup Instructions

1.  **Clone the Repository:**

    ```bash
    git clone https://github.com/SAAD2003D/Real-Time-Data-Streaming-Pipeline-with-Kafka-Spark-Streaming
    cd https://github.com/SAAD2003D/Real-Time-Data-Streaming-Pipeline-with-Kafka-Spark-Streaming
    ```

2.  **Create topics**

    ```bash
    docker exec -it broker kafka-topics --create --topic location_data --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
    docker exec -it broker kafka-topics --create --topic motorcycle_data --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
    docker exec -it broker kafka-topics --create --topic weather_data --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
    ```

3.  **Build and Start the Services:**

    ```bash
    docker-compose up --build
    ```

    This command will:

    *   Build the Docker images (if necessary).
    *   Start all the services defined in the `docker-compose.yml` file in detached mode (`-d`).

4.  **Access the Applications:**

    *   **Flask API:**  `http://localhost:5000/data` (Provides the processed data in JSON format)
    *   **Spark Master UI:** `http://localhost:9090` (Spark UI to monitor jobs.)

## Configuration

*   **Kafka Bootstrap Servers:** Configured in both `main.py` (producer) and `consumer.py` (consumer) as `broker:29092`.
*   **Kafka Topics:** `location_data`, `motorcycle_data`, and `weather_data`.
*   **Data Producer:** The `main.py` script simulates data generation.  You can modify the simulation parameters (e.g., speed range, temperature range, location increments) within this file.
*   **Data Consumer:** The `consumer.py` script defines the Spark Streaming application.  You can adjust the processing logic, join conditions, and alert thresholds within this file.
*   **Flask API:** The Flask API is configured to run on port 5000.  You can change this in the `consumer.py` file if needed.

## Key Files

*   **`docker-compose.yml`:** Defines the services, networks, and volumes for the application.
*   **`jobs/main.py`:** Python script for producing simulated data to Kafka topics.
*   **`jobs/consumer.py`:**  Spark Streaming application that consumes data from Kafka, processes it, and exposes it via a Flask API.
*   **`Dockerfile`:** Defines the environment for the data consumer.
*   **`requirements.txt`:**  Lists the Python dependencies for the producer application.

## Customization

*   **Data Simulation:**  Modify the `main.py` script to adjust the simulation parameters for data generation (e.g., location movement, speed, temperature ranges, weather conditions).
*   **Data Processing:**  Customize the Spark Streaming application in `consumer.py` to implement different data transformations, aggregations, and alert logic. You can change the join conditions, add new features, and adjust alert thresholds based on your specific requirements.
*   **Adding more data sources :** The architecture is designed in a way that can support adding more data sources by adding a producer for each new data source and a consumer for the new data source.

## Scalability

*   **Kafka:** Kafka is inherently scalable. You can increase the number of partitions for each topic to improve throughput.
*   **Spark Streaming:**  Spark Streaming can be scaled by adding more worker nodes to the Spark cluster.  The data will be automatically distributed across the workers for parallel processing.
*   **Flask API:**  For production deployments, consider using a production-ready WSGI server (e.g., Gunicorn, uWSGI) and load balancer to handle increased traffic to the Flask API.

## Notes

*   This project is a simplified demonstration.  For production deployments.
* The simulation uses a basic linear location update. Consider a more sophisticated trajectory model for realistic location progression.
