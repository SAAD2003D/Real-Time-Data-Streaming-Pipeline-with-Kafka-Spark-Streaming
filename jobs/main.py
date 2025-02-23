import time
import random
import uuid
import json
from confluent_kafka import Producer

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
MOTORCYCLE_TOPIC = "motorcycle_data"
WEATHER_TOPIC = "weather_data"
LOCATION_TOPIC = "location_data"

# Starting location (e.g., London)
current_location = {"latitude": 51.5074, "longitude": -0.1278}

# Increment values for location progression
LATITUDE_INCREMENT = 0.001  # Simulates northward movement per step
LONGITUDE_INCREMENT = 0.001  # Simulates eastward movement per step

# Kafka Producer
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def simulate_location_progress():
    """
    Simulates progressive movement of the vehicle by incrementing latitude and longitude.
    """
    global current_location
    current_location["latitude"] += LATITUDE_INCREMENT + random.uniform(-0.0001, 0.0001)
    current_location["longitude"] += LONGITUDE_INCREMENT + random.uniform(-0.0001, 0.0001)
    return current_location


def generate_location_data():
    """
    Generates a location data payload.
    """
    location = simulate_location_progress()
    return {
        "id": str(uuid.uuid4()),
        "latitude": round(location["latitude"], 5),
        "longitude": round(location["longitude"], 5),
        "timestamp": time.time()
    }


def generate_motorcycle_data(location_id, timestamp):
    """
    Generates motorcycle data with a reference to location_id.
    """
    return {
        "id": str(uuid.uuid4()),
        "speed": random.uniform(0, 120),
        "temperature": random.uniform(20, 35),
        "fuel_level": random.uniform(0, 100),
        "engine_status": random.choice(["ON", "OFF"]),
        "location_id": location_id,
        "timestamp": timestamp
    }


def generate_weather_data(location_id, timestamp):
    """
    Generates weather data with a reference to location_id.
    """
    return {
        "id": str(uuid.uuid4()),
        "location_id": location_id,
        "temperature": random.uniform(-10, 35),
        "humidity": random.uniform(20, 80),
        "condition": random.choice(["Sunny", "Cloudy", "Rainy", "Snowy"]),
        "wind_speed": random.uniform(0, 20),
        "timestamp": timestamp
    }


def delivery_report(err, msg):
    """
    Kafka delivery report callback. Logs message delivery status.
    """
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_data():
    """
    Continuously generates and sends location, motorcycle, and weather data to Kafka topics.
    """
    while True:
        # Generate location data
        location_data = generate_location_data()

        # Generate dependent data using location_id
        motorcycle_data = generate_motorcycle_data(location_data["id"], location_data["timestamp"])
        weather_data = generate_weather_data(location_data["id"], location_data["timestamp"])

        # Send location data
        producer.produce(
            LOCATION_TOPIC,
            key=location_data["id"],
            value=json.dumps(location_data).encode("utf-8"),
            callback=delivery_report
        )

        # Send motorcycle data
        producer.produce(
            MOTORCYCLE_TOPIC,
            key=motorcycle_data["id"],
            value=json.dumps(motorcycle_data).encode("utf-8"),
            callback=delivery_report
        )

        # Send weather data
        producer.produce(
            WEATHER_TOPIC,
            key=weather_data["id"],
            value=json.dumps(weather_data).encode("utf-8"),
            callback=delivery_report
        )

        # Trigger the delivery callback
        producer.poll(0)

        # Log the data locally (optional, for debugging)
        print("Produced location data:", json.dumps(location_data, indent=2))
        print("Produced motorcycle data:", json.dumps(motorcycle_data, indent=2))
        print("Produced weather data:", json.dumps(weather_data, indent=2))

        # Simulate data generation every second
        time.sleep(1)


if __name__ == "__main__":
    try:
        print("Starting data simulation...")
        produce_data()
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        producer.flush()
