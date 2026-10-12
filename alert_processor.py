from kafka import KafkaConsumer, KafkaProducer

from configs import (
    kafka_config,
    BUILDING_SENSORS_TOPIC,
    TEMPERATURE_ALERTS_TOPIC,
    HUMIDITY_ALERTS_TOPIC,
)
from serializers import JSONDeserializer, JSONSerializer, StringSerializer

TEMPERATURE_THRESHOLD = 40
HUMIDITY_LOWER_THRESHOLD = 20
HUMIDITY_UPPER_THRESHOLD = 80

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=JSONDeserializer(),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="alert_processor_group_v2",
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=JSONSerializer(),
    key_serializer=StringSerializer(),
)

consumer.subscribe([BUILDING_SENSORS_TOPIC])

print(f"Subscribed to topic '{BUILDING_SENSORS_TOPIC}', filtering sensor data...")

try:
    for message in consumer:
        data = message.value
        sensor_id = data["sensor_id"]
        measured_at = data["timestamp"]
        temperature = data["temperature"]
        humidity = data["humidity"]
        print(f"Received data from sensor {sensor_id}: {data}")

        temperature_exceeded = temperature > TEMPERATURE_THRESHOLD
        humidity_out_of_range = humidity > HUMIDITY_UPPER_THRESHOLD or humidity < HUMIDITY_LOWER_THRESHOLD
        print(
            f"Filter result -> temperature exceeded: {temperature_exceeded}, "
            f"humidity out of range: {humidity_out_of_range}"
        )

        if temperature_exceeded:
            alert = {
                "sensor_id": sensor_id,
                "temperature": temperature,
                "timestamp": measured_at,
                "message": f"Temperature {temperature}C exceeds threshold {TEMPERATURE_THRESHOLD}C",
            }
            producer.send(TEMPERATURE_ALERTS_TOPIC, key=sensor_id, value=alert)
            producer.flush()
            print(f"Sent to topic '{TEMPERATURE_ALERTS_TOPIC}': {alert}")

        if humidity_out_of_range:
            alert = {
                "sensor_id": sensor_id,
                "humidity": humidity,
                "timestamp": measured_at,
                "message": f"Humidity {humidity}% is out of range "
                f"({HUMIDITY_LOWER_THRESHOLD}-{HUMIDITY_UPPER_THRESHOLD}%)",
            }
            producer.send(HUMIDITY_ALERTS_TOPIC, key=sensor_id, value=alert)
            producer.flush()
            print(f"Sent to topic '{HUMIDITY_ALERTS_TOPIC}': {alert}")
except KeyboardInterrupt:
    print("Stopping alert processor...")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()
    producer.close()
