import random
import time

from kafka import KafkaProducer

from configs import kafka_config, BUILDING_SENSORS_TOPIC
from serializers import JSONSerializer, StringSerializer

# Створення Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=JSONSerializer(),
    key_serializer=StringSerializer(),
)

# Один запуск скрипту імітує один датчик з постійним ID на час роботи
sensor_id = random.randint(1, 1000)

print(f"Starting sensor {sensor_id}, sending data to topic '{BUILDING_SENSORS_TOPIC}'")

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": time.time(),
            "temperature": random.randint(25, 45),
            "humidity": random.randint(15, 85),
        }
        producer.send(BUILDING_SENSORS_TOPIC, key=sensor_id, value=data)
        producer.flush()
        print(f"Sensor {sensor_id} sent data: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Stopping sensor producer...")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.close()
