from kafka import KafkaConsumer

from configs import kafka_config, TEMPERATURE_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC
from serializers import JSONDeserializer

# Створення Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=JSONDeserializer(),
    auto_offset_reset="latest",  # Зчитування лише нових повідомлень
    enable_auto_commit=True,  # Автоматичне підтвердження зчитаних повідомлень
    group_id="alerts_display_group_v2",  # Ідентифікатор групи споживачів
)

# Підписка на топіки зі сповіщеннями
consumer.subscribe([TEMPERATURE_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC])

print(f"Subscribed to topics '{TEMPERATURE_ALERTS_TOPIC}' and '{HUMIDITY_ALERTS_TOPIC}'")

# Обробка повідомлень з топіків
try:
    for message in consumer:
        print(f"[{message.topic}] Alert: {message.value}")
except KeyboardInterrupt:
    print("Stopping alert consumer...")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Закриття consumer
