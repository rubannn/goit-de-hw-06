from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import (
    kafka_config,
    my_name,
    BUILDING_SENSORS_TOPIC,
    TEMPERATURE_ALERTS_TOPIC,
    HUMIDITY_ALERTS_TOPIC,
    ALERTS_TOPIC,
)

# Створення клієнта Kafka
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
)

# Топіки, які необхідно створити для системи моніторингу IoT
num_partitions = 2
replication_factor = 1

new_topics = [
    NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    for topic_name in (
        BUILDING_SENSORS_TOPIC,
        TEMPERATURE_ALERTS_TOPIC,
        HUMIDITY_ALERTS_TOPIC,
        ALERTS_TOPIC,
    )
]

# Створення топіків
for topic in new_topics:
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic.name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic.name}' already exists.")
    except Exception as e:
        print(f"An error occurred while creating topic '{topic.name}': {e}")

# Перевіряємо список існуючих топіків користувача
print([topic for topic in admin_client.list_topics() if my_name in topic])

# Закриття зв'язку з клієнтом
admin_client.close()
