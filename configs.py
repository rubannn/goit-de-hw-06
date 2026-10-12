import os

from dotenv import load_dotenv

load_dotenv()

kafka_config = {
    "bootstrap_servers": [os.environ["KAFKA_BOOTSTRAP_SERVERS"]],
    "username": os.environ["KAFKA_USERNAME"],
    "password": os.environ["KAFKA_PASSWORD"],
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Ідентифікатор, що додається до імен топіків, щоб вони не дублювалися з іншими студентами
my_name = os.environ.get("SENSOR_OWNER_NAME", "mruban")

BUILDING_SENSORS_TOPIC = f"{my_name}_building_sensors"
TEMPERATURE_ALERTS_TOPIC = f"{my_name}_temperature_alerts"
HUMIDITY_ALERTS_TOPIC = f"{my_name}_humidity_alerts"
ALERTS_TOPIC = f"{my_name}_alerts"
