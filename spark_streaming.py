"""Spark Structured Streaming застосунок для пошуку алертів сенсорів.

Читає потік даних сенсорів з Kafka, обчислює середні значення температури та
вологості у sliding window (1 хвилина, крок 30 секунд, watermark 10 секунд),
порівнює їх з умовами з alerts_conditions.csv та записує алерти у Kafka.
"""

from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    from_json,
    lit,
    struct,
    to_json,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import DoubleType, LongType, StructField, StructType

from configs import ALERTS_TOPIC, BUILDING_SENSORS_TOPIC, kafka_config
from spark_config import ALERTS_CONDITIONS_PATH, create_spark_session

SENSOR_SCHEMA = StructType(
    [
        StructField("sensor_id", LongType(), True),
        StructField("timestamp", DoubleType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
    ]
)

WINDOW_DURATION = "1 minute"
SLIDE_DURATION = "30 seconds"
WATERMARK_DURATION = "10 seconds"


def build_jaas_config() -> str:
    """Формує JAAS-рядок для SASL/PLAIN автентифікації Kafka."""
    return (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";'
    )


def read_sensor_stream(spark):
    """Читає потік даних сенсорів з Kafka та розпарсює JSON у колонки."""
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", build_jaas_config())
        .option("subscribe", BUILDING_SENSORS_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    return raw_stream.select(
        from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("data")
    ).select(
        col("data.sensor_id").alias("sensor_id"),
        to_timestamp(col("data.timestamp")).alias("timestamp"),
        col("data.temperature").alias("temperature"),
        col("data.humidity").alias("humidity"),
    )


def aggregate_sensor_data(parsed_stream):
    """Рахує середню температуру та вологість у sliding window з watermark."""
    return (
        parsed_stream.withWatermark("timestamp", WATERMARK_DURATION)
        .groupBy(window(col("timestamp"), WINDOW_DURATION, SLIDE_DURATION))
        .agg(avg("temperature").alias("t_avg"), avg("humidity").alias("h_avg"))
    )


def read_alert_conditions(spark):
    """Зчитує статичний довідник умов алертів із csv-файлу."""
    return spark.read.csv(ALERTS_CONDITIONS_PATH, header=True, inferSchema=True)


def build_alerts(aggregated_stream, conditions):
    """Об'єднує агрегати з умовами (cross join) та фільтрує спрацьовані алерти."""
    joined = aggregated_stream.crossJoin(conditions)

    temperature_triggered = (col("temperature_min") == -999) | (col("temperature_max") == -999) | (
        (col("t_avg") >= col("temperature_min")) & (col("t_avg") <= col("temperature_max"))
    )
    humidity_triggered = (col("humidity_min") == -999) | (col("humidity_max") == -999) | (
        (col("h_avg") >= col("humidity_min")) & (col("h_avg") <= col("humidity_max"))
    )

    # Умова застосовується лише до того показника, для якого межі задані (не -999)
    temperature_applies = (col("temperature_min") != -999) & (col("temperature_max") != -999)
    humidity_applies = (col("humidity_min") != -999) & (col("humidity_max") != -999)

    matches = when(temperature_applies, temperature_triggered).otherwise(lit(True)) & when(
        humidity_applies, humidity_triggered
    ).otherwise(lit(True))

    alerts = joined.filter((temperature_applies | humidity_applies) & matches)

    return alerts.select(
        col("window").alias("window"),
        col("t_avg"),
        col("h_avg"),
        col("code").cast("string").alias("code"),
        col("message"),
        current_timestamp().alias("timestamp"),
    )


def write_alerts_to_kafka(alerts_stream):
    """Серіалізує алерти у JSON та пише їх у вихідний Kafka-топік."""
    value_column = to_json(
        struct(
            struct(
                col("window.start").alias("start"),
                col("window.end").alias("end"),
            ).alias("window"),
            col("t_avg"),
            col("h_avg"),
            col("code"),
            col("message"),
            col("timestamp"),
        )
    ).alias("value")

    return (
        alerts_stream.select(value_column)
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option("kafka.sasl.jaas.config", build_jaas_config())
        .option("topic", ALERTS_TOPIC)
        .option("checkpointLocation", "./checkpoints/alerts")
        .outputMode("update")
        .trigger(processingTime=SLIDE_DURATION)
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    parsed_stream = read_sensor_stream(spark)
    aggregated_stream = aggregate_sensor_data(parsed_stream)
    conditions = read_alert_conditions(spark)
    alerts_stream = build_alerts(aggregated_stream, conditions)

    query = write_alerts_to_kafka(alerts_stream)
    query.awaitTermination()


if __name__ == "__main__":
    main()
