"""Конфігурація SparkSession для домашнього завдання."""

import os
import sys

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

APP_NAME = "goit-de-hw-06"
MASTER = "local[*]"

# Версія spark-sql-kafka connector має відповідати версії pyspark
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

SPARK_CONFIG = {
    "spark.driver.host": "127.0.0.1",
    "spark.driver.bindAddress": "127.0.0.1",
    "spark.sql.shuffle.partitions": "2",
    "spark.jars.packages": KAFKA_PACKAGE,
    "spark.sql.streaming.schemaInference": "false",
}

ALERTS_CONDITIONS_PATH = "./data/alerts_conditions.csv"


def create_spark_session() -> SparkSession:
    """Створює та повертає SparkSession на основі заданої конфігурації."""
    builder = SparkSession.builder.appName(APP_NAME).master(MASTER)
    for key, value in SPARK_CONFIG.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
