from pyspark.sql import SparkSession

from src.config.settings import Settings


class SparkSessionManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def create(self) -> SparkSession:
        return (
            SparkSession.builder
            .appName(self.settings.app_name)
            .master(self.settings.spark_master)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
