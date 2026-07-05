from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class DataHandler:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    @staticmethod
    def pedidos_schema() -> StructType:
        return StructType(
            [
                StructField("ID_PEDIDO", StringType(), nullable=False),
                StructField("PRODUTO", StringType(), nullable=True),
                StructField("VALOR_UNITARIO", DoubleType(), nullable=True),
                StructField("QUANTIDADE", IntegerType(), nullable=True),
                StructField("DATA_CRIACAO", TimestampType(), nullable=True),
                StructField("UF", StringType(), nullable=True),
                StructField("ID_CLIENTE", IntegerType(), nullable=True),
            ]
        )

    @staticmethod
    def pagamentos_schema() -> StructType:
        return StructType(
            [
                StructField("id_pedido", StringType(), nullable=False),
                StructField("forma_pagamento", StringType(), nullable=True),
                StructField("valor_pagamento", DoubleType(), nullable=True),
                StructField("status", BooleanType(), nullable=True),
                StructField("data_processamento", StringType(), nullable=True),
                StructField(
                    "avaliacao_fraude",
                    StructType(
                        [
                            StructField("fraude", BooleanType(), nullable=True),
                            StructField("score", DoubleType(), nullable=True),
                        ]
                    ),
                    nullable=True,
                ),
            ]
        )

    def read_pedidos(self, input_path: str) -> DataFrame:
        return (
            self.spark.read
            .option("header", "true")
            .option("sep", ";")
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
            .schema(self.pedidos_schema())
            .csv(input_path)
        )

    def read_pagamentos(self, input_path: str) -> DataFrame:
        return (
            self.spark.read
            .schema(self.pagamentos_schema())
            .json(input_path)
        )

    def write_parquet(self, dataframe: DataFrame, output_path: str) -> None:
        (
            dataframe.write
            .mode("overwrite")
            .parquet(output_path)
        )
