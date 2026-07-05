from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.processing.transformations import PedidosPagamentosTransformations


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .appName("pytest-data-engineering-programming")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    spark_session.stop()


def test_build_report_returns_only_refused_legitimate_payments_from_2025(spark):
    pedidos_schema = StructType(
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

    pagamentos_schema = StructType(
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

    pedidos_data = [
        (
            "pedido-001",
            "MONITOR",
            600.0,
            2,
            datetime(2025, 5, 10, 12, 0, 0),
            "SP",
            1001,
        ),
        (
            "pedido-002",
            "MOUSE",
            50.0,
            1,
            datetime(2025, 5, 10, 12, 0, 0),
            "RJ",
            1002,
        ),
        (
            "pedido-003",
            "TECLADO",
            120.0,
            1,
            datetime(2024, 5, 10, 12, 0, 0),
            "MG",
            1003,
        ),
    ]

    pagamentos_data = [
        (
            "pedido-001",
            "PIX",
            1200.0,
            False,
            "2025-05-10T12:30:00",
            {"fraude": False, "score": 0.20},
        ),
        (
            "pedido-002",
            "BOLETO",
            50.0,
            False,
            "2025-05-10T12:30:00",
            {"fraude": True, "score": 0.95},
        ),
        (
            "pedido-003",
            "CARTAO_CREDITO",
            120.0,
            False,
            "2024-05-10T12:30:00",
            {"fraude": False, "score": 0.10},
        ),
    ]

    pedidos_df = spark.createDataFrame(pedidos_data, schema=pedidos_schema)
    pagamentos_df = spark.createDataFrame(pagamentos_data, schema=pagamentos_schema)

    transformations = PedidosPagamentosTransformations()

    result_df = transformations.build_report(
        pedidos_df=pedidos_df,
        pagamentos_df=pagamentos_df,
        year=2025,
    )

    result = result_df.collect()

    assert len(result) == 1
    assert result[0]["id_pedido"] == "pedido-001"
    assert result[0]["uf_pedido"] == "SP"
    assert result[0]["forma_pagamento"] == "PIX"
    assert result[0]["valor_total_pedido"] == 1200.0
