from src.config.settings import Settings
from src.io_utils.data_handler import DataHandler
from src.pipeline.pipeline import PedidosPagamentosPipeline
from src.processing.transformations import PedidosPagamentosTransformations
from src.session.spark_session import SparkSessionManager


def main() -> None:
    settings = Settings.from_yaml()

    spark = SparkSessionManager(settings).create()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        data_handler = DataHandler(spark)
        transformations = PedidosPagamentosTransformations()

        pipeline = PedidosPagamentosPipeline(
            settings=settings,
            data_handler=data_handler,
            transformations=transformations,
        )

        report_df = pipeline.run()
        total_records = report_df.count()

        print("Pipeline executado com sucesso.")
        print(f"Registros gerados no relatório: {total_records}")
        print(f"Caminho de saída: {settings.output_report_path}")

        report_df.show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
