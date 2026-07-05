from pyspark.sql import DataFrame

from src.config.settings import Settings
from src.io_utils.data_handler import DataHandler
from src.processing.transformations import PedidosPagamentosTransformations


class PedidosPagamentosPipeline:
    def __init__(
        self,
        settings: Settings,
        data_handler: DataHandler,
        transformations: PedidosPagamentosTransformations,
    ) -> None:
        self.settings = settings
        self.data_handler = data_handler
        self.transformations = transformations

    def run(self) -> DataFrame:
        pedidos_df = self.data_handler.read_pedidos(
            self.settings.pedidos_input_path
        )

        pagamentos_df = self.data_handler.read_pagamentos(
            self.settings.pagamentos_input_path
        )

        report_df = self.transformations.build_report(
            pedidos_df=pedidos_df,
            pagamentos_df=pagamentos_df,
            year=self.settings.report_year,
        )

        self.data_handler.write_parquet(
            dataframe=report_df,
            output_path=self.settings.output_report_path,
        )

        return report_df
