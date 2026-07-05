from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class PedidosPagamentosTransformations:
    def filter_pedidos_by_year(self, pedidos_df: DataFrame, year: int) -> DataFrame:
        start_date = f"{year}-01-01"
        end_date = f"{year + 1}-01-01"

        return pedidos_df.filter(
            (F.col("DATA_CRIACAO") >= F.lit(start_date).cast("timestamp"))
            & (F.col("DATA_CRIACAO") < F.lit(end_date).cast("timestamp"))
        )

    def calculate_order_totals(self, pedidos_df: DataFrame) -> DataFrame:
        return (
            pedidos_df
            .withColumn(
                "valor_item",
                F.col("VALOR_UNITARIO") * F.col("QUANTIDADE")
            )
            .groupBy(
                F.col("ID_PEDIDO").alias("id_pedido"),
                F.col("UF").alias("uf_pedido"),
                F.col("DATA_CRIACAO").alias("data_pedido")
            )
            .agg(
                F.round(F.sum("valor_item"), 2).alias("valor_total_pedido")
            )
        )

    def filter_pagamentos_recusados_legitimos(self, pagamentos_df: DataFrame) -> DataFrame:
        return (
            pagamentos_df
            .filter(F.col("status") == F.lit(False))
            .filter(F.col("avaliacao_fraude.fraude") == F.lit(False))
            .select(
                F.col("id_pedido"),
                F.col("forma_pagamento")
            )
        )

    def build_report(
        self,
        pedidos_df: DataFrame,
        pagamentos_df: DataFrame,
        year: int
    ) -> DataFrame:
        pedidos_ano_df = self.filter_pedidos_by_year(pedidos_df, year)
        pedidos_totais_df = self.calculate_order_totals(pedidos_ano_df)
        pagamentos_filtrados_df = self.filter_pagamentos_recusados_legitimos(pagamentos_df)

        return (
            pedidos_totais_df
            .join(pagamentos_filtrados_df, on="id_pedido", how="inner")
            .select(
                "id_pedido",
                "uf_pedido",
                "forma_pagamento",
                "valor_total_pedido",
                "data_pedido"
            )
            .orderBy(
                "uf_pedido",
                "forma_pagamento",
                "data_pedido"
            )
        )
