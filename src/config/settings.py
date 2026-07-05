from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    app_name: str
    spark_master: str
    pedidos_input_path: str
    pagamentos_input_path: str
    output_report_path: str
    report_year: int

    @classmethod
    def from_yaml(cls, config_path: str = "config/settings.yaml") -> "Settings":
        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

        with path.open("r", encoding="utf-8") as file:
            config: dict[str, Any] = yaml.safe_load(file)

        return cls(
            app_name=config["app"]["name"],
            spark_master=config["app"]["master"],
            pedidos_input_path=config["paths"]["pedidos_input"],
            pagamentos_input_path=config["paths"]["pagamentos_input"],
            output_report_path=config["paths"]["output_report"],
            report_year=int(config["report"]["year"]),
        )
