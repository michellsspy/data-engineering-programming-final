# Data Engineering Programming - Projeto Final PySpark

Projeto final da disciplina **Data Engineering Programming**, desenvolvido com **Python**, **PySpark** e **orientação a objetos**.

## Objetivo

Gerar um relatório com os pedidos de venda realizados em **2025** que atendam simultaneamente aos seguintes critérios:

- pagamento recusado (`status = false`);
- avaliação de fraude classificada como legítima (`avaliacao_fraude.fraude = false`).

O resultado final é gravado em formato **Parquet**.

## Campos do relatório final

O relatório gerado contém os seguintes campos:

| Campo | Descrição |
|---|---|
| `id_pedido` | Identificador do pedido |
| `uf_pedido` | UF do pedido |
| `forma_pagamento` | Forma de pagamento |
| `valor_total_pedido` | Valor total do pedido |
| `data_pedido` | Data de criação do pedido |

## Regra de negócio

A regra aplicada no pipeline é:

1. Ler os dados de pedidos em CSV compactado (`.csv.gz`);
2. Ler os dados de pagamentos em JSON compactado (`.json.gz`);
3. Aplicar schemas explícitos na leitura dos DataFrames;
4. Filtrar pedidos com `DATA_CRIACAO` no ano de 2025;
5. Calcular o valor total do pedido por meio de `VALOR_UNITARIO * QUANTIDADE`;
6. Filtrar pagamentos com:
   - `status = false`;
   - `avaliacao_fraude.fraude = false`;
7. Realizar o join entre pedidos e pagamentos por `id_pedido`;
8. Selecionar os campos finais do relatório;
9. Ordenar por UF, forma de pagamento e data do pedido;
10. Gravar o resultado em Parquet.

## Estrutura do projeto

```text
.
├── config/
│   └── settings.yaml
├── data/
│   └── input/
│       ├── dataset-json-pagamentos/
│       └── datasets-csv-pedidos/
├── output/
│   └── relatorio_pedidos_recusados_legitimos/
├── src/
│   ├── main.py
│   ├── config/
│   │   └── settings.py
│   ├── session/
│   │   └── spark_session.py
│   ├── io_utils/
│   │   └── data_handler.py
│   ├── processing/
│   │   └── transformations.py
│   └── pipeline/
│       └── pipeline.py
├── tests/
│   └── unit/
│       └── test_transformations.py
├── requirements.txt
├── pyproject.toml
├── MANIFEST.in
└── README.md
````

## Principais componentes

### `src/main.py`

Ponto de entrada da aplicação e Aggregation Root do projeto.
Responsável por instanciar configurações, SparkSession, DataHandler, Transformations e Pipeline.

### `src/config/settings.py`

Classe responsável por carregar as configurações definidas em `config/settings.yaml`.

### `src/session/spark_session.py`

Classe responsável pela criação da SparkSession.

### `src/io_utils/data_handler.py`

Classe responsável pela leitura dos datasets com schemas explícitos e pela escrita do resultado em Parquet.

### `src/processing/transformations.py`

Classe responsável pela aplicação das regras de negócio e construção do relatório final.

### `src/pipeline/pipeline.py`

Classe responsável pela orquestração do fluxo de leitura, transformação e escrita.

## Requisitos

* Python 3.10 ou superior;
* Java 17;
* PySpark;
* PyYAML;
* pytest.

## Instalação

Crie e ative um ambiente virtual:

```bash
python -m venv venv
source venv/bin/activate
```

Instale as dependências:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Execução do pipeline

Execute o projeto pela raiz do repositório:

```bash
python -m src.main
```

Saída esperada:

```text
Pipeline executado com sucesso.
Registros gerados no relatório: 1473
Caminho de saída: output/relatorio_pedidos_recusados_legitimos
```

## Execução dos testes

Execute:

```bash
pytest
```

Saída esperada:

```text
1 passed
```

## Resultado

O relatório final é salvo em:

```text
output/relatorio_pedidos_recusados_legitimos
```

Formato de saída:

```text
Parquet
```

Quantidade de registros gerados:

```text
1473
```

## Observações

Este projeto utiliza:

* programação orientada a objetos;
* injeção de dependências;
* schemas explícitos em todos os DataFrames de entrada;
* pipeline PySpark executável;
* teste unitário com pytest;
* empacotamento com `pyproject.toml` e `MANIFEST.in`.
  EOF

````


