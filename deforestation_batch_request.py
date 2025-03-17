"""
Este módulo contém a implementação da classe `DeforestationBatchRequestProcessor`,
que integra a funcionalidade de requisições à API com o processamento de arquivos CSV ou Excel.
A classe permite atualizar os dados conforme os códigos de imóvel (CAR) e os intervalos de
anos fornecidos.

Classes:
    DeforestationBatchRequestProcessor: Classe que herda de `APIClient` e `CSVProcessor`
    para realizar requisições à API e processar arquivos de entrada, atualizando os dados
    conforme os códigos de imóvel (CAR) e os intervalos de anos.

Exemplo de utilização:
    FILE_PATH = "input/Tropoc_Geo_2024_v1.xls"
    OUTPUT_FILE = "output/Tropoc_Geo_2024_v1_updated.xlsx"
    CAR_COLUMN = "CAR"
    YEAR_RANGES = [2004, 2023]
    processador = DeforestationBatchRequestProcessor(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=FILE_PATH,
        output_file=OUTPUT_FILE,
        car_column=CAR_COLUMN,
        year_ranges=YEAR_RANGES,
    )
    processador.processar()
"""

import os

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore
from tools.tools import APIClient, CSVProcessor

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()


class DeforestationBatchRequestProcessor(APIClient, CSVProcessor):
    """
    Classe que integra a funcionalidade de requisições à API com o processamento
    do arquivo, atualizando os dados conforme os códigos de imovel (CAR) e os intervalos de anos.
    """

    def __init__(
        self,
        access_token: str,
        api_url: str,
        file_path: str,
        output_file: str,
        car_column: str,
        year_ranges: list,
    ):
        APIClient.__init__(self, access_token, api_url)
        CSVProcessor.__init__(self, file_path, car_column)
        self.output_file = output_file

        # Se o primeiro elemento for inteiro, converte para lista de listas
        if year_ranges and isinstance(year_ranges[0], int):
            self.year_ranges = [year_ranges]
        else:
            self.year_ranges = year_ranges

        # Inicializa as colunas para armazenar os resultados dinamicamente
        for years in self.year_ranges:
            coluna = "deforestation_" + "_".join(map(str, years))
            self.df[coluna] = None

    def processar(self) -> None:
        """
        Itera sobre os códigos de imovel (CAR) e realiza as requisições para cada conjunto de anos.
        Atualiza o DataFrame e salva o arquivo atualizado.
        """
        total_registros = len(self.df)
        print(f"Iniciando processamento de {total_registros} registros...")

        for index, car_code in self.df[self.car_column].items():
            if pd.isna(car_code):
                print(f"Ignorando registro {index} pois o CAR está vazio.")
                continue  # Pula linhas onde o código é NaN

            # Realiza as requisições para cada conjunto de anos passado como parâmetro
            for years in self.year_ranges:
                payload = {"name": "test", "codImovel": car_code, "yearsBiomas": years}
                try:
                    response = self.enviar_requisicao(payload)
                    response_data = response.json()

                    if response.status_code == 201 and "data" in response_data:
                        record_id = response_data["data"]["id"]
                        coluna = "deforestation_" + "_".join(map(str, years))
                        self.df.at[index, coluna] = record_id
                        print(
                            f"Registro {index} (CAR: {car_code}) processado com "
                            f"sucesso para anos {years}."
                        )
                    else:
                        print(
                            f"Falha para CAR {car_code} com anos {years}: {response_data}"
                        )
                except requests.exceptions.RequestException as e:
                    print(
                        f"Erro de requisição processando CAR {car_code} com anos {years}: {e}"
                    )
                except ValueError as e:
                    print(
                        f"Erro de valor processando CAR {car_code} com anos {years}: {e}"
                    )
                except KeyError as e:
                    print(
                        f"Erro de chave processando CAR {car_code} com anos {years}: {e}"
                    )

        self.salvar_dados(self.output_file)


# Exemplo de utilização:
if __name__ == "__main__":
    #   NÃO MODIFICAR
    API_URL = f"{os.getenv('API_BASE_URL')}/deforestation"
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if ACCESS_TOKEN is None:
        raise ValueError("ACCESS_TOKEN environment variable not set")

    #   MODIFICAR
    FILE_PATH = "input/Monteccer_2024_CAR.csv"  # Caminho do arquivo com os CODIMOVEL's pode ser CSV ou Excel
    OUTPUT_FILE = "output/Tropoc_Geo_2024_v1_updated.xlsx"  # Caminho do arquivo de saída
    CAR_COLUMN = "CAR"  # Nome da Coluna que contém o código do imóvel
    YEAR_RANGES = [2004, 2023] # Intervalo de anos para processamento
    # Parâmetro dinâmico: pode ser um único intervalo ou uma lista de intervalos.
    # Exemplo de um único intervalo:
    # YEAR_RANGES = [2004, 2023]
    # Exemplo de múltiplos intervalos:
    # YEAR_RANGES = [[2004, 2023], [2010, 2015]]

    # NÃO MODIFICAR
    processor = DeforestationBatchRequestProcessor(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=FILE_PATH,
        output_file=OUTPUT_FILE,
        car_column=CAR_COLUMN,
        year_ranges=YEAR_RANGES,
    )
    processor.processar()
    print(f"Arquivo atualizado salvo em {OUTPUT_FILE}")
