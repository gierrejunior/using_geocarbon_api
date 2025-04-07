"""
Este módulo contém a implementação da classe `DeforestationAnalysisProdesBatchRequestProcessor`,
que integra a funcionalidade de requisições à API com o processamento de arquivos CSV ou Excel.
A classe permite atualizar os dados conforme os códigos de imóvel (CAR).

Classes:
    DeforestationAnalysisProdesBatchRequestProcessor: Classe que herda de `APIClient` e `CSVProcessor`
    para realizar requisições à API e processar arquivos de entrada, atualizando os dados
    conforme os códigos de imóvel (CAR).

Exemplo de utilização:
    FILE_PATH = "input/Monteccer_2024_CAR.csv"
    OUTPUT_FILE = "output/Monteccer_2024_CAR_updated.csv"
    CAR_COLUMN = "CAR"
    processador = DeforestationAnalysisProdesBatchRequestProcessor(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=FILE_PATH,
        output_file=OUTPUT_FILE,
        car_column=CAR_COLUMN,
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


class DeforestationAnalysisProdesBatchRequestProcessor(APIClient, CSVProcessor):
    """
    Classe que integra a funcionalidade de requisições à API com o processamento
    do arquivo, atualizando os dados conforme os códigos de imóvel (CAR).
    """

    def __init__(
        self,
        access_token: str,
        api_url: str,
        file_path: str,
        output_file: str,
        car_column: str,
    ):
        APIClient.__init__(self, access_token, api_url)
        CSVProcessor.__init__(self, file_path, car_column)
        self.output_file = output_file

        # Inicializa a coluna para armazenar o resultado da análise PRODES
        self.df["deforestation_prodes"] = None

    def processar(self) -> None:
        """
        Itera sobre os códigos de imóvel (CAR) e realiza as requisições.
        Atualiza o DataFrame e salva o arquivo atualizado.
        """
        total_registros = len(self.df)
        print(f"Iniciando processamento de {total_registros} registros...")

        for index, car_code in self.df[self.car_column].items():
            if pd.isna(car_code):
                print(f"Ignorando registro {index} pois o CAR está vazio.")
                continue  # Pula linhas onde o código é NaN

            car_code = str(car_code).strip()
            payload = {"name": "test", "codImovel": car_code}
            try:
                response = self.enviar_requisicao(payload)
                response_data = response.json()

                if response.status_code == 201 and "data" in response_data:
                    record_id = response_data["data"]["id"]
                    self.df.at[index, "deforestation_prodes"] = record_id
                    print(f"Registro {index} (CAR: {car_code}) processado com sucesso.")
                else:
                    print(f"Falha para CAR {car_code}: {response_data}")
            except requests.exceptions.RequestException as e:
                print(f"Erro de requisição processando CAR {car_code}: {e}")
            except ValueError as e:
                print(f"Erro de valor processando CAR {car_code}: {e}")
            except KeyError as e:
                print(f"Erro de chave processando CAR {car_code}: {e}")

        self.salvar_dados(self.output_file)


# Exemplo de utilização:
if __name__ == "__main__":
    # NÃO MODIFICAR
    API_URL = f"{os.getenv('API_BASE_URL')}/deforestation/prodes"
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if ACCESS_TOKEN is None:
        raise ValueError("ACCESS_TOKEN environment variable not set")

    # MODIFICAR
    FILE_PATH = "input/Tropoc_Geo_2024_v1.xls"  # Caminho do arquivo com os códigos de imóvel (CAR)
    OUTPUT_FILE = "output/Tropoc_Geo_2024_prodes.csv"  # Caminho do arquivo de saída
    CAR_COLUMN = "CAR"  # Nome da coluna que contém o código do imóvel

    # NÃO MODIFICAR
    processor = DeforestationAnalysisProdesBatchRequestProcessor(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=FILE_PATH,
        output_file=OUTPUT_FILE,
        car_column=CAR_COLUMN,
    )
    processor.processar()
    print(f"Arquivo atualizado salvo em {OUTPUT_FILE}")
