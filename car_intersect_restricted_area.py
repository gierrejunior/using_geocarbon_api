"""
Módulo para verificação de interseção de CAR's com áreas restritas via API.

Este módulo define a classe CarIntersectionChecker, que integra a funcionalidade de r
equisições à API com o processamento de arquivos CSV ou Excel. Para cada CAR
(Cadastro Ambiental Rural) presente na coluna escolhida, é realizada uma requisição PATCH para o
endpoint /cars/check-intersection. A requisição envia o seguinte payload:
    {
        "carIdentifier": "<CAR>",
        "force": true
    }
Ao enviar "force": true, o endpoint força o reprocessamento do CAR mesmo que já haja resultados
salvos no banco de dados.  a API retorna os dados existentes, evitando processamento desnecessário.

Os resultados de cada requisição são armazenados e, ao final, são exportados para um arquivo JSON.

Uso:
    1. Certifique-se de que as variáveis de ambiente (como ACCESS_TOKEN e API_BASE_URL) estejam
    definidas em um arquivo .env.
    2. Ajuste os parâmetros do script conforme necessário:
            - FILE_PATH: Caminho para o arquivo de entrada com os CAR's (CSV ou Excel).
            - OUTPUT_FILE: Caminho para salvar o arquivo JSON com os resultados.
            - ID_COLUMN: Nome da coluna que contém os CAR's.
    3. Execute o módulo para processar os dados e salvar os resultados.
"""

import json
import os

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore

from tools.tools import APIClient, CSVProcessor

# Carrega as variáveis de ambiente
load_dotenv()


class CarIntersectionChecker(APIClient, CSVProcessor):
    """
    Classe para verificar a interseção de CAR's com áreas restritas via API.

    Para cada CAR presente no arquivo de entrada, monta a URL para acessar o endpoint
    /cars/check-intersection, realiza uma requisição PATCH enviando o payload
    {"carIdentifier": <CAR>, "force": true} e armazena o resultado.

    Ao enviar "force": true, o endpoint força o reprocessamento do CAR mesmo que já
    haja resultados salvos no banco de dados, a API retorna os dados existentes
    sem reprocessar.

    Ao final, os resultados são salvos em um arquivo JSON.
    """

    def __init__(
        self,
        access_token: str,
        api_base_url: str,
        file_path: str,
        output_file: str,
        id_column: str,
    ):
        # A URL de PATCH é construída a partir da API_BASE_URL e o endpoint fixo
        endpoint = "/cars/check-intersection"
        APIClient.__init__(self, access_token, api_url=f"{api_base_url}{endpoint}")
        CSVProcessor.__init__(self, file_path, id_column)
        self.output_file = output_file
        self.id_column = id_column

    def processar(self) -> None:
        """
        Processa os CAR's do arquivo, realizando uma requisição PATCH para cada um e salvando
        os resultados em um arquivo JSON.

        Para cada CAR, envia o payload:
            {
                "carIdentifier": "<CAR>",
                "force": true
            }
        O parâmetro "force": true força o reprocessamento mesmo se os dados
        já estiverem salvos no banco, garantindo que sempre se obtenha a versão
        mais atualizada.
        """
        resultados: dict[str, list] = {"data": []}
        total_registros = len(self.df)
        print(f"Iniciando processamento de {total_registros} registros...")

        for index, car in self.df[self.id_column].items():
            if pd.isna(car):
                print(f"Ignorando registro {index} pois o CAR está vazio.")
                continue

            payload = {"carIdentifier": str(car), "force": True}
            try:
                response = requests.patch(
                    self.api_url, json=payload, headers=self.headers, timeout=60
                )
                try:
                    response_data = response.json()
                except json.JSONDecodeError:
                    response_data = {"error": "Resposta não é um JSON válido."}

                if response.status_code in [200, 201, 204]:
                    print(f"Registro {index} (CAR: {car}) processado com sucesso.")
                else:
                    print(f"Falha para o CAR {car}: {response_data}")

                resultados["data"].append(
                    {
                        "index": index,
                        "carIdentifier": car,
                        "status_code": response.status_code,
                        "response": response_data.get("data"),
                    }
                )

            except requests.exceptions.RequestException as e:
                print(f"Erro de requisição para o CAR {car}: {e}")
                resultados["data"].append(
                    {
                        "index": index,
                        "carIdentifier": car,
                        "status_code": None,
                        "error": str(e),
                    }
                )

        # Salva os resultados no arquivo JSON de saída
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(resultados, f, ensure_ascii=False, indent=4)
            print(f"Arquivo JSON salvo em {self.output_file}")
        except (IOError, OSError) as e:
            print(f"Erro ao salvar o arquivo JSON: {e}")


if __name__ == "__main__":
    # NÃO MODIFICAR
    API_BASE_URL = os.getenv("API_BASE_URL")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não definido.")
    if not API_BASE_URL:
        raise ValueError("API_BASE_URL não definido.")

    # MODIFICAR
    FILE_PATH = "input/Tropoc_Geo_2024_mapbiomas.csv"  # Caminho do arquivo com os CAR's
    OUTPUT_FILE = "output/Tropoc_Geo_2024_mapbiomas_restricted_area.csv"  # Caminho para o arquivo JSON de saída
    ID_COLUMN = "CAR"  # Nome da coluna que contém os CAR's

    # NÃO MODIFICAR
    processor = CarIntersectionChecker(
        access_token=ACCESS_TOKEN,
        api_base_url=API_BASE_URL,
        file_path=FILE_PATH,
        output_file=OUTPUT_FILE,
        id_column=ID_COLUMN,
    )
    processor.processar()
    print("Processamento concluído!")
