"""
Módulo para processamento de downloads a partir de uma API.

Este módulo define a classe DownloadProcessor, que herda de APIClient e CSVProcessor,
para automatizar o processo de download de arquivos a partir de uma API. E
le realiza as seguintes funções:

- Carrega as variáveis de ambiente necessárias (como ACCESS_TOKEN e API_BASE_URL) a
partir de um arquivo .env.
- Lê um arquivo de entrada (CSV ou Excel) que contém os IDs para processamento.
- Para cada ID, monta dinamicamente a URL para acessar a API, faz a requisição para obter o
link de download, e salva esse link no DataFrame.
- Cria uma pasta específica para cada ID dentro do diretório "download" e realiza o download
do arquivo (por exemplo, .zip).
- Exporta o DataFrame atualizado com os links de download para um arquivo de saída.

Uso:
    1. Certifique-se de que as variáveis de ambiente (como ACCESS_TOKEN e API_BASE_URL)
    estejam definidas em um arquivo .env.
    2. Ajuste os parâmetros do script conforme necessário:
        - ENTITY_TYPE: Tipo da entidade (ex.: "AnalysisData").
        - FILE_PATH: Caminho para o arquivo de entrada com os IDs.
        - OUTPUT_FILE: Caminho para salvar o arquivo de saída com os links de download.
        - ID_COLUMN: Nome da coluna que contém os IDs.
        - SOMENTE_LINK: se True, apenas gera a coluna com link e não faz o download dos arquivos.
    3. Execute o módulo diretamente para processar os dados e efetuar o download dos arquivos
    correspondentes.

Exceções:
    São tratadas exceções para erros HTTP, conexão, timeout, requisição e erros de sistema
    durante o processamento.
"""

import mimetypes
import os
from urllib.parse import urlparse

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv

from tools.tools import APIClient, CSVProcessor

# Carrega as variáveis de ambiente
load_dotenv()


class DeforestationDownloadProcessor(APIClient, CSVProcessor):
    """
    Classe para processar o download dos resultados de desmatamento.
    Para cada ID do arquivo de entrada, gera a URL de download, baixa o arquivo,
    salva o link de download no DataFrame e exporta os resultados atualizados.
    """

    def __init__(
        self,
        access_token: str,
        entity_type: str,
        file_path: str,
        output_file: str,
        id_column: str,
        somente_link: bool = False,
    ):
        # A API_URL será construída dinamicamente para cada ID
        APIClient.__init__(self, access_token, api_url="")
        CSVProcessor.__init__(self, file_path, id_column)
        self.output_file = output_file
        self.id_column = id_column
        # Cria coluna para armazenar o link de download
        self.df["download_link"] = None
        self.entity_type = entity_type
        self.somente_link = somente_link

    def processar(self) -> None:
        """
        Realiza o download dos arquivos correspondentes...
        """
        base_download_dir = os.getenv("DOWNLOAD_DIR", "download")
        os.makedirs(base_download_dir, exist_ok=True)

        # Define subpasta com base no tipo de entidade
        entity_folder = (
            "DeforestationAnalysisMapBiomas"
            if self.entity_type == "DeforestationAnalysis"
            else self.entity_type
        )

        for index, deforestation_id in self.df[self.id_column].items():
            if pd.isna(deforestation_id):
                print(f"Ignorando registro {index} pois o id está vazio.")
                continue

            api_url = f"{os.getenv('API_BASE_URL')}/download/{self.entity_type}/{deforestation_id}"
            try:
                response = requests.get(api_url, headers=self.headers, timeout=10)
                response.raise_for_status()
                response_data = response.json()

                if response.status_code == 200 and "data" in response_data:
                    download_url = response_data["data"].get("url")
                    if download_url:
                        self.df.at[index, "download_link"] = download_url

                        if self.somente_link:
                            print(
                                f"[SOMENTE_LINK] Link gerado para id {deforestation_id}"
                            )
                        else:
                            # Novo caminho do diretório: DOWNLOAD_DIR/entity_type/UUID
                            folder_path = os.path.join(
                                base_download_dir, entity_folder
                            )
                            os.makedirs(folder_path, exist_ok=True)

                            file_response = requests.get(
                                download_url, stream=True, timeout=60
                            )
                            file_response.raise_for_status()

                            parsed = urlparse(download_url)
                            ext = os.path.splitext(parsed.path)[1]
                            if not ext:
                                content_type = file_response.headers.get(
                                    "Content-Type", ""
                                )
                                ext = (
                                    mimetypes.guess_extension(
                                        content_type.split(";")[0]
                                    )
                                    or ""
                                )

                            file_name = os.path.join(
                                folder_path, f"{deforestation_id}{ext or '.bin'}"
                            )

                            with open(file_name, "wb") as f:
                                for chunk in file_response.iter_content(
                                    chunk_size=8192
                                ):
                                    if chunk:
                                        f.write(chunk)
                            print(f"Download concluído para id {deforestation_id}")
                    else:
                        print(
                            f"URL de download não encontrada para id {deforestation_id}"
                        )
                else:
                    print(
                        f"Falha na requisição para id {deforestation_id}: {response_data}"
                    )
            except requests.exceptions.HTTPError as e:
                print(f"Erro HTTP para id {deforestation_id}: {e}")
            except requests.exceptions.ConnectionError as e:
                print(f"Erro de conexão para id {deforestation_id}: {e}")
            except requests.exceptions.Timeout as e:
                print(f"Timeout para id {deforestation_id}: {e}")
            except requests.exceptions.RequestException as e:
                print(f"Erro de requisição para id {deforestation_id}: {e}")
            except OSError as e:
                print(f"Erro de sistema para id {deforestation_id}: {e}")

        self.salvar_dados(self.output_file)


if __name__ == "__main__":
    # NÃO MODIFICAR
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não definido.")

    # MODIFICAR SE NECESSÁRIO
    # ENTITY_TYPE = "DeforestationAnalysis"  # Tipo de entidade para mapbiomas
    ENTITY_TYPE = "DeforestationAnalysisProdes" # Tipo de entidade para Prodes
    # ENTITY_TYPE = "ReportRestrictionsDetailed" # Tipo relatorio detalhado

    # MODIFICAR
    INPUT_FILE = "Tropoc_Geo_2024_prodes.csv"  # Pode ser um CSV ou Excel
    OUTPUT_FILE = "Tropoc_Geo_2024_prodes_DOWNLOAD.csv"
    ID_COLUMN = "deforestation_prodes"  # Nome da coluna com os IDs
    SOMENTE_LINK = False  # Se True, apenas gera a coluna com link e não faz o download dos arquivos

    # NÃO MODIFICAR
    INPUT_PATH = os.getenv("INPUT_DIR", ".") + "/" + INPUT_FILE
    OUTPUT_PATH = os.getenv("OUTPUT_DIR", ".") + "/" + OUTPUT_FILE
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {INPUT_PATH}")

    # NÃO MODIFICAR
    processor = DeforestationDownloadProcessor(
        access_token=ACCESS_TOKEN,
        entity_type=ENTITY_TYPE,
        file_path=INPUT_PATH,
        output_file=OUTPUT_PATH,
        id_column=ID_COLUMN,
        somente_link=SOMENTE_LINK,
    )
    processor.processar()
    print("Processamento concluído!")
