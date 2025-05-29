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
    ):
        # A API_URL será construída dinamicamente para cada ID
        APIClient.__init__(self, access_token, api_url="")
        CSVProcessor.__init__(self, file_path, id_column)
        self.output_file = output_file
        self.id_column = id_column
        # Cria coluna para armazenar o link de download
        self.df["download_link"] = None
        self.entity_type = entity_type

    def processar(self) -> None:
        """
        Realiza o download dos arquivos correspondentes. Este método percorre os registros  no
        DataFrame, monta a URL para acessar a API de download, faz a requisição para obter o
        link de download, e realiza o download do arquivo correspondente. O arquivo é salvo
        em uma pasta específica para cada ID de desmatamento dentro da pasta "download".
        O link de download também é salvo no DataFrame.

        Exceções são tratadas para erros HTTP, de conexão, timeout, requisição e sistema.
        Raises:
            requests.exceptions.HTTPError: Se ocorrer um erro HTTP durante a requisição.
            requests.exceptions.ConnectionError: Se ocorrer um erro de conexão durante a requisição.
            requests.exceptions.Timeout: Se ocorrer um erro de timeout durante a requisição.
            requests.exceptions.RequestException: Se ocorrer um erro de requisição durante a
            requisição.
            OSError: Se ocorrer um erro de sistema ao criar diretórios ou salvar arquivos.
        """

        download_base_dir = "download"
        os.makedirs(download_base_dir, exist_ok=True)

        for index, deforestation_id in self.df[self.id_column].items():
            if pd.isna(deforestation_id):
                print(f"Ignorando registro {index} pois o id está vazio.")
                continue

            # Monta a URL para acessar a API de download
            api_url = f"{os.getenv('API_BASE_URL')}/download/{self.entity_type}/{deforestation_id}"
            try:
                # Faz a requisição à API para gerar o link de download
                response = requests.get(api_url, headers=self.headers, timeout=10)
                response.raise_for_status()
                response_data = response.json()

                # Verifica se a resposta contém o link de download
                if response.status_code == 200 and "data" in response_data:
                    download_url = response_data["data"].get("url")
                    if download_url:
                        # Salva o link de download no DataFrame
                        self.df.at[index, "download_link"] = download_url

                        # Cria pasta específica para o ID (dentro da pasta "download")
                        folder_id = os.path.join(
                            download_base_dir, str(deforestation_id)
                        )
                        os.makedirs(folder_id, exist_ok=True)

                        # Realiza o download do arquivo
                        file_response = requests.get(
                            download_url, stream=True, timeout=60
                        )
                        file_response.raise_for_status()

                        # Define o caminho e nome do arquivo (por exemplo, um .zip)
                        # Extrai a parte do path da URL (antes dos query params)
                        parsed = urlparse(download_url)
                        ext = os.path.splitext(parsed.path)[1]
                        # se não vier extensão na URL, tenta a partir do Content-Type
                        if not ext:
                            content_type = file_response.headers.get("Content-Type", "")
                            ext = (
                                mimetypes.guess_extension(content_type.split(";")[0])
                                or ""
                            )

                        # monta o nome de arquivo com a extensão correta
                        file_name = os.path.join(
                            folder_id, f"{deforestation_id}{ext or '.bin'}"
                        )

                        with open(file_name, "wb") as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        print(f"Download concluído para id {deforestation_id}")
                    else:
                        print(f"Download URL não encontrada para id {deforestation_id}")
                else:
                    print(
                        f"Falha na requisição para id {deforestation_id}: {response_data}"
                    )
            except requests.exceptions.HTTPError as e:
                print(f"Erro HTTP processando id {deforestation_id}: {e}")
            except requests.exceptions.ConnectionError as e:
                print(f"Erro de conexão processando id {deforestation_id}: {e}")
            except requests.exceptions.Timeout as e:
                print(f"Erro de timeout processando id {deforestation_id}: {e}")
            except requests.exceptions.RequestException as e:
                print(f"Erro de requisição processando id {deforestation_id}: {e}")
            except OSError as e:
                print(f"Erro de sistema processando id {deforestation_id}: {e}")

        # Salva o DataFrame atualizado com os links de download
        self.salvar_dados(self.output_file)


if __name__ == "__main__":
    # NÃO MODIFICAR
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não definido.")

    # MODIFICAR SE NECESSÁRIO
    # ENTITY_TYPE = "DeforestationAnalysis"
    # ENTITY_TYPE = "DeforestationAnalysisProdes"
    ENTITY_TYPE = "ReportRestrictionsDetailed"

    # MODIFICAR
    FILE_PATH = "output/TROPOC_report_detailed.csv"  # Pode ser um CSV ou Excel
    OUTPUT_FILE = "output/TROPOC_report_detailed_download.csv"
    ID_COLUMN = "restriction_id"  # Nome da coluna com os IDs

    # NÃO MODIFICAR
    processor = DeforestationDownloadProcessor(
        access_token=ACCESS_TOKEN,
        entity_type=ENTITY_TYPE,
        file_path=FILE_PATH,
        output_file=OUTPUT_FILE,
        id_column=ID_COLUMN,
    )
    processor.processar()
    print("Processamento concluído!")
