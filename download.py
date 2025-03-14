import os
import requests
import pandas as pd
from dotenv import load_dotenv
from tools import APIClient, CSVProcessor

# Carrega as variáveis de ambiente
load_dotenv()

class DeforestationDownloadProcessor(APIClient, CSVProcessor):
    """
    Classe para processar o download dos resultados de desmatamento.
    Para cada ID do arquivo de entrada, gera a URL de download, baixa o arquivo,
    salva o link de download no DataFrame e exporta os resultados atualizados.
    """
    def __init__(self, access_token: str, entity_type: str, file_path: str, output_file: str, id_column: str):
        # A API_URL será construída dinamicamente para cada ID
        APIClient.__init__(self, access_token, api_url=None)
        CSVProcessor.__init__(self, file_path, id_column)
        self.output_file = output_file
        self.id_column = id_column
        # Cria coluna para armazenar o link de download
        self.df["download_link"] = None
        self.entity_type = entity_type

    def processar(self) -> None:
        download_base_dir = "download"
        os.makedirs(download_base_dir, exist_ok=True)
        
        for index, deforestation_id in self.df[self.id_column].items():
            if pd.isna(deforestation_id):
                print(f"Ignorando registro {index} pois o id está vazio.")
                continue

            # Monta a URL para acessar a API de download
            api_url = f"{os.getenv('API_BASE_URL')}/download/{ENTITY_TYPE}/{deforestation_id}"
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
                        folder_id = os.path.join(download_base_dir, str(deforestation_id))
                        os.makedirs(folder_id, exist_ok=True)

                        # Realiza o download do arquivo
                        file_response = requests.get(download_url, stream=True)
                        file_response.raise_for_status()

                        # Define o caminho e nome do arquivo (por exemplo, um .zip)
                        file_name = os.path.join(folder_id, f"{deforestation_id}.zip")
                        with open(file_name, "wb") as f:
                            for chunk in file_response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                        print(f"Download concluído para id {deforestation_id}")
                    else:
                        print(f"Download URL não encontrada para id {deforestation_id}")
                else:
                    print(f"Falha na requisição para id {deforestation_id}: {response_data}")
            except Exception as e:
                print(f"Erro processando id {deforestation_id}: {e}")

        # Salva o DataFrame atualizado com os links de download
        self.salvar_dados(self.output_file)

if __name__ == "__main__":
    # NÃO MODIFICAR
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não definido.")
    
    # MODIFICAR SE NECESSÁRIO
    ENTITY_TYPE = "DeforestationAnalysis"

    # MODIFICAR
    FILE_PATH = "input/Tropoc_Geo_2024_v1_updated.xlsx"  # Pode ser um CSV ou Excel
    OUTPUT_FILE = "output/Tropoc_Geo_2024_v1_link_download.xlsx"
    ID_COLUMN = "deforestation_2004_2023"  # Nome da coluna com os IDs


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
