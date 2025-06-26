import os

import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore

from tools import APIClient, CSVProcessor

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()


class DeforestationBatchRequestProcessor(APIClient, CSVProcessor):
    """
    Classe que integra a funcionalidade de requisições à API com o processamento
    do arquivo, permitindo envio em lote (/deforestation/batch) e salvando
    o ID retornado em coluna no arquivo de saída.

    Parâmetros:
        year_ranges: lista de anos ou lista de intervalos.
            - Para anos discretos: [2004, 2023]
            - Para intervalos: [[2004, 2023], [2010, 2015]]
    """

    def __init__(
        self,
        access_token: str,
        api_url: str,
        file_path: str,
        car_column: str,
        year_ranges: list,
        output_file: str,
        timeout: int = 60,
    ):
        APIClient.__init__(self, access_token, api_url)
        CSVProcessor.__init__(self, file_path, car_column)
        self.output_file = output_file
        self.timeout = timeout
        self.year_ranges = year_ranges  # mantém conforme passado

    def processar_batch(self, name: str) -> None:
        """
        Lê códigos de imóvel (CAR), monta payload e envia POST
        para /deforestation/batch. Salva ID retornado em coluna.
        """
        codigos = [str(c).strip() for c in self.df[self.car_column].dropna()]

        # Define anosBiomas: se houver sublistas, expande intervalos
        if any(isinstance(item, (list, tuple)) for item in self.year_ranges):
            anos: list[int] = []
            for intervalo in self.year_ranges:
                start, end = intervalo
                anos.extend(range(start, end + 1))
            anos = sorted(set(anos))
        else:
            # Lista de anos discretos
            anos = sorted(set(self.year_ranges))

        payload = {"name": name, "codImoveis": codigos, "yearsBiomas": anos}
        print(f"Enviando batch com {len(codigos)} imóveis e {len(anos)} anos...")

        # Monta URL sem duplicar /batch
        url = self.api_url.rstrip("/") + (
            "/batch" if not self.api_url.rstrip("/").endswith("batch") else ""
        )
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        batch_id = None
        try:
            response = requests.post(
                url, headers=headers, json=payload, timeout=self.timeout
            )
            if response.headers.get("Content-Type", "").startswith("application/json"):
                data = response.json()
            else:
                print(f"Resposta não é JSON: {response.text}")
                data = {}

            batch_data = data.get("data", {}).get("deforestation")
            if response.status_code == 201 and batch_data and "id" in batch_data:
                batch_id = batch_data["id"]
                print(f"Batch enviado com sucesso. ID: {batch_id}")
            else:
                print(f"Falha ({response.status_code}): {data}")
        except requests.exceptions.ReadTimeout:
            print(f"Timeout ({self.timeout}s) ao enviar batch para {url}")
        except requests.exceptions.RequestException as e:
            print(f"Erro de requisição: {e}")

        # Salva resultado
        self.df["batch_id"] = batch_id
        self.salvar_dados(self.output_file)
        print(f"Saída gravada em {self.output_file}")


if __name__ == "__main__":
    load_dotenv()
    API_BASE = os.getenv("API_BASE_URL")
    if not API_BASE:
        raise ValueError("API_BASE_URL não configurada")
    API_URL = API_BASE.rstrip("/") + "/deforestation/mapbiomas"
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não configurado")

    # MODIFICAR
    INPUT_FILE = "TROPOC_teste.xlsx"
    CAR_COLUMN = "CAR"  # Nome da coluna com os CAR's
    OUTPUT_FILE = "TROPOC_teste_batch_2004_2023.csv"
    YEAR_RANGES = [[2004, 2023]]  # para anos diretos
    # YEAR_RANGES = [[2004, 2023], [2021, 2023]] # para intervalos

    # NÃO MODIFICAR
    INPUT_PATH = os.getenv("INPUT_DIR", ".") + "/" + INPUT_FILE
    OUTPUT_PATH = os.getenv("OUTPUT_DIR", ".") + "/" + OUTPUT_FILE
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(
            f"Arquivo de entrada não encontrado: {INPUT_PATH}"
        )

    processor = DeforestationBatchRequestProcessor(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=INPUT_PATH,
        car_column=CAR_COLUMN,
        year_ranges=YEAR_RANGES,
        output_file=OUTPUT_PATH,
        timeout=60,
    )

    processor.processar_batch("TROPOC_teste_batch_2004_2023")
