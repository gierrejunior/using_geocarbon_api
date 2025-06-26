import os

import requests  # type: ignore
from dotenv import load_dotenv

from tools.tools import APIClient

# Carrega variáveis de ambiente
load_dotenv()


class CarBatchUploader(APIClient):
    """
    Classe para realizar upload de arquivos .shp e auxiliares para o endpoint /car-batch.
    """

    def __init__(
        self,
        access_token: str,
        api_url: str,
        input_folder: str,
        name: str,
        availability_date: str,
    ):
        super().__init__(access_token, api_url)
        self.input_folder = input_folder
        self.name = name
        self.availability_date = availability_date

    def processar(self) -> None:
        if not os.path.isdir(self.input_folder):
            raise FileNotFoundError(f"Pasta não encontrada: {self.input_folder}")

        arquivos = [
            os.path.join(self.input_folder, f)
            for f in os.listdir(self.input_folder)
            if os.path.isfile(os.path.join(self.input_folder, f))
        ]

        if len(arquivos) > 5:
            raise ValueError("Erro: máximo de 5 arquivos são permitidos.")
        if not any(f.lower().endswith(".shp") for f in arquivos):
            raise ValueError("Erro: é obrigatório incluir pelo menos um arquivo .shp.")

        # Print debug: arquivos sendo enviados
        print(f"\nArquivos selecionados ({len(arquivos)}):")
        for arq in arquivos:
            print("  -", arq)

        files_payload = []
        file_handlers = []
        try:
            for file_path in arquivos:
                f = open(file_path, "rb")
                file_handlers.append(f)  # para fechamento posterior
                files_payload.append(("files", (os.path.basename(file_path), f)))

            form_data = {
                "not_car": (None, "true"),
                "name": (None, self.name),
                "availabilityDate": (None, self.availability_date),
            }

            full_payload = files_payload + list(form_data.items())

            # Debug da requisição

            response = requests.post(
                self.api_url,
                files=full_payload,
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=180,
            )

            # Resposta
            # content_type = response.headers.get("Content-Type", "")
            # if content_type.startswith("application/json"):
            #     print("Resposta:", response.json())
            # else:
            #     print("Resposta:", response.text)

            if response.status_code in [200, 201]:
                print("✅ Upload realizado com sucesso.")
            else:
                print(f"❌ Falha ({response.status_code}): {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na requisição: {e}")

        finally:
            # Fecha arquivos abertos
            for f in file_handlers:
                f.close()


# Exemplo de utilização:
if __name__ == "__main__":
    API_URL = f"{os.getenv('API_BASE_URL')}/car-batch"
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if ACCESS_TOKEN is None:
        raise ValueError("ACCESS_TOKEN não definido")

    # MODIFICAR
    NAME = "paragominas2"
    AVAILABILITY_DATE = "05-05-2025"
    INPUT_FOLDER = "paragominas"

    # NÃO MODIFICAR
    INPUT_PATH = os.getenv("SHP_DIR", ".") + "/" + INPUT_FOLDER
    if not INPUT_FOLDER:
        raise ValueError("❌ INPUT_FOLDER não definido")

    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Pasta de entrada não encontrada: {INPUT_PATH}")

    processor = CarBatchUploader(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        input_folder=INPUT_PATH,
        name=NAME,
        availability_date=AVAILABILITY_DATE,
    )
    processor.processar()
