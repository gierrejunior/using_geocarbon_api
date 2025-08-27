import os

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore

from tools.tools import APIClient

load_dotenv()


class ReportRestrictionsBatchRequestProcessor(APIClient):
    """
    Processa um arquivo CSV/Excel de códigos CAR e envia cada um
    ao endpoint de 'report-detailed/restrictions', salvando o ID retornado.
    """

    def __init__(
        self,
        access_token: str,
        api_url: str,
        file_path: str,
        output_file: str,
        car_column: str,
    ):
        super().__init__(access_token, api_url)
        self.file_path = file_path
        self.output_file = output_file
        # Remove possíveis espaços ao redor do nome da coluna
        self.car_column = car_column.strip()

        # Carrega DataFrame conforme extensão
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            self.df = pd.read_csv(file_path, dtype=str)
        elif ext in (".xls", ".xlsx"):
            self.df = pd.read_excel(file_path, dtype=str)
        else:
            raise ValueError(f"Formato de arquivo não suportado: {ext!r}")

        # Remove espaços nos nomes das colunas do DataFrame
        self.df.columns = self.df.columns.str.strip()

        # Verifica se a coluna CAR existe
        if self.car_column not in self.df.columns:
            raise ValueError(
                f"Coluna '{self.car_column}' não encontrada no arquivo de entrada. "
                f"Colunas disponíveis: {self.df.columns.tolist()}"
            )

        # Cria coluna para armazenar o ID retornado
        self.df["restriction_id"] = None

    def processar(self) -> None:
        """
        Itera sobre cada CAR e faz POST ao endpoint. Salva DataFrame ao final.
        """
        total = len(self.df)
        print(f"Iniciando processamento de {total} registros…")

        for idx, car in self.df[self.car_column].items():
            if pd.isna(car) or not str(car).strip():
                print(f"[{idx}] CAR vazio, pulando.")
                continue

            car = str(car).strip()
            payload = {"codImovel": car}

            try:
                resp = self.enviar_requisicao(payload)
                data = resp.json()
                if resp.status_code in (200, 201) and "data" in data:
                    new_id = data["data"].get("id") or data["data"]
                    self.df.at[idx, "restriction_id"] = new_id
                    print(f"[{idx}] CAR={car} → restriction_id={new_id}")
                else:
                    print(f"[{idx}] Falha CAR={car}: {resp.status_code} {data}")
            except requests.RequestException as e:
                print(f"[{idx}] Erro HTTP CAR={car}: {e}")
            except ValueError as e:
                print(f"[{idx}] JSON inválido CAR={car}: {e}")
            except KeyError as e:
                print(f"[{idx}] Chave não encontrada CAR={car}: {e}")

        # Determina formato de saída pelo sufixo do output_file
        ext_out = os.path.splitext(self.output_file)[1].lower()
        if ext_out == ".csv":
            self.df.to_csv(self.output_file, index=False)
        elif ext_out in (".xls", ".xlsx"):
            self.df.to_excel(self.output_file, index=False)
        else:
            raise ValueError(f"Formato de saída não suportado: {ext_out!r}")

        print(f"Processamento concluído. Arquivo salvo em '{self.output_file}'.")


if __name__ == "__main__":
    # NÃO MODIFICAR: variáveis de ambiente
    API_URL = f"{os.getenv('API_BASE_URL')}/report-detailed/restrictions"
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não definido em .env")

    # CONFIGURAÇÕES DO USUÁRIO
    INPUT_FILE = "TROPOC_teste.xlsx"
    OUTPUT_FILE = "TROPOC_teste_report.csv"
    CAR_COLUMN = "CAR"

    # NÃO MODIFICAR
    INPUT_PATH = os.getenv("INPUT_DIR", ".") + "/" + INPUT_FILE
    OUTPUT_PATH = os.getenv("OUTPUT_DIR", ".") + "/" + OUTPUT_FILE
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {INPUT_PATH}")

    processor = ReportRestrictionsBatchRequestProcessor(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=INPUT_PATH,
        output_file=OUTPUT_PATH,
        car_column=CAR_COLUMN,
    )
    processor.processar()
