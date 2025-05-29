#!/usr/bin/env python3
import json
import os
import time

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore

from tools.tools import APIClient

load_dotenv()


class ReportRestrictionsOneShotFetcher(APIClient):
    """
    A cada execução, faz um único loop de GET em /report-detailed/restrictions?id=<uuid>
    para todas as linhas cujo `taskStatus` != "COMPLETED", atualizando status e resultados
    no próprio arquivo de entrada.
    """

    def __init__(
        self,
        access_token: str,
        api_url: str,
        file_path: str,
        id_column: str,
        sheet_name: str | None = None,
    ):
        super().__init__(access_token, api_url)
        self.file_path = file_path
        self.id_column = id_column
        self.sheet_name = sheet_name

        # 1) Carrega o DataFrame do arquivo
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(file_path, dtype=str)
        elif ext in (".xls", ".xlsx"):
            df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
        else:
            raise ValueError(f"Formato não suportado: {ext!r}")

        # 2) Normaliza nomes de coluna
        df.columns = df.columns.astype(str).str.strip()

        # 3) Verifica se a coluna de ID existe
        if id_column not in df.columns:
            raise ValueError(
                f"Coluna '{id_column}' não encontrada. Colunas disponíveis: {list(df.columns)}"
            )

        # 4) Garante as colunas de status e resultados
        if "taskStatus" not in df.columns:
            df["taskStatus"] = None
        if "reportResults" not in df.columns:
            df["reportResults"] = None

        self.df = df

    def processar(self) -> None:
        """
        Para cada linha cujo taskStatus != COMPLETED:
          - faz GET ?id=<uuid>
          - atualiza taskStatus e, se COMPLETED, reportResults
        Salva tudo de volta no mesmo arquivo.
        """
        n = len(self.df)
        print(f"Iniciando one–shot fetch para {n} registros…")

        for idx, row in self.df.iterrows():
            uuid = row[self.id_column]
            status = row.get("taskStatus")
            # pula vazios e já completados
            if pd.isna(uuid) or not str(uuid).strip() or status == "COMPLETED":
                continue

            uuid = str(uuid).strip()
            print(f"[{idx}] GET id={uuid}… ", end="", flush=True)
            try:
                resp = requests.get(
                    self.api_url,
                    headers=self.headers,
                    params={"id": uuid},
                    timeout=30,
                )
                resp.raise_for_status()
                payload = resp.json()
                data_list = payload.get("data", [])
                if data_list:
                    rec = data_list[0]
                    ts = rec.get("taskStatus", "").upper()
                    self.df.at[idx, "taskStatus"] = ts
                    if ts == "COMPLETED":
                        # armazena o JSON de reportResults como string
                        self.df.at[idx, "reportResults"] = json.dumps(
                            rec.get("reportResults"), ensure_ascii=False
                        )
                        print("COMPLETED ✅")
                    else:
                        print(f"STATUS={ts} ⏳")
                else:
                    self.df.at[idx, "taskStatus"] = "NO_DATA"
                    print("NO_DATA ⚠️")
            except requests.RequestException as e:
                code = getattr(e.response, "status_code", None)
                err = f"HTTP_ERROR_{code or 'X'}"
                self.df.at[idx, "taskStatus"] = err
                print(f"{err} 🚨")
            except Exception as e:
                self.df.at[idx, "taskStatus"] = f"ERROR"
                print(f"EXCEPTION 🚨 {e}")

        # 5) Salva de volta no mesmo arquivo
        ext = os.path.splitext(self.file_path)[1].lower()
        if ext == ".csv":
            self.df.to_csv(self.file_path, index=False)
        else:
            self.df.to_excel(self.file_path, index=False)

        print(f"\nArquivo atualizado salvo em '{self.file_path}'")


if __name__ == "__main__":
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não definido em .env")

    API_URL = f"{os.getenv('API_BASE_URL')}/report-detailed/restrictions"

    # ↙️ Ajuste aqui para o seu caso:
    FILE_PATH = "output/TROPOC_report_detailed.csv"
    ID_COLUMN = "restriction_id"
    SHEET_NAME = (
        "reportResults"  # se for Excel com várias planilhas, coloque o nome da aba
    )

    fetcher = ReportRestrictionsOneShotFetcher(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=FILE_PATH,
        id_column=ID_COLUMN,
        sheet_name=SHEET_NAME,
    )
    fetcher.processar()
