#!/usr/bin/env python3
import json
import os

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore

from tools.tools import APIClient

load_dotenv()


class ReportRestrictionsOneShotFetcher(APIClient):
    """
    A cada execução, faz duas coisas:
      1) Preenche 'has_intersection' para linhas já COMPLETED no CSV (sem fazer GET).
      2) Para linhas cujo `taskStatus` != "COMPLETED", faz o GET para /report-detailed/restrictions?id=<uuid>,
         atualiza `taskStatus`, `reportResults` e atribui `has_intersection`.
    """

    def __init__(
        self,
        access_token: str,
        api_url: str,
        file_path: str,
        id_column: str,
    ):
        super().__init__(access_token, api_url)
        self.file_path = file_path
        self.id_column = id_column

        # 1) Carrega o DataFrame do arquivo CSV/Excel
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(file_path, dtype=str)
        elif ext in (".xls", ".xlsx"):
            df = pd.read_excel(file_path, dtype=str)
        else:
            raise ValueError(f"Formato não suportado: {ext!r}")

        # 2) Normaliza nomes de coluna
        df.columns = df.columns.astype(str).str.strip()

        # 3) Verifica se a coluna de ID existe
        if id_column not in df.columns:
            raise ValueError(
                f"Coluna '{id_column}' não encontrada. Colunas disponíveis: {list(df.columns)}"
            )

        # 4) Garante as colunas de status, reportResults e has_intersection
        if "taskStatus" not in df.columns:
            df["taskStatus"] = None
        if "reportResults" not in df.columns:
            df["reportResults"] = None
        if "has_intersection" not in df.columns:
            # Inicializa tudo como False por padrão
            df["has_intersection"] = False

        # 5) Para todas as linhas que já vêm com taskStatus == "COMPLETED" e têm algo em reportResults,
        #    preenche o has_intersection com base em "with_intersection" (sem fazer GET).
        for idx, row in df.iterrows():
            status = row.get("taskStatus", "")
            results_str = row.get("reportResults")
            # Se já estiver COMPLETED e reportResults não for nulo/vazio, faz parsing
            if (
                status == "COMPLETED"
                and isinstance(results_str, str)
                and results_str.strip()
            ):
                try:
                    parsed = json.loads(results_str)
                    with_int = parsed.get("with_intersection", [])
                    # Se a lista tiver pelo menos um elemento, marca True
                    df.at[idx, "has_intersection"] = bool(with_int)
                except json.JSONDecodeError:
                    # Se parsing falhar, mantém False (ou pode logar um warning aqui)
                    df.at[idx, "has_intersection"] = False

        self.df = df

    def processar(self) -> None:
        """
        1) Para cada linha cujo taskStatus != COMPLETED:
             - faz GET ?id=<uuid>
             - atualiza taskStatus
             - se COMPLETED, atualiza reportResults e has_intersection
        2) Linhas que já estavam COMPLETED não passam pelo GET (pois já foram processadas no __init__).
        """
        n = len(self.df)
        print(f"Iniciando one–shot fetch para {n} registros…")

        for idx, row in self.df.iterrows():
            uuid = row[self.id_column]
            status = row.get("taskStatus")

            # Se não há UUID ou já completou, pula (mas o has_intersection já foi preenchido no __init__)
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
                        results = rec.get("reportResults", {})

                        # Extrai a lista "with_intersection" e marca True/False
                        with_int = results.get("with_intersection", [])
                        self.df.at[idx, "has_intersection"] = bool(with_int)

                        # Armazena o JSON de reportResults como string
                        self.df.at[idx, "reportResults"] = json.dumps(
                            results, ensure_ascii=False
                        )
                        print(f"COMPLETED ✅  has_intersection={bool(with_int)}")
                    else:
                        # Continua pendente; mantemos has_intersection em False
                        print(f"STATUS={ts} ⏳  has_intersection=False")
                else:
                    # Nenhum dado retornado do endpoint; marca NO_DATA
                    self.df.at[idx, "taskStatus"] = "NO_DATA"
                    # has_intersection já era False
                    print("NO_DATA ⚠️  has_intersection=False")
            except requests.RequestException as e:
                code = getattr(e.response, "status_code", None)
                err = f"HTTP_ERROR_{code or 'X'}"
                self.df.at[idx, "taskStatus"] = err
                # has_intersection continua False
                print(f"{err} 🚨  has_intersection=False")
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                self.df.at[idx, "taskStatus"] = "ERROR"
                print(f"EXCEPTION 🚨 {e}  has_intersection=False")

        # 6) Salva de volta no mesmo arquivo (CSV ou Excel)
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
    INPUT_FILE = "TROPOC_teste_report.csv"
    ID_COLUMN = "restriction_id"


    # NÃO MODIFICAR
    INPUT_PATH = os.getenv("INPUT_DIR", ".") + "/" + INPUT_FILE
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {INPUT_PATH}")

    fetcher = ReportRestrictionsOneShotFetcher(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=INPUT_PATH,
        id_column=ID_COLUMN,
    )
    fetcher.processar()
