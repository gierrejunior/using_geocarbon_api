#!/usr/bin/env python3
import json
import os
from typing import Dict, List, Set

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore

from tools.tools import APIClient

load_dotenv()


class ReportRestrictionsOneShotFetcher(APIClient):
    """
    Fluxo:
      1) Para linhas já COMPLETED, preenche 'has_intersection' usando 'reportResults' (sem GET).
      2) Para as demais, faz GET /report-detailed/restrictions?id=<uuid>, atualiza 'taskStatus',
         'reportResults' (dict quando possível) e 'has_intersection'.

    Saídas em OUTPUT_DIR:
      - JSON (sempre): <base>_report_detailed_results.json
      - CSV wide (opcional): <base>_report_detailed_results.csv
          * Copia TODAS as colunas do CSV de entrada (mesma ordem)
          * Acrescenta: has_intersection ("true"/"false")
          * Acrescenta 2 colunas por NAME em with_intersection: "<NAME> hectares" e "<NAME> %"
          * Acrescenta 2 colunas por ANO em deter: "DETER <ano> hectares" e "DETER <ano> %"
    """

    def __init__(self, access_token: str, api_url: str, file_path: str, id_column: str):
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

        # Guardar ordem original das colunas (pra replicar no CSV wide)
        self.input_columns_order = list(df.columns)

        # 4) Garante colunas auxiliares
        if "taskStatus" not in df.columns:
            df["taskStatus"] = None
        if "reportResults" not in df.columns:
            df["reportResults"] = None
        if "has_intersection" not in df.columns:
            df["has_intersection"] = False  # bool

        def _maybe_parse_json(x):
            if isinstance(x, str):
                s = x.strip()
                if s and s[0] in "[{":
                    try:
                        return json.loads(s)
                    except json.JSONDecodeError:
                        return x
            return x

        # 5) Completa 'has_intersection' para COMPLETED e normaliza 'reportResults'
        for idx, row in df.iterrows():
            status = row.get("taskStatus", "")
            results_val = row.get("reportResults")
            if (
                status == "COMPLETED"
                and isinstance(results_val, str)
                and results_val.strip()
            ):
                parsed = _maybe_parse_json(results_val)
                df.at[idx, "reportResults"] = parsed
                if isinstance(parsed, dict):
                    with_int = parsed.get("with_intersection", [])
                    df.at[idx, "has_intersection"] = bool(with_int)
                else:
                    df.at[idx, "has_intersection"] = False

        self.df = df

    def processar(self, csv_output: bool = False) -> None:
        """
        Executa GETs pendentes e salva as saídas.
        """
        n = len(self.df)
        print(f"Iniciando one–shot fetch para {n} registros…")

        for idx, row in self.df.iterrows():
            uuid = row[self.id_column]
            status = row.get("taskStatus")

            # COMPLETED já foi tratado no __init__
            if pd.isna(uuid) or not str(uuid).strip() or status == "COMPLETED":
                continue

            uuid = str(uuid).strip()
            print(f"[{idx}] GET id={uuid}… ", end="", flush=True)
            try:
                resp = requests.get(
                    self.api_url,
                    headers=self.headers,
                    params={"id": uuid},
                    timeout=180,
                )
                resp.raise_for_status()
                payload = resp.json()
                data_list = payload.get("data", [])

                if data_list:
                    rec = data_list[0]
                    ts = str(rec.get("taskStatus", "")).upper()
                    self.df.at[idx, "taskStatus"] = ts

                    if ts == "COMPLETED":
                        results = rec.get("reportResults", {})
                        if isinstance(results, str):
                            try:
                                results = json.loads(results)
                            except json.JSONDecodeError:
                                pass

                        with_int = []
                        if isinstance(results, dict):
                            with_int = results.get("with_intersection", [])

                        self.df.at[idx, "has_intersection"] = bool(with_int)
                        self.df.at[idx, "reportResults"] = results
                        print(f"COMPLETED ✅  has_intersection={bool(with_int)}")
                    else:
                        print(f"STATUS={ts} ⏳  has_intersection=False")
                else:
                    self.df.at[idx, "taskStatus"] = "NO_DATA"
                    print("NO_DATA ⚠️  has_intersection=False")

            except requests.RequestException as e:
                code = getattr(e.response, "status_code", None)
                err = f"HTTP_ERROR_{code or 'X'}"
                self.df.at[idx, "taskStatus"] = err
                print(f"{err} 🚨  has_intersection=False")
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                self.df.at[idx, "taskStatus"] = "ERROR"
                print(f"EXCEPTION 🚨 {e}  has_intersection=False")

        # === SAÍDAS ===
        out_dir = os.getenv("OUTPUT_DIR")
        if not out_dir:
            raise ValueError("Defina OUTPUT_DIR no .env")
        os.makedirs(out_dir, exist_ok=True)

        base = os.path.splitext(os.path.basename(self.file_path))[0]

        # 1) JSON (sempre)
        json_path = os.path.join(out_dir, f"{base}_report_detailed_results.json")
        self._save_json(json_path)

        # 2) CSV wide (opcional)
        if csv_output:
            csv_path = os.path.join(out_dir, f"{base}_report_detailed_results.csv")
            self._save_csv_wide(csv_path)
            print(f"Arquivo CSV salvo em '{csv_path}'")

    # ---------- Helpers de saída ----------

    def _save_json(self, out_path: str) -> None:
        df_out = self.df.where(pd.notna(self.df), None)

        def _maybe_parse_json(x):
            if isinstance(x, str):
                s = x.strip()
                if s and s[0] in "[{":
                    try:
                        return json.loads(s)
                    except json.JSONDecodeError:
                        return x
            return x

        if "reportResults" in df_out.columns:
            df_out["reportResults"] = df_out["reportResults"].apply(_maybe_parse_json)

        records = df_out.to_dict(orient="records")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2, allow_nan=False)
        print(f"\nArquivo JSON salvo em '{out_path}'")

    def _save_csv_wide(self, out_path: str) -> None:
        """
        CSV 'wide':
          - começa com as colunas originais do input (mesma ordem),
          - acrescenta 'has_intersection' ('true'/'false'),
          - acrescenta 2 colunas por NAME em with_intersection: '<NAME> hectares' e '<NAME> %',
          - acrescenta 2 colunas por ANO em deter: 'DETER <ano> hectares' e 'DETER <ano> %'.
        """
        # --- 1) Esquema de colunas adicionais ---
        names_set: Set[str] = set()
        deter_years_set: Set[int] = set()

        # iterar sobre a coluna reportResults
        for results in self.df["reportResults"]:
            if isinstance(results, dict):
                # with_intersection -> nomes
                for item in results.get("with_intersection", []) or []:
                    name = item.get("name")
                    if name:
                        names_set.add(str(name))

                # deter -> anos
                for d in results.get("deter", []) or []:
                    year = d.get("year")
                    try:
                        if year is not None and str(year).strip() != "":
                            deter_years_set.add(int(year))
                    except (TypeError, ValueError):
                        continue

        names = sorted(names_set)
        deter_years = sorted(deter_years_set)

        # --- 2) Montar registros linha a linha ---
        output_rows: List[Dict] = []
        for _, row in self.df.iterrows():
            out: Dict = {}

            # (a) Colunas originais do input
            for col in self.input_columns_order:
                out[col] = row.get(col)

            # (b) has_intersection como string
            has_int = bool(row.get("has_intersection"))
            out["has_intersection"] = "true" if has_int else "false"

            # (c) Inicializa colunas de each NAME e DETER year com 0.0
            for name in names:
                out[f"{name} hectares"] = 0.0
                out[f"{name} %"] = 0.0

            for year in deter_years:
                out[f"DETER {year} hectares"] = 0.0
                out[f"DETER {year} %"] = 0.0

            # (d) Agrega valores
            totals_names: Dict[str, Dict[str, float]] = {}
            totals_deter: Dict[int, Dict[str, float]] = {}

            results = row.get("reportResults")
            if isinstance(results, dict):
                # with_intersection
                for item in results.get("with_intersection", []) or []:
                    name = item.get("name")
                    if not name:
                        continue
                    ha = float(item.get("ha", 0) or 0)
                    pct = float(item.get("pct", 0) or 0)
                    key = str(name)
                    if key not in totals_names:
                        totals_names[key] = {"ha": 0.0, "pct": 0.0}
                    totals_names[key]["ha"] += ha
                    totals_names[key]["pct"] += pct

                # deter (por ano)
                for d in results.get("deter", []) or []:
                    year = d.get("year")
                    try:
                        year = int(year)
                    except (TypeError, ValueError):
                        year = None
                    if year is None:
                        continue
                    ha = float(d.get("ha", 0) or 0)
                    pct = float(d.get("pct", 0) or 0)
                    if year not in totals_deter:
                        totals_deter[year] = {"ha": 0.0, "pct": 0.0}
                    totals_deter[year]["ha"] += ha
                    totals_deter[year]["pct"] += pct

            # (e) Grava agregados
            for name, agg in totals_names.items():
                out[f"{name} hectares"] = agg["ha"]
                out[f"{name} %"] = agg["pct"]

            for year, agg in totals_deter.items():
                out[f"DETER {year} hectares"] = agg["ha"]
                out[f"DETER {year} %"] = agg["pct"]

            output_rows.append(out)

        # --- 3) Ordem final das colunas ---
        final_cols = list(self.input_columns_order) + ["has_intersection"]
        for name in names:
            final_cols.append(f"{name} hectares")
            final_cols.append(f"{name} %")
        for year in deter_years:
            final_cols.append(f"DETER {year} hectares")
            final_cols.append(f"DETER {year} %")

        csv_df = pd.DataFrame(output_rows, columns=final_cols)
        csv_df.to_csv(out_path, index=False)


if __name__ == "__main__":
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não definido em .env")

    API_URL = f"{os.getenv('API_BASE_URL')}/report-detailed/restrictions"

    # ↙️ Ajuste aqui para o seu caso:
    INPUT_FILE = "car_tropoc_base_1100_IDreportcompleto_3.csv"
    ID_COLUMN = "restriction_id"

    # ✅ Flag para gerar CSV wide além do JSON (coloque aqui ANTES do 'NÃO MODIFICAR')
    CSV_OUTPUT = True  # ou False
    # (se quiser por .env, poderia usar:)
    # CSV_OUTPUT = os.getenv("CSV_OUTPUT", "false").strip().lower() in {"1","true","yes","y"}

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
    fetcher.processar(csv_output=CSV_OUTPUT)
