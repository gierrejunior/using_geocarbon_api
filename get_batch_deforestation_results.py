"""
Este módulo contém um script para buscar resultados de desmatamento em lote utilizando uma API.
O script lê IDs de um arquivo CSV ou Excel, faz requisições GET para a API com esses IDs e processa
os resultados. IDs que não estão concluídos são reprocessados até 10 vezes, com um intervalo
de 1 minuto
entre as tentativas. IDs que retornam erro são registrados e exportados em um arquivo CSV de erros.

Classes:
    DeforestationIDFetcher: Classe que integra a funcionalidade de requisições à API com o
    processamento
    de arquivos CSV ou Excel para buscar dados via GET utilizando o ID presente em uma
    coluna específica.

Funções:
    buscar_dados(id_param: str) -> dict: Realiza uma requisição GET para a API usando
    o id_param na query string.
    processar() -> None: Processa todos os IDs extraídos do arquivo, faz requisição GET para 
    cada um e    verifica o status.

Exemplo de utilização:
        ID_COLUMN = os.getenv("ID_COLUMN", "id")  # Coluna que contém os IDs a serem consultados

"""

import json
import os
import time

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore

from tools import APIClient, CSVProcessor

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()


class DeforestationIDFetcher(APIClient, CSVProcessor):
    """
    Classe que integra a funcionalidade de requisições à API com o processamento
    de arquivos CSV ou Excel para buscar dados via GET utilizando o ID presente
    em uma coluna específica. Se o processamento não estiver COMPLETED (ou seja,
    não houver "analysisResults") a requisição é ignorada nesta rodada. Após
    percorrer a lista de IDs, o script espera 1 minuto e tenta novamente para os
    IDs que ainda não completaram, repetindo este ciclo até 10 vezes ou até que
    não haja mais pendências. IDs que retornam ERROR são imediatamente registrados
    como erros e os que não forem concluídos após 10 tentativas são exportados
    em um CSV de erros com os detalhes do status.
    """

    def __init__(
        self,
        access_token: str,
        api_url: str,
        file_path: str,
        output_file: str,
        id_column: str,
    ):
        # Inicializa a APIClient para autenticação e headers
        APIClient.__init__(self, access_token, api_url)
        # Inicializa a CSVProcessor para carregar o arquivo com os IDs
        CSVProcessor.__init__(self, file_path, id_column)
        self.output_file = output_file
        self.id_column = id_column
        # Lista para armazenar IDs que ainda não foram concluídos com sucesso
        self.pending_ids: list[dict] = []
        # Lista para armazenar IDs que apresentaram erro (status ERROR ou outros erros definitivos)
        self.error_ids: list[dict] = []

    def buscar_dados(self, id_param: str) -> dict:
        """
        Realiza uma requisição GET para a API usando o id_param na query string.
        Exemplo de URL: /deforestation?id=<id_param>
        """
        url = f"{self.api_url}?id={id_param}"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados para o id {id_param}: {e}")
            return {}


    def processar(self) -> None:
        """
        Processa todos os IDs extraídos do arquivo: percorre a lista de IDs e faz requisição GET
        para cada um, verificando se o status é COMPLETED (com "analysisResults") no 
        primeiro registro        contido em "data". IDs que não completaram são reprocessados
        após 1 minuto, repetindo o ciclo
        até 10 vezes. IDs com status ERROR são imediatamente adicionados à lista de erros. Ao final,
        os resultados bem-sucedidos são salvos em um arquivo JSON e os IDs com erro são exportados
        em um CSV com os detalhes.
        """
        resultados: dict[str, list] = {"data": []}
        total_registros = len(self.df)
        print(f"Iniciando busca de dados para {total_registros} registros...")

        # Cria a lista de IDs pendentes: cada item é um dicionário contendo
        # 'index', 'id', 'attempts' (inicialmente 0) e 'last_status'
        self.pending_ids = []
        for index, id_value in self.df[self.id_column].items():
            if pd.isna(id_value):
                print(f"Ignorando registro {index} pois o id está vazio.")
                continue
            self.pending_ids.append(
                {"index": index, "id": str(id_value), "attempts": 0, "last_status": ""}
            )

        max_iteracoes = 10
        iteracao = 0

        while self.pending_ids and iteracao < max_iteracoes:
            iteracao += 1
            print(
                f"\n=== Iteração {iteracao}: Tentando {len(self.pending_ids)} IDs pendentes ==="
            )
            new_pending = []

            for item in self.pending_ids:
                item["attempts"] += 1
                id_str = item["id"]
                index = item["index"]
                print(
                    f"Processando ID {id_str} (registro {index}), tentativa {item['attempts']}..."
                )
                resposta = self.buscar_dados(id_str)

                if resposta and "data" in resposta and len(resposta["data"]) > 0:
                    registro = resposta["data"][0]
                    task_list = registro.get("Task", [])
                    if task_list:
                        status = task_list[0].get("status", "").upper()
                        item["last_status"] = status
                        if (
                            status == "COMPLETED"
                            and registro.get("analysisResults") is not None
                        ):
                            print(f"ID {id_str} (registro {index}) COMPLETADO com sucesso.")
                            resultados["data"].append(resposta)
                        elif status in ["STARTING", "PROCESSING"]:
                            print(
                                f"ID {id_str} (registro {index}) está em status {status}."
                            )
                            new_pending.append(item)
                        elif status == "ERROR":
                            print(
                                f"ID {id_str} (registro {index}) retornou ERROR. "
                                "Não será reprocessado."
                            )
                            item["last_status"] = status
                            self.error_ids.append(item)
                        else:
                            print(
                                f"ID {id_str} (registro {index}) retornou status "
                                f"inesperado: {status}."
                            )
                            new_pending.append(item)
                    else:
                        if registro.get("analysisResults") is not None:
                            print(
                                f"ID {id_str} (registro {index}) COMPLETADO com sucesso (sem Task)."
                            )
                            resultados["data"].append(resposta)
                        else:
                            print(
                                f"ID {id_str} (registro {index}) não possui 'Task' nem "
                                "'analysisResults'."
                            )
                            new_pending.append(item)
                else:
                    print(f"Nenhuma resposta válida para o ID {id_str} (registro {index}).")
                    new_pending.append(item)

            # Atualiza a lista de pendentes
            self.pending_ids = new_pending

            if self.pending_ids:
                print(
                    f"\nAguardando 1 minuto antes da próxima tentativa para "
                    f"{len(self.pending_ids)} IDs pendentes...\n"
                )
                time.sleep(60)

        if self.pending_ids:
            print(
                f"\nApós {max_iteracoes} iterações, "
                f"{len(self.pending_ids)} IDs não foram concluídos."
            )
            # IDs restantes são adicionados à lista de erros
            self.error_ids.extend(self.pending_ids)

        # Salva os resultados bem-sucedidos em um arquivo JSON
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(resultados, f, ensure_ascii=False, indent=4)
            print(f"\nArquivo JSON salvo em {self.output_file}")
        except (IOError, OSError) as e:
            print(f"Erro ao salvar o arquivo JSON: {e}")

        # Se houver IDs com erro, exporta um CSV com eles na pasta raiz com detalhes do erro
        if self.error_ids:
            error_df = pd.DataFrame(self.error_ids)
            error_csv = "errors.csv"
            try:
                error_df.to_csv(error_csv, index=False)
                print(f"Arquivo CSV de erros salvo em {error_csv}")
            except (IOError, OSError, pd.errors.EmptyDataError) as e:
                print(f"Erro ao salvar o CSV de erros: {e}")


# Exemplo de utilização:
if __name__ == "__main__":
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if ACCESS_TOKEN is None:
        raise ValueError("ACCESS_TOKEN environment variable not set")

    API_URL = f"{os.getenv('API_BASE_URL')}/deforestation"

    FILE_PATH = "output/Tropoc_Geo_2024_v1_updated.xlsx"  # Pode ser Excel ou CSV
    OUTPUT_FILE = "output/resultados.json"
    ID_COLUMN = "deforestation_2004_2023"  # Coluna que contém os IDs a serem consultados

    fetcher = DeforestationIDFetcher(
        access_token=ACCESS_TOKEN,
        api_url=API_URL,
        file_path=FILE_PATH,
        output_file=OUTPUT_FILE,
        id_column=ID_COLUMN,
    )
    fetcher.processar()
