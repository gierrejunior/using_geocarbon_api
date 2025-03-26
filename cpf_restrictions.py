"""
Módulo para checagem de restrições de CPF ou CNPJ via API.

Este módulo define a classe RestrictionChecker, que integra a funcionalidade de
requisições à API com o processamento de arquivos CSV ou Excel. Para cada
registro presente na coluna escolhida, realiza-se os seguintes passos:
    1. Limpeza dos dados: remove todos os caracteres não numéricos.
    2. Validação: verifica se o documento limpo possui o número correto de dígitos
        (11 para CPF, 14 para CNPJ).
    3. Envio da requisição GET para o endpoint
        /restriction/check-restrictions com o parâmetro:
            ?<cpf|cnpj>=<documento_limpo>
        onde a chave do documento dependerá do tipo definido (CPF ou CNPJ).

Se o documento for inválido (ex.: número incorreto de dígitos), o registro é
marcado com erro e não é enviado à API. Ao final, os resultados de cada requisição
são salvos em um arquivo JSON.

Uso:
    1. Certifique-se de que as variáveis de ambiente (ACCESS_TOKEN e API_BASE_URL)
        estejam definidas em um arquivo .env.
    2. Ajuste os parâmetros:
            - FILE_PATH: Caminho para o arquivo com os documentos (CSV ou Excel).
            - OUTPUT_FILE: Caminho para salvar o arquivo JSON com os resultados.
            - ID_COLUMN: Nome da coluna que contém os documentos (CPF ou CNPJ).
            - DOCUMENT_TYPE: Tipo de documento ("CPF" ou "CNPJ").
    3. Execute o módulo para processar os dados e salvar os resultados.
"""

import json
import os

import pandas as pd  # type: ignore
import requests  # type: ignore
from dotenv import load_dotenv  # type: ignore

from tools.tools import APIClient, CSVProcessor, DocumentValidator

# Carrega as variáveis de ambiente
load_dotenv()


class RestrictionChecker(APIClient, CSVProcessor):
    """
    Classe para checar restrições de CPF ou CNPJ via API.

    Para cada registro, o documento (CPF ou CNPJ) é limpo e validado. Se válido,
    envia uma requisição GET para o endpoint
    /restriction/check-restrictions com o parâmetro:
            ?<cpf|cnpj>=<documento_limpo>
    O resultado de cada requisição é armazenado e, ao final, exportado para um
    arquivo JSON.
    """

    def __init__(
        self,
        access_token: str,
        api_base_url: str,
        file_path: str,
        output_file: str,
        id_column: str,
        document_type: str,
    ):
        # Constrói a URL base para o endpoint de restrição
        endpoint = "/restriction/check-restrictions"
        APIClient.__init__(self, access_token, api_url=f"{api_base_url}{endpoint}")
        CSVProcessor.__init__(self, file_path, id_column)
        self.output_file = output_file
        self.id_column = id_column
        self.document_type = document_type.upper()  # "CPF" ou "CNPJ"

    def processar(self) -> None:
        """
        Processa os documentos do arquivo, limpando, validando e enviando a
        requisição GET para cada registro. Registra os resultados (ou erros de
        validação) e os salva em um arquivo JSON.
        """
        resultados: dict[str, list] = {"data": []}
        total_registros = len(self.df)
        print(f"Iniciando processamento de {total_registros} registros...")

        # Determina a chave do documento no parâmetro com base no tipo
        document_key = "cpf" if self.document_type == "CPF" else "cnpj"

        for index, doc in self.df[self.id_column].items():
            if pd.isna(doc):
                print(f"Ignorando registro {index} pois o documento está vazio.")
                continue

            # Limpa o documento removendo caracteres não numéricos
            cleaned_doc = DocumentValidator.clean_document(str(doc))
            # Valida a quantidade de dígitos
            if not DocumentValidator.is_valid(cleaned_doc, self.document_type):
                error_msg = (
                    f"Documento inválido para o registro {index}: '{doc}' "
                    f"(limpo: '{cleaned_doc}') não possui o número correto de dígitos "
                    f"para {self.document_type}."
                )
                print(error_msg)
                resultados["data"].append(
                    {
                        "index": index,
                        "original": doc,
                        "cleaned": cleaned_doc,
                        "error": error_msg,
                    }
                )
                continue

            # Monta a URL com o parâmetro do documento
            url = f"{self.api_url}?{document_key}={cleaned_doc}"
            try:
                response = requests.get(url, headers=self.headers, timeout=60)
                try:
                    response_data = response.json()
                except json.JSONDecodeError:
                    response_data = {"error": "Resposta não é um JSON válido."}

                if response.status_code in [200, 201, 204]:
                    print(
                        f"Registro {index} ({self.document_type}: {cleaned_doc}) "
                        "processado com sucesso."
                    )
                else:
                    print(
                        f"Falha para o {self.document_type} {cleaned_doc}: {response_data}"
                    )

                resultados["data"].append(
                    {
                        "index": index,
                        "status_code": response.status_code,
                        "document": str(doc),
                        "document_type": self.document_type,
                        "response": response_data.get("data").get("hasRestrictions"),
                    }
                )

            except requests.exceptions.RequestException as e:
                print(
                    f"Erro de requisição para o {self.document_type} {cleaned_doc}: {e}"
                )
                resultados["data"].append(
                    {
                        "index": index,
                        "document": cleaned_doc,
                        "status_code": None,
                        "error": str(e),
                    }
                )

        # Salva os resultados no arquivo JSON de saída
        try:
            with open(self.output_file, "w", encoding="utf-8") as f:
                json.dump(resultados, f, ensure_ascii=False, indent=4)
            print(f"Arquivo JSON salvo em {self.output_file}")
        except (IOError, OSError) as e:
            print(f"Erro ao salvar o arquivo JSON: {e}")


if __name__ == "__main__":
    # NÃO MODIFICAR
    API_BASE_URL = os.getenv("API_BASE_URL")
    if not API_BASE_URL:
        raise ValueError("API_BASE_URL não definido.")
    ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
    if not ACCESS_TOKEN:
        raise ValueError("ACCESS_TOKEN não definido.")

    # MODIFICAR
    FILE_PATH = "input/Tropoc_Geo_2024_v1.xls"  # Caminho do arquivo
    OUTPUT_FILE = "output/cpf_restrictions.json"  # Caminho para o arquivo JSON de saída
    ID_COLUMN = "CPF_Produtor"  # Coluna que contém os CPF ou CNPJ
    DOCUMENT_TYPE = "CPF"  # Ou "CNPJ", conforme o tipo de documento

    # NÃO MODIFICAR
    processor = RestrictionChecker(
        access_token=ACCESS_TOKEN,
        api_base_url=API_BASE_URL,
        file_path=FILE_PATH,
        output_file=OUTPUT_FILE,
        id_column=ID_COLUMN,
        document_type=DOCUMENT_TYPE,
    )
    processor.processar()
    print("Processamento concluído!")
