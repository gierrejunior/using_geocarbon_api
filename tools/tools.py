"""
Este módulo contém ferramentas para interagir com uma API de desmatamento e processar arquivos CSV ou Excel.

Classes:
    APIClient: Classe para realizar requisições à API de desmatamento.
    CSVProcessor: Classe para carregar e salvar dados de arquivos CSV ou Excel.

Exemplos de uso:
    # Criar uma instância do cliente da API
    api_client = APIClient(access_token="seu_token", api_url="https://api.exemplo.com")

    # Enviar uma requisição para a API
    resposta = api_client.enviar_requisicao(payload={"chave": "valor"})

    # Processar um arquivo CSV
    csv_processor = CSVProcessor(file_path="caminho/para/arquivo.csv", car_column="nome_da_coluna")
    dados = csv_processor.df

    # Salvar os dados processados em um novo arquivo
    csv_processor.salvar_dados(output_file="caminho/para/arquivo_saida.csv")
"""

import os

import pandas as pd  # type: ignore
import requests  # type: ignore

# Cria os diretórios "input" e "output" se não existirem
os.makedirs("input", exist_ok=True)
os.makedirs("output", exist_ok=True)


class APIClient:
    """
    Classe para realizar requisições à API de desmatamento.
    """

    def __init__(self, access_token: str, api_url: str):
        self.access_token = access_token
        self.api_url = api_url
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    def enviar_requisicao(self, payload: dict) -> requests.Response:
        """
        Envia uma requisição POST para a API com o payload fornecido.
        """
        try:
            response = requests.post(
                self.api_url, json=payload, headers=self.headers, timeout=10
            )
            return response
        except requests.exceptions.RequestException as e:
            print(f"Erro ao enviar requisição: {e}")
            raise


class CSVProcessor:
    """
    Classe para carregar e salvar dados de arquivos CSV ou Excel.
    """

    def __init__(self, file_path: str, car_column: str):
        self.file_path = file_path
        self.car_column = car_column
        self.df = self.carregar_dados()

    def carregar_dados(self) -> pd.DataFrame:
        """
        Carrega os dados do arquivo. Detecta o tipo do arquivo pela extensão.
        """
        try:
            if self.file_path.lower().endswith(".csv"):
                df = pd.read_csv(self.file_path)
            elif self.file_path.lower().endswith(".xls"):
                df = pd.read_excel(self.file_path, engine="xlrd")
            elif self.file_path.lower().endswith(".xlsx"):
                df = pd.read_excel(self.file_path, engine="openpyxl")
            else:
                raise ValueError("Formato de arquivo não suportado.")

            if self.car_column not in df.columns:
                raise ValueError(
                    f"Coluna '{self.car_column}' não encontrada no arquivo."
                )
            print(f"Arquivo {self.file_path} carregado com sucesso!")
            return df
        except Exception as e:
            print(f"Erro ao carregar dados: {e}")
            raise

    def salvar_dados(self, output_file: str) -> None:
        """
        Salva o DataFrame atualizado em um novo arquivo, respeitando a extensão.
        """
        try:
            if output_file.lower().endswith(".csv"):
                self.df.to_csv(output_file, index=False)
            else:
                self.df.to_excel(output_file, index=False)
            print(f"Arquivo atualizado salvo em {output_file}")
        except Exception as e:
            print(f"Erro ao salvar arquivo: {e}")
            raise


class DocumentValidator:
    """
    Classe auxiliar para limpeza e validação de documentos.
    """

    @staticmethod
    def clean_document(doc: str) -> str:
        """Remove todos os caracteres não numéricos do documento."""
        return "".join(filter(str.isdigit, doc))

    @staticmethod
    def is_valid(doc: str, document_type: str) -> bool:
        """
        Verifica se o documento limpo possui a quantidade correta de dígitos.
        CPF deve ter 11 dígitos e CNPJ 14 dígitos.
        """
        cleaned = DocumentValidator.clean_document(doc)
        if document_type.upper() == "CPF":
            return len(cleaned) == 11
        elif document_type.upper() == "CNPJ":
            return len(cleaned) == 14
        else:
            raise ValueError("Tipo de documento inválido. Use 'CPF' ou 'CNPJ'.")
