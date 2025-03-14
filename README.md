# Geocarbon API Scripts

Este repositório contém scripts para interagir com a API do Geocarbon. Esses scripts permitem que você acesse e manipule dados relacionados ao carbono geográfico.

## Funcionalidades

- Conectar-se à API do Geocarbon
- Recuperar dados de carbono geográfico
- Manipular e analisar os dados recuperados

## Pré-requisitos

Antes de começar, certifique-se de ter os seguintes itens instalados:

- [Python 3.x](https://www.python.org/downloads/)
- [pip](https://pip.pypa.io/en/stable/installation/)

## Instalação

Siga os passos abaixo para configurar o ambiente e instalar as dependências necessárias:

1. Clone o repositório para o seu ambiente local:

    ```bash
    git clone https://github.com/gierrejunior/using_geocarbon_api.git
    cd geocarbon/using_geocarbon_api
    ```

2. Crie um ambiente virtual (opcional, mas recomendado):

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # No Windows use `venv\Scripts\activate`
    ```

3. Instale as dependências:

    ```bash
    pip install -r requirements.txt
    ```

## Uso

Siga os passos abaixo para usar os scripts:

1. Configure suas credenciais de API:
    - Crie um arquivo `.env` na raiz do diretório `scripts_to_use_api` com o seguinte conteúdo:

        ```env
        ACCESS_TOKEN=your_access_token_here
        API_BASE_URL=https://api.exemplo.com
        ```

2. Execute o script desejado:

    ```bash
    python nome_do_script.py
    ```

    Substitua `nome_do_script.py` pelo nome do script que você deseja executar.

## Descrição dos Scripts

Aqui está uma breve descrição do que cada script faz:

- `tools.py`: Contém classes utilitárias para interagir com a API e processar arquivos CSV ou Excel.
  - `APIClient`: Classe para realizar requisições à API.
  - `CSVProcessor`: Classe para carregar e salvar dados de arquivos CSV ou Excel.

- `get_batch_deforestation_results.py`: Este script busca resultados de desmatamento em lote utilizando uma API. Ele lê IDs de um arquivo CSV ou Excel, faz requisições GET para a API com esses IDs e processa os resultados. IDs que não estão concluídos são reprocessados até 10 vezes, com um intervalo de 1 minuto entre as tentativas. IDs que retornam erro são registrados e exportados em um arquivo CSV de erros.

- `deforestation_batch_request.py`: Este script processa arquivos CSV ou Excel para realizar requisições à API e atualizar os dados conforme os códigos de imóvel (CAR) e os intervalos de anos fornecidos. Ele lê os dados de um arquivo CSV ou Excel, faz requisições POST para a API com esses dados e processa os resultados. Os resultados são exportados em um arquivo CSV.

  ### Como usar

  1. Configure suas credenciais de API no arquivo `.env`.
  2. Execute o script:

      ```bash
      python3 get_batch_deforestation_results.py
      ```

      ou

     ```bash
      python3 deforestation_batch_request.py
      ```
