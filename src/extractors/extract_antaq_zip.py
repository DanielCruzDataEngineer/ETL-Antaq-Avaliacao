"""
Args:
Código que contém funções para extração de dados da Antaq
"""
import zipfile
import os
import requests


def extract_from_antaq(ano, output_dir):
    """Função responsável pela extração dos dados da Antaq.
        Args:
            ano (int): Ano, Referência do arquivo a ser extraído.
            output_dir (str): Diretório a ser gerado durante a extração.
            Caso seu diretório não exista, será criado
       """
    # Criar o diretório se não existir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_name_zip = str(ano) + '.zip'
    url = f'https://web3.antaq.gov.br/ea/txt/{ano}.zip'
    response = requests.get(url, timeout=10)
    with open(os.path.join(output_dir, file_name_zip), "wb") as f:
        f.write(response.content)
        print(f'Extração do {file_name_zip} concluída')
    with zipfile.ZipFile(os.path.join(output_dir,  file_name_zip), "r") as zip_ref:  # noqa E501
        zip_ref.extractall(output_dir)
