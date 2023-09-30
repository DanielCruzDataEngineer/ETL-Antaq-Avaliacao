import requests
import os
import zipfile


def extract_from_antaq(ano,output_dir):
    file_name_zip = str(ano) +  '.zip'
    url = f'https://web3.antaq.gov.br/ea/txt/{ano}.zip'

            
    response = requests.get(url,timeout=10)
    with open(os.path.join(output_dir, file_name_zip), "wb") as f:
            f.write(response.content)
            
    with zipfile.ZipFile(os.path.join(output_dir,  file_name_zip), "r") as zip_ref:
            zip_ref.extractall(output_dir)
    
   