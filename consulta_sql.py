import requests
import zipfile
import os
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col
import pymongo
import threading
from antaq_to_lake import write_to_mongo
from antaq_to_sql import write_to_sql
from pyspark.sql.functions import col, to_date, month, year

# Inicializar o SparkSession
spark = SparkSession.builder.\
    appName("ANTAQDataProcessing").config("spark.mongodb.input.uri", "mongodb://localhost:27017/local") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/local") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config("spark.driver.extraClassPath", "D:\spark-3.3.2-bin-hadoop3\jars\mssql-jdbc-12.4.1.jre11-sources.jar")\
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

# URL base dos arquivos
base_url = "https://web3.antaq.gov.br/ea/txt/"

# Diretório de destino para salvar os arquivos zip e os dados processados
output_dir = "antaq_data"

# Criar o diretório se não existir
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
tabelas_e_colunas = {
    "Atracacao": [
        "IDAtracacao", "CDTUP", "IDBerco", "Berço", "Porto Atracação",
        "Apelido Instalação Portuária", "Complexo Portuário", "Tipo da Autoridade Portuária",
        "Data Atracação", "Data Chegada", "Data Desatracação", "Data Início Operação",
        "Data Término Operação",
        "Mês da data de início da operação",
        "Ano da data de início da operação"
    ],
    "Carga": [
"IDCarga", "IDAtracacao" ,"Origem" ,"Destino" ,"CDMercadoria","Tipo Operação da Carga","Carga Geral Acondicionamento","ConteinerEstado","Tipo Navegação", "FlagAutorizacao", 
"FlagCabotagem", "FlagCabotagemMovimentacao","FlagConteinerTamanho","FlagLongoCurso","FlagMCOperacaoCarga", "FlagOffshore"
    ]
}

url_db = "jdbc:sqlserver://localhost:1433;databaseName=master"
# Anos de interesse
anos = range(2023, 2020, -1)    # De 2023 a 2021
# Configurar as propriedades de conexão com o MSSQL
spark.conf.set("spark.datasource.jdbc.url", "jdbc:sqlserver://localhost:1433;databaseName=master")
spark.conf.set("spark.datasource.jdbc.driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
spark.conf.set("spark.datasource.jdbc.username", "UserData")
spark.conf.set("spark.datasource.jdbc.password", "DataUser")

spark.sql('Select * from Cargafato').show()

