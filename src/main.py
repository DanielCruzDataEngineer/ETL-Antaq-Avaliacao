from pyspark.sql import SparkSession
from loaders.antaq_to_lake import write_to_mongo
from loaders.antaq_to_sql import write_to_sql
from extractors.extract_antaq_zip import extract_from_antaq
from transform.etl_antaq import spark_sql_atracacao, spark_sql_carga
from results.consulta_sql import spark_read_from_sql

# Inicializar o SparkSession
spark = SparkSession.builder.\
    appName("ANTAQDataProcessing").config("spark.mongodb.input.uri", "mongodb://localhost:27017/local") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/local") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    .config("spark.driver.extraClassPath", "mssql-jdbc-6.4.0.jre8.jar")\
    .getOrCreate()

# Diretório de destino para salvar os arquivos zip e os dados processados
output_dir = "antaq_data"

tabelas_e_colunas = {
    "Atracacao": [
        "Atracacao",
        "TemposAtracacao"
    ],
    "Carga": [
        "Carga",
        "Carga_Conteinerizada"
    ]
}

url_db = "jdbc:sqlserver://localhost:1433;databaseName=master"

# Anos de interesse
# De 2023 a 2021 + Trazendo ano de 2020 para complementar possível histórico
anos = range(2023, 2019, -1)

for ano in anos:



    for tabela, file_names in tabelas_e_colunas.items():
        for file_name in file_names:

            df = spark.read.csv(f"antaq_data/{ano}{file_name}.txt", sep=";", header=True)  # noqa E501

            df.createOrReplaceTempView(f"{file_name}")


spark_read_from_sql(spark)
