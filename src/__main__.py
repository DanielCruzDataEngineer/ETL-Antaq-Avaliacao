"""Args:
Código que contém todo o fluxo do ETL Antaq
Rode python src/ para executa-lo
"""
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
    .config('spark.jars.packages', 'com.crealytics:spark-excel_2.12:3.3.1_0.18.5,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config("spark.driver.extraClassPath", "drivers/mssql-jdbc-6.4.0.jre8.jar")\
    .getOrCreate()


# Diretório de destino para salvar os arquivos zip e os dados processados
output_dir = "antaq_data"

# Dict com as tabelas. Chave - Nome da tabela Atracacao_fato, e Carga_fato, e seus itens, tabelas necessárias pro fluxo de ETL. # noqa E501
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

# Aqui é o início do loop para automação elegante. A cada referência, irá extrair ps dadps da Antaq, fazer os ETLs e armazenar as tabelas no Mongo e SQL Server # noqa E501

for ano in anos:
    # Extraindo dados da Antaq
    extract_from_antaq(ano, output_dir)
    for tabela, file_names in tabelas_e_colunas.items():
        for file_name in file_names:
            # Leitura dos dados em Txt
            df = spark.read.csv(f"antaq_data/{ano}{file_name}.txt", sep=";", header=True) # noqa E501

            # Criando uma view para cada tabela
            df.createOrReplaceTempView(f"{file_name}")

    #  Criação de Views Spark para cada tabela, etapa de ETL
    atracacao_fato = spark_sql_atracacao(spark)
    carga_fato = spark_sql_carga(spark)
    # Dict para inserção de dados, contém Spark DataFrame e nome da tabela
    parameters_sql = [(atracacao_fato,"atracacao"),(carga_fato,"carga")] # noqa E501

    write_to_mongo(parameters_sql)
    write_to_sql(parameters_sql,url_db)

# Gerando resultado final, report para os economistas.
spark_read_from_sql(spark)
