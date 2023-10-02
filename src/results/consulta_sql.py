

def spark_read_from_sql(spark):
    """Função responsável pela query do SQL Server que retorna os dados requeridos pelos economistas. Metadados disponíveis no arquivo columns_metadata.json em Documentation # noqa E501
        Args:
            spark: Variável de Spark Session, Configurado no arquivo main.py .
    """
    # Query para ler atracacao_fato e gerar uma spark sql view
    with open('src/querys/select_view_antaq.sql','r',encoding='utf-8') as f:
        query_view = f.read()

    dados_spark = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:sqlserver://localhost:1433;databaseName=master") \
    .option("dbtable", f"({query_view}) AS consulta") \
    .option("user", "user") \
    .option("password", "password") \
    .option("numPartitions", 10) \
    .load()

    dados_spark.createOrReplaceTempView("AtracacoesNordesteCeara")

    # Query para ler a view gerada pelo spark sql e gerar o resultado em Excel para os economistas  # noqa E501
    with open('src/querys/select_result_antaq.sql','r',encoding='utf-8') as f:
        query = f.read()

    # Leitura da view
    dados_spark = spark.sql(query)

    # Escrevendo dados em Excel
    dados_spark.write \
    .format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Report - Análise Antaq'!A1") \
    .option("Header", "true") \
    .mode("overwrite") \
    .save("Antaq_report/ReportAntaq.xlsx")

    return dados_spark
