"""
Args:
    Código que contém função para inserir dados no SQL Server
"""


def write_to_sql(df_selecionados, url_db):
    """

    Args:
        df_selecionados (SparkDataframe): Spark Dataframe a ser escrevido no SQL Server. Detalhe para utilização do método JBDC dp spark Write. # noqa E501
        url_db (str): Url do banco de dados a ser inserido .
    """

    for df_selecionado, file_name in df_selecionados:
        df_selecionado.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", url_db) \
            .option("dbtable", f"{file_name}_fato") \
            .option("user", "DataUser") \
            .option("password", "userdata") \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .option("trustServerCertificate", "true")\
            .save()
        print(f'Tabela {file_name} teve dados inseridos')
