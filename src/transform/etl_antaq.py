"""
Args:
Código que contém funções para fazer querys SQL Spark e gerar views para fazer o ETL
"""

def spark_sql_atracacao(spark):
    """
    Args:
    Função criada para gerar as tabelas requisitadas,
    Foram utilizadas as tabelas temps criadas durante o processo de extração, sendo armazenadas no Hive Metastore Temp do Spark.

    spark_sql_atracacao gera a tabela atracacao e utiliza a variável spark, criada no código main.py
    """
    with open('src/querys/creation_atracacao_fato.sql','r',encoding='utf-8') as f:
        query = f.read()

    atracacao_fato = spark.sql(query)
    return atracacao_fato

def spark_sql_carga(spark):
    """Args:
    Função criada para gerar as tabelas requisitadas,
    Foram utilizadas as tabelas temps criadas durante o processo de extração, sendo armazenadas no Hive Metastore Temp do Spark.
    
    spark_sql_carga gera a tabela carga e utiliza a variável spark, criada no código main.py
    """
    with open('src/querys/creation_carga_fato.sql','r',encoding='utf-8') as f:
        query_carga = f.read()
    carga_fato = spark.sql(query_carga)

    return carga_fato

