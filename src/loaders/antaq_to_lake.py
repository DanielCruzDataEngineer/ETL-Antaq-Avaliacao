"""Args:
Código para escrever dados no mongo. 
Percorre uma tuple, e seleciona o SparkDataframe e o nome da Collection no Mongo # noqa E501
"""


def write_to_mongo(df_selecionados):
    """
    Args:
    Função responsável pela inserção dos dados em DataLake Mongo.
    df_selecionado (tuple): Tuple com nome da Collection no Mongo e Spark Dataframe a ser escrevido no Mongo # noqaE501
    """
    for df_selecionado, file_name in df_selecionados:
        mongo_uri = f"mongodb://localhost:27017/Lake_Antaq.{file_name}_fato"
        df_selecionado.write.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", mongo_uri) \
        .mode("append").save()
        print('Dados Inseridos em Collection no Antaq Lake')
