
def write_to_sql(df_selecionados,url_db):

    for df_selecionado, file_name in df_selecionados:
        df_selecionado.write \
            .format("jdbc") \
            .mode("append") \
            .option("url", url_db) \
            .option("dbtable", f"{file_name}_fato") \
            .option("user", "DataUser") \
            .option("password", "userdata") \
            .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")\
            .option("trustServerCertificate","true")\
            .save()
