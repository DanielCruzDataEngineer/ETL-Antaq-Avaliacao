
def write_to_mongo(df,mongo_uri):
    """
    """

    df.write.format("com.mongodb.spark.sql.DefaultSource") \
                    .option("uri", mongo_uri) \
                    .mode("append").save()