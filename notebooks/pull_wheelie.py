c = spark.read.format("jdbc") \
    .option("url", f"jdbc:mysql://{os.env.get('MYSQL_HOST')}/{os.env.get('MYSQL_DB')}") \
    .option("user", os.env.get('MYSQL_USERNAME')) \
    .option("password", os.env.get('MYSQL_PASSWORD'))
