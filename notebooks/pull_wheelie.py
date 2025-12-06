# Instead of os.environ.get(), use dbutils.secrets.get()
c = spark.read.format("jdbc") \
    .option("url", f"jdbc:mysql://{dbutils.secrets.get('wheelie', 'MYSQL_HOST')}/{dbutils.secrets.get('wheelie', 'MYSQL_DB')}") \
    .option("user", dbutils.secrets.get('wheelie', 'MYSQL_USERNAME')) \
    .option("password", dbutils.secrets.get('wheelie', 'MYSQL_PASSWORD'))