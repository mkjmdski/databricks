import os
c = spark.read.format("jdbc") \
    .option("url", f"jdbc:mysql://{os.environ.get('MYSQL_HOST')}/{os.environ.get('MYSQL_DB')}") \
    .option("user", os.environ.get('MYSQL_USERNAME')) \
    .option("password", os.environ.get('MYSQL_PASSWORD'))
