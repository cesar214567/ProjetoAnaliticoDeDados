import findspark
from pyspark.sql import SparkSession
from os import path

from constants import drivers_path, pgsql_driver_filename

print('----> [SPARK] Iniciando sessão\n')

findspark.init()
spark = SparkSession \
        .builder \
        .appName("Relação importações / preços de combustíveis no Brasil") \
        .master("local[2]") \
        .config("spark.jars", path.join(drivers_path, pgsql_driver_filename)) \
        .getOrCreate()

print('----> [SPARK] Iniciando sessão OK!\n')

prices_df = spark.read.format("jdbc") \
        .options(
                url='jdbc:postgresql://localhost:5432/pad_constellation',
                dbtable='fprecos',
                user='postgres',
                driver='org.postgresql.Driver') \
        .load()

prices_df.printSchema()