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

print('----> [SPARK] Obtendo as tabelas via PostgreSQL\n')

imports_sdf = spark.read.format("jdbc") \
        .options(
                url='jdbc:postgresql://localhost:5432/pad_constellation',
                dbtable='fimportacoes',
                user='postgres',
                driver='org.postgresql.Driver') \
        .load()

prices_sdf = spark.read.format("jdbc") \
        .options(
                url='jdbc:postgresql://localhost:5432/pad_constellation',
                dbtable='fprecos',
                user='postgres',
                driver='org.postgresql.Driver') \
        .load()

date_sdf = spark.read.format("jdbc") \
        .options(
                url='jdbc:postgresql://localhost:5432/pad_constellation',
                dbtable='ddata',
                user='postgres',
                driver='org.postgresql.Driver') \
        .load()

products_sdf = spark.read.format("jdbc") \
        .options(
                url='jdbc:postgresql://localhost:5432/pad_constellation',
                dbtable='dproduto',
                user='postgres',
                driver='org.postgresql.Driver') \
        .load()

state_sdf = spark.read.format("jdbc") \
        .options(
                url='jdbc:postgresql://localhost:5432/pad_constellation',
                dbtable='duf',
                user='postgres',
                driver='org.postgresql.Driver') \
        .load()

country_sdf = spark.read.format("jdbc") \
        .options(
                url='jdbc:postgresql://localhost:5432/pad_constellation',
                dbtable='dpais',
                user='postgres',
                driver='org.postgresql.Driver') \
        .load()

print('----> [SPARK] Obtendo as tabelas via PostgreSQL OK!\n')

print('----> [SPARK] Testando as tabelas obtidas\n')

print('FImportacoes:')
imports_sdf.printSchema()
print('FPrecos:')
prices_sdf.printSchema()
print('DData:')
date_sdf.printSchema()
print('DProduto:')
products_sdf.printSchema()
print('DUF:')
state_sdf.printSchema()
print('DPais:')
country_sdf.printSchema()

print('----> [SPARK] Testando as tabelas obtidas OK!\n')
