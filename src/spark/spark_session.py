import findspark
from pyspark.sql import SparkSession
from os import path

from src.utils.constants import drivers_path, pgsql_driver_filename

class SparkDF:
  def __init__(self):
    print('----> [SPARK] Iniciando sessão\n')

    findspark.init()
    self.spark = SparkSession \
          .builder \
          .appName("Relação importações / preços de combustíveis no Brasil") \
          .master("local[2]") \
          .config("spark.jars", path.join(drivers_path, pgsql_driver_filename)) \
          .getOrCreate()

    self.imports_df = self.spark.read.format("jdbc") \
        .options(
                url='jdbc:postgresql://localhost:5432/pad_constellation',
                dbtable='fimportacoes',
                user='postgres',
                driver='org.postgresql.Driver') \
        .load()

    self.prices_df = self.spark.read.format("jdbc") \
            .options(
                    url='jdbc:postgresql://localhost:5432/pad_constellation',
                    dbtable='fprecos',
                    user='postgres',
                    driver='org.postgresql.Driver') \
            .load()

    self.date_df = self.spark.read.format("jdbc") \
            .options(
                    url='jdbc:postgresql://localhost:5432/pad_constellation',
                    dbtable='ddata',
                    user='postgres',
                    driver='org.postgresql.Driver') \
            .load()

    self.products_df = self.spark.read.format("jdbc") \
            .options(
                    url='jdbc:postgresql://localhost:5432/pad_constellation',
                    dbtable='dproduto',
                    user='postgres',
                    driver='org.postgresql.Driver') \
            .load()

    self.state_df = self.spark.read.format("jdbc") \
            .options(
                    url='jdbc:postgresql://localhost:5432/pad_constellation',
                    dbtable='duf',
                    user='postgres',
                    driver='org.postgresql.Driver') \
            .load()

    self.country_df = self.spark.read.format("jdbc") \
            .options(
                    url='jdbc:postgresql://localhost:5432/pad_constellation',
                    dbtable='dpais',
                    user='postgres',
                    driver='org.postgresql.Driver') \
            .load()

    print('----> [SPARK] Obtendo as tabelas via PostgreSQL OK!\n')

    print('----> [SPARK] Iniciando sessão OK!\n')

  def get_session(self):
    return self.spark

  def get_imports_df(self):
    return self.imports_df

  def get_prices_df(self):
    return self.prices_df

  def get_date_df(self):
    return self.date_df

  def get_products_df(self):
    return self.products_df

  def get_state_df(self):
    return self.state_df

  def get_country_df(self):
    return self.country_df
