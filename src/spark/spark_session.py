import findspark
from pyspark.sql import SparkSession
from os import path

from sqlalchemy import table

from src.utils.constants import \
  drivers_path,\
  pgsql_driver_filename, \
  query_volume_import_roll_up, \
  query_average_prices_roll_up, \
  query_imports_semester_slice, \
  query_country_imports_pivot \

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

    print('----> [SPARK] Iniciando sessão OK!\n')

  def get_session(self):
    return self.spark

  def get_query_volume_import_roll_up(self):
    volume_import_roll_up_df = self.spark.read.jdbc( \
      url = 'jdbc:postgresql://localhost:5432/pad_constellation',
      table = query_volume_import_roll_up,
      properties = { \
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
      }
    )

    return volume_import_roll_up_df

  def get_query_average_prices_roll_up(self):
    average_prices_roll_up_df = self.spark.read.jdbc( \
      url = 'jdbc:postgresql://localhost:5432/pad_constellation',
      table = query_average_prices_roll_up,
      properties = { \
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
      }
    )

    return average_prices_roll_up_df

  def get_query_imports_semester_slice(self):
    query_imports_semester_slice_df = self.spark.read.jdbc( \
      url = 'jdbc:postgresql://localhost:5432/pad_constellation',
      table = query_imports_semester_slice,
      properties = { \
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
      }
    )

    return query_imports_semester_slice_df

  def get_query_country_imports_pivot(self):
    query_country_imports_pivot_df = self.spark.read.jdbc( \
      url = 'jdbc:postgresql://localhost:5432/pad_constellation',
      table = query_country_imports_pivot,
      properties = { \
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
      }
    )

    return query_country_imports_pivot_df