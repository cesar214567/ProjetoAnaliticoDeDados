from src.spark.spark_session import SparkDF
from src.etl.transform import Transformer
from src.etl.load import Loader

if __name__ == "__main__":
  transformer = Transformer()
  # loader = Loader()
  transformer.transform_data()

  print(transformer.get_importacoes())
  print(transformer.get_precos())
  print(transformer.get_produtos())
  print(transformer.get_uf())

  loader.loadTables()

  sparkDF = SparkDF()
  
  sparkDF.get_query_volume_import_roll_up().show()
  sparkDF.get_query_average_prices_roll_up().show()
  sparkDF.get_query_imports_semester_slice().show()
  sparkDF.get_query_country_imports_pivot().show()
  sparkDF.get_query_prices_imports_drill_across().show()
  