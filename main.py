from src.spark.testing_spark import testing_spark_session
from src.etl.transform import Transformer

if __name__ == "__main__":
  transformer = Transformer()
  transformer.transform_data()

  testing_spark_session()
  print(transformer.get_importacoes())
  print(transformer.get_precos())
  print(transformer.get_produtos())
  
