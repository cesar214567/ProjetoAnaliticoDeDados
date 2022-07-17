from src.spark.spark_session import SparkDF

def testing_spark_session():
  print('----> [SPARK] Obtendo as tabelas via PostgreSQL\n')
  spark = SparkDF()

  print('----> [SPARK] Testando as tabelas obtidas\n')

  print('FImportacoes:')
  spark.get_imports_df().printSchema()
  print('FPrecos:')
  spark.get_prices_df().printSchema()
  print('DData:')
  spark.get_date_df().printSchema()
  print('DProduto:')
  spark.get_products_df().printSchema()
  print('DUF:')
  spark.get_state_df().printSchema()
  print('DPais:')
  spark.get_country_df().printSchema()

  print('----> [SPARK] Testando as tabelas obtidas OK!\n')
