from src.etl.extract import Extractor
import pandas as pd
from os import path
from data_generator import generate_year_month_map, map_year_month, write_data, map_year_month_number
from src.utils.constants import base_csv_data_path, gen_ddata_filename, derivados_to_exclude

def transform_price_unit(row):
  if row[6] > 100:
    row[6] = row[6] / 100
  if row[7] > 100:
    row[7] = row[7] / 100
  if row[8] > 100:
    row[8] = row[8] / 100

  if row[4] == 'R$/l':
    row[6] = row[6] / 1000
    row[7] = row[7] / 1000
    row[8] = row[8] / 1000

  if row[4] == 'R$/13Kg':
    row[6] = row[6] / 5.4
    row[7] = row[7] / 5.4
    row[8] = row[8] / 5.4

  return row

class Transformer:
  def __init__(self):
    extractor = Extractor()

    self.precos_df = extractor.get_precos_df()
    self.usd_brl_df = extractor.get_usd_brl_df()
    self.importacoes_df = extractor.get_importacoes_df()
    self.produtos_df = None

  def __transform_importacoes__(self):
    # Remove e corrige valores
    self.importacoes_df.drop(columns = 'Código NCM')
    self.importacoes_df.replace({'Gasóleo (óleo diesel)': 'ÓLEO DIESEL'}, inplace=True)
    self.importacoes_df.replace({'Gás liquefeito de petróleo (glp)': 'GLP'}, inplace=True)
    self.importacoes_df.replace({'Álcool etílico não desnaturado, com volume de teor alcoólico >= 80%': 'ETANOL HIDRATADO'}, inplace=True)
    self.importacoes_df.replace({'Outras gasolinas, exceto para aviação': 'GASOLINA'}, inplace=True)

    # Renomeia colunas    
    self.importacoes_df.rename(columns={'Ano': 'ANO', 'Mês': 'MES', 'Países': 'PAIS', 'UF do Produto': 'UF', 'Descrição NCM': 'PRODUTO', 'Valor FOB (US$)': 'VALOR FOB', 'Quantidade Estatística': 'VOLUME'}, inplace=True)


  def __transform_precos__(self):
    # Remove e corrige valores
    self.precos_df.drop(columns = 'NÚMERO DE POSTOS PESQUISADOS', inplace=True)
    self.precos_df.drop(columns = 'PREÇO MÁXIMO REVENDA', inplace=True)
    self.precos_df.drop(columns = 'MARGEM MÉDIA REVENDA', inplace=True)
    self.precos_df.drop(columns = 'COEF DE VARIAÇÃO REVENDA', inplace=True)
    self.precos_df.drop(columns = 'PREÇO MÉDIO DISTRIBUIÇÃO', inplace=True)
    self.precos_df.drop(columns = 'DESVIO PADRÃO DISTRIBUIÇÃO', inplace=True)
    self.precos_df.drop(columns = 'PREÇO MÍNIMO DISTRIBUIÇÃO', inplace=True)
    self.precos_df.drop(columns = 'PREÇO MÁXIMO DISTRIBUIÇÃO', inplace=True)
    self.precos_df.drop(columns = 'COEF DE VARIAÇÃO DISTRIBUIÇÃO', inplace=True)
    self.precos_df.drop(self.precos_df[self.precos_df['PRODUTO'] == 'GNV'].index, inplace=True)

    self.precos_df.replace(regex={'OLEO': 'ÓLEO'}, inplace=True)
    self.precos_df.replace({'GASOLINA ADITIVADA': 'GASOLINA'}, inplace=True)
    self.precos_df.replace({'GASOLINA COMUM': 'GASOLINA'}, inplace=True)
    self.precos_df.replace({'ÓLEO DIESEL S10': 'ÓLEO DIESEL'}, inplace=True)

    # Transforma os preços
    self.precos_df.apply(transform_price_unit, axis = 1)

    # Remove colunas restantes
    self.precos_df.drop(columns = 'UNIDADE DE MEDIDA', inplace=True)
    self.precos_df.drop(columns = 'REGIÃO', inplace=True)


  def __transform_importacoes_dates__(self):
    ano_mes = zip(self.importacoes_df['ANO'], self.importacoes_df['MES'])
    
    new_column = list(map(map_year_month_number, ano_mes))
    self.importacoes_df = self.importacoes_df.assign(datapk = new_column)

  
  def __transform_precos_dates__(self):
    ano_mes = zip(self.precos_df['ANO'], self.precos_df['MES'])
    new_column = list(map(map_year_month, ano_mes))
    self.precos_df = self.precos_df.assign(datapk = new_column)
    
  def __filter_produtos_from_tables__(self):
    products_list = list(enumerate(self.importacoes_df['PRODUTO'].unique(), start=1))
    self.produtos_df = pd.DataFrame(products_list, columns =['produtopk', 'name'])

  # def __transform_precos_and_importacoes_by_product__(self):
  #   print(self.importacoes_df)
  #   print(self.precos_df)

  def transform_data(self):
    print('----> Transformando dados\n')

    self.__transform_importacoes__()
    self.__transform_precos__()

    # #map year and month to DData Id
    generate_year_month_map()
    self.__transform_importacoes_dates__()
    # self.__transform_precos_dates__()

    # self.__transform_precos_and_importacoes_by_product__()
    
    # #create and map all produtos
    self.__filter_produtos_from_tables__()
    
    # csv's
    self.generate_date_csv()

    print('----> Transformando dados OK!\n')

  def get_importacoes(self):
    return self.importacoes_df

  def get_precos(self):
    return self.precos_df
  
  def get_produtos(self):
    return self.produtos_df

  def generate_date_csv(self):
    write_data(path.join(base_csv_data_path, gen_ddata_filename))