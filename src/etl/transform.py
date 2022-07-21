from tabnanny import check
from src.etl.extract import Extractor
import pandas as pd
from os import path
from data_generator import generate_year_month_map, write_data, map_year_month_number
from src.utils.constants import base_csv_data_path, gen_ddata_filename, derivados_to_exclude, dollar_real_price
from src.utils.regions import get_uf_acronym, get_uf_region
from datetime import datetime
import time
import calendar
import math

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
    self.importacoes_df.drop(columns = ['Código NCM'])
    self.importacoes_df.replace({'Gasóleo (óleo diesel)': 'ÓLEO DIESEL'}, inplace=True)
    self.importacoes_df.replace({'Gás liquefeito de petróleo (glp)': 'GLP'}, inplace=True)
    self.importacoes_df.replace({'Álcool etílico não desnaturado, com volume de teor alcoólico >= 80%': 'ETANOL HIDRATADO'}, inplace=True)
    self.importacoes_df.replace({'Outras gasolinas, exceto para aviação': 'GASOLINA'}, inplace=True)

    # Renomeia colunas    
    self.importacoes_df.rename(columns={'Ano': 'ANO', 'Mês': 'MES', 'Países': 'PAIS', 'UF do Produto': 'UF', 'Descrição NCM': 'PRODUTO', 'Valor FOB (US$)': 'VALOR FOB', 'Quantidade Estatística': 'VOLUME'}, inplace=True)

    self.importacoes_df["VALOR FOB"] = self.importacoes_df["VALOR FOB"].apply(lambda x: x * dollar_real_price)


  def delete_column_from_dataframe(self,dataframe, columns):
    dataframe.drop(columns = columns,inplace = True)          
  def __transform_precos__(self):
    # Remove e corrige valores
    columns_to_remove = [
    'NÚMERO DE POSTOS PESQUISADOS',
    'PREÇO MÁXIMO REVENDA',
    'MARGEM MÉDIA REVENDA',
    'COEF DE VARIAÇÃO REVENDA',
    'PREÇO MÉDIO DISTRIBUIÇÃO',
    'DESVIO PADRÃO DISTRIBUIÇÃO',
    'PREÇO MÍNIMO DISTRIBUIÇÃO',
    'PREÇO MÁXIMO DISTRIBUIÇÃO',
    'COEF DE VARIAÇÃO DISTRIBUIÇÃO',      
    
    ]
    self.delete_column_from_dataframe(self.precos_df,columns_to_remove)

    self.precos_df.drop(self.precos_df[self.precos_df['PRODUTO'] == 'GNV'].index, inplace=True)

    self.precos_df.replace(regex={'OLEO': 'ÓLEO'}, inplace=True)
    self.precos_df.replace({'GASOLINA ADITIVADA': 'GASOLINA'}, inplace=True)
    self.precos_df.replace({'GASOLINA COMUM': 'GASOLINA'}, inplace=True)
    self.precos_df.replace({'ÓLEO DIESEL S10': 'ÓLEO DIESEL'}, inplace=True)

    # Transforma os preços
    self.precos_df.apply(transform_price_unit, axis = 1)
    columns_to_remove = ['UNIDADE DE MEDIDA','REGIÃO']
    self.delete_column_from_dataframe(self.precos_df,columns_to_remove)


  def __transform_importacoes_dates__(self):
    ano_mes = zip(self.importacoes_df['ANO'], self.importacoes_df['MES'])
    
    new_column = list(map(map_year_month_number, ano_mes))
    self.importacoes_df = self.importacoes_df.assign(datapk = new_column)

  def days_month(self,currentDate):
    return calendar.monthrange(currentDate.tm_year, currentDate.tm_mon)[1]

  def map_duplicate(self,data_inicial_final,duplicate):
    data_inicial = data_inicial_final[0]
    data_final = data_inicial_final[1]
    if duplicate:
      data_inicial = time.strptime("{}-{}-{}".format(data_final.tm_year,data_final.tm_mon,1),"%Y-%m-%d")
      return data_inicial
    else:
      data_final = time.strptime("{}-{}-{}".format(data_inicial.tm_year,data_inicial.tm_mon,self.days_month(data_inicial)),"%Y-%m-%d")
      return data_final

  def check_times(self,dataframe_row,duplicate):
    data_inicial = time.strptime(dataframe_row['DATA INICIAL'],"%Y-%m-%d")
    data_final = time.strptime(dataframe_row['DATA FINAL'],"%Y-%m-%d")
    if data_final.tm_year > data_inicial.tm_year or data_final.tm_mon> data_inicial.tm_mon:
      return datetime.fromtimestamp(time.mktime(self.map_duplicate((data_inicial,data_final),duplicate))).strftime('%Y-%m-%d')
    else:
      return datetime.fromtimestamp(time.mktime(data_final)).strftime('%Y-%m-%d') 
  
  def check_times_duplicated(self,dataframe_row):
    return self.check_times(dataframe_row,True)
          
  def check_times_not_duplicated(self,dataframe_row):
    return self.check_times(dataframe_row,False)
    
  def get_week(self,data): 
    return math.ceil(data/7)
  
  def __filter_month_year_precos(self):
    temp_data_inicial = pd.to_datetime(self.precos_df['DATA INICIAL'])
    temp_data_final = pd.to_datetime(self.precos_df['DATA FINAL'])
    rows_to_change = (temp_data_final.dt.year > temp_data_inicial.dt.year) | (temp_data_final.dt.month > temp_data_inicial.dt.month)
    
    duplicated_dataframe = self.precos_df[rows_to_change]
    duplicated_dataframe['DATA INICIAL'] = duplicated_dataframe.apply(self.check_times_duplicated,axis = 1)
    self.precos_df['DATA FINAL'] = self.precos_df.apply(self.check_times_not_duplicated,axis = 1)
    print("rows: ",self.precos_df.shape[0])
    self.precos_df = pd.concat([self.precos_df,duplicated_dataframe])
    print("rows: ",self.precos_df.shape[0])
    
    #getting year, months and week
    years_list = pd.to_datetime(self.precos_df['DATA FINAL']).dt.year
    month_list = pd.to_datetime(self.precos_df['DATA FINAL']).dt.month
    day_list = pd.to_datetime(self.precos_df['DATA FINAL']).dt.day
    week_list = list(map(self.get_week, day_list))
    self.precos_df = self.precos_df.assign(ANO = years_list)
    self.precos_df = self.precos_df.assign(MES = month_list)
    self.precos_df = self.precos_df.assign(semana = week_list)
    self.delete_column_from_dataframe(self.precos_df,['DATA FINAL','DATA INICIAL'])
    
  
  def __transform_precos_dates__(self):
    ano_mes = zip(self.precos_df['ANO'], self.precos_df['MES'])
    new_column = list(map(map_year_month_number, ano_mes))
    self.precos_df = self.precos_df.assign(datapk = new_column)
    
  def __filter_produtos_from_tables__(self):
    products_list = list(enumerate(self.importacoes_df['PRODUTO'].unique(), start=1))
    self.produtos_df = pd.DataFrame(products_list, columns =['produtopk', 'nome'])
    
  def __filter_countrys_from_tables__(self):
    countrys_list = list(enumerate(self.importacoes_df['PAIS'].unique(), start=1))
    self.countries_df = pd.DataFrame(countrys_list, columns =['paispk', 'nome'])
    
  def normalize(self,s):
    s = s.lower()
    replacements = (
      ("á", "a"),
      ("é", "e"),
      ("í", "i"),
      ("ó", "o"),
      ("ú", "u"),
      ("ã", "a"),
      ("õ", "o"),
      ("ê", "e"),
      ("ç", "c"),
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
    return s

    
  def __filter_uf_from_tables__(self):
    self.importacoes_df['UF'] = self.importacoes_df['UF'].apply(self.normalize)
    self.precos_df['ESTADO'] = self.precos_df['ESTADO'].apply(self.normalize)
    
    uf_list = list(self.importacoes_df['UF'].unique())
    uf_list += list(self.precos_df['ESTADO'].unique())
    uf_list = set(uf_list)
    uf_list = list(enumerate(uf_list, start=1))
    self.uf_df = pd.DataFrame(uf_list, columns =['ufpk', 'nome'])

    self.uf_df['sigla'] = \
      self.uf_df.apply(lambda row : get_uf_acronym(row), axis = 1)
    self.uf_df['regiao'] = \
      self.uf_df.apply(lambda row : get_uf_region(row), axis = 1)

  def map_produto(self, produto):
    return int(self.produtos_df[self.produtos_df['nome']==produto]['produtopk'])
  
  def map_pais(self, pais):
    return int(self.countries_df[self.countries_df['nome']==pais]['paispk'])
  
  def map_uf(self, uf):
    return int(self.uf_df[self.uf_df['nome']==uf]['ufpk'])
  
  
  def __transform_precos_produto__(self):
    produtos_list = self.precos_df['PRODUTO']
    new_column = list(map(self.map_produto, produtos_list))
    self.precos_df = self.precos_df.assign(produtopk = new_column)
    
  def __transform_importacoes_produto__(self):
    produtos_list = self.importacoes_df['PRODUTO']
    new_column = list(map(self.map_produto, produtos_list))
    self.importacoes_df = self.importacoes_df.assign(produtopk = new_column)
  
  def __transform_importacoes_pais__(self):
    produtos_list = self.importacoes_df['PAIS']
    new_column = list(map(self.map_pais, produtos_list))
    self.importacoes_df = self.importacoes_df.assign(paispk = new_column)
    
  def __transform_importacoes_uf__(self):
    uf_list = self.importacoes_df['UF']
    new_column = list(map(self.map_uf, uf_list))
    self.importacoes_df = self.importacoes_df.assign(ufpk = new_column)
  
  def __transform_precos_uf__(self):
    uf_list = self.precos_df['ESTADO']
    new_column = list(map(self.map_uf, uf_list))
    self.precos_df = self.precos_df.assign(ufpk = new_column)

  def transform_data(self):
    print('----> Transformando dados\n')

    self.__transform_importacoes__()
    self.__transform_precos__()
    self.__filter_month_year_precos()
    # #map year and month to DData Id
    generate_year_month_map()

    # self.__transform_precos_and_importacoes_by_product__()
    
    # #create and map all produtos
    self.__transform_importacoes_dates__()
    self.__transform_precos_dates__()
    
    self.__filter_produtos_from_tables__()
    self.__filter_countrys_from_tables__()
    self.__filter_uf_from_tables__()
    
    self.__transform_precos_produto__()
    self.__transform_importacoes_produto__()
    
    self.__transform_importacoes_pais__()
    
    self.__transform_importacoes_uf__()
    self.__transform_precos_uf__()
    
    # csv's
    self.generate_date_csv()
    self.generate_products_csv()
    self.generate_countries_csv()
    self.generate_uf_csv()
    self.generate_precos_csv()
    self.generate_importacoes_csv()
    print('----> Transformando dados OK!\n')

  def get_importacoes(self):
    return self.importacoes_df

  def get_precos(self):
    return self.precos_df
  
  def get_produtos(self):
    return self.produtos_df

  def get_uf(self):
    return self.uf_df

  def generate_date_csv(self):
    write_data(path.join(base_csv_data_path, gen_ddata_filename))
    
  def generate_products_csv(self):
    self.produtos_df.to_csv("Arquivos/dproduto.csv",index = False)
  
  def generate_countries_csv(self):
    self.countries_df.to_csv("Arquivos/dpais.csv",index = False)
  
  def generate_uf_csv(self):
    self.uf_df.to_csv("Arquivos/duf.csv",index = False)

  def generate_precos_csv(self):
    self.precos_df.to_csv("Arquivos/fprecos.csv",index = False)

  def generate_importacoes_csv(self):
    self.importacoes_df.to_csv("Arquivos/fimportacoes.csv",index = False)
