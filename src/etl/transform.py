from src.etl.extract import Extractor
import pandas as pd

class Transformer:
  def __init__(self):
    extractor = Extractor()

    self.gas_df = extractor.get_gas_df()
    self.derivados_df = extractor.get_derivados_df()
    self.etanol_df = extractor.get_etanol_df()
    self.precos_df = extractor.get_precos_df()
    self.importacoes_df = {}

  def __transform_gas__(self):
    # Remove dados de exportações e exclui a coluna de operação comercial
    self.gas_df.drop(self.gas_df[self.gas_df['OPERAÇÃO COMERCIAL'] == 'EXPORTAÇÃO'].index, inplace=True)
    self.gas_df.drop('OPERAÇÃO COMERCIAL', axis=1, inplace=True)
    
    # Remove e corrige valores
    self.gas_df.replace({'GÁS NATURAL': 'GNV'}, inplace=True)

  def __transform_derivados__(self):
    # Remove dados de exportações e exclui a coluna de operação comercial
    self.derivados_df.drop(self.derivados_df[self.derivados_df['OPERAÇÃO COMERCIAL'] == 'EXPORTAÇÃO'].index, inplace=True)
    self.derivados_df.drop('OPERAÇÃO COMERCIAL', axis=1, inplace=True)
    
    # Remove e corrige valores
    self.derivados_df.drop(self.derivados_df[self.derivados_df['PRODUTO'].isin(['GASOLINA A', 'GLP', 'ÓLEO DIESEL'])].index, inplace=True)
    
    # Renomeia colunas
    self.derivados_df.rename(columns={'IMPORTADO / EXPORTADO': 'IMPORTADO', 'DISPÊNDIO / RECEITA' : 'DISPÊNDIO'}, inplace=True)

  def __transform__etanol__(self):
    # Remove dados de exportações e exclui a coluna de operação comercial
    self.etanol_df.drop(self.etanol_df[self.etanol_df['OPERAÇÃO COMERCIAL'] == 'EXPORTAÇÃO'].index, inplace=True)
    self.etanol_df.drop('OPERAÇÃO COMERCIAL', axis=1, inplace=True)

    # Remove e corrige valores
    self.etanol_df.drop(self.etanol_df[self.etanol_df['PRODUTO'] != 'ETANOL HIDRATADO'].index, inplace=True)
    
    # Renomeia colunas
    self.etanol_df.rename(columns={'IMPORTADO / EXPORTADO': 'IMPORTADO', 'DISPÊNDIO / RECEITA' : 'DISPÊNDIO'}, inplace=True)

  def __transform__prices__(self):
    # Remove e corrige valores
    self.precos_df.replace(regex={'OLEO': 'ÓLEO'}, inplace=True)

  def transform_data(self):
    print('----> Transformando dados\n')

    self.__transform_gas__()
    self.__transform_derivados__()
    self.__transform__etanol__()
    self.__transform__prices__()

    # Concatena os dfs
    self.importacoes_df = pd.concat(
      [self.gas_df, self.derivados_df, self.etanol_df]
    )

    print('----> Transformando dados OK!\n')

  def get_importacoes(self):
    return self.importacoes_df

  def get_precos(self):
    return self.precos_df