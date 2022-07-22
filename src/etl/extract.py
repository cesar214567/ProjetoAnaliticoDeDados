import pandas as pd
from os import path
import src.utils.constants as c

class Extractor:
  def __init__(self):
    print('----> Extraindo dados\n')

    self.importacoes_df = pd \
      .read_csv(path.join(c.base_csv_data_path, c.importacoes_filename), sep=';')

    self.precos_df = pd \
      .read_csv(path.join(c.base_csv_data_path, c.prices_filename), sep='\t')

    print('----> Extraindo dados OK!\n')

  def get_importacoes_df(self):
    return self.importacoes_df

  def get_precos_df(self):
    return self.precos_df
