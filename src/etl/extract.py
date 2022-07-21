import pandas as pd
from os import path
import src.utils.constants as c

class Extractor:
  def __init__(self):
    print('----> Extraindo dados\n')

    self.gas_df = pd \
      .read_csv(path.join(c.base_csv_data_path, c.natural_gas_filename), sep=';')

    self.derivados_df = pd \
      .read_csv(path.join(c.base_csv_data_path, c.derived_imports_filename), sep=';')

    self.etanol_df = pd \
      .read_csv(path.join(c.base_csv_data_path, c.ethanol_imports_filename), sep=';')

    self.precos_df = pd \
      .read_csv(path.join(c.base_csv_data_path, c.prices_filename), sep='\t')

    print('----> Extraindo dados OK!\n')

  def get_gas_df(self):
    return self.gas_df

  def get_derivados_df(self):
    return self.derivados_df

  def get_etanol_df(self):
    return self.etanol_df

  def get_precos_df(self):
    return self.precos_df