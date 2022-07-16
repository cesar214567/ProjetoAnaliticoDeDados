import pandas as pd

# * Extract

print('Extraindo dados\n')

gas_df = pd.read_csv('Arquivos\importacao-gas-natural-2000-2022.csv', sep=';')
derivados_df = pd.read_csv('Arquivos\importacoes-exportacoes-derivados-2000-2022.csv', sep=';')
etanol_df = pd.read_csv('Arquivos\importacoes-exportacoes-etanol-2012-2022.csv', sep=';')
precos_df = pd.read_csv('Arquivos\preços-combustiveis-2004-2021.tsv', sep='\t')

# * Transform

print('Transformando dados')

# Remove dados de exportações e exclui a coluna de operação comercial
gas_df.drop(gas_df[gas_df['OPERAÇÃO COMERCIAL'] == 'EXPORTAÇÃO'].index, inplace=True)
derivados_df.drop(derivados_df[derivados_df['OPERAÇÃO COMERCIAL'] == 'EXPORTAÇÃO'].index, inplace=True)
etanol_df.drop(etanol_df[etanol_df['OPERAÇÃO COMERCIAL'] == 'EXPORTAÇÃO'].index, inplace=True)
gas_df.drop('OPERAÇÃO COMERCIAL', axis=1, inplace=True)
derivados_df.drop('OPERAÇÃO COMERCIAL', axis=1, inplace=True)
etanol_df.drop('OPERAÇÃO COMERCIAL', axis=1, inplace=True)

# Remove e corrige valores
gas_df.replace({'GÁS NATURAL': 'GNV'}, inplace=True)
derivados_df.drop(derivados_df[~derivados_df['PRODUTO'].isin(['GASOLINA A', 'GLP', 'ÓLEO DIESEL'])].index, inplace=True)
etanol_df.drop(etanol_df[etanol_df['PRODUTO'] != 'ETANOL HIDRATADO'].index, inplace=True)
precos_df.replace(regex={'OLEO': 'ÓLEO'}, inplace=True)

# Renomeia colunas e concatena os dfs
derivados_df.rename(columns={'IMPORTADO / EXPORTADO': 'IMPORTADO', 'DISPÊNDIO / RECEITA' : 'DISPÊNDIO'}, inplace=True)
etanol_df.rename(columns={'IMPORTADO / EXPORTADO': 'IMPORTADO', 'DISPÊNDIO / RECEITA' : 'DISPÊNDIO'}, inplace=True)
importacoes_df = pd.concat([gas_df, derivados_df, etanol_df])

# * Load