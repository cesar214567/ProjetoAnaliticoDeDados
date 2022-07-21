regions = {
  'piaui': {
    'acronym': 'PI',
    'region': 'Nordeste'
  },
  'pernambuco': {
    'acronym': 'PE',
    'region': 'Nordeste'
  },
  'sao paulo': {
    'acronym': 'SP',
    'region': 'Sudeste'
  },
  'distrito federal': {
    'acronym': 'DF',
    'region': 'Centro-Oeste'
  },
  'alagoas': {
    'acronym': 'AL',
    'region': 'Nordeste'
  },
  'maranhao': {
    'acronym': 'MA',
    'region': 'Nordeste'
  },
  'santa catarina': {
    'acronym': 'SC',
    'region': 'Sul'
  },
  'mato grosso do sul': {
    'acronym': 'MS',
    'region': 'Centro-Oeste'
  },
  'sergipe': {
    'acronym': 'SE',
    'region': 'Nordeste'
  },
  'para': {
    'acronym': 'PA',
    'region': 'Norte'
  },
  'mato grosso': {
    'acronym': 'MT',
    'region': 'Centro-Oeste'
  },
  'acre': {
    'acronym': 'AC',
    'region': 'Norte'
  },
  'roraima': {
    'acronym': 'RR',
    'region': 'Norte'
  },
  'espirito santo': {
    'acronym': 'ES',
    'region': 'Sudeste'
  },
  'amapa': {
    'acronym': 'AP',
    'region': 'Norte'
  },
  'paraiba': {
    'acronym': 'PB',
    'region': 'Nordeste'
  },
  'rio grande do norte': {
    'acronym': 'RN',
    'region': 'Nordeste'
  },
  'parana': {
    'acronym': 'PR',
    'region': 'Sul'
  },
  'tocantins': {
    'acronym': 'TO',
    'region': 'Norte'
  },
  'ceara': {
    'acronym': 'CE',
    'region': 'Nordeste'
  },
  'amazonas': {
    'acronym': 'AM',
    'region': 'Norte'
  },
  'rio grande do sul': {
    'acronym': 'RS',
    'region': 'Sul'
  },
  'rio de janeiro': {
    'acronym': 'RJ',
    'region': 'Sudeste'
  },
  'minas gerais': {
    'acronym': 'MG',
    'region': 'Sudeste'
  },
  'bahia': {
    'acronym': 'BA',
    'region': 'Nordeste'
  },
  'goias': {
    'acronym': 'GO',
    'region': 'Centro-Oeste'
  },
  'rondonia': {
    'acronym': 'RO',
    'region': 'Norte'
  },
}

def get_uf_acronym(row):
  global regions

  return regions[row['nome']]['acronym']

def get_uf_region(row):
  global regions
  return regions[row.nome]['region']