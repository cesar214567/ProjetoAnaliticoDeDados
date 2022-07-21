base_csv_data_path = 'Arquivos/'
drivers_path = 'drivers/'

natural_gas_filename = 'importacao-gas-natural-2000-2022.csv'
derived_imports_filename = 'importacoes-exportacoes-derivados-2000-2022.csv'
ethanol_imports_filename = 'importacoes-exportacoes-etanol-2012-2022.csv'
prices_filename = 'preços-combustiveis-2004-2021.tsv'

pgsql_driver_filename = 'postgresql-42.4.0.jar'

query_volume_import_roll_up = """
(select
  dp.nome,
  dd.mes_nome,
  sum(fi.quantidadeEstatistica)
from
  DProduto dp,
  DData dd,
  FImportacoes fi
where
  fi.produtopk = dp.produtopk and 
  fi.datapk = dd.datapk
group by
  dp.nome,
  dd.mes_nome) tmp
"""

query_average_prices_roll_up = """
(select
	dp.nome,
	dd.mes,
	avg(fp.precosMedios) 
from
	fprecos fp,
	dproduto dp,
	ddata dd
where
	fp.produtopk = dp.produtopk and
	fp.datapk = dd.datapk
group by
	dp.nome,
	dd.mes) tmp
"""

query_imports_semester_slice = """
(select 
	dp.nome,
	dd.mes,
	sum(fi.valorFOB) 
from
	fimportacoes fi,
	ddata dd,
	dproduto dp
where
	dd.semestre = 1
	AND dd.ano = 2020
group by
	dp.nome,
	dd.mes) tmp
"""

query_country_imports_pivot = """
(select
	dp.nome,
	dd.mes,
	sum(fi.valorFOB)
from
	fimportacoes fi,
	dpais dp,
	ddata dd
where
	fi.paispk = dp.paispk and
	fi.datapk = dd.datapk
group by
	dp.nome,
	dd.mes) tmp
"""
