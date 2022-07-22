base_csv_data_path = 'Arquivos/'
drivers_path = 'drivers/'

prices_filename = 'precos-combustiveis-2004-2021.tsv'
importacoes_filename = 'importacoes-ncm.csv'
dollar_real_price = 5.5

gen_ddata_filename = 'ddata.csv'

pgsql_driver_filename = 'postgresql-42.4.0.jar'

query_volume_import_roll_up = """
(select
  dp.nome as "Produto / Combustível",
  dd.mes_nome as "Mês",
  dd.ano as "Ano",
  sum(fi.quantidadeEstatistica) as "Volume (m3)"
from
  DProduto dp,
  DData dd,
  FImportacoes fi
where
  fi.produtopk = dp.produtopk and 
  fi.datapk = dd.datapk
group by
  dp.nome,
  dd.mes,
  dd.mes_nome,
  dd.ano
order by
 	dd.ano asc,
 	dd.mes asc
) tmp
"""

query_average_prices_roll_up = """
(select
	dp.nome as "Produto / Combustível",
	dd.mes_nome as "Mês",
	dd.ano as "Ano",
	avg(fp.precosMedios) as "Preços (R$)" 
from
	fprecos fp,
	dproduto dp,
	ddata dd
where
	fp.produtopk = dp.produtopk and
	fp.datapk = dd.datapk
group by
	dp.nome,
	dd.mes,
	dd.mes_nome,
	dd.ano
order by 
	dd.ano asc,
	dd.mes asc) tmp
"""

query_imports_semester_slice = """
(select 
	dp.nome as "Produto / Combustível",
	dd.mes_nome as "Mês",
	dd.ano as "Ano",
	sum(fi.valorFOB) as "Valor FOB (R$)" 
from
	fimportacoes fi,
	ddata dd,
	dproduto dp
where
	fi.datapk = dd.datapk and
	fi.produtopk = dp.produtopk AND
	dd.semestre = 1 AND
	dd.ano = 2020
group by
	dp.nome,
	dd.mes,
	dd.mes_nome,
	dd.ano
order by 
	dd.ano asc,
	dd.mes asc) tmp
"""

query_country_imports_pivot = """
(select
	dp.nome as "País",
	dd.mes_nome as "Mês",
	dd.ano as "Ano",
	sum(fi.valorFOB) as "Valor FOB (R$)"
from
	fimportacoes fi,
	dpais dp,
	ddata dd
where
	fi.paispk = dp.paispk and
	fi.datapk = dd.datapk
group by
	dp.nome,
	dd.mes,
	dd.mes_nome,
	dd.ano
order by 
	dd.ano asc,
	dd.mes asc) tmp
"""

query_prices_imports_drill_across = """
(select
	dp.nome as "Produto / Combustível",
	dd.mes_nome as "Mês",
	dd.ano as "Ano",
	fp.avgprecosmedios as "Preço Médio (R$)",
	fi.fivalorfob as "Valor FOB (R$)"
from
	(
		select
			fp.produtopk,
			dd.datapk,
			avg(fp.precosmedios) as avgprecosmedios
		from
			fprecos fp,
			ddata dd
		where
			fp.datapk = dd.datapk
		group by
			produtopk,
			dd.datapk
	) fp,
	(
	select
		fi.produtopk,
		dd.datapk,
		sum(fi.valorfob) as fivalorfob
	from
		fimportacoes fi,
		ddata dd
	where
		dd.datapk = fi.datapk
	group by 
		fi.produtopk,
		dd.datapk
	) fi,
	ddata dd,
	dproduto dp
where
	fp.produtopk = fi.produtopk and
	fp.produtopk = dp.produtopk and
	dd.datapk  = fp.datapk and
	fp.datapk = fi.datapk
order by 
	dd.ano asc,
	dd.mes asc) tmp
"""

derivados_to_exclude = ['ASFALTO', 'COQUE', 'GASOLINA DE AVIAÇÃO', 'LUBRIFICANTE', 'NAFTA', 'OUTROS NÃO ENERGÉTICOS', 'PARAFINA', 'QUEROSENE DE AVIAÇÃO', 'QUEROSENE ILUMINANTE', 'SOLVENTE', 'ÓLEO COMBUSTÍVEL']
