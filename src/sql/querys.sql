select DProduto.nome, DData.mes,sum(FImportacoes.quantidadeEstatistica) 
from FImportacoes JOIN DProduto JOIN DData 
group by DProduto.nome,DData.mes;

select DProduto.nome, DData.mes,avg(FImporacoes.precosMedios) 
from FImportacoes JOIN DProduto JOIN DData 
group by DProduto.nome,DData.mes;

select DProduto.nome, DData.mes,sum(FImportacoes.valorFOB) 
from FImportacoes JOIN DProduto JOIN DData 
where Ddata.mes between 1 and 6 AND Ddata.ano = 2020
group by DProduto.nome,DData.mes;

select DProduto.pais, DData.mes,sum(FImportacoes.valorFOB) 
from FImportacoes JOIN DProduto JOIN DData 
group by DProduto.nome,DData.mes;
  
