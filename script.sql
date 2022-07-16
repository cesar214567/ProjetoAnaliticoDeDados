
DROP TABLE IF EXISTS FImportacoes;
DROP TABLE IF EXISTS FPrecos;
DROP TABLE IF EXISTS DData;
DROP TABLE IF EXISTS DProduto;
DROP TABLE IF EXISTS DUF;
DROP TABLE IF EXISTS DPais;

CREATE TABLE DData (
    dataPK int,
    mes int,
    mes_nome varchar(255),
    trimestre int,
    semestre int,
    ano int,
    PRIMARY KEY (dataPK)

);
  
CREATE TABLE DProduto (
    produtoPK int,
    nome varchar(255),
    PRIMARY KEY (produtoPK)

);
  
CREATE TABLE DUF (
    UFPK int,
    nome varchar(255),
    sigla varchar(255),
    regiao varchar(255),
    PRIMARY KEY (UFPK)

);
  
CREATE TABLE DPais (
    paisPK int,
    nome varchar(255),
    PRIMARY KEY (paisPK)

);

CREATE TABLE FImportacoes (
    id int,
    dataPK int,
    produtoPK int,
    paisPK int,
    UFPK int,
    valorFOB float,
    quantidadeEstatistica float,
    PRIMARY KEY (id),
    FOREIGN KEY (dataPK) REFERENCES DData(dataPK),
    FOREIGN KEY (produtoPK) REFERENCES DProduto(produtoPK),
    FOREIGN KEY (paisPK) REFERENCES DPais(paisPK),
    FOREIGN KEY (UFPK) REFERENCES DUF(UFPK)
);

CREATE TABLE FPrecos (
    id int,
    dataPK int,
    produtoPK int,
    UFPK int,
    semana int,
    precosMedios float,
    desvioPadraoMedios float,
    precoMinimo float,
    PRIMARY KEY (id),
    FOREIGN KEY (dataPK) REFERENCES DData(dataPK),
    FOREIGN KEY (produtoPK) REFERENCES DProduto(produtoPK),
    FOREIGN KEY (UFPK) REFERENCES DUF(UFPK)
);
