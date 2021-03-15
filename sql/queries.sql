create database testeboticario;

use testeboticario;

drop table sales;

show create table vendas;

CREATE TABLE `sales` (
  `ID_MARCA` bigint(20) DEFAULT NULL,
  `MARCA` text,
  `ID_LINHA` bigint(20) DEFAULT NULL,
  `LINHA` text,
  `DATA_VENDA` datetime DEFAULT NULL,
  `QTD_VENDA` bigint(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

select count(1) from sales;

CREATE TABLE `sales_consolidated_year_month` (
  `ANO` int,
  `MES` int,
  `QTD_VENDA` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

select * from sales_consolidated_year_month;

CREATE TABLE `sales_consolidated_marca_linha` (
  `MARCA` text,
  `LINHA` text,
  `QTD_VENDA` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

select * from sales_consolidated_marca_linha;

CREATE TABLE `sales_consolidated_marca_ano_mes` (
  `MARCA` text,
  `ANO` int,
  `MES` int,
  `QTD_VENDA` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

select * from sales_consolidated_marca_ano_mes;

CREATE TABLE `sales_consolidated_linha_ano_mes` (
  `LINHA` text,
  `ANO` int,
  `MES` int,
  `QTD_VENDA` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

select * from sales_consolidated_linha_ano_mes;

# Consolidado de vendas por ano e mês;

select 
YEAR(DATA_VENDA) as ANO, 
month(DATA_VENDA) as MES,
sum(QTD_VENDA) QTD_VENDA
from sales 
group by YEAR(DATA_VENDA), month(DATA_VENDA)
order by 1,2 asc;

#Consolidado de vendas por marca e linha;

select 
MARCA, 
LINHA,
sum(QTD_VENDA) QTD_VENDA
from sales 
group by MARCA,LINHA
order by 3 desc;

#Consolidado de vendas por marca, ano e mês;

select 
MARCA, 
YEAR(DATA_VENDA) as ANO, 
month(DATA_VENDA) as MES,
sum(QTD_VENDA) QTD_VENDA
from sales 
group by MARCA,YEAR(DATA_VENDA), month(DATA_VENDA)
order by 1,2,3 asc;

#Consolidado de vendas por linha, ano e mês;

select 
LINHA, 
YEAR(DATA_VENDA) as ANO, 
month(DATA_VENDA) as MES,
sum(QTD_VENDA) QTD_VENDA
from sales 
group by LINHA,YEAR(DATA_VENDA), month(DATA_VENDA)
order by 1,2,3 asc;

