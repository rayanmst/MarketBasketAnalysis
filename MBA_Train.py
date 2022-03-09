# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 21:08:06 2022
@title:  Market Basket: Gera Modelos

@author: Rayan M. Steinbach
"""

## Import das bibliotecas
# Pandas para manipulação de arquivos e dataframes
import pandas as pd
# Pandasql para manipulação de dataframes utilizando SQL
import pandasql as ps
# Módulos do Spark para utilizar a estrutura do Apache Spark
import findspark
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import collect_set
from pyspark.sql import SparkSession

## Inicialização do Spark
findspark.init()
spark = SparkSession.builder.master("local[*]").config('spark.driver.memory','16g').config("spark.driver.maxResultSize",'2G').getOrCreate()

# UI: http://localhost:4040/jobs/

## Leitura dos arquivos de base
path = r'C:\Github\MarketBasketAnalysis\Dataset'
dataset = pd.read_csv(path+r'\dataset.csv',sep=';')
clientes = pd.read_csv(path+r'\clientes.csv',sep=';')
grupo_cliente = pd.read_csv(path+r'\grupo_cliente.csv',sep=';')
itens = pd.read_csv(path+r'\itens.csv',sep=';')

## Manipulação dos dataframes para a obtenção do dataset desejado
## Query das tabelas
query_tabelas = """
SELECT
    ds.id_compra,
    ds.cd_compra,
    it.cd_item,
    it.tx_item,
    cl.cd_cliente,
    cl.cliente_nome,
    gc.cd_grupo_cliente,
    gc.cd_pais,
    gc.tx_pais,
    gc.cd_estado,
    ds.dt_compra,
    ds.nm_quantidade,
    ds.nm_vl_item,
    ds.nm_vl_total	
FROM
    dataset ds
LEFT JOIN
    clientes cl
ON
    cl.id_cliente = ds.id_cliente
LEFT JOIN
    grupo_cliente gc
ON
    gc.id_grupo = ds.id_grupo
LEFT JOIN
    itens it
ON
    it.id_item = ds.id_item
WHERE
    gc.cd_grupo_cliente in (0,1,2,3,4,6,7,10,19,22) -- Seleção dos grupos desejados
"""

df = ps.sqldf(query_tabelas)

## Aquisição da lista com grupos de clientes
grupos_query = """
SELECT DISTINCT
    cd_grupo_cliente,
    cd_estado as tx_grupo_cliente
FROM
    df
ORDER BY
    1    
"""
grupos = ps.sqldf(grupos_query)
grupos['tx_grupo_cliente'] = grupos['tx_grupo_cliente'].replace(', City of','',regex=True).replace([" ","-"],"_",regex=True).str.lower()
list_grupos = grupos['cd_grupo_cliente'].values

## Criação de baskets diferentes para cada grupo
baskets = []
sparkdf = spark.createDataFrame(df)
sparkdf.registerTempTable("spdf")
for i in list_grupos:
    bask = 'basket_' + grupos.loc[grupos['cd_grupo_cliente'] == i,'tx_grupo_cliente'].values[0]
    raw = spark.sql(f"SELECT                       \
                        cd_cliente,                \
                        cd_item                    \
                    FROM                           \
                        spdf                       \
                    WHERE                          \
                        cd_grupo_cliente = {i} AND \
                        nm_quantidade >= 1         \
                    ORDER BY                       \
                        cd_cliente,               \
                        cd_item                    ")
    # Utilização de globals()[string] para gerar variáveis utilizando o nome do basket atual
    globals()[bask] = raw.groupBy(['cd_cliente']).agg(collect_set('cd_item').alias('items')).select(['cd_cliente','items']).repartition(500)
    baskets.append(bask)


# Treinamento de modelos FPGrowth distribuídos
models = []
fp_growth = FPGrowth(itemsCol='items',minSupport=0.2,minConfidence=0.6)
for i in baskets:
    mdl = 'model_' + i.replace('basket_','')
    models.append(mdl)
    print(f"--------------------------------------------\nModel {mdl} fit - Begin")
    globals()[i].createOrReplaceTempView('baskets')
    bask_tmp = spark.sql("select items from baskets")
    globals()[mdl] = fp_growth.fit(bask_tmp)
    print(f"Model {mdl} fit - End\n--------------------------------------------")
    
for i in models:
    print('saving ' + i + ' begin')
    globals()[i].write().overwrite().save(r'C:\Github\MarketBasketAnalysis'+f'\\Modelos\\{i}')
    print('saving ' + i + ' end\n')
spark.stop()
