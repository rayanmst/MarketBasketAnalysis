# -*- coding: utf-8 -*-
"""
Created on Tue Feb 17 20:10:32 2022
@title:  Market Basket: Gera Sugestões

@author: Rayan M. Steinbach
"""

import findspark
import pandas as pd
from pyspark.ml.fpm import FPGrowthModel
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import collect_set
from datetime import datetime
# Pandasql para manipulação de dataframes utilizando SQL
import pandasql as ps

## Inicialização do Spark
findspark.init()
spark = SparkSession.builder.master("local[*]").config('spark.driver.memory','16g').config("spark.driver.maxResultSize",'2G').getOrCreate()

# UI: http://localhost:4040/jobs/

## Leitura dos arquivos de base
path = r'C:\Github\MarketBasketAnalysis'
dataset = pd.read_csv(path+r'\Dataset\dataset.csv',sep=';')
clientes = pd.read_csv(path+r'\Dataset\clientes.csv',sep=';')
grupo_cliente = pd.read_csv(path+r'\Dataset\grupo_cliente.csv',sep=';')
itens = pd.read_csv(path+r'\Dataset\itens.csv',sep=';')


data_schema = StructType([StructField("cd_cliente",types.LongType(),True),\
                          StructField("items",types.ArrayType(types.StringType(),False),False)])
 
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
    gc.cd_grupo_cliente in (0,1,2,3,4,6,7,10,19,22) and -- Seleção dos grupos desejados 
    dt_compra >= '2010-10-01'
"""

df = ps.sqldf(query_tabelas)    
        
    
## Gerando grupos que terão pedidos preditos
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
grupos_cliente = grupos['cd_grupo_cliente'].values

## Carregando modelos salvos
models = []
for i in grupos_cliente:
    mdl_name = "model_" + grupos.loc[grupos['cd_grupo_cliente'] == i,'tx_grupo_cliente'].values[0]
    print(f"Reading: {mdl_name}")
    models.append(mdl_name)
    globals()[mdl_name] = FPGrowthModel.read().load(path+f"\\Modelos\\{mdl_name}")
    print(f"Readed: {mdl_name}\n")
print("Done!")

## Gerando base de dados para transformação 

sparkDF = spark.createDataFrame(df)
sparkDF.registerTempTable("spdf")

## Separando emitentes em baskets
pedidos = []
for i in grupos_cliente:
    ped = 'pedidos_'+grupos.loc[grupos['cd_grupo_cliente']==i,'tx_grupo_cliente'].values[0]
    pedidos.append(ped)
    raw = spark.sql(f"SELECT                       \
                        cd_cliente,                \
                        cd_grupo_cliente,          \
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
    globals()[ped] = raw.groupBy(['cd_cliente']).agg(collect_set('cd_item').alias('items')).select(['cd_cliente','items']).repartition(500)
    print(ped + f" shape: ({globals()[ped].count()}, {len(globals()[ped].columns)})")

## Gerando Sugestões de compras
predicoes = []
for i in pedidos:
    res = i.replace('pedidos','predicao')
    predicoes.append(res)
    print(f'Working on: {res}')
    globals()[res] = globals()[i.replace('pedidos','model')].transform(globals()[i])
    print(f'Done: {res}\n')
print('Done!')

clientesdf = spark.createDataFrame(clientes)
clientesdf.registerTempTable('clientes')

itensdf = spark.createDataFrame(itens)
itensdf.registerTempTable('itens')

gruposdf = spark.createDataFrame(grupo_cliente)
gruposdf.registerTempTable('grupos')

## Tratando sugestões
for i in predicoes:
    globals()[i].registerTempTable("pred")
    tmp = spark.sql("""
                WITH cte AS(
                    SELECT
                        pred.cd_cliente,
                        items,
                        tmp.cd_grupo_cliente,
                        prediction,
                        explode(prediction) as cd_items_exp
                    FROM
                        pred
                    left join (
                        select distinct
                            cd_cliente,
                            cd_grupo_cliente
                        from
                            spdf
                    ) tmp
                    on
                    tmp.cd_cliente = pred.cd_cliente
                    )
                SELECT
                    cte.cd_cliente,
                    clientes.cliente_nome as tx_cliente,
                    cte.cd_grupo_cliente,
                    grupos.cd_estado as tx_grupo_cliente,
                    cte.items, 
                    prediction,
                    itens.cd_item, 
                    itens.tx_item as suggestion
                FROM
                    cte
                LEFT JOIN
                    itens
                ON
                    itens.cd_item = cte.cd_items_exp
                LEFT JOIN
                    clientes
                ON
                    clientes.cd_cliente = cte.cd_cliente
                LEFT JOIN
                    grupos
                ON
                    grupos.cd_grupo_cliente = cte.cd_grupo_cliente
                ORDER BY
                       cd_cliente
                """)
    tmp.registerTempTable("pred2")
    globals()[i] = spark.sql("""
                                WITH cte AS(
                                    SELECT
                                        cd_cliente,
                                        tx_cliente,
                                        cd_grupo_cliente,
                                        tx_grupo_cliente,
                                        items, 
                                        explode(items) as cd_items_exp,
                                        prediction,
                                        cd_item, 
                                        suggestion
                                    FROM
                                        pred2
                                ),
                                cte2 as (
                                    SELECT
                                        cte.cd_cliente,
                                        cte.tx_cliente,
                                        cte.cd_grupo_cliente,
                                        cte.tx_grupo_cliente,
                                        cte.items,
                                        itens.tx_item,
                                        cte.cd_item,
                                        suggestion
                                    FROM
                                        cte
                                    LEFT JOIN
                                        itens
                                    ON
                                        itens.cd_item = cte.cd_items_exp)
                                SELECT
                                    cd_cliente, 
                                    tx_cliente,
                                    cd_grupo_cliente,
                                    tx_grupo_cliente,
                                    items as cd_itens_comprados,
                                    collect_set(tx_item) as tx_itens_comprados, 
                                    cd_item as cd_itens_sugeridos,
                                    suggestion as tx_itens_sugeridos,
                                    current_date as dt_predicao
                                FROM
                                    cte2
                                GROUP BY
                                    cd_grupo_cliente,tx_grupo_cliente, cd_cliente, tx_cliente, items, cd_itens_sugeridos,tx_itens_sugeridos
                                ORDER BY
                                    cd_cliente
                                """)

## Salvando predições
predicted = pd.DataFrame()
for i in predicoes:
    print(f"Appending: {i}")
    predicted = predicted.append(globals()[i].toPandas())
    print(f"Appended: {i}\n")
predicted.reset_index(drop=True,inplace=True)

hoje = datetime.today().strftime('%Y-%m-%d')
predicted.to_excel(path + r'\Predicoes\predicao_'+hoje+'.xlsx',index = False)

## Encerrando sessão do Spark
spark.stop()
