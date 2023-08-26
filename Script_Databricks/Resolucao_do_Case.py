# Databricks notebook source
import psycopg2

def conecta_db():
  con = psycopg2.connect(host='psql-mock-database-cloud.postgres.database.azure.com', 
                         database='ecom1692840458298ymitjshyedrfjlvq',
                         user='afesdunqrmfllihdbzrnacwp@psql-mock-database-cloud', 
                         password='rekuivjzextaqdbnlabkitie')
  return con

# COMMAND ----------

# MAGIC %md
# MAGIC Criando uma consulta no database que traz o nome das tabelas disponíveis:
# MAGIC

# COMMAND ----------

def consultar_db(sql):
  con = conecta_db()
  cur = con.cursor()
  cur.execute(sql)
  recset = cur.fetchall()
  tables=[]
  for rec in recset:
      for table_name in rec:
        tables.append(table_name)
  
  return tables

tables = consultar_db("select table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables= tables[0:8]
tables

# COMMAND ----------

# MAGIC %md
# MAGIC Iniciando a sessão do spark e adicionando as informações do banco

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession
from delta.tables import *


database_host = "psql-mock-database-cloud.postgres.database.azure.com"
database_port = "5432" # update if you use a non-default port
database_name = "ecom1692817202223mzgwryukwxpjowao"
#table = "customers"
user = "fbdcebxotbvbdjdkbgnpbwyr@psql-mock-database-cloud"
password = "epznsbacfhkvgklpcclrkegw"
url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"
driver = "org.postgresql.Driver"

spark=SparkSession.builder.master('local[*]').appName("case").getOrCreate()



# COMMAND ----------

# MAGIC %md
# MAGIC Criando um código que lê as tabelas do postgree, transforma elas em um dataframe spark "df_<nome da tabela>", e depois salva cada uma delas no formato parquet

# COMMAND ----------

 for table in tables: 
  globals()['df_%s' % table] = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .load()
  )
  globals()['df_%s' % table].write.format('parquet').save(("FileStore/Join_Case/Parquets/{}.parquet").format(table))
 

# COMMAND ----------

for table in tables:
  globals()['df_%s' % table].write.mode("overwrite").saveAsTable("{}".format(table))

# COMMAND ----------

# MAGIC %md
# MAGIC Tranformando todos os dataframes em tabelas no formato delta

# COMMAND ----------

for table in tables:
  globals()['df_%s' % table].write.format("delta").mode("overwrite").save(("dbfs:/FileStore/Join_Case/Delta_Tables/{}").format(table))
  

# COMMAND ----------

# MAGIC %md
# MAGIC Definindo as chaves primárias de cada tabela para o merge, e colocando elas dentro de um dicionário

# COMMAND ----------

PK={
    'customers': 'customer_number',
    'employees': 'employee_number',
    'offices': 'office_code',
    'orderdetails': 'order_number',
    'orders': 'order_number',
    'payments': 'customer_number',
    'product_lines': 'product_line',
    'products': 'product_code'
}

# COMMAND ----------

# MAGIC %md
# MAGIC Fazendo o merge de cada uma das tabelas delta com os arquivos parquet. De maneira geral(utilizando um for para pecorrer todos os nomes)

# COMMAND ----------

for table in tables:
    globals()['delta_table_%s' % table] = DeltaTable.forPath(spark, 'dbfs:/FileStore/Join_Case/Delta_Tables/{}'.format(table))
    dfUpdates = spark.read.format("parquet").load("dbfs:/FileStore/Join_Case/Parquets/{}.parquet".format(table))

    #dfUpdates = deltaTableCustomersUpdates.toDF()

    globals()['delta_table_%s' % table].alias(table) \
    .merge(
        dfUpdates.alias('updates'),
        '{}.{} = updates.{}'. format(table,PK[table],PK[table])
    ) \
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .whenNotMatchedBySourceDelete()

# COMMAND ----------

# MAGIC %md
# MAGIC A célula abaixo é apenas para exemplificar um merge caso eu quisesse fazer o update, o insert ou o delete de colunas específicas
# MAGIC

# COMMAND ----------

from delta.tables import *

deltaTableCustomers = DeltaTable.forPath(spark, 'dbfs:/FileStore/Join_Case/Delta_Tables/customers')
dfUpdates = spark.read.format("parquet").load(f"dbfs:/FileStore/Join_Case/Parquets/customers.parquet")

#dfUpdates = deltaTableCustomersUpdates.toDF()

deltaTableCustomers.alias('customers') \
  .merge(
    dfUpdates.alias('updates'),
    'customers.customer_number = updates.customer_number'
  ) \
  .whenMatchedUpdate(set =
    {
      "customer_name": "updates.customer_name",
      "contact_last_name": "updates.contact_last_name",
      "contact_first_name": "updates.contact_first_name"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "customer_number": "updates.customer_number",
      "contact_last_name": "updates.contact_last_name"
      
    }
  ).whenNotMatchedBySourceDelete() \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC No notebook crie código Spark que respondam as seguintes perguntas:
# MAGIC - **Qual país possui a maior quantidade de itens cancelados?**
# MAGIC
# MAGIC Resp: New Zealand
# MAGIC

# COMMAND ----------

country= spark.sql("""SELECT 
                            country as Pais,
                            COUNT(o.status) as Qtd_cancelados
                            FROM parquet.`/Join_case/customers.parquet` as c
                            INNER JOIN parquet.`/Join_case/orders.parquet` as o
                            ON c.customer_number = o.customer_number
                            WHERE lower(o.status) = "cancelled"
                            GROUP BY 1
                            ORDER BY 2 DESC
                            LIMIT 1  """)
country.show(10)


# COMMAND ----------

country.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Join_Case/Delta_Results/country")


# COMMAND ----------

# MAGIC %md
# MAGIC - **Qual o faturamento da linha de produto mais vendido, considere como os itens
# MAGIC Shipped, cujo o pedido foi realizado no ano de 2005?**
# MAGIC
# MAGIC Resp: Classic Cars com 47574393.22
# MAGIC

# COMMAND ----------

billing = spark.sql(""" SELECT
                pl.product_line as Linha_do_produto,
                SUM(pa.amount) as Faturamento,
                COUNT(distinct(o.order_number)) as Qtd_Pedidos,
                year(o.order_date) as Ano_do_pedido,
                lower(o.status) as Status
                FROM parquet.`/Join_case/product_lines.parquet` as pl
                INNER JOIN parquet.`/Join_case/products.parquet` as p
                ON pl.product_line = p.product_line
                INNER JOIN parquet.`/Join_case/orderdetails.parquet` as od
                ON od.product_code = p.product_code
                INNER JOIN parquet.`/Join_case/orders.parquet` as o
                ON od.order_number = o.order_number
                INNER JOIN parquet.`/Join_case/payments.parquet` as pa
                ON pa.customer_number = o.customer_number

                WHERE lower(o.status) = "shipped"
                AND year(o.order_date)="2005"
                GROUP BY 1,4,5
                ORDER BY 2 DESC """)
billing.show(10)                


# COMMAND ----------

billing.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Join_Case/Delta_Results/billing")

# COMMAND ----------

# MAGIC %md
# MAGIC - **Nome, sobrenome e e-mail dos vendedores do Japão, o local-part do e-mail
# MAGIC deve estar mascarado.**
# MAGIC
# MAGIC Resp: Mami Nishi e Yoshimi Kato, email mascarado

# COMMAND ----------

japan_employees= spark.sql(""" SELECT 
                first_name as Nome,
                last_name as Sobrenome,
                concat(sha1(replace(email, '@classicmodelcars.com', '')),'@classicmodelcars.com') as Email
                FROM parquet.`/Join_case/employees.parquet` as em
                INNER JOIN parquet.`/Join_case/offices.parquet` as o
                ON o.office_code = em.office_code 
                WHERE lower(o.country) = "japan"

                ORDER BY 2 desc """)
japan_employees.show()


# COMMAND ----------

japan_employees.write.format("delta").mode("overwrite").save("dbfs:/FileStore/Join_Case/Delta_Results/japan_employees")

# COMMAND ----------

# MAGIC %md
# MAGIC Deixando a consulta em sql aqui também, porque a visualização do último campo está melhor

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC first_name,
# MAGIC last_name,
# MAGIC concat(sha1(replace(email, '@classicmodelcars.com', '')),'@classicmodelcars.com') as email
# MAGIC FROM parquet.`/Join_case/employees.parquet` as em
# MAGIC INNER JOIN parquet.`/Join_case/offices.parquet` as o
# MAGIC ON o.office_code = em.office_code 
# MAGIC WHERE lower(o.country) = "japan"
# MAGIC
# MAGIC ORDER BY 2 desc 
# MAGIC

# COMMAND ----------


