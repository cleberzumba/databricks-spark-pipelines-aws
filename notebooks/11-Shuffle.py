# Databricks notebooks source
# MAGIC %md
# MAGIC Notebook 11: Shuffle - Ocorre em operações wide (join, groupBy, distinct)
# MAGIC
# MAGIC - Broadcast vs Shuffle
# MAGIC - Execution Plan (`explain`)
# MAGIC - Join Strategies
# MAGIC - Particionamento
# MAGIC - Data Skew
# MAGIC - Catalyst

# COMMAND ------------ 

# MAGIC %md
# MAGIC - Exemplo (generates shuffle)

# COMMAND ------------ 

df.groupBy("dept_id").count().explain("formatted")

# You'll see:

Exchange

# MAGIC That mean: **shuffle happening**
# MAGIC %md
# MAGIC Execution Plan (`explain()`)
# MAGIC - Exemplo com join

from pyspark.sql.functions import broadcast

df = employees.join(broadcast(departments), "dept_id")
df.explain("formatted")

# MAGIC What to look out for:

`BroadcastHashJoin` -> broadcast applied
`Exchange` -> shuffle
`Scan` -> data reading

---

# MAGIC - Join Strategies

# MAGIC - Broadcast Hash Join

employees.join(broadcast(departments), "dept_id").explain()

# MAGIC - Expected:

BroadcastHashJoin

# MAGIC - Sort Merge Join (pattern)

employees.join(departments, "dept_id").explain()

# MAGIC - Expected:

SortMergeJoin
Exchange

# MAGIC - Particionamento
# MAGIC
# MAGIC - Repartition (generates shuffle)

df = employees.repartition(10)
df.rdd.getNumPartitions()

# MAGIC - Coalesce (don't generates shuffle)

df = employees.coalesce(2)
df.rdd.getNumPartitions()

# MAGIC - Data Skew
# MAGIC
# MAGIC - Simulando skew

data = [(1, "A")] * 1000000 + [(2, "B")] * 10
df = spark.createDataFrame(data, ["key", "value"])

df.groupBy("key").count().show()

# MAGIC - chave `1` concentra quase tudo
# MAGIC
# MAGIC - Solução simples (salting)

from pyspark.sql.functions import rand, concat

df_salted = df.withColumn("salt", (rand() * 10).cast("int"))

# MAGIC - Catalyst Optimizer
# MAGIC
# MAGIC - Exemplo (column pruning)

employees.select("emp_id").explain("formatted")

# MAGIC - Spark lê **apenas a coluna necessária**
# MAGIC 
# MAGIC - Predicate Pushdown

employees.filter("dept_id = 1").explain("formatted")

# MAGIC - filtro aplicado na leitura (mais eficiente)
# MAGIC

# MAGIC - Boas práticas de Join
# MAGIC 
# MAGIC - ERRADO (pesado)

employees.join(departments, "dept_id")

# MAGIC - CERTO (otimizado)

employees.select("emp_id", "dept_id") \
    .join(broadcast(departments.select("dept_id", "dept_name")), "dept_id")

# MAGIC - Detecção de problemas
# MAGIC 
# MAGIC - Identificar shuffle

df.explain("formatted")

# MAGIC - procure:

Exchange

# MAGIC - Identificar broadcast

df.explain()

# MAGIC - procure:

BroadcastHashJoin



👉 posso te dar **um problema real de 600TB (igual seu cenário)** e te fazer otimizar como em entrevista técnica.
