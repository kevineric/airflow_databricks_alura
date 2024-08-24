from pyspark.sql.functions import to_date, first, col, round

df_taxa = spark.read.parquet("dbfs:/databricks-results/bronze/*/*/*")

moedas = ["USD", "EUR", "GBP"]

df_moedas = df_taxa.filter(df_taxa.moeda.isin(moedas))

df_moedas = df_moedas.withColumn("data", to_date("data"))

df_moedas_ordenado = (df_moedas.groupBy("data")
                                .pivot("moeda")
                                .agg(first("taxa"))
                                .orderBy("data", ascending=False))

df_moedas_real = df_moedas_ordenado.select("*")

for moeda in moedas:
    df_moedas_real = df_moedas_real.withColumn(moeda, round(1/col(moeda),4))


df_moedas_ordenado = df_moedas_ordenado.coalesce(1)
df_moedas_real = df_moedas_real.coalesce(1)

(df_moedas_ordenado.write
                    .mode("overwrite")
                    .format("csv")
                    .option("header", "true")
                    .save("dbfs:/databricks-results/prata/taxa_conversao"))

(df_moedas_real.write
                    .mode("overwrite")
                    .format("csv")
                    .option("header", "true")
                    .save("dbfs:/databricks-results/prata/valores_reais"))
