from pyspark.sql.functions import explode, col

df = spark.read.json("/Users/radek/projects/bigdatapassion/spark-training/openlineage.json")

df.printSchema()

df_flat = df.select(
    explode(col("inputs")).alias("input_item")
).select(
    col("input_item.facets.schema.fields") # The '*' expands all fields inside the schema struct
)

# df_flat.show()
df_flat.show(truncate=False)
