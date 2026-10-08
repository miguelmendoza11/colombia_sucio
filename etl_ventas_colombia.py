from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType

spark = (SparkSession.builder.appName("ETL_Ventas_Colombia")
         .config("spark.sql.ansi.enabled", "false")  # valores inválidos -> NULL en vez de error
         .getOrCreate())
# Los valores inválidos (fechas/números malos) deben volverse NULL y luego filtrarse, no romper el job
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.option("header", True).option("encoding", "UTF-8").csv("ventas_colombia_sucio.csv")

# ---------- 1. Limpieza de texto / normalización ----------
def quitar_acentos(c):
    return F.translate(c, "áéíóúÁÉÍÓÚñÑ", "aeiouAEIOUnN")

for c in df.columns:
    df = df.withColumn(c, F.trim(F.col(c)))
    df = df.withColumn(c, F.when(F.col(c) == "", None).otherwise(F.col(c)))

# Clave de comparación: sin acentos, minúsculas, sin espacios
def clave(c):
    return F.regexp_replace(F.lower(quitar_acentos(F.col(c))), r"\s+", "")

# Valor canónico = la variante "bien escrita" (la más frecuente) de cada clave
def normalizar(df, col):
    k = clave(col)
    d = df.withColumn("_k", k)
    freq = d.groupBy("_k", col).count()
    w = Window.partitionBy("_k").orderBy(F.desc("count"))
    canon = (freq.filter(F.col(col).isNotNull())
                 .withColumn("rn", F.row_number().over(w))
                 .filter("rn = 1").select("_k", F.col(col).alias("_canon")))
    return d.join(canon, "_k", "left").withColumn(col, F.coalesce("_canon", F.col(col))).drop("_k", "_canon")

df = normalizar(df, "city")
df = normalizar(df, "product")

# ---------- 2. Tipos ----------
df = (df.withColumn("order_date", F.to_timestamp("order_date"))
        .withColumn("quantity", F.col("quantity").cast(DoubleType()))
        .withColumn("unit_price_cop", F.col("unit_price_cop").cast(DoubleType()))
        .withColumn("discount", F.col("discount").cast(DoubleType()))
        .withColumn("returned_qty", F.col("returned_qty").cast(DoubleType()))
        .withColumn("customer_age", F.col("customer_age").cast(DoubleType()))
        .withColumn("shipping_days", F.col("shipping_days").cast(DoubleType())))

# ---------- 3. Reglas de calidad ----------
df = df.filter(F.col("order_id").isNotNull())
df = df.dropDuplicates(["order_id"])
df = df.filter(
    F.col("order_date").isNotNull() & (F.year("order_date") == 2025) &
    F.col("city").isNotNull() & F.col("product").isNotNull() &
    (F.col("quantity") > 0) & (F.col("quantity") <= 20) &
    (F.col("unit_price_cop") > 0) & (F.col("unit_price_cop") < 10_000_000) &
    F.col("customer_age").between(18, 100) &
    F.col("shipping_days").between(1, 15) &
    (F.col("returned_qty") >= 0) & (F.col("returned_qty") <= F.col("quantity"))
)
df = df.withColumn("sales_value", F.col("quantity") * F.col("unit_price_cop"))
df.cache()
ent = df.filter(F.col("status") == "Entregado")

# ---------- 4. Preguntas ----------
print("P1", ent.groupBy("product").agg(F.sum("quantity").alias("u")).orderBy(F.desc("u")).first())
print("P2", df.filter(F.col("status") == "Devuelto").agg(F.avg("returned_qty")).first())
print("P3", ent.groupBy("city").count().orderBy(F.desc("count")).first())
print("P4", ent.groupBy("category").agg(F.sum("sales_value").alias("v")).orderBy(F.desc("v")).first())
print("P5", ent.groupBy("channel").count().orderBy(F.desc("count")).first())
print("P6", ent.agg(F.avg("sales_value")).first())
print("P7", ent.groupBy("department").agg(F.sum("quantity").alias("u")).orderBy(F.desc("u")).first())
print("P8", ent.groupBy("payment_method").count().orderBy(F.desc("count")).first())
print("P9", ent.agg(F.avg("shipping_days")).first())
print("P10", ent.groupBy("customer_type").agg(F.sum("sales_value").alias("v")).orderBy(F.desc("v")).first())
