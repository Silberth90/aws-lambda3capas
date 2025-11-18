# ==========================================================
# athena_3capas.py
# Script de Glue Spark - ETL 3 Capas (Bronze → Silver → Gold)
# Autor: Silver García
# ==========================================================

import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# ==========================================================
# PARÁMETROS DEL JOB
# ==========================================================
args = getResolvedOptions(sys.argv, [
    'S3_BUCKET',
    'BRONZE_PATH',
    'SILVER_PATH',
    'GOLD_PATH'
])

S3_BUCKET = args['S3_BUCKET']
BRONZE_PATH = args['BRONZE_PATH']
SILVER_PATH = args['SILVER_PATH']
GOLD_PATH = args['GOLD_PATH']

# ==========================================================
# INICIALIZACIÓN DE SPARK Y GLUE
# ==========================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

print("=== INICIO DEL JOB SPARK EN GLUE ===")
print(f"Bucket: {S3_BUCKET}")
print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")
print(f"Gold:   {GOLD_PATH}")

# ==========================================================
# CAPA BRONZE → LECTURA DE DATOS CRUDOS
# ==========================================================
df_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(BRONZE_PATH)
)

print("Vista previa de los datos crudos (BRONZE):")
df_bronze.show(5)
print("Cantidad de registros en Bronze:", df_bronze.count())

# ==========================================================
# CAPA SILVER → LIMPIEZA Y TRANSFORMACIÓN
# ==========================================================
df_silver = (
    df_bronze
    .dropna(subset=["id", "nombre", "edad", "obra_social", "fecha_turno"])
    .filter(col("edad") > 0)
    .dropDuplicates(["id"])
)

print("Cantidad de registros en Silver (después de limpieza):", df_silver.count())

# Guardar en Parquet (Silver)
df_silver.write.mode("overwrite").parquet(f"{SILVER_PATH}pacientes_refinados/")
print("Capa SILVER creada: datos limpios y validados.")

# ==========================================================
# CAPA GOLD → AGREGACIÓN Y ANÁLISIS
# ==========================================================
df_silver.createOrReplaceTempView("pacientes")

df_gold = spark.sql("""
    SELECT 
        obra_social,
        COUNT(*) AS total_pacientes,
        AVG(edad) AS edad_promedio
    FROM pacientes
    GROUP BY obra_social
""")

print("Cantidad de registros en Gold (agrupados):", df_gold.count())

df_gold.write.mode("overwrite").parquet(f"{GOLD_PATH}pacientes_analytics/")
print("Capa GOLD creada: datos listos para análisis.")

# ==========================================================
# VERIFICACIÓN FINAL
# ==========================================================
df_check = spark.read.parquet(f"{GOLD_PATH}pacientes_analytics/")
print("Vista previa de la capa GOLD:")
df_check.show()

print("=== JOB FINALIZADO CORRECTAMENTE ===")
