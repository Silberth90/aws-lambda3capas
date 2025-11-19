# ==========================================================
# athena_3capas.py
# Script de Glue Spark - ETL 3 Capas + Creación automática en Athena
# ==========================================================

import sys
import boto3
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
    .csv(f"{BRONZE_PATH}*.csv")
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
# CREACIÓN DE BASE Y TABLA EN ATHENA
# ==========================================================
athena = boto3.client("athena", region_name="us-east-1")

database_name = "athena_3capas"
table_name = "pacientes_analytics"
output_location = f"s3://{S3_BUCKET}/athena-results/"

query_create_db = f"""
CREATE DATABASE IF NOT EXISTS {database_name};
"""

query_create_table = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database_name}.{table_name} (
  obra_social STRING,
  total_pacientes BIGINT,
  edad_promedio DOUBLE
)
STORED AS PARQUET
LOCATION '{GOLD_PATH}pacientes_analytics/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");
"""

print("Creando base y tabla en Athena...")

athena.start_query_execution(
    QueryString=query_create_db,
    ResultConfiguration={"OutputLocation": output_location}
)

athena.start_query_execution(
    QueryString=query_create_table,
    ResultConfiguration={"OutputLocation": output_location}
)

print(f"Base '{database_name}' y tabla '{table_name}' creadas o actualizadas en Athena.")
print("=== JOB FINALIZADO CORRECTAMENTE ===")
