CREATE DATABASE IF NOT EXISTS athena_3capas;

CREATE EXTERNAL TABLE IF NOT EXISTS athena_3capas.pacientes_refinados (
  id INT,
  nombre STRING,
  edad INT,
  obra_social STRING,
  fecha_turno STRING
)
STORED AS PARQUET
LOCATION 's3://pruebasilver/silver/';

SELECT obra_social, COUNT(*) AS total_pacientes, AVG(edad) AS edad_promedio
FROM athena_3capas.pacientes_refinados
GROUP BY obra_social;
