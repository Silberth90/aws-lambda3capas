SELECT obra_social, COUNT(*) AS total_pacientes, AVG(edad) AS edad_promedio
FROM athena_3capas.pacientes_refinados
GROUP BY obra_social;
