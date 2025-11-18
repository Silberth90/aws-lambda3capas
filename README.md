proyecto de lambda de 3 capas en aws

primero creamos en s3 un bucket. dentro del bucket creamos 3 carpetas llamadas:

s3://pruebasilver/

  ├── bronze/
  
  ├── silver/
  
  └── gold/

dentro de la capa bronze debe estar el csv que se irá actualizando y que lambda automatizará:

pruebasilver/

  ├── bronze/pacientes_crudo.csv
  
  ├── silver/
  
  └── gold/

creamos otra carpeta llamada scrip y alojamos el archivo .py que se ejecutará en glue.

luego vamos a iam, creamos el rol con el nombre de la función y le asignamos los permisos:

AWSGlueServiceRole
AmazonS3FullAccess
CloudWatchLogsFullAccess

clic en “Add permissions” → “Create inline policy”
seleccionamos la pestaña JSON y pegamos el contenido del archivo permiso.json.
clic en “Next” y asignamos el nombre: LambdaGluePolicy.

luego vamos a glue y creamos un nuevo job.
nombre de la función: athenalambda
tipo: python spark

en script path agregamos la ruta del archivo .py:
s3://pruebasilver/scrip/athena_3capas.py

en job parameters agregamos:

Key Value
--S3_BUCKET pruebasilver
--BRONZE_PATH s3://pruebasilver/bronze/
--SILVER_PATH s3://pruebasilver/silver/
--GOLD_PATH s3://pruebasilver/gold/

luego guardamos el proceso y ejecutamos.
si todo está correcto, se crearán los archivos procesados en las carpetas silver y gold.

para automatizar con lambda:

vamos a lambda → create function
nombre del evento: lambda_trigger_glue_athena
runtime: Python 3.11
architecture: x86_64
rol: usar el rol creado con permisos de glue, s3 y cloudwatch.

pegamos el script lambda que vamos a usar del archivo .py

agregamos permisos:

AWSGlueServiceRole
CloudWatchLogsFullAccess
AmazonS3FullAccess

ahora elegimos “Create inline policy”
en la pestaña JSON agregamos el contenido de permiso.json
hacemos clic en “Next” y le colocamos el nombre del permiso: LambdaGluePolicy
aceptamos “Create policy” (esto crea una inline policy personalizada).

vamos a lambda y creamos el evento, ponemos un nombre
guardamos y desplegamos con deploy.

en configuración, dentro de triggers, seleccionamos “Add trigger”.
completamos con los siguientes valores:

source: S3
bucket: pruebasilver
event type: PUT
prefix: bronze/
suffix: .csv
permitir que lambda lea del bucket: marcar casilla

guardamos el trigger y desplegamos.

para probar el flujo completo:

subir o reemplazar el archivo pacientes_crudo.csv en la carpeta bronze.
lambda detectará el nuevo archivo y ejecutará el job de glue.
al finalizar, se crearán los datos procesados en silver y gold.
verificar los logs en cloudwatch (grupo /aws/lambda/lambda_trigger_glue_athena).

flujo completo:
s3 (bronze) → lambda trigger → glue job (spark) → s3 (silver/gold)

— verificar el trigger de s3

en la pestaña configuration → triggers, agregar un trigger S3.
si no está, hacer clic en add trigger.
configurar así:

trigger type: S3
bucket: pruebasilver
event type: PUT
prefix: bronze/
suffix: .csv
enable trigger: marcado

esto hace que solo se dispare cuando se sube un CSV a la carpeta bronze/.

— rol iam con permisos correctos

el rol de lambda debe tener estas políticas:

AWSGlueServiceRole
AmazonS3FullAccess
CloudWatchLogsFullAccess

si no las tiene, agregar una inline policy manual:
entrar al rol que usa lambda (configuration → permissions → role name).
clic en add permissions → create inline policy → pestaña JSON.
pegar en el scrip el contenido de permiso.json

— guardar y probar

una vez configurado todo, subir nuevamente el archivo pacientes_crudo.csv al bucket s3://pruebasilver/bronze/.
esperar unos segundos: la función lambda se disparará automáticamente.

revisar en cloudwatch logs → lambda_trigger_glue_athena
y en aws glue → job runs


debería aparecer el mensaje de ejecución exitosa.

