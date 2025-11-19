import json
import boto3

glue = boto3.client("glue")

def lambda_handler(event, context):
    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']

        print(f"Nuevo archivo detectado: s3://{bucket_name}/{object_key}")

        if object_key.endswith(".csv"):
            response = glue.start_job_run(
                JobName="athenalambda",
                Arguments={
                    "--S3_BUCKET": bucket_name,
                    "--BRONZE_PATH": f"s3://{bucket_name}/bronze/",
                    "--SILVER_PATH": f"s3://{bucket_name}/silver/",
                    "--GOLD_PATH": f"s3://{bucket_name}/gold/"
                }
            )
            job_run_id = response["JobRunId"]
            print(f"Glue Job iniciado con ID: {job_run_id}")
        else:
            print("El archivo no es CSV, no se dispara el Job.")

        return {"statusCode": 200, "body": json.dumps("Lambda ejecutada correctamente.")}

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
