import json
import boto3
import os
from datetime import datetime, timezone

s3 = boto3.client("s3")
sns = boto3.client("sns")
sfn = boto3.client("stepfunctions")


def normalize(name: str) -> str:
    return name.strip().lower()


def build_glue_job(job, file_name=None):
    return {
        "job_id": job["job_id"],
        "job_name": job["job_name"],
        "source_file_name": file_name or job["source_file_name"],
        "target_table": job["target_table"],
        "upsert_keys": job["upsert_keys"]
    }


def lambda_handler(event, context):

    # ---------------------------------------------------------
    # DEFENSIVE LOGGING (VERY IMPORTANT)
    # ---------------------------------------------------------
    print("🚀 Lambda invoked")
    print(json.dumps(event))

    if event.get("source") != "aws.s3":
        print("Not an EventBridge S3 event → ignoring")
        return {"message": "Ignored non-S3 event"}

    # ---------------------------------------------------------
    # EXTRACT EVENT DETAILS
    # ---------------------------------------------------------
    detail = event.get("detail", {})
    bucket = detail["bucket"]["name"]
    key = detail["object"]["key"]

    print(f"Processing S3 event → bucket={bucket}, key={key}")

    # ---------------------------------------------------------
    # ENV VARIABLES
    # ---------------------------------------------------------
    CONFIG_BUCKET = os.environ["CONFIG_BUCKET"]
    CONFIG_KEY = os.environ["CONFIG_KEY"]
    SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
    STEP_FUNCTION_ARN = os.environ["STEP_FUNCTION_ARN"]

    # ---------------------------------------------------------
    # LOAD CONFIG.JSON
    # ---------------------------------------------------------
    try:
        config_obj = s3.get_object(Bucket=CONFIG_BUCKET, Key=CONFIG_KEY)
        config_jobs = json.loads(config_obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"Failed to load config.json: {e}")
        raise

    # ---------------------------------------------------------
    # CASE 1: CONFIG.JSON UPLOADED → RE-RUN ALL DATA FILES
    # ---------------------------------------------------------
    if key.startswith("config/") and key.endswith(".json"):

        print("Detected config.json upload → processing all data/in/*.csv")

        response = s3.list_objects_v2(Bucket=bucket, Prefix="data/in/")
        if "Contents" not in response:
            print("No data files found under data/in/")
            return {"message": "No data files found"}

        for obj in response["Contents"]:
            if not obj["Key"].endswith(".csv"):
                continue

            file_name = obj["Key"].split("/")[-1]
            normalized_file = normalize(file_name)

            for job in config_jobs:
                # FIX: Extract filename from config path
                config_file = normalize(job["source_file_name"].split("/")[-1])

                if normalized_file == config_file and job.get("is_active", False):

                    execution_name = (
                        f"run-{job['job_id']}-"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
                    )

                    sfn.start_execution(
                        stateMachineArn=STEP_FUNCTION_ARN,
                        name=execution_name,
                        input=json.dumps({
                            "glue_jobs": [
                                build_glue_job(job, file_name)
                            ]
                        })
                    )

                    print(f"Triggered job for {file_name}")

        return {"message": "Config refresh completed"}

    # ---------------------------------------------------------
    # CASE 2: NEW FILE ARRIVED IN data/in/
    # ---------------------------------------------------------
    if key.startswith("data/in/") and key.endswith(".csv"):

        file_name = key.split("/")[-1]
        normalized_file = normalize(file_name)

        print(f"Processing new data file → {file_name}")

        matched_jobs = [
            build_glue_job(job, file_name)
            for job in config_jobs
            # FIX: Extract filename from config path
            if normalize(job["source_file_name"].split("/")[-1]) == normalized_file
            and job.get("is_active", False)
        ]

        if matched_jobs:

            execution_name = (
                f"run-{normalized_file}-"
                f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
            )

            sfn.start_execution(
                stateMachineArn=STEP_FUNCTION_ARN,
                name=execution_name,
                input=json.dumps({
                    "glue_jobs": matched_jobs
                })
            )

            print(f"Step Function triggered for {file_name}")
            return {"message": "Job triggered"}

        # -----------------------------------------------------
        # FILE NOT IN CONFIG → SNS ALERT
        # -----------------------------------------------------
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject="Unmapped S3 CSV File Uploaded",
            Message=(
                f"A CSV file was uploaded but not found in config.json\n\n"
                f"Bucket: {bucket}\n"
                f"Key: {key}"
            )
        )

        print(f"SNS sent for unmapped file → {file_name}")
        return {"message": "SNS sent for unmapped file"}

    # ---------------------------------------------------------
    # CASE 3: EVERYTHING ELSE → IGNORE
    # ---------------------------------------------------------
    print("Ignoring non-relevant S3 event")
    return {"message": "Ignored"}
