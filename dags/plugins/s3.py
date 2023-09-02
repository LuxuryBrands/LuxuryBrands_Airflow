import logging

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def check_and_copy_files(**context):
    # S3 폴더 경로
    bucket_name = Variable.get("aws_bucket")
    source_bucket = Variable.get("aws_raw_bucket")
    dest_bucket = Variable.get("aws_stage_bucket")
    prefix = context["prefix"]

    # S3Hook 초기화
    s3_hook = S3Hook()

    # S3 파일이 있을경우 stage 폴더로 이동
    files = s3_hook.list_keys(bucket_name=bucket_name, prefix=f"{source_bucket}/{prefix}", delimiter="/")

    if not files:
        raise Exception(f"Empty files key --> {source_bucket}/{prefix}")

    for file_path in files:
        source_file = f"s3://{bucket_name}/{file_path}"
        dest_file = f"s3://{bucket_name}/{dest_bucket}/{prefix}/{file_path.split('/')[-1]}"

        s3_hook.copy_object(source_file, dest_file)


# helper function
def upload_to_s3(**context):
    bucket_name = Variable.get("aws_bucket")
    emr_keys = context["key"]
    files = context["files"]

    s3 = S3Hook()

    try:
        for file, key in zip(files, emr_keys):
            s3.load_file(filename=file, bucket_name=bucket_name, replace=True, key=key)
    except Exception as e:
        logging.warning(f"An error occurred: {e}")
        raise
