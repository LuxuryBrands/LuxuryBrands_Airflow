from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def check_and_copy_files(**context):
    print(">>>", context)

    # S3 폴더 경로
    bucket_name = context["bucket_name"]
    source_bucket = context["source_bucket"]
    dest_bucket = context["dest_bucket"]
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

        print(source_file)
        print(dest_file)
        s3_hook.copy_object(source_file, dest_file)
