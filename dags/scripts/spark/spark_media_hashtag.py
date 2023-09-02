import sys

from pyspark.sql.functions import date_format, to_timestamp, lit, col

from etl_job import ETLJob


def main():
    file_prefix = sys.argv[1]
    logical_date = sys.argv[2]
    table = sys.argv[3]

    etl_job = ETLJob(table=table, file_prefix=file_prefix)
    etl_job.create_meta_view()

    json_df = etl_job.get_json_df()
    result_df = transfer_df(json_df, logical_date)

    # (23.09.01) EMR 메인노드 부하로 주석처리
    # etl_job.validate_df(result_df)

    etl_job.save(result_df)
    etl_job.close()


def transfer_df(df, logical_date):
    """
    브랜드 해시태그 미디어 데이터를 추출하는 함수
    """

    # rename, format
    df = df.withColumnRenamed("timestamp", "ts")
    df = df.withColumn("ts", date_format(to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ssX"), "yyyy-MM-dd HH:mm:ss"))

    # 컬럼추가
    df = df.withColumn("created_at", date_format(to_timestamp(lit(logical_date)), "yyyy-MM-dd HH:mm:ss"))

    return df


if __name__ == "__main__":
    main()
