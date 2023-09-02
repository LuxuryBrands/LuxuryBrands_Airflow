import sys
from pyspark.sql.functions import lit, when, col, regexp_replace, to_timestamp, date_format
from pyspark.sql.types import StringType

from etl_job import ETLJob


def main():
    file_prefix = sys.argv[1]
    logical_date = sys.argv[2]
    table = sys.argv[3]

    etl_job = ETLJob(table=table, file_prefix=file_prefix)
    etl_job.create_meta_view()

    json_df = etl_job.get_json_df()
    result_df = transfer_df(json_df, logical_date)

    etl_job.validate_df(result_df)
    etl_job.save(result_df)
    etl_job.close()


def transfer_df(df, logical_date):
    """
    브랜드 데이터를 가공하는 함수
    - logical_date 를 참조해 created_at 은 log 테이블,  updated_at은 brand 테이블에 적재된다
    """
    # 타입 변경
    df = df.withColumn("user_id", col("user_id").cast("string"))

    # 컬럼 추가
    df = df.withColumn("del_yn", lit('N').cast(StringType()))
    df = df.withColumn("tag_name", regexp_replace(col("user_name"), "maison|official", ""))
    df = df.withColumn("created_at", date_format(to_timestamp(lit(logical_date)), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("updated_at", col("created_at"))

    # 결측치
    df = df.withColumn("name", when(col("name") == "", col("user_name")).otherwise(col("name")))

    return df


if __name__ == "__main__":
    main()
