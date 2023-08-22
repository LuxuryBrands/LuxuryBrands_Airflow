import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import lit, to_timestamp, col

file_prefix = sys.argv[1]
logical_date = sys.argv[2]

# Spark 연결
spark = SparkSession.builder.appName("S3toSpark").getOrCreate()
df = spark.read.option("multiline", "true").json(file_prefix)

# FIXME media 필드 검증로직 ->
# FIXME media 삭제 검증로직 ->
raw_media_cols = ["media_id", "user_id", "like_count", "comments_count", "media_product_type", "media_type",
                  "caption", "permalink", "media_url", "ts",
                  "del_yn", "created_at", "updated_at"]
raw_media_log_cols = ["media_id", "user_id", "like_count", "comments_count", "created_at"]


def transfer_df(df):
    """
    미디어별 데이터를 추출하는 함수
    """

    # 네이밍, 결측치
    df = df.withColumnRenamed("timestamp", "ts")
    df = df.withColumn("ts", to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ssX"))

    # 컬럼추가
    df = df.withColumn("del_yn", lit('N').cast(StringType()))
    df = df.withColumn("created_at", to_timestamp(lit(logical_date)))
    df = df.withColumn("updated_at", to_timestamp(lit(logical_date)))

    return df


result_df = transfer_df(df)

# 결과 확인
result_df.printSchema()
result_df.show(truncate=False)

# S3 paquet 형태로 저장
result_df.write.mode("overwrite").parquet(f"{file_prefix}.parquet")

spark.stop()
