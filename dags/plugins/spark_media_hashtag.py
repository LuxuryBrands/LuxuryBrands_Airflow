import sys

from pyspark.sql import SparkSession

from pyspark.sql.functions import lit, to_timestamp, col, date_format

file_prefix = sys.argv[1]
logical_date = sys.argv[2]

# Spark 연결
spark = SparkSession.builder.appName("S3toSpark").getOrCreate()
df = spark.read.option("multiline", "true").json(file_prefix)

# FIXME brand 필드 검증로직 ->
# FIXME brand 삭제 검증로직 ->
media_hashtag_cols = ["user_id", "media_id", "media_type", "caption", "comments_count", "like_count", "ts",
                      "created_at"]


def transfer_df(df):
    """
    브랜드별 데이터를 추출하는 함수
    """

    # 컬럼추가
    df = df.withColumn("created_at",
                       date_format(to_timestamp(lit(logical_date), "yyyy-MM-dd'T'HH:mm:ssXXX"), "yyyy-MM-dd HH:mm:ss"))

    # 리네이밍, 포맷팅
    df = df.withColumnRenamed("timestamp", "ts")
    df = df.withColumn("ts", date_format(to_timestamp(col("ts"), "yyyy-MM-dd'T'HH:mm:ssX"), "yyyy-MM-dd HH:mm:ss"))

    return df


result_df = transfer_df(df)

# 결과 확인
result_df.printSchema()
result_df.show(truncate=False)

# S3 paquet 형태로 저장
result_df.write.mode("overwrite").format("avro").save(f"{file_prefix}.avro")

spark.stop()
