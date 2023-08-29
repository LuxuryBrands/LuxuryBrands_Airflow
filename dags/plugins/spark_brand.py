import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, when, col, regexp_replace, to_timestamp, date_format
from pyspark.sql.types import StringType

file_prefix = sys.argv[1]
logical_date = sys.argv[2]

# Spark 연결
spark = SparkSession.builder.appName("S3toSpark").getOrCreate()
df = spark.read.option("multiline", "true").json(file_prefix)

# FIXME brand 필드 검증로직 -> 필드명 변경시 알람, 결측치 처리
# FIXME brand 삭제 검증로직 -> 브랜드 로우 10개가 아니라면 1) db에서 브랜드 정보 모두 호출, 삭제 브랜드 색출 2)경고 및 del 처리
brand_cols = ["user_id", "user_name", "tag_name", "name", "profile_picture_url", "followers_count", "media_count",
              "del_yn", "updated_at"]
brand_log_cols = ["user_id", "followers_count", "media_count", "created_at"]


def transfer_df(df):
    """
    브랜드별 데이터를 추출하는 함수
    """

    # 컬럼추가
    df = df.withColumn("tag_name", regexp_replace(col("user_name"), "maison|official", ""))
    df = df.withColumn("del_yn", lit('N').cast(StringType()))
    df = df.withColumn("created_at",
                       date_format(to_timestamp(lit(logical_date)), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("updated_at",
                       date_format(to_timestamp(lit(logical_date)), "yyyy-MM-dd HH:mm:ss"))

    # 결측치
    df = df.withColumn("name", when(col("name") == "", col("user_name")).otherwise(col("name")))

    return df


result_df = transfer_df(df)

# 결과 확인
result_df.printSchema()
result_df.show(truncate=False)

# S3 paquet 형태로 저장
result_df.write.mode("overwrite").format("avro").save(f"{file_prefix}.avro")

spark.stop()
