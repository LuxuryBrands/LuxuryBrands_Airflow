from pyspark.sql import SQLContext, Window, SparkSession
from pyspark.sql.functions import col, expr, round, \
    avg, countDistinct, rank, split, posexplode, count, regexp_replace
from pyspark.sql.window import Window
# import sys, subprocess
# subprocess.check_call([sys.executable, "-m", "pip", "install", "boto3"])
import boto3
import json


def main():
    secrets_manager = boto3.client(service_name='secretsmanager', region_name='us-west-2')
    secret_json = secrets_manager.get_secret_value(SecretId='DE-2-1-SECRET')["SecretString"]
    url = json.loads(secret_json)['redshift_conn_url']

    # Initialize Spark session
    spark = SparkSession \
        .builder \
        .appName("AWS REDSHIFT PYSPARK APP") \
        .getOrCreate()

    # Read data from Redshift
    raw_data_schema = "RAW_DATA"
    analytics_schema = "ANALYTICS"

    # integrate for_ load
    raw_data_table_names = ["brand", "media", "media_hashtag", "brand_log"]
    read_dfs = {}
    for read_table_name in raw_data_table_names:
        df = spark.read \
            .format("jdbc") \
            .option("driver", "comm.amazon.redshift.jdbc42.Driver") \
            .option("url", url) \
            .option("dbtable", f"{raw_data_schema}.{read_table_name}") \
            .load()
        read_dfs[read_table_name] = df

    brand_df = read_dfs["brand"]
    media_df = read_dfs["media"]
    media_hashtag_df = read_dfs["media_hashtag"]
    brand_log_df = read_dfs["brand_log"]

    # !!!!!!!!!!quality checking!!!!!!!!
    brand_df.createOrReplaceTempView("brand_df")
    media_df.createOrReplaceTempView("media_df")
    media_hashtag_df.createOrReplaceTempView("media_hashtag_df")
    brand_log_df.createOrReplaceTempView("brand_log_df")

    check_brand_df = spark.sql("""
            SELECT
                (CASE WHEN COUNT(DISTINCT user_id)=COUNT(user_id) THEN TRUE ELSE FALSE END) AS isunique_user_id_brand,
                (CASE WHEN COUNT(CASE WHEN user_id IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_user_id_brand,
                (CASE WHEN COUNT(DISTINCT tag_name)=COUNT(tag_name) THEN TRUE ELSE FALSE END) AS isunique_tag_name_brand,
                (CASE WHEN COUNT(CASE WHEN tag_name IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_tag_name_brand
            FROM brand_df
        """)
    check_brand_df.show()

    check_brand_log_df = spark.sql("""
                SELECT
                    (CASE WHEN COUNT(CASE WHEN user_id IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_user_id_brand_log      
                FROM brand_log_df
            """)
    check_brand_log_df.show()

    check_media_df = spark.sql("""
                    SELECT
                        (CASE WHEN COUNT(CASE WHEN user_id IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_user_id_media,      
                        (CASE WHEN COUNT(DISTINCT media_id)=COUNT(media_id) THEN TRUE ELSE FALSE END) AS isunique_media_id_media,
                        (CASE WHEN COUNT(CASE WHEN media_id IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_media_id_media,
                        (CASE WHEN (100-(COUNT(CASE WHEN like_count=0 THEN 1 END)* 100) /COUNT(like_count)>50) THEN TRUE ELSE FALSE END) AS like_count_completeness_media,
                        (CASE WHEN (100-(COUNT(CASE WHEN comments_count=0 THEN 1 END)* 100) /COUNT(comments_count)>50) THEN TRUE ELSE FALSE END) AS comments_count_completeness_media,
                        (CASE WHEN COUNT(CASE WHEN media_type IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_media_type_media,
                        (CASE WHEN COUNT(CASE WHEN media_product_type IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_media_product_type_media
                    FROM media_df
                """)
    check_media_df.show()

    check_media_hashtag_df = spark.sql("""
                        SELECT
                            (CASE WHEN COUNT(CASE WHEN user_id IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_user_id_media_hashtag,      
                            (CASE WHEN COUNT(CASE WHEN media_id IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_media_id_media_hashtag,
                            (CASE WHEN (100-(COUNT(CASE WHEN caption='' THEN 1 END)* 100) /COUNT(caption)>50) THEN TRUE ELSE FALSE END) AS like_count_completeness_media_hashtag,
                            (CASE WHEN COUNT(CASE WHEN media_type IS NULL THEN 1 END) > 0 THEN FALSE ELSE TRUE END) AS isnotnull_media_type_media_hashtag
                        FROM media_hashtag_df
                    """)
    check_media_hashtag_df.show()

    filtered_df1 = check_brand_df.select(
        *[col(col_name) for col_name in check_brand_df.columns if not check_brand_df.head(1)[0][col_name]])
    filtered_df2 = check_brand_log_df.select(
        *[col(col_name) for col_name in check_brand_log_df.columns if not check_brand_log_df.head(1)[0][col_name]])
    filtered_df3 = check_media_df.select(
        *[col(col_name) for col_name in check_media_df.columns if not check_media_df.head(1)[0][col_name]])
    filtered_df4 = check_media_hashtag_df.select(*[col(col_name) for col_name in check_media_hashtag_df.columns if
                                                   not check_media_hashtag_df.head(1)[0][col_name]])

    # Check if the filtered DataFrame is empty
    if filtered_df1.isEmpty():
        # If it's empty, raise an exception
        raise Exception("Quality Check Failed 1")
    elif filtered_df2.isEmpty():
        raise Exception("Quality Check Failed 2")
    elif filtered_df3.isEmpty():
        raise Exception("Quality Check Failed 3")
    elif filtered_df4.isEmpty():
        raise Exception("Quality Check Failed 4")
    else:
        # If not empty, just pass
        pass

    print("SUCCESS Quality Check")
    # !!!!!!!!!!!!!!elt!!!!!!!!!!!!!

    # create brand_basic_info table
    brand_basic_info_df = brand_df.select("tag_name", "profile_picture_url", "followers_count", "user_id")
    # brand_basic_info_df.createOrReplaceTempView("brand_basic_info")
    # brand_basic_info_df.show()

    # create brand_information table
    brand_information_df = media_df.join(
        brand_df,
        "user_id",
        "left"
    ).filter(
        col("media_product_type").isin("FEED", "REELS")
    ).select(
        brand_df["tag_name"],
        brand_df["user_id"],
        brand_df["media_count"],
        brand_df["followers_count"],
        brand_df["updated_at"],
        media_df["media_id"],
        media_df["like_count"],
        media_df["comments_count"],
        media_df["ts"],
        media_df["media_type"],
        media_df["media_product_type"]
    )

    # Calculate derived columns and add aliases
    brand_information_df = brand_information_df.withColumn(
        "engagement",
        media_df["like_count"] + media_df["comments_count"]
    ).withColumn(
        "engagement_rate",
        round((col("engagement") / col("followers_count")) * 100, 2)
    )
    # brand_information_df.createOrReplaceTempView("brand_information")
    # brand_information_df.show()

    # Add columns
    brand_information_df = brand_information_df.withColumn(
        "post_time",
        expr("EXTRACT(HOUR FROM ts)")
    ).withColumn(
        "day_of_week",
        expr("EXTRACT(DAYOFWEEK FROM ts)")
    ).withColumn(
        "post_day_of_week",
        expr("""
            CASE EXTRACT(DAYOFWEEK FROM ts)
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END AS post_day_of_week
            """
             )
    )

    # create followers_growth table
    followers_growth_df = brand_log_df.join(
        brand_df,
        "user_id",
        "left"
    ).select(
        brand_df["tag_name"],
        brand_log_df["followers_count"],
        brand_log_df["created_at"]
    )
    # followers_growth_df.createOrReplaceTempView("followers_growth")
    # followers_growth_df.show()

    # create hashtag_search table
    hashtag_search_df = media_hashtag_df.join(
        brand_df,
        "user_id",
        "left"
    ).select(
        brand_df["tag_name"],
        media_hashtag_df["media_id"],
        media_hashtag_df["caption"],
        media_hashtag_df["ts"],
        media_hashtag_df["user_id"],
        media_hashtag_df["created_at"]
    )
    # hashtag_search_df.createOrReplaceTempView("hashtag_search")
    # hashtag_search_df.show()

    # create trending_topics table
    trending_topics_df = hashtag_search_df.select(
        col("ts"),
        col("tag_name"),
        col("created_at"),
        posexplode(split(regexp_replace("caption", "#+", " #"), " ")).alias("pos", "related_hashtag")
    ).filter(
        col("related_hashtag").like("#_%")
    )
    # trending_topics_df.createOrReplaceTempView("trending_topics")

    # create hashtag_count table
    hashtag_count_df = trending_topics_df.groupBy("related_hashtag").agg(count("*").alias("hashtag_frequency"))
    # hashtag_count_df.createOrReplaceTempView("hashtag_count_df")

    # hashtag_count_df.show()

    # 1000
    hashtag_count_df = hashtag_count_df.orderBy(col("hashtag_frequency").desc()) \
        .limit(1000)
    # hashtag_count_df.show()

    # create aggregated_brand_information table
    aggregated_brand_information_df = brand_information_df.groupBy("tag_name").agg(
        round(avg("engagement_rate"), 4).alias("avg_engagement_rate"),
        countDistinct("media_id").alias("brand_media_cnt")
    )
    # aggregated_brand_information_df.createOrReplaceTempView("aggregated_brand_information")
    # aggregated_brand_information_df.show()

    # create aggregated_hashtag_search table
    aggregated_hashtag_search_df = hashtag_search_df.groupBy("tag_name").agg(
        countDistinct("media_id").alias("hashtaged_media_cnt")
    )

    # aggregated_hashtag_search_df.createOrReplaceTempView("aggregated_hashtag_search")
    # aggregated_hashtag_search_df.show()

    # create awareness table
    # awareness_df = hashtag_search_df.select("tag_name", "created_at", "ts", "media_id")

    # create popularity_factor_early_stage table
    popularity_factor_early_stage_df = aggregated_brand_information_df.join(
        aggregated_hashtag_search_df,
        "tag_name",
        "left"
    ).select(
        aggregated_brand_information_df["tag_name"],
        aggregated_brand_information_df["avg_engagement_rate"],
        aggregated_hashtag_search_df["hashtaged_media_cnt"]
    )
    # popularity_factor_early_stage_df.createOrReplaceTempView("popularity_factor_early_stage")
    # popularity_factor_early_stage_df.show()

    # create popularity_factor table
    # Assuming popularity_factor_early_stage_df and brand_basic_info_df are your input DataFrames
    popularity_factor_df = popularity_factor_early_stage_df.alias("pfe").join(
        brand_basic_info_df.alias("bbi"),
        "tag_name",
        "left"
    ).select(
        col("pfe.tag_name"),
        col("pfe.avg_engagement_rate"),
        col("pfe.hashtaged_media_cnt"),
        col("bbi.followers_count")
    )

    # popularity_factor_df.show()

    # Add rank columns using the rank() function
    popularity_factor_df = popularity_factor_df.withColumn(
        "rank_avg_engagement_rate",
        rank().over(Window.orderBy(col("avg_engagement_rate").desc()))
    ).withColumn(
        "rank_hashtaged_media_cnt",
        rank().over(Window.orderBy(col("hashtaged_media_cnt").desc()))
    ).withColumn(
        "rank_followers_count",
        rank().over(Window.orderBy(col("followers_count").desc()))
    )
    # popularity_factor_df.createOrReplaceTempView("popularity_factor")
    # popularity_factor_df.show()

    # create popularity_calculation table
    popularity_calculation_df = popularity_factor_df.select(
        col("tag_name"),
        (col("rank_avg_engagement_rate") + col("rank_hashtaged_media_cnt") + col("rank_followers_count")).alias(
            "sum_rank")
    )
    # Calculate the final popularity rank
    popularity_calculation_df = popularity_calculation_df.withColumn(
        "popularity_rank",
        rank().over(Window.orderBy(col("sum_rank")))
    )
    # popularity_calculation_df.createOrReplaceTempView("popularity_calculation")
    # popularity_calculation_df.show()

    # create brand_media_post_time table
    brand_media_post_time_df = brand_information_df.select(
        col("ts"),
        col("tag_name"),
        expr("EXTRACT(HOUR FROM ts)").alias("post_time"),
        expr("EXTRACT(DAYOFWEEK FROM ts)").alias("day_of_week"),
        expr("""
            CASE EXTRACT(DAYOFWEEK FROM ts)
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END AS post_day_of_week
        """).alias("post_day_of_week")
    )
    # brand_media_post_time_df.createOrReplaceTempView("brand_media_post_time")
    # brand_media_post_time_df.show()

    # Write transformed data to a new table in Redshift

    # integrate for_ save
    df_list_overwrite = [
        (brand_basic_info_df, "brand_basic_info"),
        (brand_information_df, "brand_information"),
        (followers_growth_df, "followers_growth"),
        (aggregated_brand_information_df, "aggregated_brand_information"),
        (aggregated_hashtag_search_df, "aggregated_hashtag_search"),
        (popularity_factor_df, "popularity_factor"),
        (popularity_calculation_df, "popularity_calculation"),
        (brand_media_post_time_df, "brand_media_post_time"),
        (hashtag_count_df, "hashtag_count")
    ]

    # !!!!!!!!!!!overwriting!!!!!!!!!!!!
    for write_df, write_table_name in df_list_overwrite:
        write_df.write \
            .format("jdbc") \
            .option("driver", "comm.amazon.redshift.jdbc42.Driver") \
            .option("url", url) \
            .option("dbtable", f"{analytics_schema}.{write_table_name}") \
            .mode("append") \
            .save()

    spark.stop()
    print("Finish")


if __name__ == "__main__":
    main()
