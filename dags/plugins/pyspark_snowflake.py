from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Window, SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, expr, round, \
    avg, countDistinct, rank, split, posexplode, count, regexp_replace
from pyspark.sql.window import Window
from airflow.models import Variable

def main():
    # Configure Spark
    '''
    spark_conf = SparkConf().setMaster('local').setAppName('SnowflakeSparkExample') \
        .set("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4")
    sc = SparkContext("local", "SnowflakeSparkExample", conf=spark_conf)
    spark = SQLContext(sc)
    '''

    spark = SparkSession.builder.appName("SnowflakeSparkExample").getOrCreate()

    # snowflake to spark option
    sfOptions = {
        "sfURL" : Variable.get("sf_url"),
        "sfUser" : Variable.get("sf_user"),
        "sfPassword" : Variable.get("sf_pw"),
        "sfDatabase" : Variable.get("sf_database"),
        "sfSchema" : "RAW_DATA",
        "sfWarehouse" : Variable.get("sf_warehouse")
    }

    # spark overwrite to snowflake option
    sfOptions2 = {
        "sfURL" : Variable.get("sf_url"),
        "sfUser" : Variable.get("sf_user"),
        "sfPassword" : Variable.get("sf_pw"),
        "sfDatabase" : Variable.get("sf_database"),
        "sfSchema" : "ANALYTICS",
        "sfWarehouse" : Variable.get("sf_warehouse")
    }


    # snowflake to spark
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
    # integrate for_ load
    raw_data_table_names= ["brand","media","media_hashtag","brand_log"]

    # !!!!!!!!reading!!!!!!!!!!!!!!
    read_dfs= {}
    for read_table_name in raw_data_table_names:
        df = spark.read \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option("dbtable", read_table_name) \
            .load()
        read_dfs[read_table_name] = df

    brand_df = read_dfs["brand"]
    media_df = read_dfs["media"]
    media_hashtag_df = read_dfs["media_hashtag"]
    brand_log_df = read_dfs["brand_log"]


    # !!!!!!!!!!!!!!elt!!!!!!!!!!!!!

    # create brand_basic_info table
    brand_basic_info_df = brand_df.select("tag_name", "profile_picture_url", "followers_count", "user_id")
    brand_basic_info_df.createOrReplaceTempView("brand_basic_info_temp")
    brand_basic_info_df.show()

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
    brand_information_df.createOrReplaceTempView("brand_information_temp")
    brand_information_df.show()

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
    followers_growth_df.createOrReplaceTempView("followers_growth_temp")
    followers_growth_df.show()


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
    hashtag_search_df.createOrReplaceTempView("hashtag_search_temp")
    #hashtag_search_df.show()


    # create trending_topics table
    trending_topics_df = hashtag_search_df.select(
        col("ts"),
        col("tag_name"),
        col("created_at"),
        posexplode(split(regexp_replace("caption", "#+", " #"), " ")).alias("pos", "related_hashtag")
    ).filter(
        col("related_hashtag").like("#_%")
    )
    trending_topics_df.createOrReplaceTempView("trending_topics_temp")


    # create hashtag_count table
    hashtag_count_df = trending_topics_df.groupBy("related_hashtag").agg(count("*").alias("hashtag_frequency"))
    hashtag_count_df.createOrReplaceTempView("hashtag_count_df_temp")


    # create aggregated_brand_information table
    aggregated_brand_information_df = brand_information_df.groupBy("tag_name").agg(
        avg("engagement_rate").alias("avg_engagement_rate"),
        countDistinct("media_id").alias("brand_media_cnt")
    )
    aggregated_brand_information_df.createOrReplaceTempView("aggregated_brand_information_temp")
    aggregated_brand_information_df.show()

    # create aggregated_hashtag_search table
    aggregated_hashtag_search_df = hashtag_search_df.groupBy("tag_name").agg(
        countDistinct("media_id").alias("hashtaged_media_cnt")
    )
    aggregated_hashtag_search_df.createOrReplaceTempView("aggregated_hashtag_search_temp")
    aggregated_hashtag_search_df.show()

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
    popularity_factor_early_stage_df.createOrReplaceTempView("popularity_factor_early_stage_temp")
    popularity_factor_early_stage_df.show()

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
    popularity_factor_df.createOrReplaceTempView("popularity_factor_temp")
    popularity_factor_df.show()


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
    popularity_calculation_df.createOrReplaceTempView("popularity_calculation_temp")
    popularity_calculation_df.show()


    # create brand_media_post_time table
    brand_media_post_time_df = brand_information_df.select(
        col("ts"),
        col("tag_name"),
        expr("EXTRACT(HOUR FROM ts)").alias("post_time"),
        expr("EXTRACT(DAYOFWEEK FROM ts)").alias("day_of_week"),
        expr("""
            CASE EXTRACT(DAYOFWEEK FROM ts)
                WHEN 0 THEN 'Sunday'
                WHEN 1 THEN 'Monday'
                WHEN 2 THEN 'Tuesday'
                WHEN 3 THEN 'Wednesday'
                WHEN 4 THEN 'Thursday'
                WHEN 5 THEN 'Friday'
                WHEN 6 THEN 'Saturday'
            END AS post_day_of_week
        """).alias("post_day_of_week")
    )
    brand_media_post_time_df.createOrReplaceTempView("brand_media_post_time_temp")
    brand_media_post_time_df.show()



    
    # spark to snowflake

    # integrate for_ save
    df_list_overwrite = [
        (brand_basic_info_df, "brand_basic_info"),
        (brand_information_df, "brand_information"),
        (followers_growth_df, "followers_growth"),
        (hashtag_search_df, "hashtag_search"),
        (trending_topics_df, "trending_topics"),
        (aggregated_brand_information_df, "aggregated_brand_information"),
        (aggregated_hashtag_search_df, "aggregated_hashtag_search"),
        (popularity_factor_early_stage_df, "popularity_factor_early_stage"),
        (popularity_factor_df, "popularity_factor"),
        (popularity_calculation_df, "popularity_calculation"),
        (brand_media_post_time_df, "brand_media_post_time"),
        (hashtag_count_df, "hashtag_count")
    ]

    # !!!!!!!!!!!overwriting!!!!!!!!!!!!
    for write_df, write_table_name in df_list_overwrite:
        write_df.write \
            .format("snowflake") \
            .options(**sfOptions2) \
            .option("dbtable", write_table_name) \
            .mode("overwrite") \
            .save()


    spark.stop()


if __name__ == "__main__":
    main()