import json
import logging

import boto3
from pydeequ.checks import ConstrainableDataTypes, Check, CheckLevel
from pydeequ.verification import VerificationSuite
from pyspark.sql import SparkSession, DataFrame


class ETLJob:

    def __init__(self, file_prefix: str, table: str, schema: str = "raw_data", ):
        self.spark = SparkSession.builder.appName("ETLJob").getOrCreate()
        self.file_prefix = file_prefix
        self.table = table
        self.schema = schema

    def get_json_df(self) -> DataFrame:
        df = self.spark.read \
            .option("multiline", "true") \
            .option("inferSchema", "ture") \
            .json(self.file_prefix)

        return df

    def create_meta_view(self):

        secrets_manager = boto3.client(service_name='secretsmanager', region_name='us-west-2')
        secret_json = secrets_manager.get_secret_value(SecretId='DE-2-1-SECRET')["SecretString"]
        url = json.loads(secret_json)['redshift_conn_url']

        sql = f"""
            WITH unique_cols as (
                SELECT a.attname AS unique_name
                  FROM pg_constraint c
                  JOIN pg_class      t ON c.conrelid = t.oid
                  JOIN pg_attribute  a ON a.attnum = ANY (c.conkey) AND a.attrelid = t.oid
                 WHERE c.contype = 'u'
                   AND t.relname = '{self.table}')
            SELECT column_name, is_nullable, data_type, character_maximum_length, b.unique_name
            FROM information_schema.columns a
            left OUTER JOIN unique_cols b on a.column_name = b.unique_name
            WHERE table_schema='{self.schema}' AND table_name='{self.table}'
            ORDER BY ordinal_position
        """

        df = self.spark.read \
            .format("jdbc") \
            .option("driver", "com.amazon.redshift.jdbc42.Driver") \
            .option("url", url) \
            .option("query", sql) \
            .load()

        df.createOrReplaceTempView("meta")

    def validate_df(self, df):
        try:
            # 테이블 필드 검증
            table_cols = self.spark.sql("SELECT column_name FROM meta").collect()
            df = df.select(*[col['column_name'] for col in table_cols])

            # Deequ 규칙 동적으로 생성
            check = Check(self.spark, CheckLevel.Error, f"{self.table} Raw Data Validation")

            # 1.unique 필드
            unique_cols = self.spark.sql("SELECT column_name FROM meta WHERE unique_name IS NOT NULL")
            if not unique_cols.rdd.isEmpty():
                for col in unique_cols.collect():
                    check = check.isUnique(col['column_name'])

            # 2.non-nullable 필드
            non_null_cols = self.spark.sql("SELECT column_name FROM meta WHERE is_nullable = 'NO'")
            if not non_null_cols.rdd.isEmpty():
                check = check.areComplete([col['column_name'] for col in non_null_cols.collect()])

            # 3.숫자 정수형 필드
            integer_cols = self.spark.sql("SELECT column_name FROM meta WHERE data_type = 'integer'")
            if not integer_cols.rdd.isEmpty():
                for col in integer_cols.collect():
                    check = check.hasDataType(col['column_name'], ConstrainableDataTypes.Integral)
                    check = check.isNonNegative(col['column_name'])

            # 4.문자열 maximum 필드
            #  (23.09.01) 주석처리
            #  - 숫자로된 문자열은 문자로 인식못해 id 컬럼은 생략 ex) '123456' -> 0.0,
            #  - 익명함수는 변수가 아닌 상수값만 유효함  ex) lambda x: x <= 30.0 (O) , lambda x: x <= max (X)
            # str_cols = spark.sql(
            #     "SELECT column_name , character_maximum_length FROM meta WHERE character_maximum_length IS NOT NULL AND column_name NOT LIKE '%id'")
            # if not str_cols.rdd.isEmpty():
            #     for col in str_cols.collect():
            #         check = check.hasDataType(col['column_name'], ConstrainableDataTypes.String)
            #         check = check.hasMaxLength(col['column_name'], lambda x: x <= float(col['character_maximum_length']))

            check_result = VerificationSuite(self.spark).onData(df).addCheck(check).run()

            if check_result.status == "Success":
                logging.info('The data passed the test, everything is fine!')
            else:
                logging.warning('We found errors in the data, the following constraints were not satisfied:')
                for check_json in check_result.checkResults:
                    if check_json['constraint_status'] != "Success":
                        raise Exception(
                            f"\t{check_json['constraint']} failed because: {check_json['constraint_message']}")

        except Exception as e:
            logging.warning(f"An error occurred: {e}")
            raise

    def save(self, df):
        df.write.mode("overwrite").format("avro").save(f"{self.file_prefix}.avro")

    def close(self):
        self.spark.stop()
