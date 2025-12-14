import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ["RAW_S3_PATH", "OUT_S3_PATH"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

raw_path = args["RAW_S3_PATH"]
out_path = args["OUT_S3_PATH"]

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("escape", "\"")
    .option("multiLine", "false")
    .load(raw_path)
)

df = df.withColumn("trans_ts", F.to_timestamp(F.col("trans_date_trans_time"), "yyyy-MM-dd HH:mm:ss"))
df = df.withColumn("trans_date", F.to_date(F.col("trans_ts")))
df = df.withColumn("trans_year", F.year(F.col("trans_ts")))
df = df.withColumn("trans_month", F.month(F.col("trans_ts")))

state_tz = {
    "AL":"America/Chicago","AK":"America/Anchorage","AZ":"America/Phoenix","AR":"America/Chicago",
    "CA":"America/Los_Angeles","CO":"America/Denver","CT":"America/New_York","DE":"America/New_York",
    "FL":"America/New_York","GA":"America/New_York","HI":"Pacific/Honolulu","ID":"America/Denver",
    "IL":"America/Chicago","IN":"America/Indiana/Indianapolis","IA":"America/Chicago","KS":"America/Chicago",
    "KY":"America/New_York","LA":"America/Chicago","ME":"America/New_York","MD":"America/New_York",
    "MA":"America/New_York","MI":"America/Detroit","MN":"America/Chicago","MS":"America/Chicago",
    "MO":"America/Chicago","MT":"America/Denver","NE":"America/Chicago","NV":"America/Los_Angeles",
    "NH":"America/New_York","NJ":"America/New_York","NM":"America/Denver","NY":"America/New_York",
    "NC":"America/New_York","ND":"America/Chicago","OH":"America/New_York","OK":"America/Chicago",
    "OR":"America/Los_Angeles","PA":"America/New_York","RI":"America/New_York","SC":"America/New_York",
    "SD":"America/Chicago","TN":"America/Chicago","TX":"America/Chicago","UT":"America/Denver",
    "VT":"America/New_York","VA":"America/New_York","WA":"America/Los_Angeles","WV":"America/New_York",
    "WI":"America/Chicago","WY":"America/Denver","DC":"America/New_York"
}

tz_map_expr = F.create_map([F.lit(x) for kv in state_tz.items() for x in kv])
df = df.withColumn("timezone", tz_map_expr.getItem(F.col("state")))
df = df.withColumn("local_hour", F.hour(F.col("trans_ts")))
df = df.withColumn("is_night_transaction", F.when((F.col("local_hour") >= 0) & (F.col("local_hour") <= 5), F.lit(1)).otherwise(F.lit(0)))

holiday_map = {
    "2019-01-01":"New Year’s Day","2019-01-21":"Martin Luther King, Jr. Day","2019-02-18":"Presidents’ Day",
    "2019-05-27":"Memorial Day","2019-07-04":"Independence Day","2019-09-02":"Labor Day",
    "2019-10-14":"Columbus Day","2019-11-11":"Veterans Day","2019-11-28":"Thanksgiving Day","2019-12-25":"Christmas Day",
    "2020-01-01":"New Year’s Day","2020-01-20":"Martin Luther King, Jr. Day","2020-02-17":"Presidents’ Day",
    "2020-05-25":"Memorial Day","2020-07-04":"Independence Day","2020-09-07":"Labor Day",
    "2020-10-12":"Columbus Day","2020-11-11":"Veterans Day","2020-11-26":"Thanksgiving Day","2020-12-25":"Christmas Day",
    "2021-01-01":"New Year’s Day","2021-01-18":"Martin Luther King, Jr. Day","2021-02-15":"Presidents’ Day",
    "2021-05-31":"Memorial Day","2021-07-04":"Independence Day","2021-09-06":"Labor Day",
    "2021-10-11":"Columbus Day","2021-11-11":"Veterans Day","2021-11-25":"Thanksgiving Day","2021-12-25":"Christmas Day"
}

holiday_map_expr = F.create_map([F.lit(x) for kv in holiday_map.items() for x in kv])
df = df.withColumn("holiday_name", holiday_map_expr.getItem(F.date_format(F.col("trans_date"), "yyyy-MM-dd")))
df = df.withColumn("is_holiday", F.when(F.col("holiday_name").isNotNull(), F.lit(1)).otherwise(F.lit(0)))

df = df.withColumn("sanctions_hit_merchant", F.lit(None).cast(T.IntegerType()))
df = df.withColumn("sanctions_hit_customer", F.lit(None).cast(T.IntegerType()))

df_out = df.drop("trans_ts")

(
    df_out.write.mode("overwrite")
    .format("parquet")
    .partitionBy("trans_year", "trans_month")
    .save(out_path)
)
