import sys
import pyspark.sql.functions as F

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.window import Window


def main():
    base_input_path = sys.argv[1]

    conf = SparkConf().setAppName('Events_DM_Users')
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    events = spark.read.parquet(base_input_path)

    event_time = events \
                    .select('event_id','event_type', 'event_timestamp', 'page_url_path' , 'user_custom_id')
    
    window = Window.partitionBy('user_custom_id').orderBy(F.col('user_custom_id'))

    mart = event_time \
                .withColumn("page_2", F.lead('page_url_path', offset= 1).over(window))\
                .withColumn("page_3", F.lead('page_url_path', offset= 2).over(window))\
                .withColumn("page_4", F.lead('page_url_path', offset= 3).over(window))\
                .selectExpr('event_id', 'user_custom_id',  'event_timestamp', 'page_url_path as page_1', "page_2",  "page_3",  "page_4") \

    mart.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql:62.84.117.206") \
        .option("dbtable", "dds.fct_events") \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .save()


if __name__ == "__main__":
    main()
