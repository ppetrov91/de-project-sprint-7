import datetime
import sys
import pyspark.sql.functions as F

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from utils import input_paths


def get_cities_by_coords(events_df, cities_df, partition_key_list):
    window = Window().partitionBy(partition_key_list).orderBy("distance")
    return events_df.crossJoin(cities_df) \
                    .withColumn("distance", F.asin(F.sqrt(F.pow(F.sin(F.radians((F.col("event_lat") - F.col("city_lat")) / F.lit(2))), 2) +
                                                          F.cos(F.radians(F.col("event_lat"))) * F.cos(F.radians(F.col("city_lat"))) *
                                                          F.pow(F.sin(F.radians((F.col("event_lon") - F.col("city_lon")) / F.lit(2))), 2)
                                                         )
                                                  ) * F.lit(2 * 6371)
                               ) \
                    .withColumn("rn", F.row_number().over(window)) \
                    .where("rn = 1") \
                    .select("user_id", "datetime", "date", "event_type", "event_lat",
                            "event_lon", "city", "timezone", "event")


def get_events_with_coords(sql, events_path_list, hdfs_url, city_path):
    events = sql.read.parquet(*events_path_list) \
                .withColumn("datetime", F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))) \
                .withColumn("date", F.substring(F.col("datetime"), 1, 10)) \
                .withColumn("user_id", F.when(F.col("event_type") == "message", F.col("event.message_from")) \
                                        .when(F.col("event_type") == "reaction", F.col("event.reaction_from")) \
                                        .when(F.col("event_type") == "subscription", F.col("event.user")) \
                                        .otherwise("")
                           ) \
                .selectExpr("user_id", "date", "datetime", "event_type", "lat as event_lat", "lon AS event_lon", "event")

    events_with_coords = events.filter((F.col("event_lat").isNotNull()) & (F.col("event_lon").isNotNull()))
    events_without_coords = events.filter((F.col("event_lat").isNull()) | (F.col("event_lon").isNull())) \
                                  .selectExpr("user_id", "datetime", "date", "event_type", "event_lat",
                                              "event_lon", "'-' as city", "'-' as timezone", "event")

    cities = sql.read \
                .options(header=True, delimiter=';').csv(f"{hdfs_url}/{city_path}") \
                .selectExpr("city", "lat as city_lat", "lng as city_lon", "timezone")


    events_cities = get_cities_by_coords(events_with_coords, cities, ["user_id", "datetime"])
    return events_cities, events_without_coords


def main():
    dt = sys.argv[1]
    depth = int(sys.argv[2])
    hdfs_url = sys.argv[3]
    uname = sys.argv[4]
    events_src_path = sys.argv[5]
    events_dst_path = sys.argv[6]
    city_path = sys.argv[7]

    conf = SparkConf().setAppName(f"LoadGeoEvents-{uname}-{dt}-d{depth}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    events_path_list = input_paths(sc, hdfs_url, events_src_path, dt, depth)

    if len(events_path_list) == 0:
        return

    for df in get_events_with_coords(sql, events_path_list, hdfs_url, city_path):
        df.write \
          .partitionBy("date", "event_type") \
          .mode("append").parquet(f"{hdfs_url}/{events_dst_path}")

if __name__ == "__main__":
    main()
