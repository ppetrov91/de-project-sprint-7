import os
import sys
import logging
import pyspark.sql.functions as F

from utils import input_paths, get_events
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import SparkSession


log = logging.getLogger(__name__)


def get_schema():
    return StructType([
               StructField("user_id", StringType(), True),
               StructField("datetime", TimestampType(), True),
               StructField("city", StringType(), True),
               StructField("event_lat", StringType(), True),
               StructField("event_lon", StringType(), True)
           ])

def get_last_events_for_user(events, topn):
    lw = Window().partitionBy(["user_id"]).orderBy(F.col("datetime").desc())
    return events.filter((F.col("event_lat").isNotNull()) & (F.col("event_lon").isNotNull())) \
                 .withColumn("rnk", F.row_number().over(lw)) \
                 .filter(F.col("rnk") <= topn) \
                 .select("user_id", "datetime", "city", "timezone", "event_lat", "event_lon")

def get_subscription_pairs(context, session, hdfs_url, events_src_path, dt, depth, topn):
    expr_list = ["user_id", "to_timestamp(substring(datetime, 1, 19), 'y-M-d H:m:s') as datetime",
                 "timezone", "city", "event_lat", "event_lon", "event.subscription_channel"]

    sb = get_events(context, session, hdfs_url, events_src_path,
                    dt, depth, expr_list, "subscription")

    if not sb:
        return sb, None

    # get last subscription event for each user
    sub = get_last_events_for_user(sb, topn)

    sb = sb.filter(F.col("subscription_channel").isNotNull()).distinct()
    sb_left = sb.selectExpr("user_id as user_id_left", "subscription_channel")
    sb_right = sb.selectExpr("user_id as user_id_right", "subscription_channel")

    sub_pairs = sb_left.join(sb_right, ["subscription_channel"]) \
                       .filter("user_id_left != user_id_right") \
                       .select("user_id_left", "user_id_right")

    return sub_pairs, sub

def get_not_writing_pairs(context, session, hdfs_url, events_src_path, dt, depth, topn, sub):
    expr_list = ["user_id", "to_timestamp(substring(datetime, 1, 19), 'y-M-d H:m:s') as datetime", "city",
                 "timezone", "event_lat", "event_lon", "event.message_from", "event.message_to"]
    
    mes = get_events(context, session, hdfs_url, events_src_path,
                     dt, depth, expr_list, "message")

    if not mes:
        return sub, session.createDataFrame([], get_schema())

    msg = get_last_events_for_user(mes, topn)

    mes = mes.filter((F.col("message_from").isNotNull()) & (F.col("message_to").isNotNull())) \
             .selectExpr("message_from as user_id_left", "message_to as user_id_right")

    mes_rev = mes.selectExpr("user_id_right as user_id_left", "user_id_left as user_id_right")
    mes = mes.union(mes_rev).distinct()
    return sub.join(mes, ["user_id_left", "user_id_right"], "anti"), msg

def get_last_reactions_for_user(context, session, hdfs_url, events_src_path, dt, depth, topn):
    expr_list = ["user_id", "to_timestamp(substring(datetime, 1, 19), 'y-M-d H:m:s') as datetime",
                 "city", "timezone", "event_lat", "event_lon"]

    reactions = get_events(context, session, hdfs_url, events_src_path,
                           dt, depth, expr_list, "reaction")

    if not reactions:
        return session.createDataFrame([], get_schema())

    return get_last_events_for_user(reactions, topn)

def get_recommendation_data_mart(sub, events):
    return sub.join(events, [sub.user_id_left == events.user_id]) \
              .selectExpr("user_id_left", "user_id_right", "datetime as user_left_datetime",
                          "city as zone_id", "timezone as user_left_timezone",
                          "event_lat as user_left_event_lat", "event_lon as user_left_event_lon") \
              .join(events, [sub.user_id_right == events.user_id]) \
              .selectExpr("user_id_left as user_left", "user_left_datetime", "user_left_timezone",
                          "user_left_event_lat", "zone_id", "user_left_event_lon", "user_id_right as user_right",
                          "event_lat as user_right_event_lat", "event_lon as user_right_event_lon"
                         ) \
              .withColumn("distance", F.asin(F.sqrt(F.pow(F.sin(F.radians((F.col("user_left_event_lat") - F.col("user_right_event_lat")) / F.lit(2))), 2) +
                                                          F.cos(F.radians(F.col("user_left_event_lat"))) * F.cos(F.radians(F.col("user_right_event_lat"))) *
                                                          F.pow(F.sin(F.radians((F.col("user_left_event_lon") - F.col("user_right_event_lon")) / F.lit(2))), 2)
                                                   )
                                            ) * F.lit(2 * 6371)
                         ) \
              .filter("distance <= 1") \
              .select(F.col("user_left"), F.col("user_right"), F.current_date().alias("processed_dttm"),
                      F.col("zone_id"), F.from_utc_timestamp(F.col("user_left_datetime"),
                                                             F.col("user_left_timezone")).alias("local_time")
                     )

def main():
    dt = sys.argv[1]
    depth = int(sys.argv[2])
    topn = int(sys.argv[3])
    hdfs_url = sys.argv[4]
    master = sys.argv[5]
    uname = sys.argv[6]
    events_src_path = sys.argv[7]
    recommendations_datamart_base_path = sys.argv[8]
    dst_path = f"{hdfs_url}/{recommendations_datamart_base_path}/date={dt}/depth={depth}"
    expr_list = ["user_id", "to_timestamp(substring(datetime, 1, 19), 'y-M-d H:m:s') as datetime",
                 "city", "timezone", "event_lat", "event_lon"]

    with SparkSession.builder.master(master) \
                             .appName(f"ZonesDatamart-{uname}-{dt}-d{depth}") \
                             .getOrCreate() as session:
        context = session.sparkContext
        sub_pairs, sub = get_subscription_pairs(context, session, hdfs_url, events_src_path, dt, depth, topn)

        if not sub_pairs:
            log.info("Subscription pairs were not found")
            return

        pairs_without_contact, mes = get_not_writing_pairs(context, session, hdfs_url, events_src_path, dt, depth, topn, sub_pairs)
        reactions = get_last_reactions_for_user(context, session, hdfs_url, events_src_path, dt, depth, topn)

        datetime_window = Window().partitionBy(["user_id"]).orderBy(F.col("datetime").desc())
        events = sub.unionAll(mes).unionAll(reactions) \
                    .withColumn("rnk", F.row_number().over(datetime_window)) \
                    .filter(F.col("rnk") <= topn) \
                    .select("user_id", "datetime", "city", "timezone", 
                            "event_lat", "event_lon")

        res = get_recommendation_data_mart(pairs_without_contact, events)
        res.write.mode("overwrite").parquet(dst_path)

if __name__ == "__main__":
    main()