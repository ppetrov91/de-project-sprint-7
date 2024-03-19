import os
import sys
import logging
import pyspark.sql.functions as F

from utils import input_paths, get_events
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

def get_events_stats(e, group_by_list, prefix, return_col_list):
    return e.groupBy(*group_by_list) \
            .agg(
                  F.sum("message").alias(f"{prefix}_message"),
                  F.sum("reaction").alias(f"{prefix}_reaction"),
                  F.sum("subscription").alias(f"{prefix}_subscription"),
                  F.countDistinct("user_id").alias(f"{prefix}_user")
                ).select(*return_col_list)


def get_zones_datamart(events):
    e = events.withColumn("month", F.date_trunc("month", F.col("datetime"))) \
              .withColumn("week", F.date_trunc("week", F.col("datetime"))) \
              .withColumn("message",
                          F.when(F.col("event_type") == "message", 1).otherwise(0)) \
              .withColumn("reaction",
                          F.when(F.col("event_type") == "reaction", 1).otherwise(0)) \
              .withColumn("subscription",
                          F.when(F.col("event_type") == "subscription", 1).otherwise(0))

    emc = get_events_stats(e, ["month", "city"], "month",
                              ["month", "city", "month_message", "month_reaction",
                               "month_subscription", "month_user"]
                          )

    emwc = get_events_stats(e, ["month", "week", "city"], "week",
                               ["month", "week", "city", "week_message", "week_reaction",
                                "week_subscription", "week_user"]
                           )

    return emc.join(emwc, ["month", "city"]) \
              .select(emwc.month, emwc.week, emwc.city, emwc.week_message,
                      emwc.week_reaction, emwc.week_subscription, emwc.week_user,
                      emc.month_message, emc.month_reaction, emc.month_subscription,
                      emc.month_user)

def main():
    dt = sys.argv[1]
    depth = int(sys.argv[2])
    hdfs_url = sys.argv[3]
    master = sys.argv[4]
    uname = sys.argv[5]
    events_src_path = sys.argv[6]
    zones_datamart_base_path = sys.argv[7]
    dst_path = f"{hdfs_url}/{zones_datamart_base_path}/date={dt}/depth={depth}"
    expr_list = ["user_id", "to_timestamp(substring(datetime, 1, 19), 'y-M-d H:m:s') as datetime",
                 "event_type", "city"]

    with SparkSession.builder.master(master) \
                             .appName(f"ZonesDatamart-{uname}-{dt}-d{depth}") \
                             .getOrCreate() as session:
        context = session.sparkContext
        events = get_events(context, session, hdfs_url, events_src_path,
                            dt, depth, expr_list)

        if not events:
            log.info("No events were not found")
            return

        events = events.filter((F.col("city") != '-'))
        zones_datamart = get_zones_datamart(events)
        zones_datamart.write.mode("overwrite").parquet(dst_path)


if __name__ == "__main__":
    main()
