import os
import sys
import logging
import pyspark.sql.functions as F

from utils import input_paths, get_events
from pyspark.sql.window import Window
from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

def get_user_city_stats(df):
    base_window = Window().partitionBy(["user_id"])
    datetime_window = base_window.orderBy(F.col("datetime"))
    datetime_window_desc = base_window.orderBy(F.col("datetime").desc())

    actual_city = df.where("event_type == 'message'") \
                    .withColumn("rnk", F.row_number().over(datetime_window_desc)) \
                    .where("rnk == 1") \
                    .selectExpr("user_id", "city as act_city")

    travel_cities = df.withColumn("next_city", F.lead("city", 1, "finish").over(datetime_window)) \
                      .withColumn("prev_city", F.lag("city", 1, "start").over(datetime_window)) \
                      .filter("city != prev_city or city != next_city") \
                      .withColumn("next_datetime",
                                  F.coalesce(F.lead("datetime", 1).over(datetime_window), F.col("datetime") + F.expr('INTERVAL 24 HOURS') )) \
                      .withColumn("prev_datetime",
                                  F.coalesce(F.lag("datetime", 1).over(datetime_window), F.col("datetime"))) \
                      .filter(F.col("city") != F.col("next_city")) \
                      .withColumn("diff_in_days", F.datediff("next_datetime", "prev_datetime")) \
                      .withColumn("travel_array", F.collect_list(F.col("city")).over(datetime_window)) \
                      .withColumn("travel_count", F.count(F.col("city")).over(base_window)) \
                      .withColumn("rnk", F.row_number().over(datetime_window_desc))

    home_city = travel_cities.filter("travel_count == 1 or diff_in_days > 27") \
                             .withColumn("rnk", F.row_number().over(datetime_window_desc)) \
                             .filter("rnk == 1").selectExpr("user_id", "city as home_city")

    travel_cities = travel_cities.filter("rnk == 1") \
                                 .withColumn("local_time",
                                             F.from_utc_timestamp(F.col("datetime"), F.col("timezone"))) \
                                 .select("user_id", "travel_count", "travel_array", "local_time", "timezone")

    return travel_cities.join(actual_city, ["user_id"], "left") \
                        .join(home_city, ["user_id"], "left") \
                        .select(travel_cities.user_id, actual_city.act_city, 
                                home_city.home_city, travel_cities.timezone, 
                                travel_cities.travel_count, travel_cities.travel_array, 
                                travel_cities.local_time)

def main():
    dt = sys.argv[1]
    depth = int(sys.argv[2])
    hdfs_url = sys.argv[3]
    master = sys.argv[4]
    uname = sys.argv[5]
    events_src_path = sys.argv[6]
    users_datamart_base_path = sys.argv[7]
    dst_path = f"{hdfs_url}/{users_datamart_base_path}/date={dt}/depth={depth}"
    expr_list = ["user_id", "to_timestamp(substring(datetime, 1, 19), 'y-M-d H:m:s') as datetime",
                 "event_type", "city", "timezone"]

    with SparkSession.builder.master(master) \
                             .appName(f"UsersDatamart-{uname}-{dt}-d{depth}") \
                             .getOrCreate() as session:
        context = session.sparkContext
        events = get_events(context, session, hdfs_url, events_src_path, 
                            dt, depth, expr_list)

        if not events:
            log.info("No events were found")
            return

        events = events.filter((F.col("city") != '-'))        
        users_datamart = get_user_city_stats(events)
        users_datamart.write.mode("overwrite").parquet(dst_path)

if __name__ == "__main__":
    main()