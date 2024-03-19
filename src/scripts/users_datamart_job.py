import os
import sys
import logging
import findspark
import pyspark.sql.functions as F

from utils import input_paths, get_events
from pyspark.sql.window import Window

findspark.init()
findspark.find()

from pyspark.sql import SparkSession

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"

log = logging.getLogger(__name__)

def get_user_city_stats(df):
    bw = Window().partitionBy(["user_id"])
    w = bw.orderBy(F.col("datetime"))

    ac = df.where("event_type == 'message'") \
           .withColumn("act_city", F.last_value(F.col("city")).over(w)) \
           .withColumn("rnk", F.row_number().over(w)) \
           .withColumn("cnt", F.count(F.col("city")).over(bw)) \
           .where("rnk == cnt").select("user_id", "act_city")

    r = df.withColumn("next_city", F.lead("city", 1, "start").over(w)) \
          .withColumn("max_datetime", F.last_value(F.col("datetime")).over(w)) \
          .withColumn("next_datetime",
                      F.coalesce(F.lead("datetime", 1).over(w), F.col("max_datetime"))) \
          .withColumn("diff_in_days", F.datediff("next_datetime", "datetime")) \
          .where("city != next_city") \
          .withColumn("travel_count", F.count(F.col("city")).over(bw))

    hc = r.where("travel_count == 1 or diff_in_days > 27") \
          .withColumn("rnk", F.row_number().over(w)) \
          .withColumn("cnt", F.count(F.col("city")).over(bw)) \
          .where("rnk == cnt").selectExpr("user_id", "city as home_city")

    ucs = r.withColumn("travel_array", F.collect_list(F.col("city")).over(w)) \
           .withColumn("rnk", F.row_number().over(w)) \
           .where("rnk == travel_count") \
           .withColumn("local_time",
                       F.from_utc_timestamp(F.col("datetime"), F.col("timezone"))) \
           .select("user_id", "travel_count", "travel_array", "local_time", "timezone")

    return ucs.join(ac, ["user_id"], "left") \
              .join(hc, ["user_id"], "left") \
              .select(ucs.user_id, ac.act_city, hc.home_city, ucs.timezone,
                      ucs.travel_count, ucs.travel_array, ucs.local_time)

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
                             .config("spark.executor.memory", "16g") \
                             .config("spark.driver.memory", "16g") \
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