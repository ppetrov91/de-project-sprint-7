import logging
import os
from airflow.decorators import dag
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

spark_conn_id = "yarn_spark"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

@dag(dag_id="zones_datamart",
     schedule_interval=None,
     start_date=datetime(2021, 1, 1),
     catchup=False,
     tags=["sprint7_project", "zones_datamart"],
     is_paused_upon_creation=True
)
def zones_datamart():
    t_start = EmptyOperator(task_id="start")
    t_finish = EmptyOperator(task_id="finish")

    t_submit = SparkSubmitOperator(
                   task_id="zones_datamart",
                   application ="/lessons/scripts/zones_datamart_job.py",
                   conn_id= "yarn_spark",
                   application_args = ["2022-05-31",
                                       "31",
                                       "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020",
                                       "yarn",
                                       "sppetrov12",
                                       "/user/sppetrov12/data/events",
                                       "/user/sppetrov12/data/analytics/zones_datamart"
                                      ],
                   conf={"spark.driver.maxResultSize": "20g"},
                   num_executors=2,
                   executor_memory="4g",
                   executor_cores=2
               )
    t_start >> t_submit >> t_finish

zones_datamart_dag = zones_datamart()
