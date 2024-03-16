from datetime import date, timedelta


def input_paths(sc, hdfs_url, input_path, dt, depth, event_type=""):
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._gateway.jvm.java.net.URI(hdfs_url), 
                                                     sc._jsc.hadoopConfiguration())
    dt = date.fromisoformat(dt)
    event_type_filter = ("", f"event_type={event_type}")[event_type != ""]

    return [f"{hdfs_url}/{input_path}/date={dt - timedelta(days=i)}/{event_type_filter}" for i in range(depth) \
            if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(f"{input_path}/date={dt - timedelta(days=i)}/{event_type_filter}"))
           ]



