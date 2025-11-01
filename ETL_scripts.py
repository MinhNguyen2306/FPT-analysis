import findspark
findspark.init()
import time
from uuid import UUID
import uuid
import time_uuid
import datetime
import pandas as pd
import json
import os
import sys
import pyspark.sql.functions as sf

from pyspark.sql import SparkSession
from pyspark.sql import *
from cassandra.cqltypes import TimeUUIDType
from dateutil import parser

from pyspark.sql.functions import when, isnan
from astrapy import DataAPIClient
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col, coalesce, lit


# Force PySpark to use the same Python as your driver
os.environ['PYSPARK_PYTHON'] = sys.executable       # workers
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable  # driver


url_azure = os.getenv("url")
azure_driver = os.getenv("driver")
azure_user = os.getenv("user")
azure_password = os.getenv("password")


def get_spark(app_name="SparkApp"):
    """
    Utility to create one SparkSession for the whole script.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.jars.packages",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
        )
        .config(
            "spark.jars",
            "/home/minh-nguyen/Desktop/Exercises/1st_project/sqljdbc_13.2/enu/jars/mssql-jdbc-13.2.0.jre8.jar"
        )
        .getOrCreate()
    )
    
def normalize_doc(doc):
    """Normalize one document safely for Spark."""
    normalized = {}

    for k, v in doc.items():
        # Skip system/internal fields
        if k == "_id":
            continue

        # --- Special handling for create_time ---
        if k == "create_time":
            try:
                # unwrap if nested (e.g., {"$uuid": "xxxx"})
                if isinstance(v, dict):
                    v = v.get("$uuid") or v.get("uuid") or v.get("value")
                # validate
                UUID(str(v))
                normalized[k] = str(v)
            except Exception:
                normalized[k] = None
            continue

        if isinstance(v, dict):
            if "$numberDouble" in v:
                normalized[k] = float(v["$numberDouble"])
            elif "$numberInt" in v:
                normalized[k] = int(v["$numberInt"])
            elif "$uuid" in v:
                normalized[k] = str(v["$uuid"])
            else:
                normalized[k] = json.dumps(v)
        elif isinstance(v, (list, tuple)):
            normalized[k] = json.dumps(v)  # store lists as JSON
        else:
            normalized[k] = v

    return normalized



def read_file_cassandra():
    """Read Astra DB collection safely into Spark DataFrame."""
    api_key = os.getenv("API_client")
    endpoint = os.getenv("API_endpoint")
    
    client = DataAPIClient(api_key)

    db = client.get_database_by_api_endpoint(
        endpoint,
        keyspace="studyde",
    )
    print(f"Connected to Astra DB: {db.list_collection_names()}")

    collection = db.get_collection("tracking")
    docs = collection.find()

    if not docs:
        raise ValueError("No documents found!")

    clean_docs = []
    for d in docs:
        nd = {}
        for k, v in d.items():
            if k == "_id":  # skip internal
                continue
            try:
                # Convert dicts/lists to JSON string
                if isinstance(v, (dict, list)):
                    nd[k] = json.dumps(v)
                # Ensure UUID strings are valid
                elif k == "create_time":
                    nd[k] = str(UUID(str(v)))
                else:
                    nd[k] = v
            except Exception:
                nd[k] = None
        clean_docs.append(nd)

    if not clean_docs:
        raise ValueError("No valid documents found!")

    pdf = pd.DataFrame(clean_docs)

    # Numeric columns
    for c in ["bid", "group_id", "campaign_id"]:
        if c in pdf.columns:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").fillna(0)

    # String columns
    for c in ["publisher_id", "custom_track", "job_id", "create_time"]:
        if c in pdf.columns:
            pdf[c] = pdf[c].astype(str).fillna("unknown")

    # Drop columns with unsupported types
    for col_name in pdf.columns:
        if pdf[col_name].dtype == 'object' and col_name not in ["publisher_id", "custom_track", "job_id", "create_time"]:
            pdf[col_name] = pdf[col_name].astype(str).fillna("unknown")

    # Create Spark DataFrame safely
    sdf = spark.createDataFrame(pdf)

    print(f"Cassandra loaded safely: {sdf.count()} rows")
    return sdf


def read_mysql():
    url = url_azure
    driver = azure_driver

    jobs = "(SELECT id AS job_id, company_id, group_id, campaign_id FROM jobs) AS A"

    return spark.read.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", jobs) \
        .option("user", azure_user) \
        .option("password", azure_password) \
        .option("encrypt", "true") \
        .option("trustServerCertificate", "false") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .option("loginTimeout", "30") \
        .load()


def _to_datetime_str(x):
    if x is None:
        return None
    try:
        u = UUID(str(x))  # ensure string
        t = time_uuid.TimeUUID(bytes=u.bytes)
        return t.get_datetime().strftime('%Y-%m-%d %H:%M:%S')
    except Exception:
        return None

# wrap it as a Spark UDF
to_datetime_str_udf = udf(_to_datetime_str, StringType())



def process_df(df):
    # UDF to convert Cassandra TimeUUID to datetime
    @udf(TimestampType())
    def uuid_to_datetime(u):
        if u is None:
            return None
        try:
            return time_uuid.TimeUUID(bytes=UUID(str(u)).bytes).get_datetime()
        except Exception:
            return None


    # Apply the UDF
    df_with_ts = df.withColumn("ts", uuid_to_datetime(col("create_time")))


    # Replace NULLs
    df_clean = df_with_ts.withColumn("job_id", coalesce(col("job_id"), lit(0))) \
                         .withColumn("bid", coalesce(col("bid"), lit(0))) \
                         .withColumn("campaign_id", coalesce(col("campaign_id"), lit(0))) \
                         .withColumn("group_id", coalesce(col("group_id"), lit(0))) \
                         .withColumn("publisher_id", coalesce(col("publisher_id"), lit("unknown"))) \
                         .withColumn("custom_track", coalesce(col("custom_track"), lit("unknown")))

    # Select required columns
    result = df_clean.select("create_time", "ts", "job_id", "custom_track", "bid",
                             "campaign_id", "group_id", "publisher_id")
    return result


def category_df(data):
    #process click data 
    
    existing_views = [t.name for t in spark.catalog.listTables() if t.isTemporary]
    for view_name in ['clickdata', 'conversiondata', 'qualified', 'disqualified']:
        if view_name in existing_views:
            spark.catalog.dropTempView(view_name)
            
    click_data = data.filter(data.custom_track == 'click')
    click_data.createTempView('clickdata')
    click_data = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, avg(bid) as bid_set , sum(bid) as spend_hour , count(*) as click from clickdata group by date(ts),
    hour(ts),job_id,publisher_id,campaign_id,group_id""")

    #process conversion data
    conversion_data = data.filter(data.custom_track == 'conversion')
    conversion_data.createTempView('conversiondata')
    conversion_data = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as conversion from conversiondata group by date(ts),
    hour(ts),job_id,publisher_id,campaign_id,group_id""")
    #process qualified data
    qualified_data = data.filter(data.custom_track == 'qualified')
    qualified_data.createTempView('qualified')
    qualified_data = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as qualified from qualified group by date(ts),
    hour(ts),job_id,publisher_id,campaign_id,group_id""")
    #process    fied data 
    disqualified_data = data.filter(data.custom_track == 'disqualified')
    disqualified_data.createTempView('disqualified')
    disqualified_data = spark.sql("""select date(ts) as date,hour(ts) as hour ,job_id,publisher_id,campaign_id,group_id, count(*) as disqualified from disqualified group by date(ts),
    hour(ts),job_id,publisher_id,campaign_id,group_id""")
    #filter null data 
    click_data = click_data.filter(click_data.date.isNotNull())
    conversion_data = conversion_data.filter(conversion_data.date.isNotNull())
    qualified_data = qualified_data.filter(qualified_data.date.isNotNull())
    disqualified_data = disqualified_data.filter(disqualified_data.date.isNotNull())

    #finalize output full join
    result = click_data.join(conversion_data, on = ['date','hour','job_id','publisher_id','campaign_id','group_id'],how='full').\
        join(qualified_data,on = ['date','hour','job_id','publisher_id','campaign_id','group_id'],how='full').\
            join(disqualified_data,on = ['date','hour','job_id','publisher_id','campaign_id','group_id'],how='full')
    
    return result 



def events_with_time():
    url = url_azure
    driver = azure_driver

    events= "(SELECT job_id FROM events) AS A"

    return spark.read.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", events) \
        .option("user", azure_user) \
        .option("password", azure_password) \
        .option("encrypt", "true") \
        .option("trustServerCertificate", "false") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .option("loginTimeout", "30") \
        .load()

def sanitize_for_sqlserver(df):
    numeric_cols = ['bid_set', 'spend_hour', 'campaign_id', 'group_id']
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, when(col(c).isNull() | isnan(col(c)), lit(0.0))
                                         .otherwise(col(c).cast("double")))
    
    long_cols = ['clicks', 'conversion', 'qualified_application', 'disqualified_application']
    for c in long_cols:
        if c in df.columns:
            df = df.withColumn(c, when(col(c).isNull() | isnan(col(c)), lit(0))
                                         .otherwise(col(c).cast("long")))
    
    string_cols = ['job_id', 'publisher_id', 'sources']
    for c in string_cols:
        if c in df.columns:
            df = df.withColumn(c, coalesce(col(c), lit("unknown")))
    
    return df


def insert_into(final_df):
    (
        final_df.write.format("jdbc")
        .option("driver", azure_driver)
        .option("url", url_azure)
        .option("dbtable", "events_update")
        .mode("append")
        .option("user", azure_user)      
        .option("password", azure_password)   
        .save()
    )


def cdc_map():
    url = url_azure
    driver = azure_driver

    sql = "(SELECT max(updated_at) as updated_at FROM events_update) AS A"

    return spark.read.format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", sql) \
        .option("user", azure_user) \
        .option("password", azure_password) \
        .option("encrypt", "true") \
        .option("trustServerCertificate", "false") \
        .option("hostNameInCertificate", "*.database.windows.net") \
        .option("loginTimeout", "30") \
        .load()


spark = get_spark("My ETL job")

def normalize_time_string(t):
    
    if not t:
        return '1997-01-01 23:59:59'
    s = str(t)
    if '.' in s:   # nếu có microsecond
        s = s.split('.')[0]
    if 'T' in s:   # nếu có dạng ISO
        s = s.replace('T', ' ')
    return s.strip()


def main_task():
        print("------------------Retrieving data from mysql------------------")
        
        mysql_df = read_mysql()
        mysql_df.show(5)
        
        print("------------------Retrieving data from Cassandra------------------")
        
        cassandra_df = read_file_cassandra()
        cassandra_df.show(5)
        
        print("------------------Processing data------------------")
        
        df = process_df(cassandra_df)
        df = df.cache() 
        df.show(5)
        data = df
        
        print("------------------Categorizinng data------------------")
        category_data = category_df(data)
        category_data.show(5)

        # join tất cả lại
        final_df = category_data.join(mysql_df, on='job_id', how='left').drop('campaign_id').drop('group_id')

        final_df = final_df.withColumn('updated_at',sf.lit(data.agg({'ts':'max'}).take(1)[0][0]))


        # build final
        final = (final_df
                .withColumnRenamed('date','dates')
                .withColumnRenamed('hour','hours')
                .withColumnRenamed('qualified','qualified_application')
                .withColumnRenamed('disqualified','disqualified_application')
                .withColumnRenamed('click','clicks')
                .withColumn('sources',sf.lit('Cassandra'))
                )

        final = final.withColumn('sources',sf.lit('Cassandra'))
        final.printSchema()

        final = sanitize_for_sqlserver(final)
        final.show(5)
        
        insert_into(final)
        

def updated_data(data):
        print("------------------Retrieving data from mysql------------------")
        mysql_df = read_mysql()
        mysql_df.show(5)
        print("------------------Processing data------------------")
        df = process_df(data)
        df = df.cache() 
        df.show(5)
        
        print("------------------Categorizinng data------------------")
        category_data = category_df(df)
        
        final_df = category_data.join(mysql_df, on='job_id', how='left').drop("campaign_id").drop("group_id")

        final_df = final_df.withColumn('updated_at',sf.lit(df.agg({'ts':'max'}).take(1)[0][0]))


        final = (final_df
                .withColumnRenamed('date','dates')
                .withColumnRenamed('hour','hours')
                .withColumnRenamed('qualified','qualified_application')
                .withColumnRenamed('disqualified','disqualified_application')
                .withColumnRenamed('click','clicks')
                .withColumn('sources',sf.lit('Cassandra'))
                )

        final = final.withColumn('sources',sf.lit('Cassandra'))
        final = sanitize_for_sqlserver(final)

        return final        
  
main_task()
