import findspark
findspark.init()
from pyspark.sql import SparkSession
from astrapy import DataAPIClient
from datetime import datetime, timezone
import uuid
import random
import time
import os
import sys

# ==========================
# PYSPARK CONFIG
# ==========================
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = (
    SparkSession.builder
    .appName("ETL_Fake_Data_v2")
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0")
    .config("spark.jars", "/home/minh-nguyen/Desktop/Exercises/1st_project/sqljdbc_13.2/enu/jars/mssql-jdbc-13.2.0.jre8.jar")
    .getOrCreate()
)

# ==========================
# AZURE SQL SETTINGS
# ==========================
AZURE_URL = (
    "jdbc:sqlserver://minhnguyendb.database.windows.net:1433;"
    "database=studyde;"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)
AZURE_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
AZURE_USER = "minhnguyen@minhnguyendb"
AZURE_PASSWORD = "B@n94102"

# ==========================
# ASTRA SETTINGS
# ==========================
ASTRA_DB_TOKEN = "AstraCS:zJRKIyklsPkshLHgzwtYCzPX:fc7595b745e0fa3ce313fc138b5e90ea97ba7df979c24806deb46354328ce7d8"
ASTRA_DB_ENDPOINT = "https://970c3312-6476-40d6-ab13-c5129fcf9533-westus3.apps.astra.datastax.com"
ASTRA_KEYSPACE = "studyde"
ASTRA_COLLECTION = "tracking"

# ==========================
# READ FROM AZURE
# ==========================
def read_jobs_from_azure():
    query = "(SELECT id AS job_id, company_id, group_id, campaign_id FROM jobs) AS A"
    return (
        spark.read.format("jdbc")
        .option("url", AZURE_URL)
        .option("driver", AZURE_DRIVER)
        .option("dbtable", query)
        .option("user", AZURE_USER)
        .option("password", AZURE_PASSWORD)
        .load()
    )

def read_publishers_from_azure():
    query = "(SELECT DISTINCT id AS publisher_id FROM master_publishers) AS P"
    return (
        spark.read.format("jdbc")
        .option("url", AZURE_URL)
        .option("driver", AZURE_DRIVER)
        .option("dbtable", query)
        .option("user", AZURE_USER)
        .option("password", AZURE_PASSWORD)
        .load()
    )

# ==========================
# GENERATE FAKE DATA V2
# ==========================
def generate_fake_data_v2(jobs_df, publishers_df, n=10):
    jobs = jobs_df.collect()
    publishers = [row["publisher_id"] for row in publishers_df.collect()]
    fake_data = []

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0"
    ]
    screens = ["1366x768", "1536x864", "1920x1080", "1156x714"]
    interactions = ["click", "conversion", "qualified", "unqualified"]

    for _ in range(n):
        job = random.choice(jobs)
        publisher_id = random.choice(publishers)
        now = datetime.now(timezone.utc)
        create_time_uuid = uuid.uuid1()

        dl = f"http://fe.dev.gotoro.io/candidate-portal/job/{job['job_id']}?param1={job['group_id']}&param2=1"
        ua = random.choice(user_agents)
        screen = random.choice(screens)

        record = {
            "_id": str(uuid.uuid4()),
            "create_time": str(create_time_uuid),
            "bid": round(random.uniform(0.1, 1.0), 2),
            "bn": "Chrome 103",
            "campaign_id": int(job["campaign_id"]) if job["campaign_id"] is not None else 0,
            "cd": 24,
            "custom_track": random.choice(interactions),
            "de": "UTF-8",
            "dl": dl,
            "dt": "CandidatePortal",
            "ed": 1,
            "ev": random.randint(1, 100),
            "group_id": int(job["group_id"]) if job["group_id"] is not None else 0,
            "id": str(uuid.uuid4()),
            "job_id": str(job["job_id"]),
            "md": screen,
            "publisher_id": str(publisher_id),
            "rl": 1,
            "sr": screen,
            "ts": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "tz": -420,
            "ua": ua,
            "uid": str(uuid.uuid4()),
            "utm_campaign": "fake_campaign",
            "utm_content": "fake_content",
            "utm_medium": "cpc",
            "utm_source": "google",
            "utm_term": "fake_term",
            "v": 2,
            "vp": screen,
            "source": "fake_test"
        }

        fake_data.append(record)

    return fake_data

# ==========================
# WRITE TO ASTRA
# ==========================
def write_to_astra(fake_data):
    client = DataAPIClient(ASTRA_DB_TOKEN)
    db = client.get_database_by_api_endpoint(ASTRA_DB_ENDPOINT, keyspace=ASTRA_KEYSPACE)
    collection = db.get_collection(ASTRA_COLLECTION)

    collection.insert_many(fake_data)
    print(f"✅ Đã insert {len(fake_data)} bản ghi fake v2 vào AstraDB")

# ==========================
# MAIN
# ==========================
if __name__ == "__main__":
    print("🚀 Bắt đầu fake data từ Azure SQL → AstraDB (v2)...")
    jobs_df = read_jobs_from_azure()
    publishers_df = read_publishers_from_azure()
    jobs_df.show(3)
    publishers_df.show(3)

    n_records = random.randint(5, 15)
    fake_data = generate_fake_data_v2(jobs_df, publishers_df, n=n_records)
    write_to_astra(fake_data)
