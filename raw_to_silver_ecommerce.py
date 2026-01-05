import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ---------- Glue bootstrap ----------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------- CONFIG ----------
BUCKET = "surya-de-project-1-2026"
RAW_PREFIX = "raw/"
SILVER_PREFIX = "silver/"

s3 = boto3.client("s3")

# ---------- Helpers ----------
def list_all_keys(bucket, prefix):
    keys = []
    token = None
    while True:
        params = {"Bucket": bucket, "Prefix": prefix}
        if token:
            params["ContinuationToken"] = token
        resp = s3.list_objects_v2(**params)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            token = resp["NextContinuationToken"]
        else:
            break
    return keys

def pick_csv(keys, keyword):
    keyword = keyword.lower()
    matches = [
        k for k in keys
        if k.lower().endswith(".csv")
        and keyword in k.lower()
        and not k.endswith("/")
    ]
    if not matches:
        raise Exception(f"No CSV found for '{keyword}' in raw/")
    matches.sort(key=len)
    return matches[0]

def s3uri(key):
    return f"s3://{BUCKET}/{key}"

# ---------- Discover files ----------
all_keys = list_all_keys(BUCKET, RAW_PREFIX)

customers_key = pick_csv(all_keys, "customer")
products_key  = pick_csv(all_keys, "product")
orders_key    = pick_csv(all_keys, "order")

print("Files used:")
print(customers_key)
print(products_key)
print(orders_key)

# ---------- Read CSV ----------
def read_csv(path):
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)

customers = read_csv(s3uri(customers_key)).dropDuplicates()
products  = read_csv(s3uri(products_key)).dropDuplicates()
orders    = read_csv(s3uri(orders_key)).dropDuplicates()

# ---------- Write SILVER ----------
customers.write.mode("overwrite").parquet(f"s3://{BUCKET}/{SILVER_PREFIX}customers/")
products.write.mode("overwrite").parquet(f"s3://{BUCKET}/{SILVER_PREFIX}products/")
orders.write.mode("overwrite").parquet(f"s3://{BUCKET}/{SILVER_PREFIX}orders/")

job.commit()
