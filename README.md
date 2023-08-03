
# Powering Event-Driven Workloads with Hudi Read Stream &amp; AWS Glue Streaming JOBS!
![aas](https://github.com/soumilshah1995/Powering-Event-Driven-Workloads-with-Hudi-Read-Stream-AWS-Glue-Streaming-JOBS-/assets/39345855/aefc1ad8-40e6-4862-a50d-d8c3f8b8b423)

### Glue Job parameters 

![image](https://github.com/soumilshah1995/Powering-Event-Driven-Workloads-with-Hudi-Read-Stream-AWS-Glue-Streaming-JOBS-/assets/39345855/36302819-9099-4570-8a29-d1cf457175ed)

```
--conf                        |  spark.serializer=org.apache.spark.serializer.KryoSerializer  --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.hive.convertMetastoreParquet=false --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog --conf spark.sql.legacy.pathOptionBehavior.enabled=true --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension

--datalake-formats            | hudi
```

# Glue Code

```
# Import necessary libraries
try:
    import os
    import sys
    import boto3
    import time
    import datetime
    import json
    from datetime import datetime
    from awsglue.utils import getResolvedOptions
    from pyspark.sql.functions import lit, udf
    from pyspark.sql.session import SparkSession
    from awsglue.context import GlueContext
    from awsglue.job import Job
except Exception as e:
    print("Modules are missing: {} ".format(e))

# Get the job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Create a Spark session
spark = (SparkSession.builder
         .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
         .config('spark.sql.hive.convertMetastoreParquet', 'false')
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog')
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true')
         .getOrCreate())

# Create a Spark context and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)

# Define the bucket and paths
global BUCKET, hudi_path, check_point_path, event_bus_name, TABLE_NAME
BUCKET = "<BUCKET>"
TABLE_NAME = '<TABLE>'
hudi_path = f"s3://{BUCKET}/silver/table_name={TABLE_NAME}/"
check_point_path = f"s3://{BUCKET}/check_point/{TABLE_NAME}/"
region = 'us-west-1'
event_bus_name = "<EVENT BUS NAME GOES HERE>"

# Read stream to streaming DataFrame
stream = spark.readStream \
    .format("hudi") \
    .load(hudi_path)


class AWSEventBus:
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, EventBusName):
        self.EventBusName = EventBusName
        self.client = boto3.client(
            "events"
        )

    def send_events(self, json_message, DetailType, Source):
        response = self.client.put_events(
            Entries=[
                {
                    'Time': datetime.now(),
                    'Source': Source,
                    'Resources': [],
                    'DetailType': DetailType,
                    'Detail': json.dumps(json_message),
                    'EventBusName': self.EventBusName,
                },
            ]
        )
        return response


def send_to_event_bridge(df, region_name='', event_bus_name='', source_name="hudi", detail_type=''):
    helper = AWSEventBus(EventBusName=event_bus_name)
    for row in df.collect():
        row_dict = row.asDict()
        res = helper.send_events(json_message=row_dict, Source=source_name, DetailType=detail_type)


def process_batch(spark_df, batch_num):
    try:
        # Process the batch
        spark_df.show()

        if spark_df.count() > 0:
            send_to_event_bridge(
                region_name=region,
                event_bus_name=event_bus_name,
                source_name="hudi",
                detail_type=TABLE_NAME,
                df=spark_df
            )

    except Exception as e:
        print("Error in processing batch:", batch_num)
        print(e)


# Write stream to console and process each batch using the defined function
stream_obj = stream.writeStream \
    .format("console") \
    .option("checkpointLocation", check_point_path) \
    .foreachBatch(process_batch) \
    .start()

# Wait for the streaming to finish
stream_obj.awaitTermination()


```
