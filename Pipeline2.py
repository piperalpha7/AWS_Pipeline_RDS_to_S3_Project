import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import concurrent.futures
import re
import boto3

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1722975761665 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://retail-project-demo/orders/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1722975761665")

# Script generated for node MySQL
MySQL_node1722975797327 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "valid_order_status_lk",
        "connectionName": "Mysql connection new",
    },
    transformation_ctx = "MySQL_node1722975797327"
)

# Script generated for node Join
AmazonS3_node1722975761665DF = AmazonS3_node1722975761665.toDF()
MySQL_node1722975797327DF = MySQL_node1722975797327.toDF()
Join_node1722975831864 = DynamicFrame.fromDF(AmazonS3_node1722975761665DF.join(MySQL_node1722975797327DF, (AmazonS3_node1722975761665DF['order_status'] == MySQL_node1722975797327DF['status_name']), "left"), glueContext, "Join_node1722975831864")

# Script generated for node SQL Query
SqlQuery0 = '''
select order_id,order_last_updated,customer_id,order_status,
coalesce(status_name,'INVALID') as status_check from myDataSource
'''
SQLQuery_node1722976223007 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1722975831864}, transformation_ctx = "SQLQuery_node1722976223007")

# Script generated for node Conditional Router
ConditionalRouter_node1722976358750 = threadedRoute(glueContext,
  source_DyF = SQLQuery_node1722976223007,
  group_filters = [GroupFilter(name = "invalid_orders", filters = lambda row: (bool(re.match("INVALID", row["status_check"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("INVALID", row["status_check"])))))])

# Script generated for node default_group
default_group_node1722976358910 = SelectFromCollection.apply(dfc=ConditionalRouter_node1722976358750, key="default_group", transformation_ctx="default_group_node1722976358910")

# Script generated for node invalid_orders
invalid_orders_node1722976358939 = SelectFromCollection.apply(dfc=ConditionalRouter_node1722976358750, key="invalid_orders", transformation_ctx="invalid_orders_node1722976358939")

# Script generated for node Change Schema
ChangeSchema_node1722976449747 = ApplyMapping.apply(frame=default_group_node1722976358910, mappings=[("order_last_updated", "timestamp", "order_last_updated", "timestamp"), ("customer_id", "int", "customer_id", "int"), ("order_id", "int", "order_id", "int"), ("order_status", "string", "order_status", "string")], transformation_ctx="ChangeSchema_node1722976449747")

# Script generated for node Change Schema
ChangeSchema_node1722976472430 = ApplyMapping.apply(frame=invalid_orders_node1722976358939, mappings=[("order_last_updated", "timestamp", "order_last_updated", "timestamp"), ("customer_id", "int", "customer_id", "int"), ("order_id", "int", "order_id", "int"), ("order_status", "string", "order_status", "string")], transformation_ctx="ChangeSchema_node1722976472430")

# Script generated for node Amazon S3
AmazonS3_node1722976771979 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1722975761665, connection_type="s3", format="glueparquet", connection_options={"path": "s3://retail-project-demo/orders/archive/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1722976771979")

# Script generated for node Amazon S3
AmazonS3_node1722976531225 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1722976449747, connection_type="s3", format="glueparquet", connection_options={"path": "s3://retail-project-demo/orders/staging/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1722976531225")

# Script generated for node Amazon S3
AmazonS3_node1722976550983 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1722976472430, connection_type="s3", format="glueparquet", connection_options={"path": "s3://retail-project-demo/orders/discarded/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1722976550983")

s3_client = boto3.client('s3')

bucket_name = 'retail-project-demo'
folder_prefix = 'orders/landing/'

# List all objects in the folder
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

if 'Contents' in response:
    for item in response['Contents']:
        if not item['Key'].endswith('/'):
            print(f"Deleting {item['Key']}")
            s3_client.delete_object(Bucket=bucket_name, Key=item['Key'])

print("All files deleted from the folder.")

job.commit()

