import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F 

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node orders
orders_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "orders",
        "connectionName": "Mysql connection new",
    },
    transformation_ctx = "orders_dynamic_frame"
)

# Script generated for node orders_lf
fetch_details_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "fetch_details",
        "connectionName": "Mysql connection new",
    },
    transformation_ctx = "fetch_details_dynamic_frame"
)

# Script generated for node SQL Query
SqlQuery920 = '''
select * from orders where order_last_updated > 
(select max(last_fetched) from orders_fd
where tablename = 'orders')
'''

filtered_orders_dynamic_frame = sparkSqlQuery(glueContext, query = SqlQuery920, mapping = {"orders_fd":fetch_details_dynamic_frame, "orders":orders_dynamic_frame}, transformation_ctx = "filtered_orders_dynamic_frame")


filtered_orders_df = filtered_orders_dynamic_frame.toDF().coalesce(1)

filtered_orders_dynamic_frame_single_file = DynamicFrame.fromDF(filtered_orders_df, glueContext, "filtered_orders_dynamic_frame_single_file")

# Script generated for node Amazon S3
AmazonS3_node1722675901037 = glueContext.getSink(path="s3://retail-project-demo/orders/landing/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1722675901037")
AmazonS3_node1722675901037.setCatalogInfo(catalogDatabase="retail-project",catalogTableName="orders_landing")
AmazonS3_node1722675901037.setFormat("glueparquet", compression="snappy")
AmazonS3_node1722675901037.writeFrame(filtered_orders_dynamic_frame_single_file)

max_timestamp_df = orders_dynamic_frame.toDF().agg(F.max("order_last_updated").alias("max_order_last_updated"))

max_order_last_updated = max_timestamp_df.collect()[0]['max_order_last_updated']

print(f"Max order_last_updated: {max_order_last_updated}")

#prepare a dataframe for the insert operation

insert_data = [(max_order_last_updated,'orders')]

insert_df = spark.createDataFrame(insert_data, ['last_fetched','tablename'])

insert_df.show()

insert_dynamic_frame = DynamicFrame.fromDF(insert_df, glueContext, "insert_dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame = insert_dynamic_frame,
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "fetch_details",
        "connectionName": "Mysql connection new",
    },
    transformation_ctx = "insert_orders_fd"
)

job.commit()











