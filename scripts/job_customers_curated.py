import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer_trusted_table
accelerometer_trusted_table_node1775867554186 = glueContext.create_dynamic_frame.from_catalog(database="db_test", table_name="accelerometer_trusted_i", transformation_ctx="accelerometer_trusted_table_node1775867554186")

# Script generated for node customer_trusted_table
customer_trusted_table_node1775867519162 = glueContext.create_dynamic_frame.from_catalog(database="db_test", table_name="customers_trusted", transformation_ctx="customer_trusted_table_node1775867519162")

# Script generated for node SQL Query
SqlQuery0 = '''
select  distinct serialnumber, sharewithpublicasofdate, birthday, registrationdate, sharewithresearchasofdate, customername, email, 
lastupdatedate, phone, sharewithfriendsasofdate
from 
customers_trusted

inner join accelerometer_trusted_i 
on customers_trusted.email =  accelerometer_trusted_i.user
'''
SQLQuery_node1775867574329 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customers_trusted":customer_trusted_table_node1775867519162, "accelerometer_trusted_i":accelerometer_trusted_table_node1775867554186}, transformation_ctx = "SQLQuery_node1775867574329")

# Script generated for node customers_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1775867574329, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775866862292", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customers_curated_node1775868146867 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1775867574329, connection_type="s3", format="glueparquet", connection_options={"path": "s3://jsedan-files/curated/customers/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="customers_curated_node1775868146867")

job.commit()
