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

# Script generated for node customers
customers_node1775864843523 = glueContext.create_dynamic_frame.from_catalog(database="db_test", table_name="customers_trusted", transformation_ctx="customers_node1775864843523")

# Script generated for node raw_accelerometer
raw_accelerometer_node1775865230747 = glueContext.create_dynamic_frame.from_catalog(database="db_test", table_name="raw_accelerometer", transformation_ctx="raw_accelerometer_node1775865230747")

# Script generated for node SQL Query
SqlQuery0 = '''
select 
cast(ra.timestamp as bigint) as timestamp,
ra.user,
cast(x as float) as x
,cast(y as float) as y
,cast(z as float) as z

from raw_accelerometer ra

inner join customer_trusted c on c.email = ra.user
'''
SQLQuery_node1775865241653 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"raw_accelerometer":raw_accelerometer_node1775865230747, "customer_trusted":customers_node1775864843523}, transformation_ctx = "SQLQuery_node1775865241653")

# Script generated for node accelerometer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1775865241653, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775861731723", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_trusted_node1775865356965 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1775865241653, connection_type="s3", format="glueparquet", connection_options={"path": "s3://jsedan-files/trusted/accelerometer/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="accelerometer_trusted_node1775865356965")

job.commit()
