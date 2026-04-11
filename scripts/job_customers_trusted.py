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

# Script generated for node costumers_table
costumers_table_node1775861938519 = glueContext.create_dynamic_frame.from_catalog(database="db_test", table_name="raw_customers", transformation_ctx="costumers_table_node1775861938519")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from ct
  where sharewithresearchasofdate != 0
'''
SQLQuery_node1775861947272 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"ct":costumers_table_node1775861938519}, transformation_ctx = "SQLQuery_node1775861947272")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1775861947272, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775861731723", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1775863750375 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1775861947272, connection_type="s3", format="glueparquet", connection_options={"path": "s3://jsedan-files/trusted/customers/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1775863750375")

job.commit()
