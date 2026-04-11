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

# Script generated for node customers_curated
customers_curated_node1775874397447 = glueContext.create_dynamic_frame.from_catalog(database="db_test", table_name="customers_curated", transformation_ctx="customers_curated_node1775874397447")

# Script generated for node step_trainer
step_trainer_node1775874422923 = glueContext.create_dynamic_frame.from_catalog(database="db_test", table_name="raw_step_trainer", transformation_ctx="step_trainer_node1775874422923")

# Script generated for node SQL trusted
SqlQuery0 = '''
select distinct sr.* from step_trainer  sr

inner join customers_curated cc on cc.serialnumber = sr.serialnumber

'''
SQLtrusted_node1775874460124 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer":step_trainer_node1775874422923, "customers_curated":customers_curated_node1775874397447}, transformation_ctx = "SQLtrusted_node1775874460124")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLtrusted_node1775874460124, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775866862292", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1775874830855 = glueContext.write_dynamic_frame.from_options(frame=SQLtrusted_node1775874460124, connection_type="s3", format="glueparquet", connection_options={"path": "s3://jsedan-files/trusted/step_trainer/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1775874830855")

job.commit()
