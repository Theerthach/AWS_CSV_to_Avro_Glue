import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1754870910953 = glueContext.create_dynamic_frame.from_catalog(database="ordersgluetable", table_name="input_csv_thee", transformation_ctx="AWSGlueDataCatalog_node1754870910953")

# Script generated for node Select Fields
SelectFields_node1754871538145 = SelectFields.apply(frame=AWSGlueDataCatalog_node1754870910953, paths=["orderid", "customername", "product", "quantity", "price", "orderdate", "shipdate", "country"], transformation_ctx="SelectFields_node1754871538145")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SelectFields_node1754871538145, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1754870340187", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1754871543891 = glueContext.write_dynamic_frame.from_options(frame=SelectFields_node1754871538145, connection_type="s3", format="avro", connection_options={"path": "s3://output-csv-thee", "partitionKeys": []}, transformation_ctx="AmazonS3_node1754871543891")

job.commit()
