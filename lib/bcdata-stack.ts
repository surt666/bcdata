import * as cdk from "aws-cdk-lib/core";
import { Construct } from "constructs";
import { aws_s3tables as s3tables } from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cr from "aws-cdk-lib/custom-resources";
import * as lakeformation from "aws-cdk-lib/aws-lakeformation";
import * as glue from "aws-cdk-lib/aws-glue";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3Deployment from "aws-cdk-lib/aws-s3-deployment";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as ssm from "aws-cdk-lib/aws-ssm";

// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class BcdataStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const tableBucket = "billing10";
    const namespace_name = "billingdata6";

    // S3 bucket for Athena query results
    const athenaResultsBucket = new s3.Bucket(this, "AthenaResultsBucket", {
      bucketName: `${tableBucket}-athena-results`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // S3 bucket for meter stats CSV files from ingestion
    const meterStatsIngestionBucket = new s3.Bucket(
      this,
      "MeterStatsIngestionBucket",
      {
        bucketName: `billing-${cdk.Aws.ACCOUNT_ID}-${cdk.Aws.REGION}`,
        versioned: false,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: false,
        lifecycleRules: [
          {
            id: "DeleteOldCsvFiles",
            enabled: true,
            prefix: "meter-stats/",
            expiration: cdk.Duration.days(90),
          },
        ],
      },
    );

    // Store bucket name in SSM Parameter Store for other stacks to use
    new ssm.StringParameter(this, "MeterStatsIngestionBucketParameter", {
      parameterName: "/bcdata/meter-stats-ingestion-bucket",
      stringValue: meterStatsIngestionBucket.bucketName,
      description: "S3 bucket name for meter stats CSV ingestion",
      tier: ssm.ParameterTier.ADVANCED,
    });

    // Get ingestion account ID from context (for cross-account access)
    const ingestionAccountId = this.node.tryGetContext("INGESTION_ACCOUNT_ID");

    // Only create cross-account resources if INGESTION_ACCOUNT_ID is provided
    if (ingestionAccountId) {
      // Create role that allows ingestion Lambda to read SSM parameters
      const ssmReadRole = new iam.Role(this, "BcdataSSMReadRole", {
        roleName: "BcdataSSMReadRole",
        assumedBy: new iam.AccountPrincipal(ingestionAccountId),
        description:
          "Role to allow meter stats ingestion Lambda to read SSM parameters",
      });

      ssmReadRole.addToPolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: ["ssm:GetParameter", "ssm:GetParameters"],
          resources: [
            `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/bcdata/*`,
          ],
        }),
      );

      // Grant cross-account S3 bucket access to ingestion account
      meterStatsIngestionBucket.addToResourcePolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          principals: [new iam.AccountPrincipal(ingestionAccountId)],
          actions: ["s3:PutObject", "s3:PutObjectAcl", "s3:GetObject"],
          resources: [`${meterStatsIngestionBucket.bucketArn}/*`],
        }),
      );

      meterStatsIngestionBucket.addToResourcePolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          principals: [new iam.AccountPrincipal(ingestionAccountId)],
          actions: ["s3:ListBucket", "s3:GetBucketLocation"],
          resources: [meterStatsIngestionBucket.bucketArn],
        }),
      );
    } else {
      // Add a warning that cross-account access won't be configured
      cdk.Annotations.of(this).addWarning(
        "INGESTION_ACCOUNT_ID not provided. Cross-account S3 bucket access and SSM read role will not be created. " +
          "Provide via: cdk deploy -c INGESTION_ACCOUNT_ID=123456789012",
      );
    }

    const s3TableBucket = new s3tables.CfnTableBucket(
      this,
      "BillingTableBucket",
      {
        tableBucketName: tableBucket,
        encryptionConfiguration: {
          sseAlgorithm: "AES256",
        },
        unreferencedFileRemoval: {
          noncurrentDays: 7,
          status: "Enabled",
          unreferencedDays: 14,
        },
      },
    );

    const namespace = new s3tables.CfnNamespace(this, "BillingDataNamespace", {
      namespace: namespace_name,
      tableBucketArn: s3TableBucket.attrTableBucketArn,
    });
    namespace.addDependency(s3TableBucket);

    // Create Glue resource link for S3 Tables
    const resourceLink = new glue.CfnDatabase(this, "S3TablesResourceLink", {
      catalogId: cdk.Aws.ACCOUNT_ID,
      databaseInput: {
        name: `${namespace.namespace}_link`,
        targetDatabase: {
          catalogId: `${cdk.Aws.ACCOUNT_ID}:s3tablescatalog/${tableBucket}`,
          databaseName: namespace.namespace,
        },
      },
    });
    resourceLink.node.addDependency(namespace);

    const configureTableFnRole = new iam.Role(this, "LambdaRole", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole",
        ),
      ],
    });
    // Grant permissions to Lambda
    configureTableFnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "glue:GetDataCatalog",
          "glue:CreateTable",
        ],
        resources: ["*"],
      }),
    );

    configureTableFnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "s3tables:GetTable",
          "s3tables:GetTableMetadata",
          "s3tables:PutTableData",
          "s3tables:GetTableData",
          "s3tables:DeleteTableData",
          "s3tables:UpdateTableMetadata",
        ],
        resources: [s3TableBucket.attrTableBucketArn + "/*"],
      }),
    );

    configureTableFnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
        ],
        resources: [
          athenaResultsBucket.bucketArn,
          `${athenaResultsBucket.bucketArn}/*`,
        ],
      }),
    );

    configureTableFnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["glue:GetDatabase", "glue:GetTable"],
        resources: ["*"],
      }),
    );

    configureTableFnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "lakeformation:GetDataAccess",
          "lakeformation:GrantPermissions",
          "lakeformation:RevokePermissions",
        ],
        resources: ["*"],
      }),
    );

    // Grant Lake Formation permissions on the resource link database
    const lambdaResourceLinkDbPermissions = new lakeformation.CfnPermissions(
      this,
      "LambdaResourceLinkDbPermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: configureTableFnRole.roleArn,
        },
        resource: {
          databaseResource: {
            name: `${namespace.namespace}_link`,
            catalogId: cdk.Aws.ACCOUNT_ID,
          },
        },
        permissions: ["ALL"],
      },
    );
    lambdaResourceLinkDbPermissions.node.addDependency(resourceLink);
    lambdaResourceLinkDbPermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on resource link tables
    const lambdaResourceLinkTablePermissions = new lakeformation.CfnPermissions(
      this,
      "LambdaResourceLinkTablePermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: configureTableFnRole.roleArn,
        },
        resource: {
          tableResource: {
            databaseName: `${namespace.namespace}_link`,
            catalogId: cdk.Aws.ACCOUNT_ID,
            tableWildcard: {},
          },
        },
        permissions: ["SELECT", "INSERT", "DELETE", "DESCRIBE", "ALTER"],
      },
    );
    lambdaResourceLinkTablePermissions.node.addDependency(resourceLink);
    lambdaResourceLinkTablePermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on actual S3 Tables database
    const lambdaS3TablesDbPermissions = new lakeformation.CfnPermissions(
      this,
      "LambdaS3TablesDbPermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: configureTableFnRole.roleArn,
        },
        resource: {
          databaseResource: {
            name: namespace.namespace,
            catalogId: `${cdk.Aws.ACCOUNT_ID}:s3tablescatalog/${tableBucket}`,
          },
        },
        permissions: ["ALL"],
      },
    );
    lambdaS3TablesDbPermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on actual S3 Tables
    const lambdaS3TablesTablePermissions = new lakeformation.CfnPermissions(
      this,
      "LambdaS3TablesTablePermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: configureTableFnRole.roleArn,
        },
        resource: {
          tableResource: {
            databaseName: namespace.namespace,
            catalogId: `${cdk.Aws.ACCOUNT_ID}:s3tablescatalog/${tableBucket}`,
            tableWildcard: {},
          },
        },
        permissions: ["SELECT", "INSERT", "DELETE", "DESCRIBE", "ALTER"],
      },
    );
    lambdaS3TablesTablePermissions.node.addDependency(namespace);

    // const metersTable = new s3tables.CfnTable(this, "meterstable", {
    //   namespace: namespace.namespace,
    //   openTableFormat: "ICEBERG",
    //   tableBucketArn: s3TableBucket.attrTableBucketArn,
    //   tableName: "meters",

    //   // the properties below are optional
    //   compaction: {
    //     status: "enabled",
    //     targetFileSizeMb: 256,
    //   },
    //   icebergMetadata: {
    //     icebergSchema: {
    //       schemaFieldList: [
    //         {
    //           name: "timestamp",
    //           type: "timestamp",
    //           required: true,
    //         },
    //         {
    //           name: "company_id",
    //           type: "int",
    //           required: true,
    //         },
    //         {
    //           name: "property_id",
    //           type: "int",
    //         },
    //         {
    //           name: "building_id",
    //           type: "int",
    //           required: true,
    //         },
    //         {
    //           name: "company_name",
    //           type: "string",
    //           required: true,
    //         },
    //         {
    //           name: "building_name",
    //           type: "string",
    //           required: true,
    //         },
    //         {
    //           name: "total",
    //           type: "int",
    //           required: true,
    //         },
    //         {
    //           name: "actively_remote_read",
    //           type: "int",
    //         },
    //         {
    //           name: "active_manual_read",
    //           type: "int",
    //         },
    //         {
    //           name: "active_calculation_meters",
    //           type: "int",
    //         },
    //         {
    //           name: "inactive_remotely_read",
    //           type: "int",
    //         },
    //         {
    //           name: "inactive_manually_read",
    //           type: "int",
    //         },
    //         {
    //           name: "inactive_calculation_meters",
    //           type: "int",
    //         },
    //         {
    //           name: "unsupported_remotely_read",
    //           type: "int",
    //         },
    //         {
    //           name: "unsupported_manually_read",
    //           type: "int",
    //         },
    //         {
    //           name: "unsupported_calculation_meters",
    //           type: "int",
    //         },
    //         {
    //           name: "active_management_read",
    //           type: "int",
    //         },
    //         {
    //           name: "active_garbage_meter_read",
    //           type: "int",
    //         },
    //       ],
    //     },
    //   },
    //   snapshotManagement: {
    //     maxSnapshotAgeHours: 24,
    //     minSnapshotsToKeep: 3,
    //     status: "enabled",
    //   },
    //   storageClassConfiguration: {
    //     storageClass: "STANDARD",
    //   },
    //   tags: [
    //     {
    //       key: "Name",
    //       value: "MetersTable",
    //     },
    //   ],
    // });
    // metersTable.addDependency(namespace);

    // Lambda function to configure partitioning and sort order
    const configureTableFn = new lambda.Function(
      this,
      "ConfigureTableFunction",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "index.handler",
        timeout: cdk.Duration.minutes(5),
        role: configureTableFnRole,
        code: lambda.Code.fromInline(`
import json
import boto3
import time
import cfnresponse

# athena = boto3.client('athena')
s3_tables = boto3.client('s3tables')
glue_client = boto3.client('glue')

def handler(event, context):
    try:
        print(f"Event: {json.dumps(event)}")

        request_type = event['RequestType']
        if request_type == 'Delete':
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
            return

        # Get properties from event
        props = event['ResourceProperties']
        namespace = props['Namespace']
        table_name = props['TableName']
#        table_bucket_arn = props['TableBucketArn']
        table_bucket_name = props['TableBucketName']
#        output_location = props['OutputLocation']

        # Only configure on Create and Update
        if request_type in ['Create', 'Update']:
            query_execution_ids = []

            # Create table in S3 Tables catalog
#             query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
#   timestamp timestamp,
#   company_id int,
#   parent_id int,
#   building_id int,
#   building_name string,
#   total int,
#   actively_remote_read int,
#   active_manual_read int,
#   active_calculation_meters int,
#   inactive_remotely_read int,
#   inactive_manually_read int,
#   inactive_calculation_meters int,
#   unsupported_remotely_read int,
#   unsupported_manually_read int,
#   unsupported_calculation_meters int,
#   active_management_read int,
#   active_waste_remote int, 
#   active_waste_nonremote int, 
#   active_waste_calc int, 
#   active_waste_total int, 
#   inactive_waste_remote int, 
#   inactive_waste_nonremote int, 
#   inactive_waste_calc int, 
#   inactive_waste_total int, 
#   unsupported_waste_remote int, 
#   unsupported_waste_nonremote int, 
#   unsupported_waste_calc int, 
#   unsupported_waste_total int Y)
# PARTITIONED BY (month(timestamp), bucket(4, company_id), bucket(4, parent_id), bucket(4, building_id))
# TBLPROPERTIES (
#   'table_type'='iceberg',
# )"""
            table_input = {
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": [
                        {"Name": "timestamp", "Type": "timestamp"},
                        {"Name": "company_id", "Type": "int"},
                        {"Name": "parent_id", "Type": "int"},
                        {"Name": "building_id", "Type": "int"},
                        {"Name": "building_name", "Type": "string"},
                        {"Name": "total", "Type": "int"},
                        {"Name": "actively_remote_read", "Type": "int"},
                        {"Name": "active_manual_read", "Type": "int"},
                        {"Name": "active_calculation_meters", "Type": "int"},
                        {"Name": "inactive_remotely_read", "Type": "int"},
                        {"Name": "inactive_manually_read", "Type": "int"},
                        {"Name": "inactive_calculation_meters", "Type": "int"},
                        {"Name": "unsupported_remotely_read", "Type": "int"},
                        {"Name": "unsupported_manually_read", "Type": "int"},
                        {"Name": "unsupported_calculation_meters", "Type": "int"},
                        {"Name": "active_management_read", "Type": "int"},
                        {"Name": "active_waste_remote", "Type": "int"},
                        {"Name": "active_waste_nonremote", "Type": "int"},
                        {"Name": "active_waste_calc", "Type": "int"},
                        {"Name": "active_waste_total", "Type": "int"},
                        {"Name": "inactive_waste_remote", "Type": "int"},
                        {"Name": "inactive_waste_nonremote", "Type": "int"},
                        {"Name": "inactive_waste_calc", "Type": "int"},
                        {"Name": "inactive_waste_total", "Type": "int"},
                        {"Name": "unsupported_waste_remote", "Type": "int"},
                        {"Name": "unsupported_waste_nonremote", "Type": "int"},
                        {"Name": "unsupported_waste_calc", "Type": "int"},
                        {"Name": "unsupported_waste_total", "Type": "int"}
                    ]
                },
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {
                    "table_type": "ICEBERG",
                    "format": "ICEBERG"
                },
                "PartitionKeys": [
                    {
                        "Name": "timestamp",
                        "Type": "timestamp"
                    },
                    {
                        "Name": "company_id",
                        "Type": "int"
                    },
                    {
                        "Name": "parent_id",
                        "Type": "int"
                    },
                    {
                        "Name": "building_id",
                        "Type": "int"
                    },
                ]  
            }

            try:
                response = glue_client.create_table(
                    CatalogId=f's3tablescatalog/{table_bucket_name}',
                    DatabaseName=namespace,
                    TableInput=table_input
                )
                print("Table created successfully!")
                print(json.dumps(response, indent=2, default=str))
            except Exception as e:
                print(f"Error creating table: {str(e)}")
            # response = athena.start_query_execution(
            #     QueryString=query,
            #     QueryExecutionContext={
            #         'Catalog': f's3tablescatalog/{table_bucket_name}',
            #         'Database': namespace
            #     },
            #     WorkGroup='billing'
            # )
            # query_execution_ids.append(response['QueryExecutionId'])

            # # Wait for all queries to complete
            # for query_id in query_execution_ids:
            #     while True:
            #         response = athena.get_query_execution(QueryExecutionId=query_id)
            #         status = response['QueryExecution']['Status']['State']

            #         if status in ['SUCCEEDED']:
            #             print(f"Query {query_id} succeeded")
            #             break
            #         elif status in ['FAILED', 'CANCELLED']:
            #             reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            #             raise Exception(f"Query {query_id} failed: {reason}")

            #         time.sleep(2)

        cfnresponse.send(event, context, cfnresponse.SUCCESS, {
            'Message': 'Table configured successfully'
        })

    except Exception as e:
        print(f"Error: {str(e)}")
        cfnresponse.send(event, context, cfnresponse.FAILED, {
            'Error': str(e)
        })
`),
      },
    );

    // Create custom resource provider
    const provider = new cr.Provider(this, "ConfigureTableProvider", {
      onEventHandler: configureTableFn,
    });

    const tablename = "billing_meters2";
    // Create custom resource
    const configureTableResource = new cdk.CustomResource(
      this,
      "ConfigureTableResource",
      {
        serviceToken: provider.serviceToken,
        properties: {
          Namespace: namespace.namespace,
          TableName: tablename,
          // TableBucketArn: s3TableBucket.attrTableBucketArn,
          TableBucketName: tableBucket,
          // OutputLocation: `s3://${tableBucket}-athena-results/`,
          // Force update by changing this version when needed
          Version: "26",
        },
      },
    );

    configureTableResource.node.addDependency(namespace);
    configureTableResource.node.addDependency(athenaResultsBucket);
    configureTableResource.node.addDependency(configureTableFn);
    configureTableResource.node.addDependency(resourceLink);

    // Table bucket policy to control access
    new s3tables.CfnTableBucketPolicy(this, "BillingTableBucketPolicy", {
      resourcePolicy: {
        Version: "2012-10-17",
        Statement: [
          {
            Effect: "Allow",
            Principal: {
              AWS: `arn:aws:iam::${cdk.Aws.ACCOUNT_ID}:root`,
            },
            Action: [
              "s3tables:GetTable",
              "s3tables:GetTableMetadata",
              "s3tables:PutTableData",
              "s3tables:GetTableData",
              "s3tables:DeleteTableData",
              "s3tables:UpdateTableMetadata",
            ],
            Resource: `arn:aws:s3tables:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:bucket/${tableBucket}/*`,
          },
        ],
      },
      tableBucketArn: s3TableBucket.attrTableBucketArn,
    });

    const scriptBucket = s3.Bucket.fromBucketName(
      this,
      "GlueAssetsBucket",
      `aws-glue-assets-${this.account}-${this.region}`,
    );

    const script = `
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import gs_now
import gs_derived

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1768987646296 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="meter_stats", transformation_ctx="AWSGlueDataCatalog_node1768987646296")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1768987644464 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="billing", transformation_ctx="AWSGlueDataCatalog_node1768987644464")

# Script generated for node Join
AWSGlueDataCatalog_node1768987644464DF = AWSGlueDataCatalog_node1768987644464.toDF()
AWSGlueDataCatalog_node1768987646296DF = AWSGlueDataCatalog_node1768987646296.toDF()
Join_node1768989107485 = DynamicFrame.fromDF(AWSGlueDataCatalog_node1768987644464DF.join(AWSGlueDataCatalog_node1768987646296DF, (AWSGlueDataCatalog_node1768987644464DF['firmaid'] == AWSGlueDataCatalog_node1768987646296DF['company_id']) & (AWSGlueDataCatalog_node1768987644464DF['bygningid'] == AWSGlueDataCatalog_node1768987646296DF['building_id']), "right"), glueContext, "Join_node1768989107485")

# Script generated for node Add Current Timestamp
AddCurrentTimestamp_node1768988742764 = Join_node1768989107485.gs_now(colName="timestamp", dateFormat="%Y-%m-%d %H:%M:%S")

# Script generated for node Active Management
ActiveManagement_node1768989553161 = AddCurrentTimestamp_node1768988742764.gs_derived(colName="active_management_read", expr="temperatursum + kanalantal")

# Script generated for node Change Schema
ChangeSchema_node1769415812360 = ApplyMapping.apply(frame=ActiveManagement_node1768989553161, mappings=[("timestamp", "string", "timestamp", "timestamp"), ("company_id", "long", "company_id", "int"), ("parent_id", "long", "parent_id", "int"), ("building_id", "long", "building_id", "int"), ("building", "string", "building_name", "string"), ("total", "long", "total", "int"), ("active_manual_readings", "long", "actively_remote_read", "int"), ("active_manual_readings", "long", "active_manual_read", "int"), ("active_calculation_meters", "long", "active_calculation_meters", "int"), ("inactive_remotely_read", "long", "inactive_remotely_read", "int"), ("inactive_manual_readings", "long", "inactive_manually_read", "int"), ("inactive_calculation_meters", "long", "inactive_calculation_meters", "int"), ("unsupported_remotely_read", "long", "unsupported_remotely_read", "int"), ("unsupported_manual_readings", "long", "unsupported_manually_read", "int"), ("unsupported_calculation_meters", "long", "unsupported_calculation_meters", "int"), ("active_management_read", "long", "active_management_read", "int"), ("active_waste_remote", "long", "active_waste_remote", "int"), ("active_waste_nonremote", "long", "active_waste_nonremote", "int"), ("active_waste_calc", "long", "active_waste_calc", "int"), ("active_waste_total", "long", "active_waste_total", "int"), ("inactive_waste_remote", "long", "inactive_waste_remote", "int"), ("inactive_waste_nonremote", "long", "inactive_waste_nonremote", "int"), ("inactive_waste_calc", "long", "inactive_waste_calc", "int"), ("inactive_waste_total", "long", "inactive_waste_total", "int"), ("unsupported_waste_remote", "long", "unsupported_waste_remote", "int"), ("unsupported_waste_nonremote", "long", "unsupported_waste_nonremote", "int"), ("unsupported_waste_calc", "long", "unsupported_waste_calc", "int"), ("unsupported_waste_total", "long", "unsupported_waste_total", "int")], transformation_ctx="ChangeSchema_node1769415812360")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1768987880200_df = ChangeSchema_node1769415812360.toDF()
AWSGlueDataCatalog_node1768987880200 = glueContext.write_data_frame.from_catalog(frame=AWSGlueDataCatalog_node1768987880200_df, database="${namespace_name}_link", table_name="${tablename}", additional_options={})

job.commit()`;

    const deployment = new s3Deployment.BucketDeployment(this, "DeployScript", {
      sources: [s3Deployment.Source.data("glue_billing_job.py", script)],
      destinationBucket: scriptBucket,
      destinationKeyPrefix: "glue-scripts/",
      prune: false,
    });

    // Reference the uploaded script
    const scriptLocation = `s3://${scriptBucket.bucketName}/glue-scripts/glue_billing_job.py`;

    // Create IAM role for Glue job
    const glueJobRole = new iam.Role(this, "GlueJobRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole",
        ),
      ],
    });

    // Add S3 permissions for specific paths
    glueJobRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["s3:GetObject", "s3:PutObject"],
        resources: [
          `arn:aws:s3:::wannafind-${cdk.Aws.ACCOUNT_ID}-${cdk.Aws.REGION}/laoj/*`,
          `arn:aws:s3:::billing-${cdk.Aws.ACCOUNT_ID}/meter-stats/*`,
        ],
        conditions: {
          StringEquals: {
            "aws:ResourceAccount": cdk.Aws.ACCOUNT_ID,
          },
        },
      }),
    );

    // Add Lake Formation permissions
    glueJobRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["lakeformation:GetDataAccess"],
        resources: ["*"],
      }),
    );

    // Add Glue catalog permissions
    glueJobRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
        ],
        resources: [
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:catalog`,
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:database/${namespace_name}_link`,
          `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:table/${namespace_name}_link/*`,
        ],
      }),
    );

    // Add S3 Tables permissions
    glueJobRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "s3tables:GetTable",
          "s3tables:GetTableMetadataLocation",
          "s3tables:GetTableBucket",
          "s3tables:PutTableData",
          "s3tables:GetTableData",
        ],
        resources: ["*"],
      }),
    );

    // Add general S3 permissions
    glueJobRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ],
        resources: ["*"],
      }),
    );

    // Grant Lake Formation permissions on resource link database
    const glueResourceLinkDbPermissions = new lakeformation.CfnPermissions(
      this,
      "GlueResourceLinkDbPermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: glueJobRole.roleArn,
        },
        resource: {
          databaseResource: {
            name: `${namespace_name}_link`,
            catalogId: cdk.Aws.ACCOUNT_ID,
          },
        },
        permissions: ["ALL"],
      },
    );
    glueResourceLinkDbPermissions.node.addDependency(resourceLink);
    glueResourceLinkDbPermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on resource link tables
    const glueResourceLinkTablePermissions = new lakeformation.CfnPermissions(
      this,
      "GlueResourceLinkTablePermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: glueJobRole.roleArn,
        },
        resource: {
          tableResource: {
            databaseName: `${namespace_name}_link`,
            catalogId: cdk.Aws.ACCOUNT_ID,
            tableWildcard: {},
          },
        },
        permissions: ["SELECT", "INSERT", "DELETE", "DESCRIBE", "ALTER"],
      },
    );
    glueResourceLinkTablePermissions.node.addDependency(resourceLink);
    glueResourceLinkTablePermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on actual S3 Tables database
    const glueS3TablesDbPermissions = new lakeformation.CfnPermissions(
      this,
      "GlueS3TablesDbPermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: glueJobRole.roleArn,
        },
        resource: {
          databaseResource: {
            name: namespace_name,
            catalogId: `${cdk.Aws.ACCOUNT_ID}:s3tablescatalog/${tableBucket}`,
          },
        },
        permissions: ["ALL"],
      },
    );
    glueS3TablesDbPermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on actual S3 Tables
    const glueS3TablesTablePermissions = new lakeformation.CfnPermissions(
      this,
      "GlueS3TablesTablePermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: glueJobRole.roleArn,
        },
        resource: {
          tableResource: {
            databaseName: namespace_name,
            catalogId: `${cdk.Aws.ACCOUNT_ID}:s3tablescatalog/${tableBucket}`,
            tableWildcard: {},
          },
        },
        permissions: ["SELECT", "INSERT", "DELETE", "DESCRIBE", "ALTER"],
      },
    );
    glueS3TablesTablePermissions.node.addDependency(namespace);

    // Create the Glue Job
    const billingJob = new glue.CfnJob(this, "DaqBillingJob", {
      name: "billing-job",
      role: glueJobRole.roleArn,
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: scriptLocation,
      },
      glueVersion: "5.1",
      workerType: "G.1X",
      numberOfWorkers: 2,
      executionProperty: {
        maxConcurrentRuns: 3, // Allow multiple concurrent runs for billing job reliability
      },
      defaultArguments: {
        "--enable-metrics": "true",
        "--enable-spark-ui": "true",
        "--enable-job-insights": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-glue-datacatalog": "true",
        "--job-language": "python",
        "--enable-auto-scaling": "false",
        "--TempDir": `s3://aws-glue-assets-${this.env?.account}-${this.env?.region}/temporary/`,
        "--spark-event-logs-path": `s3://aws-glue-assets-${this.env?.account}-${this.env?.region}/sparkHistoryLogs/`,
        "--datalake-formats": "iceberg",
        "--conf": [
          "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
          "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog",
          `spark.sql.catalog.glue_catalog.warehouse=s3://billing-${this.env?.account}-${this.env?.region}/warehouse/`,
          "spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog",
          "spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        ].join(" --conf "),
        "--extra-py-files":
          "s3://aws-glue-studio-transforms-560373232017-prod-eu-central-1/gs_common.py,s3://aws-glue-studio-transforms-560373232017-prod-eu-central-1/gs_now.py,s3://aws-glue-studio-transforms-560373232017-prod-eu-central-1/gs_derived.py",
      },
      executionClass: "FLEX",
      jobRunQueuingEnabled: true,
    });
    billingJob.node.addDependency(deployment);

    const jobName = billingJob.name || billingJob.node.id;

    new cloudwatch.Alarm(this, "GlueJobFailureAlarm", {
      alarmName: `glue-job-failure-${jobName}`,
      metric: new cloudwatch.Metric({
        namespace: "AWS/Glue",
        metricName: "glue.driver.aggregate.numFailedTasks",
        dimensionsMap: {
          JobName: jobName,
        },
        statistic: "Sum",
        period: cdk.Duration.minutes(1),
      }),
      threshold: 0,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      evaluationPeriods: 1,
      alarmDescription: `Alarm triggered when Glue job ${jobName} has failed tasks`,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });
  }
}
