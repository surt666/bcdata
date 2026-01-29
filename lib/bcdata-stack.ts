import * as cdk from "aws-cdk-lib/core";
import { Construct } from "constructs";
import { aws_s3tables as s3tables } from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cr from "aws-cdk-lib/custom-resources";
import * as lakeformation from "aws-cdk-lib/aws-lakeformation";
import * as glue from "aws-cdk-lib/aws-glue";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3Deployment from "aws-cdk-lib/aws-s3-deployment";
import * as cloudwatch from "aws-cdk-lib/aws-cloudwatch";
import * as ssm from "aws-cdk-lib/aws-ssm";
import * as athena from "aws-cdk-lib/aws-athena";
import * as crypto from "crypto";

export class BcdataStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const tableBucket = "billing12";
    const namespace_name = "billingdata8";
    const table_name = "meters";

    // Change this version to force re-running the billing-table-job
    const tableJobVersion = "2";

    // S3 bucket for Athena query results
    const athenaResultsBucket = new s3.Bucket(this, "AthenaResultsBucket", {
      bucketName: `${tableBucket}-athena-billing-results`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const daq_workgroup = new athena.CfnWorkGroup(this, "AthenaWorkgroup", {
      name: "billing-wg",
      workGroupConfiguration: {
        bytesScannedCutoffPerQuery: undefined, // or null, if TypeScript complains about undefined
        enforceWorkGroupConfiguration: false,
        publishCloudWatchMetricsEnabled: false,
        requesterPaysEnabled: false,
        resultConfiguration: {
          encryptionConfiguration: {
            encryptionOption: "SSE_S3",
          },
          outputLocation: `s3://${athenaResultsBucket.bucketName}/query`,
        },
      },
    });

    // S3 bucket for meter stats CSV files from ingestion
    const meterStatsIngestionBucket = new s3.Bucket(
      this,
      "MeterStatsIngestionBucket",
      {
        bucketName: `${namespace_name}-${cdk.Aws.ACCOUNT_ID}-${cdk.Aws.REGION}`,
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

    const configureTableFnRole = new iam.Role(this, "BillingGlueRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole",
        ),
      ],
    });
    // Grant permissions
    configureTableFnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["glue:GetDataCatalog", "glue:CreateTable"],
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
    const glueResourceLinkDbPermissions = new lakeformation.CfnPermissions(
      this,
      "glueResourceLinkDbPermissions",
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
    glueResourceLinkDbPermissions.node.addDependency(resourceLink);
    glueResourceLinkDbPermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on resource link tables
    const glueResourceLinkTablePermissions = new lakeformation.CfnPermissions(
      this,
      "glueResourceLinkTablePermissions",
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
    glueResourceLinkTablePermissions.node.addDependency(resourceLink);
    glueResourceLinkTablePermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on actual S3 Tables database
    const glueS3TablesDbPermissions = new lakeformation.CfnPermissions(
      this,
      "glueS3TablesDbPermissions",
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
    glueS3TablesDbPermissions.node.addDependency(namespace);

    // Grant Lake Formation permissions on actual S3 Tables
    const glueS3TablesTablePermissions = new lakeformation.CfnPermissions(
      this,
      "glueS3TablesTablePermissions",
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
    glueS3TablesTablePermissions.node.addDependency(namespace);

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

    const billing_table_script = `
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import year, month, dayofmonth, from_unixtime, col

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init("IcebergGlueLoaderCreateAndWrite", {})

# S3Tables Iceberg configuration

spark.conf.set("spark.sql.defaultCatalog","s3tables") 
spark.conf.set("spark.sql.catalog.s3tables", "org.apache.iceberg.spark.SparkCatalog") 
spark.conf.set("spark.sql.catalog.s3tables.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") 
spark.conf.set("spark.sql.catalog.s3tables.glue.id", "891377204778:s3tablescatalog/${s3TableBucket.tableBucketName}") 
spark.conf.set("spark.sql.catalog.s3tables.warehouse", "s3://${s3TableBucket.tableBucketName}/warehouse/") 

#variables
catalog_name = "s3tables"
name_space = "${namespace_name}"
table_name = "${table_name}"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {name_space}.{table_name} (
        timestamp TIMESTAMP,
        company_id INT,
        parent_id INT,
        building_id INT,
        building_name STRING,
        total INT,
        actively_remote_read INT,
        active_manual_read INT,
        active_calculation_meters INT,
        inactive_remotely_read INT,
        inactive_manually_read INT,
        inactive_calculation_meters INT,
        unsupported_remotely_read INT,
        unsupported_manually_read INT,
        unsupported_calculation_meters INT,
        active_management_read INT,
        active_waste_remote INT,
        active_waste_nonremote INT,
        active_waste_calc INT,
        active_waste_total INT,
        inactive_waste_remote INT,
        inactive_waste_nonremote INT,
        inactive_waste_calc INT,
        inactive_waste_total INT,
        unsupported_waste_remote INT,
        unsupported_waste_nonremote INT,
        unsupported_waste_calc INT,
        unsupported_waste_total INT 
    )
    USING iceberg
    PARTITIONED BY (months(timestamp), bucket(4, company_id), bucket(4, parent_id), bucket(4, building_id) )
    TBLPROPERTIES (
        'write.metadata.delete-after-commit.enabled'='true',
        'write.metadata.previous-versions-max'='10',
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'zstd',
        'write.order-by' = 'timestamp, company_id, parent_id, building_id',
        'format-version' = '2'
    );
""")`;

    const scriptBucket = s3.Bucket.fromBucketName(
      this,
      "GlueAssetsBucket",
      `aws-glue-assets-${this.account}-${this.region}`,
    );

    // Generate hash of script content to force updates when script changes
    const tableScriptHash = crypto
      .createHash("md5")
      .update(billing_table_script)
      .digest("hex")
      .substring(0, 8);

    const bucketDeployment = new s3Deployment.BucketDeployment(
      this,
      `DeployTableScript${tableScriptHash}`,
      {
        sources: [
          s3Deployment.Source.data(
            `billing_table_job-${tableScriptHash}.py`,
            billing_table_script,
          ),
        ],
        destinationBucket: scriptBucket,
        destinationKeyPrefix: "glue-scripts/",
        prune: false,
      },
    );
    bucketDeployment.node.addDependency(scriptBucket);

    const tableScriptLocation = `s3://${scriptBucket.bucketName}/glue-scripts/billing_table_job-${tableScriptHash}.py`;

    const tableJob = new glue.CfnJob(this, "BillingTableJob", {
      name: "billing-table-job",
      role: configureTableFnRole.roleArn,
      command: {
        name: "glueetl",
        pythonVersion: "3",
        scriptLocation: tableScriptLocation,
      },
      glueVersion: "5.0",
      workerType: "G.1X",
      numberOfWorkers: 2,
      executionProperty: {
        maxConcurrentRuns: 1,
      },
      defaultArguments: {
        "--enable-metrics": "true",
        "--enable-spark-ui": "false",
        "--enable-job-insights": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-glue-datacatalog": "true",
        "--job-language": "python",
        "--enable-auto-scaling": "false",
        "--TempDir": `s3://aws-glue-assets-${this.account}-${this.region}/temporary/`,
        "--spark-event-logs-path": `s3://aws-glue-assets-${this.account}-${this.region}/sparkHistoryLogs/`,
        "--conf": `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`,
      },
      executionClass: "STANDARD",
      jobRunQueuingEnabled: true,
    });
    tableJob.node.addDependency(s3TableBucket);

    const tableJobArn = this.formatArn({
      service: "glue",
      resource: "job",
      resourceName: tableJob.name,
    });

    const startJob = new cr.AwsCustomResource(this, "StartTableJob", {
      onCreate: {
        service: "Glue",
        action: "startJobRun",
        parameters: {
          JobName: tableJob.name,
        },
        physicalResourceId: cr.PhysicalResourceId.of(
          `TableJobStarter-${tableJobVersion}`,
        ),
      },
      onUpdate: {
        service: "Glue",
        action: "startJobRun",
        parameters: {
          JobName: tableJob.name,
        },
        physicalResourceId: cr.PhysicalResourceId.of(
          `TableJobStarter-${tableJobVersion}`,
        ),
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({
        resources: [tableJobArn],
      }),
    });
    startJob.node.addDependency(tableJob);
    startJob.node.addDependency(bucketDeployment);

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

    const billing_script = `
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
AWSGlueDataCatalog_node1768987880200 = glueContext.write_data_frame.from_catalog(frame=AWSGlueDataCatalog_node1768987880200_df, database="${namespace_name}_link", table_name="${table_name}", additional_options={})

job.commit()`;

    // Generate hash of billing script content to force updates when script changes
    const billingScriptHash = crypto
      .createHash("md5")
      .update(billing_script)
      .digest("hex")
      .substring(0, 8);

    const deployment = new s3Deployment.BucketDeployment(
      this,
      `DeployBillingScript${billingScriptHash}`,
      {
        sources: [
          s3Deployment.Source.data(
            `glue_billing_job-${billingScriptHash}.py`,
            billing_script,
          ),
        ],
        destinationBucket: scriptBucket,
        destinationKeyPrefix: "glue-scripts/",
        prune: false,
      },
    );

    // Reference the uploaded script
    const scriptLocation = `s3://${scriptBucket.bucketName}/glue-scripts/glue_billing_job-${billingScriptHash}.py`;

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

    // Create Glue trigger to run job daily at 01:00 Danish time (UTC+1/+2)
    // Danish time is UTC+1 in winter (standard time) and UTC+2 in summer (DST)
    // 01:00 Copenhagen = 00:00 UTC (winter) or 23:00 UTC (summer)
    // Using 00:00 UTC as approximation (will be 01:00 CET or 02:00 CEST)
    new glue.CfnTrigger(this, "BillingJobTrigger", {
      name: "billing-job-daily-trigger",
      type: "SCHEDULED",
      schedule: "cron(0 0 * * ? *)", // Run at 00:00 UTC daily (01:00 CET / 02:00 CEST)
      startOnCreation: true,
      actions: [
        {
          jobName: billingJob.name,
        },
      ],
    });

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
