import * as cdk from "aws-cdk-lib/core";
import { Construct } from "constructs";
import { aws_s3tables as s3tables } from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cr from "aws-cdk-lib/custom-resources";
import * as lakeformation from "aws-cdk-lib/aws-lakeformation";
// import * as sqs from 'aws-cdk-lib/aws-sqs';

export class BcdataStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const tableBucket = "billing2";
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
      namespace: "billingdata",
      tableBucketArn: s3TableBucket.attrTableBucketArn,
    });
    namespace.addDependency(s3TableBucket);

    const metersTable = new s3tables.CfnTable(this, "meterstable", {
      namespace: namespace.namespace,
      openTableFormat: "ICEBERG",
      tableBucketArn: s3TableBucket.attrTableBucketArn,
      tableName: "meters",

      // the properties below are optional
      compaction: {
        status: "enabled",
        targetFileSizeMb: 256,
      },
      icebergMetadata: {
        icebergSchema: {
          schemaFieldList: [
            {
              name: "company_id",
              type: "int",
              required: true,
            },
            {
              name: "property_id",
              type: "int",
            },
            {
              name: "building_id",
              type: "int",
              required: true,
            },
            {
              name: "company_name",
              type: "string",
              required: true,
            },
            {
              name: "building_name",
              type: "string",
              required: true,
            },
            {
              name: "total",
              type: "int",
              required: true,
            },
            {
              name: "actively_remote_read",
              type: "int",
            },
            {
              name: "active_manual_readings",
              type: "int",
            },
            {
              name: "active_calculation_meters",
              type: "int",
            },
            {
              name: "inactive_remotely_read",
              type: "int",
            },
            {
              name: "inactive_manually_read",
              type: "int",
            },
            {
              name: "inactive_calculation_meters",
              type: "int",
            },
            {
              name: "unsupported_remotely_read",
              type: "int",
            },
            {
              name: "unsupported_manually_read",
              type: "int",
            },
            {
              name: "unsupported_calculation_meters",
              type: "int",
            },
          ],
        },
      },
      snapshotManagement: {
        maxSnapshotAgeHours: 24,
        minSnapshotsToKeep: 3,
        status: "enabled",
      },
      storageClassConfiguration: {
        storageClass: "STANDARD",
      },
      tags: [
        {
          key: "Name",
          value: "MetersTable",
        },
      ],
    });
    metersTable.addDependency(namespace);

    // Lambda function to configure partitioning and sort order
    const configureTableFn = new lambda.Function(this, "ConfigureTableFunction", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.handler",
      timeout: cdk.Duration.minutes(5),
      code: lambda.Code.fromInline(`
import json
import boto3
import time
import cfnresponse

athena = boto3.client('athena')
s3_tables = boto3.client('s3tables')

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
        table_bucket_arn = props['TableBucketArn']
        partition_fields = props.get('PartitionFields', [])
        sort_fields = props.get('SortFields', [])
        output_location = props['OutputLocation']

        # Only configure on Create and Update
        if request_type in ['Create', 'Update']:
            query_execution_ids = []

            # Add partition fields
            for field in partition_fields:
                query = f"ALTER TABLE \\"{namespace}\\".\\"{table_name}\\" ADD PARTITION FIELD {field}"
                print(f"Executing: {query}")

                response = athena.start_query_execution(
                    QueryString=query,
                    ResultConfiguration={'OutputLocation': output_location},
                    QueryExecutionContext={'Catalog': 's3tables'}
                )
                query_execution_ids.append(response['QueryExecutionId'])

            # Set sort order
            if sort_fields:
                sort_clause = ', '.join(sort_fields)
                query = f"ALTER TABLE \\"{namespace}\\".\\"{table_name}\\" WRITE ORDERED BY {sort_clause}"
                print(f"Executing: {query}")

                response = athena.start_query_execution(
                    QueryString=query,
                    ResultConfiguration={'OutputLocation': output_location},
                    QueryExecutionContext={'Catalog': 's3tables'}
                )
                query_execution_ids.append(response['QueryExecutionId'])

            # Wait for all queries to complete
            for query_id in query_execution_ids:
                while True:
                    response = athena.get_query_execution(QueryExecutionId=query_id)
                    status = response['QueryExecution']['Status']['State']

                    if status in ['SUCCEEDED']:
                        print(f"Query {query_id} succeeded")
                        break
                    elif status in ['FAILED', 'CANCELLED']:
                        reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                        raise Exception(f"Query {query_id} failed: {reason}")

                    time.sleep(2)

        cfnresponse.send(event, context, cfnresponse.SUCCESS, {
            'Message': 'Table configured successfully'
        })

    except Exception as e:
        print(f"Error: {str(e)}")
        cfnresponse.send(event, context, cfnresponse.FAILED, {
            'Error': str(e)
        })
`),
    });

    // Grant permissions to Lambda
    configureTableFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ],
        resources: ["*"],
      })
    );

    configureTableFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "s3tables:GetTable",
          "s3tables:UpdateTableMetadata",
        ],
        resources: [s3TableBucket.attrTableBucketArn + "/*"],
      })
    );

    configureTableFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
        ],
        resources: [
          `arn:aws:s3:::${tableBucket}-athena-results`,
          `arn:aws:s3:::${tableBucket}-athena-results/*`,
        ],
      })
    );

    configureTableFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["glue:GetDatabase", "glue:GetTable"],
        resources: ["*"],
      })
    );

    configureTableFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "lakeformation:GetDataAccess",
          "lakeformation:GrantPermissions",
          "lakeformation:RevokePermissions",
        ],
        resources: ["*"],
      })
    );

    // Grant Lake Formation permissions on the database
    new lakeformation.CfnPrincipalPermissions(
      this,
      "LambdaDatabasePermissions",
      {
        permissions: ["DESCRIBE"],
        permissionsWithGrantOption: [],
        principal: {
          dataLakePrincipalIdentifier: configureTableFn.role!.roleArn,
        },
        resource: {
          database: {
            catalogId: "s3tablescatalog",
            name: namespace.namespace,
          },
        },
      }
    );

    // Grant Lake Formation permissions on the table
    new lakeformation.CfnPrincipalPermissions(
      this,
      "LambdaTablePermissions",
      {
        permissions: ["SELECT", "DESCRIBE", "ALTER"],
        permissionsWithGrantOption: [],
        principal: {
          dataLakePrincipalIdentifier: configureTableFn.role!.roleArn,
        },
        resource: {
          table: {
            catalogId: "s3tablescatalog",
            databaseName: namespace.namespace,
            name: metersTable.tableName,
          },
        },
      }
    );

    // Create custom resource provider
    const provider = new cr.Provider(this, "ConfigureTableProvider", {
      onEventHandler: configureTableFn,
    });

    // Create custom resource
    const configureTableResource = new cdk.CustomResource(
      this,
      "ConfigureTableResource",
      {
        serviceToken: provider.serviceToken,
        properties: {
          Namespace: namespace.namespace,
          TableName: metersTable.tableName,
          TableBucketArn: s3TableBucket.attrTableBucketArn,
          PartitionFields: ["company_id"],
          SortFields: ["company_id", "building_id"],
          OutputLocation: `s3://${tableBucket}-athena-results/`,
        },
      }
    );

    configureTableResource.node.addDependency(metersTable);

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
  }
}
