import * as cdk from "aws-cdk-lib/core";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lakeformation from "aws-cdk-lib/aws-lakeformation";

export interface MeterStatsLambdaStackProps extends cdk.StackProps {
  tableBucketName: string;
  namespace: string;
  tableName: string;
}

export class MeterStatsLambdaStack extends cdk.Stack {
  public readonly meterStatsQueryFn: lambda.Function;
  public readonly functionUrl: lambda.FunctionUrl;

  constructor(scope: Construct, id: string, props: MeterStatsLambdaStackProps) {
    super(scope, id, props);

    const { tableBucketName, namespace, tableName } = props;

    // Lambda function to query meter statistics
    this.meterStatsQueryFn = new lambda.Function(
      this,
      "MeterStatsQueryFunction",
      {
        runtime: lambda.Runtime.PYTHON_3_12,
        handler: "index.handler",
        timeout: cdk.Duration.minutes(5),
        environment: {
          TABLE_BUCKET_NAME: tableBucketName,
          NAMESPACE: namespace,
          TABLE_NAME: tableName,
          OUTPUT_LOCATION: `s3://${tableBucketName}-athena-results/`,
        },
        code: lambda.Code.fromInline(`
import json
import boto3
import time
import os

athena = boto3.client('athena')

def handler(event, context):
    try:
        print(f"Event: {json.dumps(event)}")

        # Parse input parameters - handle both direct invocation and function URL
        if 'requestContext' in event and 'http' in event['requestContext']:
            # Function URL invocation
            if event.get('body'):
                body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
            else:
                # GET request - check query string parameters
                query_params = event.get('queryStringParameters', {}) or {}
                body = query_params
        else:
            # Direct Lambda invocation
            body = event

        # Extract parameters
        month = int(body.get('month', 1))
        id_string = body.get('id', '')

        if not id_string:
            raise ValueError("Missing required parameter: id")

        # Parse ID string format: C#<nr> or C#<nr>#P#<nr> or C#<nr>#P#<nr>#B#<nr>
        # Examples: C#3, C#3#P#1, C#3#P#1#B#5
        parts = id_string.split('#')

        company_id = None
        property_id = None
        building_id = None

        for i in range(len(parts) - 1):
            if parts[i] == 'C':
                company_id = int(parts[i + 1])
            elif parts[i] == 'P':
                property_id = int(parts[i + 1])
            elif parts[i] == 'B':
                building_id = int(parts[i + 1])

        if company_id is None:
            raise ValueError(f"Invalid id format. Must contain at least C#<nr>, got: {id_string}")

        # Default year to current year
        year = 2026

        # Get environment variables
        table_bucket_name = os.environ['TABLE_BUCKET_NAME']
        namespace = os.environ['NAMESPACE']
        table_name = os.environ['TABLE_NAME']
        output_location = os.environ['OUTPUT_LOCATION']

        # Build the ID string for the result
        result_id = f'C#{company_id}'
        if property_id is not None:
            result_id += f'#P#{property_id}'
        if building_id is not None:
            result_id += f'#B#{building_id}'

        # Build WHERE clause dynamically based on provided IDs
        where_clauses = [
            f"month(timestamp) = {month}",
            f"year(timestamp) = {year}",
            f"company_id = {company_id}"
        ]

        if property_id is not None:
            where_clauses.append(f"property_id = {property_id}")

        if building_id is not None:
            where_clauses.append(f"building_id = {building_id}")

        where_clause = " AND ".join(where_clauses)

        # Build the Athena query
        # Note: We don't prefix the table name with the database since it's set in QueryExecutionContext
        query = f"""
SELECT '{result_id}' as id,
       sum(total) as total,
       sum(actively_remote_read) as actively_remote_read,
       sum(active_manual_read) as active_manual_read,
       sum(active_calculation_meters) as active_calculation_meter,
       sum(inactive_remotely_read) as inactive_remotely_read,
       sum(inactive_manually_read) as inactive_manually_read,
       sum(inactive_calculation_meters) as inactive_calculation_meters,
       sum(unsupported_remotely_read) as unsupported_remotely_read,
       sum(unsupported_manually_read) as unsupported_manually_read,
       sum(unsupported_calculation_meters) as unsupported_calculation_meters,
       sum(active_management_read) as active_management_read,
       sum(active_garbage_meter_read) as active_garbage_meter_read
FROM {table_name}
WHERE {where_clause}
"""

        print(f"Executing query: {query}")

        # Execute the query using the Glue resource link database
        # (Athena SDK can't directly access S3 Tables catalog)
        response = athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': output_location},
            QueryExecutionContext={
                'Database': f'{namespace}_link'
            },
            WorkGroup='primary'
        )

        query_execution_id = response['QueryExecutionId']
        print(f"Query execution ID: {query_execution_id}")

        # Wait for query to complete
        max_attempts = 60
        attempt = 0
        while attempt < max_attempts:
            response = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                print("Query succeeded")
                break
            elif status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                raise Exception(f"Query failed: {reason}")

            time.sleep(2)
            attempt += 1

        if attempt >= max_attempts:
            raise Exception("Query timeout")

        # Get query results
        result = athena.get_query_results(QueryExecutionId=query_execution_id)

        # Parse results
        rows = result['ResultSet']['Rows']
        if len(rows) < 2:
            return {
                'statusCode': 200,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({
                    'message': 'No data found',
                    'data': None
                })
            }

        # Extract column names from first row
        columns = [col['VarCharValue'] for col in rows[0]['Data']]

        # Extract data from second row
        values = []
        for col in rows[1]['Data']:
            val = col.get('VarCharValue', '0')
            # Try to convert to int, fallback to string
            try:
                values.append(int(val))
            except ValueError:
                values.append(val)

        # Build result object
        result_data = dict(zip(columns, values))

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(result_data)
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': str(e)
            })
        }
`),
      },
    );

    // Grant permissions to meter stats query Lambda
    this.meterStatsQueryFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
        ],
        resources: ["*"],
      }),
    );

    this.meterStatsQueryFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["s3tables:GetTable", "s3tables:GetTableMetadata"],
        resources: [
          `arn:aws:s3tables:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:bucket/${tableBucketName}/*`,
        ],
      }),
    );

    this.meterStatsQueryFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ],
        resources: [
          `arn:aws:s3:::${tableBucketName}-athena-results`,
          `arn:aws:s3:::${tableBucketName}-athena-results/*`,
        ],
      }),
    );

    this.meterStatsQueryFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["glue:GetDatabase", "glue:GetTable"],
        resources: ["*"],
      }),
    );

    this.meterStatsQueryFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["lakeformation:GetDataAccess"],
        resources: ["*"],
      }),
    );

    // Grant Lake Formation permissions on resource link database
    const resourceLinkDbPermissions = new lakeformation.CfnPermissions(
      this,
      "ResourceLinkDbPermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: this.meterStatsQueryFn.role!.roleArn,
        },
        resource: {
          databaseResource: {
            name: `${namespace}_link`,
            catalogId: cdk.Aws.ACCOUNT_ID,
          },
        },
        permissions: ["DESCRIBE"],
      },
    );

    // Grant Lake Formation permissions on resource link tables
    const resourceLinkTablePermissions = new lakeformation.CfnPermissions(
      this,
      "ResourceLinkTablePermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: this.meterStatsQueryFn.role!.roleArn,
        },
        resource: {
          tableResource: {
            databaseName: `${namespace}_link`,
            catalogId: cdk.Aws.ACCOUNT_ID,
            tableWildcard: {},
          },
        },
        permissions: ["SELECT", "DESCRIBE"],
      },
    );

    // Grant Lake Formation permissions on the S3 Tables database
    const s3TablesDbPermissions = new lakeformation.CfnPermissions(
      this,
      "S3TablesDbPermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: this.meterStatsQueryFn.role!.roleArn,
        },
        resource: {
          databaseResource: {
            name: namespace,
            catalogId: `${cdk.Aws.ACCOUNT_ID}:s3tablescatalog/${tableBucketName}`,
          },
        },
        permissions: ["DESCRIBE"],
      },
    );

    // Grant Lake Formation permissions on S3 Tables
    const s3TablesTablePermissions = new lakeformation.CfnPermissions(
      this,
      "S3TablesTablePermissions",
      {
        dataLakePrincipal: {
          dataLakePrincipalIdentifier: this.meterStatsQueryFn.role!.roleArn,
        },
        resource: {
          tableResource: {
            databaseName: namespace,
            catalogId: `${cdk.Aws.ACCOUNT_ID}:s3tablescatalog/${tableBucketName}`,
            tableWildcard: {},
          },
        },
        permissions: ["SELECT", "DESCRIBE"],
      },
    );

    // Add public function URL
    this.functionUrl = this.meterStatsQueryFn.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.NONE,
      cors: {
        allowedOrigins: ["*"],
        allowedMethods: [lambda.HttpMethod.GET, lambda.HttpMethod.POST],
        allowedHeaders: ["*"],
      },
    });

    // Output the Lambda function details
    new cdk.CfnOutput(this, "MeterStatsQueryFunctionArn", {
      value: this.meterStatsQueryFn.functionArn,
      description: "ARN of the meter statistics query Lambda function",
      exportName: `${this.stackName}-MeterStatsQueryFunctionArn`,
    });

    new cdk.CfnOutput(this, "MeterStatsQueryFunctionName", {
      value: this.meterStatsQueryFn.functionName,
      description: "Name of the meter statistics query Lambda function",
      exportName: `${this.stackName}-MeterStatsQueryFunctionName`,
    });

    new cdk.CfnOutput(this, "MeterStatsQueryFunctionUrl", {
      value: this.functionUrl.url,
      description: "Public URL for the meter statistics query Lambda function",
      exportName: `${this.stackName}-MeterStatsQueryFunctionUrl`,
    });
  }
}
