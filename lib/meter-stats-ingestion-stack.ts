import * as cdk from "aws-cdk-lib/core";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as ec2 from "aws-cdk-lib/aws-ec2";

export interface MeterStatsIngestionStackProps extends cdk.StackProps {
  s3BucketName?: string;
}

export class MeterStatsIngestionStack extends cdk.Stack {
  public readonly ingestionFunction: lambda.Function;

  constructor(
    scope: Construct,
    id: string,
    props?: MeterStatsIngestionStackProps,
  ) {
    super(scope, id, props);

    // Get environment from context parameter (e.g., -c ENVIRONMENT=staging)
    const environment = this.node.tryGetContext("ENVIRONMENT") || "production";

    // Construct endpoint environment (e.g., ed-production, ed-staging)
    const endpointEnvironment = `ed-${environment}`;

    // Get VPC configuration from context
    const vpcId = this.node.tryGetContext("VPC_ID");
    const subnetIds = this.node.tryGetContext("SUBNET_IDS");

    // Get the account ID where BcdataStack is deployed (for cross-account SSM access)
    const bcdataAccountId =
      this.node.tryGetContext("BCDATA_ACCOUNT_ID") || cdk.Aws.ACCOUNT_ID;

    // Look up VPC and subnets if provided
    let vpc: ec2.IVpc | undefined;
    let subnets: ec2.ISubnet[] | undefined;

    if (vpcId && subnetIds) {
      // Look up the VPC
      vpc = ec2.Vpc.fromLookup(this, "Vpc", {
        vpcId: vpcId,
      });

      // Parse comma-separated subnet IDs and look them up
      const subnetIdList = subnetIds.split(",").map((id: string) => id.trim());
      subnets = subnetIdList.map((subnetId: string, index: number) =>
        ec2.Subnet.fromSubnetId(this, `Subnet${index}`, subnetId),
      );
    }

    // Lambda function to fetch meter stats and upload to S3
    this.ingestionFunction = new lambda.Function(this, "IngestionFunction", {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: "index.lambda_handler",
      timeout: cdk.Duration.minutes(5),
      environment: {
        ENDPOINT_ENVIRONMENT: endpointEnvironment,
        BCDATA_ACCOUNT_ID: bcdataAccountId,
        SSM_PARAMETER_NAME: "/bcdata/meter-stats-ingestion-bucket",
      },
      vpc: vpc,
      vpcSubnets: subnets
        ? {
            subnets: subnets,
          }
        : undefined,
      code: lambda.Code.fromInline(`
import json
import urllib.request
import csv
import boto3
from io import StringIO
from datetime import datetime
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """Call internal meter billing stats endpoint, transform to CSV, and upload to S3."""

    endpoint_env = os.environ.get('ENDPOINT_ENVIRONMENT', 'ed-production')
    bcdata_account_id = os.environ.get('BCDATA_ACCOUNT_ID')
    ssm_parameter_name = os.environ.get('SSM_PARAMETER_NAME')
    aws_region = os.environ.get('AWS_REGION')

    # Assume role in BcdataStack account to read SSM parameter
    sts = boto3.client('sts')
    role_arn = f'arn:aws:iam::{bcdata_account_id}:role/BcdataSSMReadRole'

    try:
        # Assume role in the other account
        assumed_role = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName='meter-stats-ingestion-session'
        )

        # Create SSM client with assumed credentials
        ssm = boto3.client(
            'ssm',
            region_name=aws_region,
            aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role['Credentials']['SessionToken']
        )

        # Get parameter from BcdataStack account
        response = ssm.get_parameter(Name=ssm_parameter_name)
        s3_bucket = response['Parameter']['Value']
        print(f"Retrieved bucket name from SSM: {s3_bucket}")

    except Exception as e:
        print(f"Error reading SSM parameter: {e}")
        # Fallback to constructed bucket name if SSM fails
        s3_bucket = f'billing-{bcdata_account_id}-{aws_region}'
        print(f"Using fallback bucket name: {s3_bucket}")

    # Energy group IDs to include in the CSV
    ENERGY_GROUP_IDS = [13]

    payload = {
        "contextFilter": {
            "id": 1,
            "contextType": "administrator"
        }
    }

    endpoint_url = f'http://meter.{endpoint_env}/meters/stats/billing'
    print(f"Calling endpoint: {endpoint_url}")

    req = urllib.request.Request(
        endpoint_url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
        method='POST'
    )

    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode('utf-8'))

    # Determine which energy groups are present in the data
    energy_groups_present = set()
    for item in data:
        meter_groups = item.get('meters', {}).get('count', {}).get('meterGroups', [])
        for group in meter_groups:
            if group['energyMainGroupId'] in ENERGY_GROUP_IDS:
                energy_groups_present.add(group['energyMainGroupKey'].lower())

    # Sort for consistent column ordering
    energy_groups_present = sorted(energy_groups_present)

    # Transform to CSV
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)

    # Build header dynamically
    base_header = [
        'company_id',
        'parent_id',
        'building',
        'building_id',
        'total',
        'active_remotely_read',
        'active_manual_readings',
        'active_calculation_meters',
        'inactive_remotely_read',
        'inactive_manual_readings',
        'inactive_calculation_meters',
        'unSupported_remotely_read',
        'unSupported_manual_readings',
        'unSupported_calculation_meters'
    ]

    # Add energy group columns
    energy_group_columns = []
    for energy_type in energy_groups_present:
        for status in ['active', 'inactive', 'unSupported']:
            for metric in ['remote', 'nonRemote', 'calc', 'total']:
                energy_group_columns.append(f'{status}_{energy_type}_{metric}')

    csv_writer.writerow(base_header + energy_group_columns)

    # Write data rows
    for item in data:
        building = item['building']
        meters = item['meters']['count']

        # Base row data
        base_row = [
            building['company'],
            building.get('parentId'),
            building['name'],
            building['id'],
            meters['active']['total'] + meters['inactive']['total'] + meters['unSupported']['total'],
            meters['active']['remote'],
            meters['active']['nonRemote'],
            meters['active']['calc'],
            meters['inactive']['remote'],
            meters['inactive']['nonRemote'],
            meters['inactive']['calc'],
            meters['unSupported']['remote'],
            meters['unSupported']['nonRemote'],
            meters['unSupported']['calc']
        ]

        # Build energy group data
        meter_groups = meters.get('meterGroups', [])
        energy_group_data = {}
        for group in meter_groups:
            if group['energyMainGroupId'] in ENERGY_GROUP_IDS:
                energy_type = group['energyMainGroupKey'].lower()
                energy_group_data[energy_type] = group

        # Add energy group values in the same order as headers
        energy_group_values = []
        for energy_type in energy_groups_present:
            if energy_type in energy_group_data:
                group = energy_group_data[energy_type]
                for status in ['active', 'inactive', 'unSupported']:
                    for metric in ['remote', 'nonRemote', 'calc', 'total']:
                        energy_group_values.append(group[status][metric])
            else:
                # If this building doesn't have this energy type, fill with zeros
                for _ in range(12):  # 3 statuses * 4 metrics
                    energy_group_values.append(0)

        csv_writer.writerow(base_row + energy_group_values)

    # Upload to S3
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    s3_key = f'meter-stats/billing_{timestamp}.csv'

    print(f"Uploading to s3://{s3_bucket}/{s3_key}")

    s3.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    return {
        'statusCode': 200,
        's3_bucket': s3_bucket,
        's3_key': s3_key,
        'endpoint': endpoint_url,
        'records_processed': len(data)
    }
`),
    });

    // Grant S3 write permissions to the Lambda function
    this.ingestionFunction.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["s3:PutObject", "s3:PutObjectAcl"],
        resources: [
          `arn:aws:s3:::billing-${bcdataAccountId}-*/*`,
          `arn:aws:s3:::billing-*/*`, // Fallback pattern
        ],
      }),
    );

    // Grant STS assume role permission to assume role in BcdataStack account
    this.ingestionFunction.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["sts:AssumeRole"],
        resources: [`arn:aws:iam::${bcdataAccountId}:role/BcdataSSMReadRole`],
      }),
    );

    // Grant EC2 network interface permissions for VPC Lambda
    this.ingestionFunction.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "ec2:CreateNetworkInterface",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
        ],
        resources: ["*"],
      }),
    );

    // Optional: Create EventBridge rule to run on schedule (daily at 2 AM UTC)
    const dailyRule = new events.Rule(this, "DailyIngestionRule", {
      schedule: events.Schedule.cron({
        minute: "0",
        hour: "2",
        day: "*",
        month: "*",
        year: "*",
      }),
      description: "Trigger meter stats ingestion daily at 2 AM UTC",
    });

    dailyRule.addTarget(new targets.LambdaFunction(this.ingestionFunction));
  }
}
