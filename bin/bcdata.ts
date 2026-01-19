#!/usr/bin/env node
import * as cdk from "aws-cdk-lib/core";
import { BcdataStack } from "../lib/bcdata-stack";
import { MeterStatsLambdaStack } from "../lib/meter-stats-lambda-stack";

const app = new cdk.App();
new BcdataStack(app, "BcdataStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});

// Standalone stack for Lambda function experimentation
new MeterStatsLambdaStack(app, "MeterStatsLambdaStack", {
  tableBucketName: "billing5",
  namespace: "billingdata",
  tableName: "meters",
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
