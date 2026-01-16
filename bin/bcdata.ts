#!/usr/bin/env node
import * as cdk from "aws-cdk-lib/core";
import { BcdataStack } from "../lib/bcdata-stack";

const app = new cdk.App();
new BcdataStack(app, "BcdataStack", {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
