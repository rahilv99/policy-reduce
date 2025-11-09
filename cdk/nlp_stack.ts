import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import { Construct } from 'constructs';
import { CoreStack } from "./core_stack";
import * as dotenv from 'dotenv';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as cloudwatchActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';

dotenv.config();

interface ExtendedProps extends cdk.StackProps {
  readonly coreStack: CoreStack;
}

export class NlpStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ExtendedProps) {
    super(scope, id, props);

    const logGroup = new logs.LogGroup(this, "PolicyReduceNlpLogGroup", {
      logGroupName: "PolicyReduceNlpLogGroup",
      retention: cdk.aws_logs.RetentionDays.ONE_MONTH
    })

    const lambdaFunction = new lambda.DockerImageFunction(this, 'PolicyReduceNlpFunction', {
      code: lambda.DockerImageCode.fromImageAsset('src', {
        platform: Platform.LINUX_AMD64,
        buildArgs: {},
        file: 'nlp-lambda/Dockerfile'
      }),
      timeout: cdk.Duration.minutes(15),
      memorySize: 2048,
      environment: {
        BUCKET_NAME: props.coreStack.s3Bucket.bucketName,
        NLP_QUEUE_URL: props.coreStack.nlpSQSQueue.queueUrl,
        DB_URI: process.env.DB_URI!,
        GOOGLE_API_KEY: process.env.GOOGLE_API_KEY!,
        ANTHROPIC_API_KEY: process.env.ANTHROPIC_API_KEY!,
        NLP_QUEUE_ARN: props.coreStack.nlpSQSQueue.queueArn
      },
      role: props.coreStack.generalLambdaRole,
      logGroup: logGroup
    });

    const NlplambdaErrorMetric = lambdaFunction.metricErrors({
      period: cdk.Duration.hours(20),
      statistic: 'Sum',
    });

    // Alarm for Lambda function errors
    const NlpLambdaFailureAlarm = new cloudwatch.Alarm(this, 'PolicyReduceNlpLambdaFailureAlarm', {
      metric: NlplambdaErrorMetric,
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      alarmDescription: 'Alarm when NLP Lambda function fails',
    });

    const NlpLambdaFailureAlarmTopic = new sns.Topic(this, 'PolicyReduceNlpLambdaFailureAlarmTopic');
    NlpLambdaFailureAlarmTopic.addSubscription(new subs.EmailSubscription('rahilv99@gmail.com'));
    NlpLambdaFailureAlarm.addAlarmAction(new cloudwatchActions.SnsAction(NlpLambdaFailureAlarmTopic))

    // Grant Lambda permissions to send messages to the queue
    props.coreStack.nlpSQSQueue.grantSendMessages(lambdaFunction);

    // Create a specific IAM policy for EventBridge rule management
    const eventBridgeRulePolicy = new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'events:PutRule',
        'events:DeleteRule',
        'events:PutTargets',
        'events:RemoveTargets',
        'events:ListRules',
        'events:ListTargetsByRule',
        'events:DescribeRule'
      ],
      resources: [
        `arn:aws:events:${this.region}:${this.account}:rule/policy-reduce-batch-check-*`
      ]
    });

    // Add the specific EventBridge rule policy to the Lambda function's role
    lambdaFunction.addToRolePolicy(eventBridgeRulePolicy);

    // Grant EventBridge permissions to send messages to the SQS queue
    // This allows EventBridge rules to send messages to the queue as targets
    props.coreStack.nlpSQSQueue.addToResourcePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      principals: [new iam.ServicePrincipal('events.amazonaws.com')],
      actions: ['sqs:SendMessage'],
      resources: [props.coreStack.nlpSQSQueue.queueArn]
    }));

    // Grant Lambda permissions to be triggered by the queue
    lambdaFunction.addEventSource(
      new lambdaEventSources.SqsEventSource(props.coreStack.nlpSQSQueue, {
        batchSize: 1, // Process one message at a time
      })
    );
  }
}
