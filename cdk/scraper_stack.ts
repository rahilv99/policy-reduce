import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as lambdaEventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { CoreStack } from "./core_stack";
import * as dotenv from 'dotenv';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as subs from 'aws-cdk-lib/aws-sns-subscriptions';
import * as cloudwatchActions from 'aws-cdk-lib/aws-cloudwatch-actions';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Platform } from 'aws-cdk-lib/aws-ecr-assets';

dotenv.config();

interface ExtendedProps extends cdk.StackProps {
  readonly coreStack: CoreStack;
}

export class ScraperStack extends cdk.Stack {
  public readonly scraperSQSQueue: sqs.Queue;

  constructor(scope: Construct, id: string, props: ExtendedProps) {
    super(scope, id, props);

    // Create Scraper queue
    this.scraperSQSQueue = new sqs.Queue(this, 'PolicyReduceScraperQueue', {
        queueName: 'PolicyReduceScraperQueue',
        visibilityTimeout: cdk.Duration.seconds(60*30),     // 15 minutes
        deadLetterQueue: {
            maxReceiveCount: 5,
            queue: new sqs.Queue(this, 'PolicyReduceScraperDLQ', {
                queueName: 'PolicyReduceScraperQueue_DLQ',
                retentionPeriod: cdk.Duration.days(14), // Retain messages in DLQ for 14 days
            })
        },
    });
    
    // Allow cloudwatch events to send messages to the SQS queue
    this.scraperSQSQueue.addToResourcePolicy(
      new cdk.aws_iam.PolicyStatement({
        effect: cdk.aws_iam.Effect.ALLOW,
        principals: [new cdk.aws_iam.ServicePrincipal('events.amazonaws.com')],
        actions: ['sqs:SendMessage'],
        resources: [this.scraperSQSQueue.queueArn],
      })
    );

    // Create a CloudWatch Event Rule for the scraper schedule (daily)
    const scraperRule = new events.Rule(this, 'PolicyReduceScraperRule', {
      schedule: events.Schedule.cron({
      minute: '0',
      hour: '10',
      month: '*',
      year: '*',
      }),
    });

    const messagePayload = {
      "action": "e_ingest"
    };

    scraperRule.addTarget(new targets.SqsQueue(this.scraperSQSQueue, {
      message: events.RuleTargetInput.fromObject(messagePayload)
    }));

    const logGroup = new logs.LogGroup(this, "PolicyReduceScraperLogGroup", {
      logGroupName: "PolicyReduceScraperLogGroup",
      retention: cdk.aws_logs.RetentionDays.ONE_MONTH
    })

    new logs.MetricFilter(this, 'PolicyReduceErrorFilter', {
      logGroup,
      metricNamespace: 'PolicyReduce/Scraper',
      metricName: 'Error',
      filterPattern: logs.FilterPattern.literal('Error'),
      metricValue: '1',
    });

    const ErrorMetric = new cloudwatch.Metric({
      namespace: 'PolicyReduce/Scraper',
      metricName: 'Error',
      statistic: 'Sum',
      period: cdk.Duration.hours(20),
    });

    const ErrorAlarm = new cloudwatch.Alarm(this, 'PolicyReduceScraperErrorAlarm', {
      metric: ErrorMetric,
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      alarmDescription: 'Alarm when scraper unexpected error occurs',
    });

    const ErrorAlarmTopic = new sns.Topic(this, 'PolicyReduceScraperErrorAlarmTopic');
    ErrorAlarmTopic.addSubscription(new subs.EmailSubscription('rahilv99@gmail.com'));
    ErrorAlarm.addAlarmAction(new cloudwatchActions.SnsAction(ErrorAlarmTopic));
    
    new logs.MetricFilter(this, 'PolicyReduceTotalRequeryErrorFilter', {
      logGroup,
      metricNamespace: 'PolicyReduce/Scraper',
      metricName: 'TotalRequeryError',
      filterPattern: logs.FilterPattern.literal('Logging error'),
      metricValue: '1',
    });

    const TotalRequeryErrorMetric = new cloudwatch.Metric({
      namespace: 'PolicyReduce/Scraper',
      metricName: 'TotalRequeryError',
      statistic: 'Sum',
      period: cdk.Duration.hours(20),
    });

    const TotalRequeryErrorAlarm = new cloudwatch.Alarm(this, 'PolicyReduceTotalRequeryErrorAlarm', {
      metric: TotalRequeryErrorMetric,
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      alarmDescription: 'Alarm when one or more error is found in the requery process',
    });

    const TotalRequeryErrorTopic = new sns.Topic(this, 'PolicyReduceTotalRequeryErrorTopic');
    TotalRequeryErrorTopic.addSubscription(new subs.EmailSubscription('rahilv99@gmail.com'));
    TotalRequeryErrorAlarm.addAlarmAction(new cloudwatchActions.SnsAction(TotalRequeryErrorTopic));

    // Metric filter and alarm for EventBridge rule creation errors
    new logs.MetricFilter(this, 'PolicyReducePollCreationErrorFilter', {
      logGroup,
      metricNamespace: 'PolicyReduce/Scraper',
      metricName: 'PollCreationError',
      filterPattern: logs.FilterPattern.literal('Error creating eventbridge rule'),
      metricValue: '1',
    });

    const PollCreationErrorMetric = new cloudwatch.Metric({
      namespace: 'PolicyReduce/Scraper',
      metricName: 'PollCreationError',
      statistic: 'Sum',
      period: cdk.Duration.hours(20),
    });

    const PollCreationErrorAlarm = new cloudwatch.Alarm(this, 'PolicyReducePollCreationErrorAlarm', {
      metric: PollCreationErrorMetric,
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      alarmDescription: 'Alarm when EventBridge rule creation fails',
    });

    const PollCreationErrorTopic = new sns.Topic(this, 'PolicyReducePollCreationErrorTopic');
    PollCreationErrorTopic.addSubscription(new subs.EmailSubscription('rahilv99@gmail.com'));
    PollCreationErrorAlarm.addAlarmAction(new cloudwatchActions.SnsAction(PollCreationErrorTopic));

    const scraperLambdaRole = new iam.Role(this, 'PolicyReduceScraperLambdaRole', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
    });

    // Add basic Lambda execution permissions
    scraperLambdaRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole')
    );

    // Grant S3 permissions to the scraper role
    props.coreStack.s3Bucket.grantReadWrite(scraperLambdaRole);
    props.coreStack.s3ScraperBucket.grantReadWrite(scraperLambdaRole);

    const lambdaFunction = new lambda.DockerImageFunction(this, 'PolicyReduceScraperFunction', {
      code: lambda.DockerImageCode.fromImageAsset('src', {
        platform: Platform.LINUX_AMD64,
        buildArgs: {},
        file: 'scraper-lambda/Dockerfile'
      }),
      timeout: cdk.Duration.minutes(15),
      memorySize: 1024,
      architecture: lambda.Architecture.X86_64,
      environment: {
        BUCKET_NAME: props.coreStack.s3Bucket.bucketName,
        SCRAPER_BUCKET_NAME: props.coreStack.s3ScraperBucket.bucketName,
        SCRAPER_QUEUE_URL: this.scraperSQSQueue.queueUrl,
        NLP_QUEUE_URL: props.coreStack.nlpSQSQueue.queueUrl,
        CONGRESS_API_KEY: process.env.CONGRESS_API_KEY!,
        DB_ACCESS_URL: process.env.DB_ACCESS_URL!,
        DB_URI: process.env.DB_URI!,
        SCRAPER_QUEUE_ARN: this.scraperSQSQueue.queueArn

      },
      role: scraperLambdaRole,
      logGroup: logGroup
    });

    const ScraperlambdaErrorMetric = lambdaFunction.metricErrors({
      period: cdk.Duration.hours(20),
      statistic: 'Sum',
    });

    // Alarm for Lambda function errors
    const ScraperLambdaFailureAlarm = new cloudwatch.Alarm(this, 'PolicyReduceScraperLambdaFailureAlarm', {
      metric: ScraperlambdaErrorMetric,
      threshold: 1,
      evaluationPeriods: 1,
      comparisonOperator: cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      alarmDescription: 'Alarm when Scraper Lambda function fails',
    });

    const ScraperLambdaFailureAlarmTopic = new sns.Topic(this, 'PolicyReduceScraperLambdaFailureAlarmTopic');
    ScraperLambdaFailureAlarmTopic.addSubscription(new subs.EmailSubscription('rahilv99@gmail.com'));
    ScraperLambdaFailureAlarm.addAlarmAction(new cloudwatchActions.SnsAction(ScraperLambdaFailureAlarmTopic));


    // Grant Lambda permissions to send messages to the queue
    this.scraperSQSQueue.grantSendMessages(lambdaFunction);
    props.coreStack.nlpSQSQueue.grantSendMessages(lambdaFunction);
    
    // Grant Lambda permissions to be triggered by the queue
    lambdaFunction.addEventSource(
        new lambdaEventSources.SqsEventSource(this.scraperSQSQueue, {
        batchSize: 1, // Process 1 message at a time
        })
    );

  }
}
