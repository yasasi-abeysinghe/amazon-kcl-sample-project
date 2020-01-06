package com.sample.aws.kinesis.consumer;


import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.sample.aws.kinesis.configs.ConsumerConfigs;
import com.sample.aws.kinesis.consumer.impl.AmazonKinesisApplicationSampleRecordProcessor;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Sample Amazon Kinesis Application.
 */
public class KinesisEventConsumer {

    private ConsumerConfigs consumerConfigs;

    private AWSCredentialsProvider credentialsProvider;

    public KinesisEventConsumer(ConsumerConfigs consumerConfigs) {
        this.consumerConfigs = consumerConfigs;

        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        AWSCredentials awsCredentials = new BasicAWSCredentials(consumerConfigs.getAccessKey(), consumerConfigs.getSecretKey());
        this.credentialsProvider = new StaticCredentialsProvider(awsCredentials);

        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(e);
        }
    }

    public void processEvents() throws Exception {

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(consumerConfigs.getApplicationName(),
                        consumerConfigs.getStreamName(),
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(consumerConfigs.getInitialPositionInStream());

        final IRecordProcessorFactory recordProcessorFactory = () ->
                new AmazonKinesisApplicationSampleRecordProcessor(consumerConfigs);

        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        System.out.printf("Running %s to process stream %s as worker %s...\n",
                consumerConfigs.getApplicationName(),
                consumerConfigs.getStreamName(),
                workerId);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    public void deleteResources() {
        // Delete the stream
        AmazonKinesis kinesis = AmazonKinesisClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();

        System.out.printf("Deleting the Amazon Kinesis stream used by the sample. Stream Name = %s.\n",
                consumerConfigs.getStreamName());
        try {
            kinesis.deleteStream(consumerConfigs.getStreamName());
        } catch (ResourceNotFoundException ex) {
            // The stream doesn't exist.
        }

        // Delete the table
        AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        System.out.printf("Deleting the Amazon DynamoDB table used by the Amazon Kinesis Client Library. Table Name = %s.\n",
                consumerConfigs.getApplicationName());
        try {
            dynamoDB.deleteTable(consumerConfigs.getApplicationName());
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException ex) {
            // The table doesn't exist.
        }
    }
}
