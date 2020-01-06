package com.sample.aws.kinesis.consumer.impl;

/*
 * Copyright 2012-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.sample.aws.kinesis.avro.AvroDeserializer;
import com.sample.aws.kinesis.configs.ConsumerConfigs;
import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * Processes records and checkpoints progress.
 */
public class AmazonKinesisApplicationSampleRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(AmazonKinesisApplicationSampleRecordProcessor.class);
    private String kinesisShardId;
    private long nextCheckpointTimeInMillis;

    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    private ConsumerConfigs consumerConfigs;


    public AmazonKinesisApplicationSampleRecordProcessor(ConsumerConfigs consumerConfigs) {
        this.consumerConfigs = consumerConfigs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Processing " + records.size() + " records from " + kinesisShardId);

        // Process records and perform all exception handling.
        processRecordsWithRetries(records);

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + consumerConfigs.getCheckpointIntervalMillis();
        }
    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {
        for (Record record : records) {
            boolean processedSuccessfully = false;
            for (int i = 0; i < consumerConfigs.getRetriesNum(); i++) {
                try {
                    processSingleRecord(record);

                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOG.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(consumerConfigs.getBackoffTimeInMillis());
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
     * Process a single record.
     *
     * @param record The record to be processed.
     */
    private void processSingleRecord(Record record) {
        // TODO Add your own record processing logic here

        String data = null;
        try {
            // schema deserialize
            Schema schema = consumerConfigs.getSchema();

            ByteBuffer buf = record.getData();
            byte[] serializedBytes = new byte[buf.remaining()];

            buf.get(serializedBytes);

            data = String.valueOf(AvroDeserializer.deserialize(serializedBytes, schema));

            LOG.info(record.getSequenceNumber() +"," + record.getPartitionKey() + ", " + data);

        } catch (NumberFormatException e) {
            LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
        } catch (CharacterCodingException e) {
            LOG.error("Malformed data: " + data, e);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }

    /** Checkpoint with retries.
     * @param checkpointer
     */
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < consumerConfigs.getRetriesNum(); i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (consumerConfigs.getRetriesNum() - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + consumerConfigs.getRetriesNum(), e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(consumerConfigs.getBackoffTimeInMillis());
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }
}
