package com.sample.aws.kinesis.configs;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.sample.aws.kinesis.constants.EventConstants;
import com.sample.aws.kinesis.consumer.impl.AmazonKinesisApplicationSampleRecordProcessor;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ConsumerConfigs {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerConfigs.class);

    private String streamName;
    private String accessKey;
    private String secretKey;
    private String applicationName;
    private long backoffTimeInMillis;
    private int retriesNum;
    private int checkpointerRetriesNum;
    private long checkpointIntervalMillis;
    private InitialPositionInStream initialPositionInStream = InitialPositionInStream.LATEST;
    private Date timestamp;
    private Schema schema;

    public ConsumerConfigs(Properties properties){

        LOGGER.info("Setting Kinesis Consumer Configurations ... ");

        streamName = properties.getProperty(EventConstants.KinesisConstants.STREAM_NAME);
        accessKey = (properties.getProperty(EventConstants.KinesisConstants.ACCESS_KEY));
        secretKey = (properties.getProperty(EventConstants.KinesisConstants.SECRET_KEY));

        setApplicationName(properties.getProperty(EventConstants.KinesisConstants.APPLICATION_NAME));
        setInitialPositionInStream(properties.getProperty(EventConstants.KinesisConstants.INITIAL_STREAM_POSITION));
        setBackoffTimeInMillis(properties.getProperty(EventConstants.KinesisConstants.BACKOFF_TIME_IN_MILLIS));
        setCheckpointIntervalMillis(properties.getProperty(EventConstants.KinesisConstants.CHECKPOINT_INTERVAL_MILLIS));
        setRetriesNum(properties.getProperty(EventConstants.KinesisConstants.NUM_RETRIES));
        setCheckpointerRetriesNum(properties.getProperty(EventConstants.KinesisConstants.MIN_NUM_CHECKPOINT_UPDATE_RETRIES));
        if (InitialPositionInStream.AT_TIMESTAMP.equals(getInitialPositionInStream())) {
            setTimestamp(properties.getProperty(EventConstants.KinesisConstants.TIMESTAMP));
        }
        setSchema();
    }

    public Schema getSchema() {
        return schema;
    }

    public String getStreamName() {
        return streamName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getApplicationName() {
        return applicationName;
    }


    public InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }


    public void setSchema() {
        InputStream schemaInputStream = AmazonKinesisApplicationSampleRecordProcessor.class.getResourceAsStream("/payload.json.avro");
        try {
            this.schema = new Schema.Parser().parse(schemaInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setApplicationName(String applicationName){

        if (StringUtils.isBlank(applicationName)) {
            LOGGER.error("Missing configuration for application name");
        }
        this.applicationName = applicationName;
    }

    public void setInitialPositionInStream(String initialPositionInStream){
        if (StringUtils.isBlank(initialPositionInStream)) {
            LOGGER.error("Missing configuration for initial position in stream");
        } else {
            if (InitialPositionInStream.AT_TIMESTAMP.toString().equalsIgnoreCase(initialPositionInStream)) {
                this.initialPositionInStream = InitialPositionInStream.AT_TIMESTAMP;
            } else if (InitialPositionInStream.TRIM_HORIZON.toString().equalsIgnoreCase(initialPositionInStream)) {
                this.initialPositionInStream = InitialPositionInStream.TRIM_HORIZON;
            } else if (InitialPositionInStream.LATEST.toString().equalsIgnoreCase(initialPositionInStream)) {
                this.initialPositionInStream = InitialPositionInStream.LATEST;
            } else {
                LOGGER.error("Undefined value for initial position in stream");
            }
        }
    }

    public long getBackoffTimeInMillis() {
        return backoffTimeInMillis;
    }

    public void setBackoffTimeInMillis(String backoffTimeInMillis){
        if (StringUtils.isBlank(backoffTimeInMillis)) {
            LOGGER.warn("Back off time in millis is not defined. Default value will be used");
            this.backoffTimeInMillis = EventConstants.KinesisDefaultConfigConstants.BACKOFF_TIME_IN_MILLIS;
        } else {
            try {
                this.backoffTimeInMillis = Long.parseLong(backoffTimeInMillis);
            } catch (NumberFormatException e) {
                LOGGER.error("Backoff time property is expecting a long value.");
            }
        }
    }

    public int getRetriesNum() {
        return retriesNum;
    }

    public int getCheckpointerRetriesNum(){
        return checkpointerRetriesNum;
    }

    public void setRetriesNum(String retriesNum){
        if (StringUtils.isBlank(retriesNum)) {
            LOGGER.warn("Retries number is not defined. Default value will be used");
            this.retriesNum = EventConstants.KinesisDefaultConfigConstants.NUM_RETRIES;
        } else {
            try {
                this.retriesNum = Integer.parseInt(retriesNum);
            } catch (NumberFormatException e) {
                LOGGER.error("Retries number property is expecting an integer value.");
            }
        }
    }

    public void setCheckpointerRetriesNum(String checkpointerRetriesNum) {
        if (StringUtils.isBlank(checkpointerRetriesNum)) {
            LOGGER.warn("Checkpointer retries number is not defined. Default value will be used");
            this.checkpointerRetriesNum = EventConstants.KinesisDefaultConfigConstants.MIN_NUM_CHECKPOINT_UPDATE_RETRIES;
        } else {
            try {
                this.checkpointerRetriesNum = Integer.parseInt(checkpointerRetriesNum);
            } catch (NumberFormatException e) {
                LOGGER.error("Checkpointer retries number property is expecting an integer value.");
            }
        }
    }

    public long getCheckpointIntervalMillis() {
        return checkpointIntervalMillis;
    }

    public void setCheckpointIntervalMillis(String checkpointIntervalMillis) {
        if (StringUtils.isBlank(checkpointIntervalMillis)) {
            LOGGER.warn("Checkpoint interval in millis is not defined. Default value will be used");
            this.checkpointIntervalMillis = EventConstants.KinesisDefaultConfigConstants.CHECKPOINT_INTERVAL_IN_MILLIS;
        }else{
            try {
                this.checkpointIntervalMillis = Long.parseLong(checkpointIntervalMillis);
            } catch (NumberFormatException e) {
                LOGGER.error("Checkpoint interval property is expecting a long value.");
            }
        }

    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp){
        if (StringUtils.isBlank(timestamp)) {
            LOGGER.error("Missing configuration for timestamp");
        }

        try {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
            this.timestamp = formatter.parse(timestamp.replaceAll("Z$", "+0000"));
        } catch (ParseException e) {
            LOGGER.error("Timestamp property is expecting 'yyyy-MM-dd'T'HH:mm:ssZ' format value.");
        }
    }
}
