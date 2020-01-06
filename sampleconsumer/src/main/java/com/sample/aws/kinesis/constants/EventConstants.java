package com.sample.aws.kinesis.constants;

public class EventConstants {

    public static class KinesisConstants {

        public static final String STREAM_NAME = "stream.name";
        public static final String ACCESS_KEY = "access.key";
        public static final String SECRET_KEY = "secret.key";
        public static final String AWS_REGION = "aws.region";

        // Consumer specific parameters
        public static final String APPLICATION_NAME = "application.name";
        public static final String INITIAL_STREAM_POSITION = "initial.stream.position";
        public static final String BACKOFF_TIME_IN_MILLIS = "backoff.time.in.millis";
        public static final String NUM_RETRIES = "retries.num";
        public static final String CHECKPOINT_INTERVAL_MILLIS = "checkpoint.interval.in.millis";
        public static final String TIMESTAMP = "timestamp";
        public static final String MIN_NUM_CHECKPOINT_UPDATE_RETRIES = "checkpointer.retries.num";
    }

    // Inner class for default constant values related to Kinesis producer & com.sample.aws.kinesis.consumer configurations
    public static class KinesisDefaultConfigConstants {

        // Producer specific default config values
        public static final int MAX_CONNECTIONS = 25;
        public static final int CONNECTION_TIME_OUT = 60000;
        public static final long RECORD_MAX_BUFFERED_TIME = 3000;

        // Consumer specific default config values
        public static final long BACKOFF_TIME_IN_MILLIS = 3000;
        public static final long CHECKPOINT_INTERVAL_IN_MILLIS = 6000;
        public static final int NUM_RETRIES = 10;
        public static final int MIN_NUM_CHECKPOINT_UPDATE_RETRIES = 10;
    }
}
