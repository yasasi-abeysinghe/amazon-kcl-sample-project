package com.sample.aws.kinesis;

import com.sample.aws.kinesis.configs.ConsumerConfigs;
import com.sample.aws.kinesis.consumer.KinesisEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SampleConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleConsumer.class);
    private ConsumerConfigs consumerConfigs;
    private KinesisEventConsumer consumer;

    public SampleConsumer(Properties properties){

        LOGGER.info("Creating the Kinesis Event Consumer ... ");

        this.consumerConfigs = new ConsumerConfigs(properties);
        this.consumer = new KinesisEventConsumer(consumerConfigs);
    }

    public void consumeData() {
        try {
            consumer.processEvents();
            System.out.println("success");
        } catch (Exception e) {
            System.out.println("Error " + e);
        }
    }

    public static void main(String args[]) {

        InputStream consumerConfigInputStream = SampleConsumer.class.getResourceAsStream("/ConsumerConfiguration.properties");

        Properties properties = new Properties();

        try {
            LOGGER.info("Loading properties from file ...");
            properties.load(consumerConfigInputStream);

            SampleConsumer sampleConsumer = new SampleConsumer(properties);

            sampleConsumer.consumeData();


        } catch (IOException e) {
            LOGGER.error("context", e);
        }
    }
}
