package com.sample.aws.kinesis.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class AvroDeserializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDeserializer.class);

    public static Object deserialize(byte[] bytes, Schema schema) throws IOException {

        GenericDatumReader datumReader = new GenericDatumReader<GenericRecord>(schema);

        InputStream inputStream = new ByteArrayInputStream(bytes);

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);

        Object object = datumReader.read(null, decoder);

        Object result = object.toString();

        return result;
    }
}
