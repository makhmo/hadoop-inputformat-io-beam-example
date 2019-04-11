package com.google.cloud.pso;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

@RunWith(JUnit4.class)
public class AvroSchemaExtractorTest {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaExtractorTest.class);

    @Test
    public void extractSchema() throws IOException {
        String funcName = "extractSchema";
        LOG.info("{}() <<", funcName);

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File("src/test/resources/1.avro"), datumReader);
        Schema schema = dataFileReader.getSchema();
        System.out.println(schema);

        LOG.info("{} >>");
    }
}
