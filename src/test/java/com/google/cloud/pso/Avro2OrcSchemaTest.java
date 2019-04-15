package com.google.cloud.pso;

import com.google.cloud.pso.transformation.AvroToOrcUtils;
import org.apache.avro.Schema;
import org.apache.orc.TypeDescription;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@RunWith(JUnit4.class)
public class Avro2OrcSchemaTest {
    private static final Logger LOG = LoggerFactory.getLogger(Avro2OrcSchemaTest.class);

    @Test
    public void convertSchema() throws IOException {
        String funcName = "convertSchema";
        LOG.info("{}() <<", funcName);

        Schema schema = (new Schema.Parser()).parse(Avro2OrcSchemaTest.class.getResourceAsStream("/1.avsc"));

        TypeDescription typeDescription = AvroToOrcUtils.getOrcFieldType(schema);
        LOG.info("{}", typeDescription);

        LOG.info("{} >>");
    }
}
