package com.google.cloud.pso;

import com.google.cloud.pso.transformation.AvroToOrcUtils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcMapreduceRecordWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

@RunWith(JUnit4.class)
public class Avro2OrcUtilsTest {
    private static final Logger LOG = LoggerFactory.getLogger(Avro2OrcUtilsTest.class);

    @Test
    public void convertObject() throws IOException {
        String funcName = "convertSchema";
        LOG.info("{}() <<", funcName);

        Schema schema = (new Schema.Parser()).parse(Avro2OrcUtilsTest.class.getResourceAsStream("/1.avsc"));
        LOG.info("schema.avsc: {}", schema);
        TypeDescription orcSchema = AvroToOrcUtils.getOrcFieldType(schema);

        GenericDatumReader datum = new GenericDatumReader();
        File file = new File("src/test/resources/1.avro");
        DataFileReader reader = new DataFileReader(file, datum);

        LOG.info("schema: {}", reader.getSchema());

        Configuration conf = new Configuration();
        Writer writer = OrcFile.createWriter(new Path("my-file.orc"), OrcFile.writerOptions(conf).setSchema(orcSchema));
        OrcMapreduceRecordWriter output = new OrcMapreduceRecordWriter(writer);
        GenericData.Record record = new GenericData.Record(reader.getSchema());
        while (reader.hasNext()) {
            reader.next(record);
            OrcStruct orc = (OrcStruct)AvroToOrcUtils.convertToORCObject(orcSchema, record);

            output.write(NullWritable.get(), orc);

        }

        output.close(null);


        reader.close();
        LOG.info("{} >>");
    }
}
