package com.google.cloud.pso;

import com.google.cloud.pso.configuration.HadoopConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


@RunWith(JUnit4.class)
public class OrcFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(OrcFileReader.class);

    private static final String INPUT_FORMAT_KEY = "mapreduce.job.inputformat.class";

    private static final String KEY_CLASS_KEY = "key.class";

    private static final String VALUE_CLASS_KEY = "value.class";

    private static final String INPUT_DIR_KEY = "mapreduce.input.fileinputformat.inputdir";

    private static final String OPTIONAL_PREFIX = "output";

    @Test
    public void testOrcFileReader() throws IOException {
        LOG.info("testOrcFileReader() <<");

        Configuration hadoopConf = new Configuration();

        // OrcInputFormat requires the following 4 properties to be set
        hadoopConf.setClass(INPUT_FORMAT_KEY, OrcInputFormat.class, InputFormat.class);

        hadoopConf.setClass(KEY_CLASS_KEY, NullWritable.class, Object.class);

        hadoopConf.setClass(VALUE_CLASS_KEY, OrcStruct.class, Object.class);

        hadoopConf.set(INPUT_DIR_KEY, "src/test/resources");

        Reader reader = OrcFile.createReader(new Path("src/test/resources/1.orc"), OrcFile.readerOptions(hadoopConf));

        LOG.info("schema: {}" + reader.getSchema());

        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();

        while (rows.nextBatch(batch)) {
            StructColumnVector edrdata = (StructColumnVector) batch.cols[0];
            LongColumnVector event_timestamp = (LongColumnVector) edrdata.fields[0];
            BytesColumnVector agent_id = (BytesColumnVector) edrdata.fields[1];

            for(int r=0; r < 2; ++r) {
                LOG.info("r: {}, event_timestamp: {}, agent_id: {}", r, event_timestamp.vector[r], agent_id.toString(r));
            }
        }
        rows.close();

        LOG.info("testOrcFileReader >>");
    }
}
