/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.pso;

import com.google.cloud.pso.configuration.HadoopConfiguration;
import com.google.cloud.pso.transformation.AvroToOrcStructFn;
import com.google.cloud.pso.transformation.AvroToOrcUtils;
import com.google.cloud.pso.transformation.AvroToStringFn;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.hadoop.format.HDFSSynchronization;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcValue;
import org.apache.orc.mapreduce.OrcOutputFormat;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * An example pipeline that uses HadoopInputFormatIO to read files from GCS using a
 * HadoopInputFormat class.
 *
 * <p>In this example class, {@link HadoopFormatIO}, we will:
 *
 * <pre>
 *     1. Read Orc files (in GCS) using the OrcInputFormat via HadoopInputFormatIO Beam transform
 *     2. Convert OrcStruct to Avro GenericRecord
 *     3. Write Avro GenericRecords to GCS via AvroIO Beam transform
 * </pre>
 * <p>
 * The pipeline would need the following required flags: --runner=DataflowRunner
 * --project=project-id --stagingLocation=gcs-staging-location
 * --inputDir=gcs-input-directory-with-orc-files --outputDir=gcs-output-directory-with-avro-files
 * --schemaFile=gcs-avro-schema-file-location
 *
 * <p>In addition the pipeline requires either application-default credentials or the
 * GOOGLE_APPLICATION_CREDENTIALS to be set to a service account json key. The service account
 * should have the required read privileges on the buckets for the source files and schema.
 *
 * @see <a
 * href="https://orc.apache.org/api/orc-mapreduce/index.html?org/apache/orc/mapreduce/OrcInputFormat.html">OrcInputFormat</a>)
 */
public class HadoopOutputFormatIOExample {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopOutputFormatIOExample.class);

    private static final String OUTPUT_FORMAT_KEY = "mapreduce.job.outputformat.class";

    private static final String KEY_CLASS_KEY = "mapreduce.job.output.key.class";

    private static final String VALUE_CLASS_KEY = "mapreduce.job.output.value.class";

    private static final String OUTPUT_DIR_KEY = "mapreduce.output.fileoutputformat.outputdir";

    private static final String OPTIONAL_PREFIX = "output";


    private interface Options extends DataflowPipelineOptions {

        @Description("GCS path for Avro files (input)")
        @Validation.Required
        String getInputDir();

        void setInputDir(String inputDir);

        @Description("GCS path for Orc files (output)")
        @Validation.Required
        String getOutputDir();

        void setOutputDir(String outputDir);

        @Description("Schema file GCS location")
        @Validation.Required
        String getSchemaFile();

        void setSchemaFile(String schemaFile);

        @Description("GCS path for Hadoop Locks")
        @Validation.Required
        String getLockDir();

        void setLockDir(String value);
    }

    /**
     * Helper method to extract an Avro Schema object from the json file in GCS.
     *
     * @param options Options to extract schema file GCS path from
     * @return {@link Schema} Avro schema object from the json schema
     * @throws IOException
     */
    private static Schema getSchemaFromPath(Options options) throws IOException {

        GcsUtil gcsUtil = options.getGcsUtil();
        GcsPath gcsPath = GcsPath.fromUri(options.getSchemaFile());

        int bufferSize = (int) gcsUtil.fileSize(gcsPath);
        String schemaString;
        try (SeekableByteChannel sbc = gcsUtil.open(gcsPath)) {
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            sbc.read(buffer);
            buffer.flip();
            schemaString = new String(buffer.array(), StandardCharsets.UTF_8);
        }

        return new Schema.Parser().parse(schemaString);
    }

    private static String getOutputPathWithPrefix(Options options) {
        String outputPath = options.getOutputDir();
        ResourceId outputResourceId = FileBasedSink.convertToFileResourceIfPossible(outputPath);
        return (outputResourceId.isDirectory()) ? outputPath + OPTIONAL_PREFIX : outputPath;
    }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        run(options);
    }

    private static PipelineResult run(Options options) throws IOException {

        Pipeline pipeline = Pipeline.create(options);

        Schema avroSchema = getSchemaFromPath(options);
        TypeDescription orcSchema = AvroToOrcUtils.getOrcFieldType(avroSchema);
        String resolvedOutputPath = getOutputPathWithPrefix(options);

       // pipeline.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroCoder.of(avroSchema));

        Configuration hadoopConf = HadoopConfiguration.get(options);

        // OrcInputFormat requires the following 4 properties to be set
        hadoopConf.setClass(OUTPUT_FORMAT_KEY, OrcOutputFormat.class, OutputFormat.class);

        hadoopConf.setClass(KEY_CLASS_KEY, NullWritable.class, Object.class);

        hadoopConf.setClass(VALUE_CLASS_KEY, OrcValue.class, Object.class);
        hadoopConf.setClass("orc.mapred.value.type", OrcValue.class, Object.class);
        hadoopConf.setClass("mapreduce.map.output.value.class", OrcValue.class, Object.class);

        hadoopConf.set(OUTPUT_DIR_KEY, resolvedOutputPath);
        hadoopConf.set("orc.mapred.output.schema", orcSchema.toString());
        hadoopConf.set("orc.mapred.map.output.value.schema", orcSchema.toString());
        hadoopConf.set("mapreduce.job.id", String.valueOf(System.currentTimeMillis()));

        hadoopConf.setInt("mapreduce.job.reduces", 1);

        SimpleFunction<GenericRecord, OrcValue> avroToOrcStructFn = new AvroToOrcStructFn(orcSchema);

        PCollection<GenericRecord> records =
                pipeline.apply("Read Avro", AvroIO.readGenericRecords(avroSchema)
                        .from(options.getInputDir()));
        records.apply("Convert Avro to Orc", MapElements.via(avroToOrcStructFn)).setCoder(new OrcValueCoder(orcSchema))
                .apply(MapElements.via(new SimpleFunction<OrcValue, KV<NullWritable, OrcValue>>() {
                                           @Override
                                           public KV<NullWritable, OrcValue> apply(OrcValue input) {
                                               return KV.of(NullWritable.get(), input);
                                           }
                                       }
                ))
//                .apply(" Windowing ", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply("Write ORC", HadoopFormatIO.<NullWritable, OrcValue>write().withConfiguration(hadoopConf).withPartitioning()
                        .withExternalSynchronization(new HDFSSynchronization(options.getLockDir())));



        return pipeline.run();
    }

    public static class PrintOrcValue extends DoFn<OrcValue, Void> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("element: " + ReflectionToStringBuilder.toString(c.element()));
        }
    }

    public static class PrintString extends DoFn<String, Void> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            System.out.println("element:  " + c.element());
        }
    }

    public static class OrcValueCoder extends Coder<OrcValue> {

        private TypeDescription orcSchema;

        public OrcValueCoder(TypeDescription orcSchema) {
            this.orcSchema = orcSchema;
        }

        @Override
        public void encode(OrcValue orcValue, OutputStream outStream) throws IOException {
            orcValue.value.write(new DataOutputStream(outStream));
        }

        @Override
        public OrcValue decode(InputStream inStream) throws CoderException, IOException {
            OrcValue ret = new OrcValue();
            ret.value = OrcStruct.createValue(orcSchema);
            ret.readFields(new DataInputStream(inStream));
            return ret;
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
        }
    }
}
