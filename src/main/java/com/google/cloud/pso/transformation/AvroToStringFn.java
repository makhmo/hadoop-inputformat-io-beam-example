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
package com.google.cloud.pso.transformation;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcValue;

/**
 * A helper class {@link AvroToStringFn} that converts a GenericRecord {@link GenericRecord} to a
 * OrcValue {@link OrcValue}
 */
public class AvroToStringFn extends SimpleFunction<GenericRecord, String> {

    private TypeDescription orcSchema;

    public AvroToStringFn(TypeDescription orcSchema) {
        this.orcSchema = orcSchema;
    }

    @Override
    public String apply(GenericRecord input) {
        return ReflectionToStringBuilder.toString(new OrcValue(AvroToOrcUtils.convertToORCObject(orcSchema, input)));
    }


}
