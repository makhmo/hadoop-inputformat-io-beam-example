package com.google.cloud.pso.transformation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.*;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AvroToOrcUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AvroToOrcUtils.class);

    public static WritableComparable convertToORCObject(TypeDescription typeInfo, Object o) {
        LOG.trace("{}({},  {}) <<", "convertToORCObject", typeInfo.getId(),  o.getClass().getName());

        if (o != null) {
            if (o instanceof Integer) {
                return new IntWritable((int) o);
            }
            if (o instanceof Boolean) {
                return new BooleanWritable((boolean) o);
            }
            if (o instanceof Long) {
                return new LongWritable((long) o);
            }
            if (o instanceof Float) {
                return new FloatWritable((float) o);
            }
            if (o instanceof Double) {
                return new DoubleWritable((double) o);
            }
            if (o instanceof String || o instanceof Utf8 || o instanceof GenericData.EnumSymbol) {
                return new Text(o.toString());
            }
            if (o instanceof ByteBuffer) {
                return new BytesWritable(((ByteBuffer) o).array());
            }
            if (o instanceof int[]) {
                int[] array = (int[]) o;
                OrcList<IntWritable> ret = new OrcList<IntWritable>(TypeDescription.createInt(), array.length);
                Arrays.stream(array).forEach(e -> ret.add(new IntWritable(e)));

                return ret;
            }
            if (o instanceof long[]) {
                long[] array = (long[]) o;
                OrcList<LongWritable> ret = new OrcList<LongWritable>(TypeDescription.createLong(), array.length);
                Arrays.stream(array).forEach(e -> ret.add(new LongWritable(e)));

                return ret;
            }
            if (o instanceof double[]) {
                double[] array = (double[]) o;
                OrcList<DoubleWritable> ret = new OrcList<DoubleWritable>(TypeDescription.createDouble(), array.length);
                Arrays.stream(array).forEach(e -> ret.add(new DoubleWritable(e)));

                return ret;
            }

            if (o instanceof GenericData.Array) {
                GenericData.Array array = ((GenericData.Array) o);
                TypeDescription orcType = getOrcFieldType(array.getSchema());
                OrcList ret = new OrcList(orcType, array.size());
                for (Object e : array) {
                    ret.add(convertToORCObject(orcType, e));
                }
                return ret;
            }
            if (o instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) o;
                OrcStruct ret = new OrcStruct(getOrcFieldType(record.getSchema()));
                List<Schema.Field> recordFields = record.getSchema().getFields();
                if (recordFields != null) {
                    for (int i = 0; i < recordFields.size(); i++) {
                        Schema.Field field = recordFields.get(i);
                        Schema fieldSchema = field.schema();
                        Object fieldObject = record.get(field.name());
                        TypeDescription orcType = getOrcFieldType(fieldSchema);
                        ret.setFieldValue(i, convertToORCObject(orcType, fieldObject));
                    }
                }
                return ret;
            }
            throw new IllegalArgumentException("Error converting object of type " + o.getClass().getName() + " to ORC type " + typeInfo);
        } else {
            return null;
        }
    }


    public static TypeDescription getOrcFieldType(Schema fieldSchema) throws IllegalArgumentException {
        Schema.Type fieldType = fieldSchema.getType();

        switch (fieldType) {
            case INT:
            case LONG:
            case BOOLEAN:
            case BYTES:
            case DOUBLE:
            case FLOAT:
            case STRING:
            case NULL:
                return getPrimitiveOrcTypeFromPrimitiveAvroType(fieldType);

            case UNION:
                List<Schema> unionFieldSchemas = fieldSchema.getTypes();

                if (unionFieldSchemas != null) {
                    // Ignore null types in union
                    List<TypeDescription> orcFields = unionFieldSchemas.stream().filter(
                            unionFieldSchema -> !Schema.Type.NULL.equals(unionFieldSchema.getType()))
                            .map(AvroToOrcUtils::getOrcFieldType)
                            .collect(Collectors.toList());

                    // Flatten the field if the union only has one non-null element
                    if (orcFields.size() == 1) {
                        return orcFields.get(0);
                    } else {
                        TypeDescription union = TypeDescription.createUnion();
                        orcFields.stream().forEach(o -> union.addUnionChild(o));

                        return union;
                    }
                }
                return null;

            case ARRAY:
                return TypeDescription.createList(getOrcFieldType(fieldSchema.getElementType()));

            case MAP:
                return TypeDescription.createMap(
                        getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING),
                        getOrcFieldType(fieldSchema.getValueType()));

            case RECORD:
                List<Schema.Field> avroFields = fieldSchema.getFields();
                TypeDescription record = TypeDescription.createStruct();
                if (avroFields != null) {
                    avroFields.forEach(avroField -> {
                        String fieldName = avroField.name();
                        record.addField(fieldName, getOrcFieldType(avroField.schema()));
                    });
                    return record;
                }
                return null;

            case ENUM:
                // An enum value is just a String for ORC/Hive
                return getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING);

            default:
                throw new IllegalArgumentException("Did not recognize Avro type " + fieldType.getName());
        }

    }

    public static TypeDescription getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type avroType) throws IllegalArgumentException {
        if (avroType == null) {
            throw new IllegalArgumentException("Avro type is null");
        }
        switch (avroType) {
            case INT:
                return TypeDescription.createInt();
            case LONG:
                return TypeDescription.createLong();
            case BOOLEAN:
            case NULL: // ORC has no null type, so just pick the smallest. All values are necessarily null.
                return TypeDescription.createBoolean();
            case BYTES:
                return TypeDescription.createBinary();
            case DOUBLE:
                return TypeDescription.createDouble();
            case FLOAT:
                return TypeDescription.createFloat();
            case STRING:
                return TypeDescription.createString();
            default:
                throw new IllegalArgumentException("Avro type " + avroType.getName() + " is not a primitive type");
        }
    }
}
