package com.google.cloud.pso.transformation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.*;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcUnion;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AvroToOrcUtils {

    public static WritableComparable convertToORCObject(TypeDescription typeInfo, Object o) {
        if (o != null) {
            if (TypeDescription.Category.UNION.equals(typeInfo.getCategory())) {
                OrcUnion union = new OrcUnion(typeInfo);
                // Avro uses Utf8 and GenericData.EnumSymbol objects instead of Strings. This is handled in other places in the method, but here
                // we need to determine the union types from the objects, so choose String.class if the object is one of those Avro classes
                Class clazzToCompareTo = o.getClass();
                if (o instanceof org.apache.avro.util.Utf8 || o instanceof GenericData.EnumSymbol) {
                    clazzToCompareTo = String.class;
                }
                // TODO: Implementation
                return union;
            }
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
                int[] intArray = (int[]) o;
                OrcList<WritableComparable<IntWritable>> ret = new OrcList<WritableComparable<IntWritable>>(TypeDescription.createInt());
                Arrays.stream(intArray)
                        .forEach((element) -> ret.add(convertToORCObject(TypeDescription.createInt(), element)));
                return  ret;
            }
            if (o instanceof long[]) {
                long[] longArray = (long[]) o;
                OrcList<WritableComparable<LongWritable>> ret = new OrcList<WritableComparable<LongWritable>>(TypeDescription.createLong());
                Arrays.stream(longArray)
                        .forEach((element) -> ret.add(convertToORCObject(TypeDescription.createLong(), element)));
                return  ret;
            }
            if (o instanceof float[]) {
                float[] floatArray = (float[]) o;
                OrcList<WritableComparable<FloatWritable>> ret = new OrcList<WritableComparable<FloatWritable>>(TypeDescription.createFloat());
                IntStream.range(0, floatArray.length)
                        .mapToDouble(i -> floatArray[i])
                        .forEach((element) -> ret.add(convertToORCObject(TypeDescription.createFloat(), element)));
                return  ret;
            }
            if (o instanceof double[]) {
                double[] doubleArray = (double[]) o;
                OrcList<WritableComparable<DoubleWritable>> ret = new OrcList<WritableComparable<DoubleWritable>>(TypeDescription.createDouble());
                Arrays.stream(doubleArray)
                        .forEach((element) -> ret.add(convertToORCObject(TypeDescription.createDouble(), element)));
                return  ret;
            }
            if (o instanceof boolean[]) {
                boolean[] booleanArray = (boolean[]) o;

                OrcList<WritableComparable<BooleanWritable>> ret = new OrcList<WritableComparable<BooleanWritable>>(TypeDescription.createBoolean());
                IntStream.range(0, booleanArray.length)
                        .map(i -> booleanArray[i] ? 1 : 0)
                        .forEach((element) -> ret.add(convertToORCObject(TypeDescription.createBoolean(), element == 1)));
                return  ret;

            }
            if (o instanceof GenericData.Array) {
                GenericData.Array array = ((GenericData.Array) o);
                // The type information in this case is interpreted as a List
                TypeDescription listTypeInfo = typeInfo.getChildren().get(0);
                // array.stream().map((element) -> convertToORCObject(listTypeInfo, element)).collect(Collectors.toList());
            }
            if (o instanceof List) {
//                return o;
            }
            if (o instanceof Map) {
                Map map = new HashMap();
                TypeDescription keyInfo = typeInfo.getChildren().get(0);
                TypeDescription valueInfo = typeInfo.getChildren().get(1);
                // Unions are not allowed as key/value types, so if we convert the key and value objects,
                // they should return Writable objects
                ((Map) o).forEach((key, value) -> {
                    Object keyObject = convertToORCObject(keyInfo, key);
                    Object valueObject = convertToORCObject(valueInfo, value);
                    if (keyObject == null) {
                        throw new IllegalArgumentException("Maps' key cannot be null");
                    }
                    map.put(keyObject, valueObject);
                });
//                return map;
            }
            if (o instanceof GenericData.Record) {
                GenericData.Record record = (GenericData.Record) o;
                List<Schema.Field> recordFields = record.getSchema().getFields();
                if (recordFields != null) {
                    Object[] fieldObjects = new Object[recordFields.size()];
                    for (int i = 0; i < recordFields.size(); i++) {
                        Schema.Field field = recordFields.get(i);
                        Schema fieldSchema = field.schema();
                        Object fieldObject = record.get(field.name());
                        fieldObjects[i] = convertToORCObject(getOrcField(fieldSchema), fieldObject);
                    }
//                    return createOrcStruct(typeInfo, fieldObjects);
                }
            }
            throw new IllegalArgumentException("Error converting object of type " + o.getClass().getName() + " to ORC type " + typeInfo);
        } else {
            return null;
        }
    }


    public static TypeDescription getOrcField(Schema fieldSchema) throws IllegalArgumentException {
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
                            .map(AvroToOrcUtils::getOrcField)
                            .collect(Collectors.toList());

                    // Flatten the field if the union only has one non-null element
                    if (orcFields.size() == 1) {
                        return orcFields.get(0);
                    } else {
                        TypeDescription union = TypeDescription.createUnion();
                        orcFields.stream().forEach( o -> union.addUnionChild(o));

                        return  union;
                    }
                }
                return null;

            case ARRAY:
                return TypeDescription.createList(getOrcField(fieldSchema.getElementType()));

            case MAP:
                return TypeDescription.createMap(
                        getPrimitiveOrcTypeFromPrimitiveAvroType(Schema.Type.STRING),
                        getOrcField(fieldSchema.getValueType()));

            case RECORD:
                List<Schema.Field> avroFields = fieldSchema.getFields();
                TypeDescription record = TypeDescription.createStruct();
                if (avroFields != null) {
                    avroFields.forEach(avroField -> {
                        String fieldName = avroField.name();
                        record.addField(fieldName, getOrcField(avroField.schema()));
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

    public static OrcStruct createOrcStruct(TypeDescription typeInfo, WritableComparable... objs) {


        List<TypeDescription> fields = typeInfo.getChildren();

        OrcStruct result = new OrcStruct(typeInfo);

        for (int i = 0; i < fields.size(); i++) {
            result.setFieldValue(fields.get(i).getId(), objs[i]);
        }
        return result;
    }
}
