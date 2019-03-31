/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.s3.format.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.orc.TypeDescription;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;

public class OrcConverterUtils {

  /**
   * Create orc schema description from connect schema
   *
   * @param connectSchema connect schema
   * @return orc schema
   */
  public static TypeDescription fromConnectSchema(Schema connectSchema) {
    List<Field> fields = connectSchema.fields();
    TypeDescription orcSchema = fromConnectSchema(fields);
    return orcSchema;
  }

  private static TypeDescription fromConnectSchema(List<Field> fields) {
    TypeDescription struct = TypeDescription.createStruct();
    for (Field field : fields) {
      struct.addField(field.name(), fromConnectFieldSchema(field.schema()));
    }
    return struct;
  }

  private static TypeDescription fromConnectFieldSchema(Schema fieldSchema) {
    Schema.Type fieldType = fieldSchema.type();
    switch (fieldType) {
      case BOOLEAN:
        return TypeDescription.createBoolean();
      case BYTES:
        return TypeDescription.createBinary();
      case INT8:
        return TypeDescription.createByte();
      case INT16:
        return TypeDescription.createShort();
      case INT32:
        return TypeDescription.createInt();
      case INT64:
        return TypeDescription.createLong();
      case STRING:
        return TypeDescription.createString();
      case FLOAT64:
        return TypeDescription.createDouble();
      case FLOAT32:
        return TypeDescription.createFloat();
      default:
        throw new DataException("Unsupported type: " + fieldType);
    }
  }

  /**
   * Parse connector data and set its data to orc columns
   *
   * @param orcColumns columns which represent row batch
   * @param data       connect data to parse
   * @param rowIndex   index of row in batch
   */
  public static void parseConnectData(ColumnVector[] orcColumns, Struct data, int rowIndex) {
    List<Field> schemaFields = data.schema().fields();
    for (int i = 0; i < orcColumns.length; i++) {
      ColumnVector column = orcColumns[i];
      Field field = schemaFields.get(i);
      parseConnectData(column, field.schema(), data.get(field), rowIndex);
    }
  }

  private static void parseConnectData(ColumnVector column, Schema connectFieldSchema,
                                       Object fieldData, int rowIndex) {
    if (fieldData == null) {
      setNullData(column, rowIndex);
    } else {
      Schema.Type type = connectFieldSchema.type();
      switch (type) {
        case FLOAT64:
          ((DoubleColumnVector) column).vector[rowIndex] = (Double) fieldData;
          break;
        case FLOAT32:
          Double value = Double.valueOf(fieldData.toString());
          ((DoubleColumnVector) column).vector[rowIndex] = value;
          break;
        case BYTES:
          byte[] byteValue = ((ByteBuffer) fieldData).array();
          ((BytesColumnVector) column).setVal(rowIndex, byteValue, 0, byteValue.length);
          break;
        case BOOLEAN:
          long boolVal = ((Boolean) fieldData) ? 1L : 0L;
          ((LongColumnVector) column).vector[rowIndex] = boolVal;
          break;
        case INT8:
          ((LongColumnVector) column).vector[rowIndex] =
              ((Byte) fieldData).longValue();
          break;
        case INT16:
          ((LongColumnVector) column).vector[rowIndex] =
              ((Short) fieldData).longValue();
          break;
        case INT32:
          ((LongColumnVector) column).vector[rowIndex] =
              ((Integer) fieldData).longValue();
          break;
        case INT64:
          String name = connectFieldSchema.name();
          if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(name)) {
            long dateValue = ((Date) fieldData).getTime();
            ((LongColumnVector) column).vector[rowIndex] = dateValue;
          } else {
            ((LongColumnVector) column).vector[rowIndex] = (Long) fieldData;
          }
          break;
        case STRING:
          byte[] strValue = ((String) fieldData).getBytes();
          ((BytesColumnVector) column).setVal(rowIndex, strValue, 0, strValue.length);
          break;
        default:
          throw new DataException("Unsupported connect schema  field type:" + type);
      }
    }
  }

  /**
   * Mark that current column on row has null as value, mark whole column as nullable
   */
  private static void setNullData(ColumnVector column, int rowIndex) {
    column.isNull[rowIndex] = true;
    column.noNulls = false;
  }
}
