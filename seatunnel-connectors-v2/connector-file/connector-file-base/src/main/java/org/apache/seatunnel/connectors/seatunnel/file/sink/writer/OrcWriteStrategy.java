/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;

import lombok.NonNull;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class OrcWriteStrategy extends AbstractWriteStrategy {
    private final Map<String, Writer> beingWrittenWriter;

    public OrcWriteStrategy(TextFileSinkConfig textFileSinkConfig) {
        super(textFileSinkConfig);
        this.beingWrittenWriter = new HashMap<>();
    }

    @Override
    public void write(@NonNull SeaTunnelRow seaTunnelRow) {
        String filePath = getOrCreateFilePathBeingWritten(seaTunnelRow);
        Writer writer = getOrCreateWriter(filePath);
        TypeDescription schema = buildSchemaWithRowType();
        VectorizedRowBatch rowBatch = schema.createRowBatch();
        int i = 0;
        int row = rowBatch.size++;
        for (Integer index : sinkColumnsIndexInRow) {
            Object value = seaTunnelRow.getField(index);
            ColumnVector vector = rowBatch.cols[i];
            setColumn(value, vector, row);
            i++;
        }
        try {
            writer.addRowBatch(rowBatch);
            rowBatch.reset();
        } catch (IOException e) {
            String errorMsg = String.format("Write data to orc file [%s] error", filePath);
            throw new RuntimeException(errorMsg, e);
        }
    }

    @Override
    public void finishAndCloseFile() {
        this.beingWrittenWriter.forEach((k, v) -> {
            try {
                v.close();
            } catch (IOException e) {
                String errorMsg = String.format("Close file [%s] orc writer failed, error msg: [%s]", k, e.getMessage());
                throw new RuntimeException(errorMsg, e);
            }
            needMoveFiles.put(k, getTargetLocation(k));
        });
    }

    private Writer getOrCreateWriter(@NonNull String filePath) {
        Writer writer = this.beingWrittenWriter.get(filePath);
        if (writer == null) {
            TypeDescription schema = buildSchemaWithRowType();
            Path path = new Path(filePath);
            try {
                OrcFile.WriterOptions options = OrcFile.writerOptions(getConfiguration(hadoopConf))
                        .setSchema(schema)
                        // temporarily used snappy
                        .compress(CompressionKind.SNAPPY)
                        // use orc version 0.12
                        .version(OrcFile.Version.V_0_12)
                        .overwrite(true);
                Writer newWriter = OrcFile.createWriter(path, options);
                this.beingWrittenWriter.put(filePath, newWriter);
                return newWriter;
            } catch (IOException e) {
                String errorMsg = String.format("Get orc writer for file [%s] error", filePath);
                throw new RuntimeException(errorMsg, e);
            }
        }
        return writer;
    }

    private TypeDescription buildFieldWithRowType(SeaTunnelDataType<?> type) {
        if (BasicType.BOOLEAN_TYPE.equals(type)) {
            return TypeDescription.createBoolean();
        }
        if (BasicType.SHORT_TYPE.equals(type)) {
            return TypeDescription.createShort();
        }
        if (BasicType.INT_TYPE.equals(type)) {
            return TypeDescription.createInt();
        }
        if (BasicType.LONG_TYPE.equals(type)) {
            return TypeDescription.createLong();
        }
        if (BasicType.FLOAT_TYPE.equals(type)) {
            return TypeDescription.createFloat();
        }
        if (BasicType.DOUBLE_TYPE.equals(type)) {
            return TypeDescription.createDouble();
        }
        if (BasicType.BYTE_TYPE.equals(type)) {
            return TypeDescription.createByte();
        }
        return TypeDescription.createString();
    }

    private TypeDescription buildSchemaWithRowType() {
        TypeDescription schema = TypeDescription.createStruct();
        for (Integer i : sinkColumnsIndexInRow) {
            TypeDescription fieldType = buildFieldWithRowType(seaTunnelRowType.getFieldType(i));
            schema.addField(seaTunnelRowType.getFieldName(i), fieldType);
        }
        return schema;
    }

    private void setColumn(Object value, ColumnVector vector, int row) {
        if (value == null) {
            vector.isNull[row] = true;
            vector.noNulls = false;
        } else {
            switch (vector.type) {
                case LONG:
                    LongColumnVector longVector = (LongColumnVector) vector;
                    setLongColumnVector(value, longVector, row);
                    break;
                case DOUBLE:
                    DoubleColumnVector doubleColumnVector = (DoubleColumnVector) vector;
                    setDoubleVector(value, doubleColumnVector, row);
                    break;
                case BYTES:
                    BytesColumnVector bytesColumnVector = (BytesColumnVector) vector;
                    setByteColumnVector(value, bytesColumnVector, row);
                    break;
                default:
                    throw new RuntimeException("Unexpected ColumnVector subtype");
            }
        }
    }

    private void setLongColumnVector(Object value, LongColumnVector longVector, int row) {
        if (value instanceof Boolean) {
            Boolean bool = (Boolean) value;
            longVector.vector[row] = (bool.equals(Boolean.TRUE)) ? Long.valueOf(1) : Long.valueOf(0);
        }  else if (value instanceof Integer) {
            longVector.vector[row] = ((Integer) value).longValue();
        } else if (value instanceof Long) {
            longVector.vector[row] = (Long) value;
        } else if (value instanceof BigInteger) {
            BigInteger bigInt = (BigInteger) value;
            longVector.vector[row] = bigInt.longValue();
        } else {
            throw new RuntimeException("Long or Integer type expected for field");
        }
    }

    private void setByteColumnVector(Object value, BytesColumnVector bytesColVector, int rowNum) {
        if (value instanceof byte[] || value instanceof String) {
            byte[] byteVec;
            if (value instanceof String) {
                String strVal = (String) value;
                byteVec = strVal.getBytes(StandardCharsets.UTF_8);
            } else {
                byteVec = (byte[]) value;
            }
            bytesColVector.setRef(rowNum, byteVec, 0, byteVec.length);
        } else {
            throw new RuntimeException("byte[] or String type expected for field ");
        }
    }

    private void setDoubleVector(Object value, DoubleColumnVector doubleVector, int rowNum) {
        if (value instanceof Double) {
            doubleVector.vector[rowNum] = (Double) value;
        } else if (value instanceof Float) {
            Float floatValue = (Float) value;
            doubleVector.vector[rowNum] = floatValue.doubleValue();
        } else {
            throw new RuntimeException("Double or Float type expected for field ");
        }
    }
}
