/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.type;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TypeFactoryTest {
  private static final JavaTypeFactory TYPE_FACTORY = new TestJavaTypeFactoryImpl();

  @DataProvider(name = "relDataTypeConversion")
  public Iterator<Object[]> relDataTypeConversion() {
    ArrayList<Object[]> cases = new ArrayList<>();

    for (FieldSpec.DataType dataType : FieldSpec.DataType.values()) {
      RelDataType basicType;
      switch (dataType) {
        case INT: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
          break;
        }
        case LONG: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
          break;
        }
        case FLOAT: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.REAL);
          break;
        }
        case DOUBLE: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE);
          break;
        }
        case BOOLEAN: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
          break;
        }
        case TIMESTAMP: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
          break;
        }
        case STRING:
        case JSON: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
          break;
        }
        case BYTES: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.VARBINARY);
          break;
        }
        case BIG_DECIMAL: {
          basicType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL);
          break;
        }
        case LIST:
        case STRUCT:
        case MAP:
        case UNKNOWN:
          continue;
        default:
          String message = String.format("Unsupported type: %s ", dataType);
          throw new UnsupportedOperationException(message);
      }
      cases.add(new Object[]{dataType, basicType, true});
      cases.add(new Object[]{dataType, basicType, false});
    }
    return cases.iterator();
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testScalarTypes(FieldSpec.DataType dataType, RelDataType scalarType, boolean columnNullMode) {
    TypeFactory typeFactory = new TypeFactory();
    Schema testSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("col", dataType)
        .setEnableColumnBasedNullHandling(columnNullMode)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);
    boolean colNullable = isColNullable(testSchema);
    Assert.assertEquals(field.getType(), typeFactory.createTypeWithNullability(scalarType, colNullable));
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testNullableScalarTypes(FieldSpec.DataType dataType, RelDataType scalarType, boolean columnNullMode) {
    TypeFactory typeFactory = new TypeFactory();
    Schema testSchema = new Schema.SchemaBuilder()
        .addDimensionField("col", dataType, field -> field.setNullable(true))
        .setEnableColumnBasedNullHandling(columnNullMode)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    boolean colNullable = isColNullable(testSchema);
    RelDataType expectedType = typeFactory.createTypeWithNullability(scalarType, colNullable);

    Assert.assertEquals(field.getType(), expectedType);
  }


  @Test(dataProvider = "relDataTypeConversion")
  public void testNotNullableScalarTypes(FieldSpec.DataType dataType, RelDataType scalarType, boolean columnNullMode) {
    TypeFactory typeFactory = new TypeFactory();
    Schema testSchema = new Schema.SchemaBuilder()
        .addDimensionField("col", dataType, field -> field.setNullable(false))
        .setEnableColumnBasedNullHandling(columnNullMode)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    Assert.assertEquals(field.getType(), scalarType);
  }

  private boolean isColNullable(Schema schema) {
    return schema.isEnableColumnBasedNullHandling() && schema.getFieldSpecFor("col").isNullable();
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testArrayTypes(FieldSpec.DataType dataType, RelDataType arrayType, boolean columnNullMode) {
    TypeFactory typeFactory = new TypeFactory();
    Schema testSchema = new Schema.SchemaBuilder()
        .addMultiValueDimension("col", dataType)
        .setEnableColumnBasedNullHandling(columnNullMode)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    boolean nullable = isColNullable(testSchema);
    RelDataType expectedType =
        typeFactory.createTypeWithNullability(typeFactory.createArrayType(arrayType, -1), nullable);

    Assert.assertEquals(field.getType(), expectedType);
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testNullableArrayTypes(FieldSpec.DataType dataType, RelDataType arrayType, boolean columnNullMode) {
    TypeFactory typeFactory = new TypeFactory();
    Schema testSchema = new Schema.SchemaBuilder()
        .addDimensionField("col", dataType, field -> {
          field.setNullable(true);
          field.setSingleValueField(false);
        })
        .setEnableColumnBasedNullHandling(columnNullMode)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    boolean colNullable = isColNullable(testSchema);
    RelDataType expectedType =
        typeFactory.createTypeWithNullability(typeFactory.createArrayType(arrayType, -1), colNullable);

    Assert.assertEquals(field.getType(), expectedType);
  }

  @Test(dataProvider = "relDataTypeConversion")
  public void testNotNullableArrayTypes(FieldSpec.DataType dataType, RelDataType arrayType, boolean columnNullMode) {
    TypeFactory typeFactory = new TypeFactory();
    Schema testSchema = new Schema.SchemaBuilder()
        .addDimensionField("col", dataType, field -> {
          field.setNullable(false);
          field.setSingleValueField(false);
        })
        .setEnableColumnBasedNullHandling(columnNullMode)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    RelDataTypeField field = fieldList.get(0);

    RelDataType expectedType = typeFactory.createArrayType(arrayType, -1);

    Assert.assertEquals(field.getType(), expectedType);
  }

  @Test
  public void testRelDataTypeConversion() {
    TypeFactory typeFactory = new TypeFactory();
    Schema testSchema = new Schema.SchemaBuilder().addSingleValueDimension("INT_COL", FieldSpec.DataType.INT)
        .addSingleValueDimension("LONG_COL", FieldSpec.DataType.LONG)
        .addSingleValueDimension("FLOAT_COL", FieldSpec.DataType.FLOAT)
        .addSingleValueDimension("DOUBLE_COL", FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension("STRING_COL", FieldSpec.DataType.STRING)
        .addSingleValueDimension("BYTES_COL", FieldSpec.DataType.BYTES)
        .addSingleValueDimension("JSON_COL", FieldSpec.DataType.JSON)
        .addMultiValueDimension("INT_ARRAY_COL", FieldSpec.DataType.INT)
        .addMultiValueDimension("LONG_ARRAY_COL", FieldSpec.DataType.LONG)
        .addMultiValueDimension("FLOAT_ARRAY_COL", FieldSpec.DataType.FLOAT)
        .addMultiValueDimension("DOUBLE_ARRAY_COL", FieldSpec.DataType.DOUBLE)
        .addMultiValueDimension("STRING_ARRAY_COL", FieldSpec.DataType.STRING)
        .addMultiValueDimension("BYTES_ARRAY_COL", FieldSpec.DataType.BYTES)
        .build();
    RelDataType relDataTypeFromSchema = typeFactory.createRelDataTypeFromSchema(testSchema);
    List<RelDataTypeField> fieldList = relDataTypeFromSchema.getFieldList();
    for (RelDataTypeField field : fieldList) {
      switch (field.getName()) {
        case "INT_COL":
          BasicSqlType intBasicSqlType = new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.INTEGER);
          Assert.assertEquals(field.getType(), intBasicSqlType);
          checkPrecisionScale(field, intBasicSqlType);
          break;
        case "LONG_COL":
          BasicSqlType bigIntBasicSqlType = new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.BIGINT);
          Assert.assertEquals(field.getType(), bigIntBasicSqlType);
          checkPrecisionScale(field, bigIntBasicSqlType);
          break;
        case "FLOAT_COL":
          BasicSqlType realBasicSqlType = new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.REAL);
          Assert.assertEquals(field.getType(), realBasicSqlType);
          checkPrecisionScale(field, realBasicSqlType);
          break;
        case "DOUBLE_COL":
          BasicSqlType doubleBasicSqlType = new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.DOUBLE);
          Assert.assertEquals(field.getType(), doubleBasicSqlType);
          checkPrecisionScale(field, doubleBasicSqlType);
          break;
        case "STRING_COL":
        case "JSON_COL":
          Assert.assertEquals(field.getType(),
              TYPE_FACTORY.createTypeWithCharsetAndCollation(new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.VARCHAR),
                  StandardCharsets.UTF_8, SqlCollation.IMPLICIT));
          break;
        case "BYTES_COL":
          Assert.assertEquals(field.getType(), new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.VARBINARY));
          break;
        case "INT_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.INTEGER), false));
          break;
        case "LONG_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.BIGINT), false));
          break;
        case "FLOAT_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.REAL), false));
          break;
        case "DOUBLE_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.DOUBLE), false));
          break;
        case "STRING_ARRAY_COL":
          Assert.assertEquals(field.getType(), new ArraySqlType(
              TYPE_FACTORY.createTypeWithCharsetAndCollation(new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.VARCHAR),
                  StandardCharsets.UTF_8, SqlCollation.IMPLICIT), false));
          break;
        case "BYTES_ARRAY_COL":
          Assert.assertEquals(field.getType(),
              new ArraySqlType(new BasicSqlType(TypeSystem.INSTANCE, SqlTypeName.VARBINARY), false));
          break;
        default:
          Assert.fail("Unexpected column name: " + field.getName());
          break;
      }
    }
  }

  private static class TestJavaTypeFactoryImpl extends JavaTypeFactoryImpl {
    public TestJavaTypeFactoryImpl() {
      super(TypeSystem.INSTANCE);
    }

    @Override
    public Charset getDefaultCharset() {
      return StandardCharsets.UTF_8;
    }
  }

  //tests precision and scale for numeric data type
  private void checkPrecisionScale(RelDataTypeField field, BasicSqlType basicSqlType) {
    Assert.assertEquals(field.getValue().getPrecision(), basicSqlType.getPrecision());
    Assert.assertEquals(field.getValue().getScale(), basicSqlType.getScale());
  }
}
