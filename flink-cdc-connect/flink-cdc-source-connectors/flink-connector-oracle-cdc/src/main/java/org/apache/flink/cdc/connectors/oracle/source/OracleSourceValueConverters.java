/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oracle.source;

import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.Interval;
import io.debezium.time.MicroDuration;
import io.debezium.time.ZonedTimestamp;
import oracle.jdbc.OracleTypes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.sql.Types;

/** Oracle Value Converters for cdc. */
public class OracleSourceValueConverters extends OracleValueConverters {

    private final OracleConnectorConfig.IntervalHandlingMode intervalHandlingMode;

    public OracleSourceValueConverters(OracleConnectorConfig config, OracleConnection connection) {
        super(config, connection);
        this.intervalHandlingMode = config.getIntervalHandlingMode();
    }

    private SchemaBuilder getNumericSchema(Column column) {
        if (column.scale().isPresent()) {
            // return sufficiently sized int schema for non-floating point types
            Integer scale = column.scale().get();

            // a negative scale means rounding, e.g. NUMBER(10, -2) would be rounded to hundreds
            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return SchemaBuilder.int8();
                } else if (width < 5) {
                    return SchemaBuilder.int16();
                } else if (width < 10) {
                    return SchemaBuilder.int32();
                } else if (width < 19) {
                    return SchemaBuilder.int64();
                }
            }

            // larger non-floating point types and floating point types use Decimal
            return super.schemaBuilder(column);
        } else {
            return variableScaleSchema(column);
        }
    }

    private SchemaBuilder variableScaleSchema(Column column) {
        if (decimalMode == DecimalMode.PRECISE) {
            return VariableScaleDecimal.builder();
        }
        return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().orElse(-1));
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        logger.debug(
                "Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale());

        switch (column.jdbcType()) {
                // old:Oracle's float is not float as in Java but a NUMERIC without scale
                // new:Oracle's float type value use float type schema,because NUMERIC without scale
                // is not suitable When the float type has a scale.
            case Types.FLOAT:
            case OracleTypes.BINARY_FLOAT:
                return SchemaBuilder.float32();
            case Types.NUMERIC:
                return getNumericSchema(column);
            case OracleTypes.BINARY_DOUBLE:
                return SchemaBuilder.float64();
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return ZonedTimestamp.builder();
            case OracleTypes.INTERVALYM:
            case OracleTypes.INTERVALDS:
                return intervalHandlingMode == OracleConnectorConfig.IntervalHandlingMode.STRING
                        ? Interval.builder()
                        : MicroDuration.builder();
            case Types.STRUCT:
                return SchemaBuilder.string();
            case OracleTypes.ROWID:
                return SchemaBuilder.string();
            default:
                {
                    SchemaBuilder builder = super.schemaBuilder(column);
                    logger.debug(
                            "JdbcValueConverters returned '{}' for column '{}'",
                            builder != null ? builder.getClass().getName() : null,
                            column.name());
                    return builder;
                }
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.STRUCT:
            case Types.CLOB:
            case OracleTypes.ROWID:
                return data -> convertString(column, fieldDefn, data);
            case Types.BLOB:
                return data -> convertBinary(column, fieldDefn, data, binaryMode);
            case OracleTypes.BINARY_FLOAT:
            case Types.FLOAT:
                // When the value instance of float,convert it to float type.
                return data -> convertFloat(column, fieldDefn, data);
            case OracleTypes.BINARY_DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.NUMERIC:
                return getNumericConverter(column, fieldDefn);
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data);
            case OracleTypes.INTERVALYM:
                return (data) -> convertIntervalYearMonth(column, fieldDefn, data);
            case OracleTypes.INTERVALDS:
                return (data) -> convertIntervalDaySecond(column, fieldDefn, data);
            case OracleTypes.RAW:
                // Raw data types are not supported
                return null;
        }

        return super.converter(column, fieldDefn);
    }

    private ValueConverter getNumericConverter(Column column, Field fieldDefn) {
        if (column.scale().isPresent()) {
            Integer scale = column.scale().get();

            if (scale <= 0) {
                int width = column.length() - scale;
                if (width < 3) {
                    return data -> convertNumericAsTinyInt(column, fieldDefn, data);
                } else if (width < 5) {
                    return data -> convertNumericAsSmallInt(column, fieldDefn, data);
                } else if (width < 10) {
                    return data -> convertNumericAsInteger(column, fieldDefn, data);
                } else if (width < 19) {
                    return data -> convertNumericAsBigInteger(column, fieldDefn, data);
                }
            }

            // larger non-floating point types and floating point types use Decimal
            return data -> convertNumeric(column, fieldDefn, data);
        } else {
            return data -> convertVariableScale(column, fieldDefn, data);
        }
    }
}
