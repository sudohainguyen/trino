/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.connector.system;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import jakarta.validation.constraints.Min;

@DefunctConfig("system-tables.enabled")
public class SystemTablesConfig
{
    private int jdbcSchemaBatchSize = 5;

    @Min(1)
    public int getJdbcSchemaBatchSize()
    {
        return jdbcSchemaBatchSize;
    }

    @Config("system.jdbc.schema-batch-size")
    @ConfigDescription("Number of schemas to process in each batch when listing tables for system.jdbc.tables queries")
    public SystemTablesConfig setJdbcSchemaBatchSize(int jdbcSchemaBatchSize)
    {
        this.jdbcSchemaBatchSize = jdbcSchemaBatchSize;
        return this;
    }
}