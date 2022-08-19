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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.TestIcebergParquetConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.assertj.core.util.Files;
import org.testng.SkipException;

import java.io.File;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoRestCatalogConnectorTest
        extends TestIcebergParquetConnectorTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_COMMENT_ON_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_RENAME_MATERIALIZED_VIEW:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File warehouseLocation = Files.newTemporaryFolder();
        warehouseLocation.deleteOnExit();

        Catalog backend = RestCatalogTestUtils.backendCatalog(warehouseLocation);

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog
                .builder().delegate(backend).build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", "PARQUET")
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest.uri", testServer.getBaseUrl().toString())
                                .buildOrThrow())
                .setInitialTables(ImmutableList.<TpchTable<?>>builder()
                        .addAll(REQUIRED_TPCH_TABLES)
                        .add(LINE_ITEM)
                        .build())
                .build();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        // 4096 (configured limit) - 28 (additional row metadata)
        return OptionalInt.of(4096 - 28);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Failed to execute");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // This value depends on metastore type
        // The connector appends uuids to the end of all table names
        // 33 is the length of random suffix. e.g. {table name}-142763c594d54e4b9329a98f90528caf
        return OptionalInt.of(255 - 33);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*Failed to create.*|.*Failed to execute.*");
    }

    @Override
    protected int maxTableRenameLength()
    {
        // 4096 (configured limit) - 179 (additional row metadata) - 33 (length of random table suffix)
        return 4096 - 179 - 33;
    }

    @Override
    public void testAddColumnConcurrently()
    {
        throw new SkipException("Concurrent commit not supported");
    }

    @Override
    public void testCreateTableAsSelectSchemaNotFound()
    {
        throw new SkipException("Backing catalog creates schema automatically.");
    }

    @Override
    public void testCreateTableSchemaNotFound()
    {
        throw new SkipException("Backing catalog creates schema automatically.");
    }

    @Override
    public void testShowCreateSchema()
    {
        // Overridden due to REST catalog not supporting namespace principal
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg.tpch\n" +
                        "WITH \\(\n" +
                        "\\s+location = '.*/iceberg_data/tpch'\n" +
                        "\\)");
    }

    @Override
    protected void verifyIcebergTableProperties(MaterializedResult actual)
    {
        assertThat(actual)
                .anySatisfy(row -> assertThat(row).isEqualTo(new MaterializedRow(DEFAULT_PRECISION, "write.format.default", "PARQUET")))
                .anySatisfy(row -> assertThat(row.getFields()).contains("created-at"));
    }
}
