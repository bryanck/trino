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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.hdfs.ConfigurationUtils;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import javax.inject.Inject;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TrinoIcebergRestCatalogFactory
        implements TrinoCatalogFactory
{
    private static final Logger log = Logger.get(TrinoIcebergRestCatalogFactory.class);

    private final CatalogName catalogName;
    private final String trinoVersion;
    private final URI serverUri;
    private final Optional<String> credential;
    private final Optional<String> token;

    private volatile RESTSessionCatalog icebergCatalog;

    @Inject
    public TrinoIcebergRestCatalogFactory(
            CatalogName catalogName,
            IcebergRestCatalogConfig restConfig,
            NodeVersion nodeVersion)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(restConfig, "restConfig is null");
        this.serverUri = restConfig.getBaseUri();
        this.credential = restConfig.getCredential();
        this.token = restConfig.getToken();
    }

    @Override
    public TrinoCatalog create(ConnectorIdentity identity)
    {
        if (icebergCatalog == null) {
            synchronized (this) {
                if (icebergCatalog == null) {
                    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
                    properties.put(CatalogProperties.URI, serverUri.toString());
                    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
                    properties.put("trino-version", trinoVersion);
                    credential.ifPresent(v -> properties.put(OAuth2Properties.CREDENTIAL, v));
                    token.ifPresent(v -> properties.put(OAuth2Properties.TOKEN, v));

                    icebergCatalog = new RESTSessionCatalog();
                    icebergCatalog.setConf(ConfigurationUtils.getInitialConfiguration());
                    try {
                        icebergCatalog.initialize(catalogName.toString(), properties.buildOrThrow());
                    }
                    catch (Exception e) {
                        icebergCatalog = null;
                        log.error("REST session catalog initialization failed. Reason: %s", e.getMessage());
                        throw e;
                    }
                }
            }
        }

        return new TrinoRestCatalog(icebergCatalog, catalogName, trinoVersion);
    }
}
