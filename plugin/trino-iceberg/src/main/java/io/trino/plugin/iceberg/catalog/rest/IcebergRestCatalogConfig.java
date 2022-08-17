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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

public class IcebergRestCatalogConfig
{
    public enum Security
    {
        NONE,
        OAUTH2,
    }

    private String credential;
    private String token;
    private URI restUri;
    private Security security = Security.NONE;

    public Optional<String> getToken()
    {
        return Optional.ofNullable(token);
    }

    @Config("iceberg.rest.oauth2.token")
    @ConfigDescription("The Bearer token which will be used for interactions with the server")
    @ConfigSecuritySensitive
    public IcebergRestCatalogConfig setToken(String token)
    {
        this.token = token;
        return this;
    }

    public Optional<String> getCredential()
    {
        return Optional.ofNullable(credential);
    }

    @Config("iceberg.rest.oauth2.credential")
    @ConfigDescription("The credential to exchange for a token in the OAuth2 client credentials flow with the server")
    @ConfigSecuritySensitive
    public IcebergRestCatalogConfig setCredential(String credential)
    {
        this.credential = credential;
        return this;
    }

    @NotEmpty(message = "iceberg.rest.uri cannot be empty")
    public URI getBaseUri()
    {
        return this.restUri;
    }

    @Config("iceberg.rest.uri")
    @ConfigDescription("The URI to the REST server")
    public IcebergRestCatalogConfig setBaseUri(String uri)
    {
        if (uri == null) {
            this.restUri = null;
        }
        else {
            this.restUri = URI.create(uri);
        }
        return this;
    }

    @NotNull
    public Security getSecurity()
    {
        return security;
    }

    @Config("iceberg.rest.security")
    public IcebergRestCatalogConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }
}
