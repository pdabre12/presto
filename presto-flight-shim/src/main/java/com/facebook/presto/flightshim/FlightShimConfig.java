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
package com.facebook.presto.flightshim;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

public class FlightShimConfig
{
    private static final int MAX_ROWS_PER_BATCH_DEFAULT = 10000;
    private String server;
    private String flightServerSSLCertificate;
    private boolean arrowFlightServerSslEnabled;
    private Integer arrowFlightPort;
    private int maxRowsPerBatch = MAX_ROWS_PER_BATCH_DEFAULT;

    public String getFlightServerName()
    {
        return server;
    }

    @Config("flight-shim.server")
    public FlightShimConfig setFlightServerName(String server)
    {
        this.server = server;
        return this;
    }

    public Integer getArrowFlightPort()
    {
        return arrowFlightPort;
    }

    @Config("flight-shim.server.port")
    public FlightShimConfig setArrowFlightPort(Integer arrowFlightPort)
    {
        this.arrowFlightPort = arrowFlightPort;
        return this;
    }

    public String getFlightServerSSLCertificate()
    {
        return flightServerSSLCertificate;
    }

    @Config("flight-shim.server-ssl-certificate")
    public FlightShimConfig setFlightServerSSLCertificate(String flightServerSSLCertificate)
    {
        this.flightServerSSLCertificate = flightServerSSLCertificate;
        return this;
    }

    public boolean getArrowFlightServerSslEnabled()
    {
        return arrowFlightServerSslEnabled;
    }

    @Config("flight-shim.server-ssl-enabled")
    public FlightShimConfig setArrowFlightServerSslEnabled(boolean arrowFlightServerSslEnabled)
    {
        this.arrowFlightServerSslEnabled = arrowFlightServerSslEnabled;
        return this;
    }

    public int getMaxRowsPerBatch()
    {
        return maxRowsPerBatch;
    }

    @Config("flight-shim.max-rows-per-batch")
    @Min(1)
    @Max(1000000)
    @ConfigDescription("Sets the maximum number of rows an Arrow record batch will have before sending to the client")
    public FlightShimConfig setMaxRowsPerBatch(int maxRowsPerBatch)
    {
        this.maxRowsPerBatch = maxRowsPerBatch;
        return this;
    }
}
