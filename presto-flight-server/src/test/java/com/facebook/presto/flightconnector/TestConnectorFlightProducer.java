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
package com.facebook.presto.flightconnector;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.log.Logger;

import com.facebook.airlift.testing.postgresql.TestingPostgreSqlServer;

import com.facebook.presto.plugin.postgresql.PostgreSqlQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.tpch.TpchTable;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.concurrent.TimeUnit;


public class TestConnectorFlightProducer
        extends AbstractTestQueryFramework
{
    private static final CallOption CALL_OPTIONS = CallOptions.timeout(300, TimeUnit.SECONDS);
    private final TestingPostgreSqlServer postgreSqlServer;
    private RootAllocator allocator;
    private FlightServer server;

    public TestConnectorFlightProducer()
            throws Exception
    {
        this.postgreSqlServer = new TestingPostgreSqlServer("testuser", "tpch");
    }

    @BeforeClass
    public void setup()
            throws Exception
    {
        //File certChainFile = new File("src/test/resources/server.crt");
        //File privateKeyFile = new File("src/test/resources/server.key");

        allocator = new RootAllocator(Long.MAX_VALUE);
        // TODO
        //Location location = Location.forGrpcTls("localhost", findUnusedPort());
        Location location = Location.forGrpcInsecure("localhost", findUnusedPort());

        server = FlightConnectorServer.start(FlightConnectorServer.builder(allocator, location));
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ImmutableMap.of(), TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public void close()
            throws InterruptedException, IOException
    {
        if (server != null) {
            server.close();
            allocator.close();
        }
        postgreSqlServer.close();
    }

    private int findUnusedPort()
            throws IOException
    {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    public void testConnectorGetStream() throws Exception
    {
        try (BufferAllocator bufferAllocator = allocator.newChildAllocator("connector-test-client", 0, Long.MAX_VALUE);
                FlightClient client = createFlightClient(bufferAllocator, server.getPort())) {

            String split = "{\n" +
                    "  \"connectorId\" : \"postgresql\",\n" +
                    "  \"schemaName\" : \"tpch\",\n" +
                    "  \"tableName\" : \"orders\",\n" +
                    "  \"tupleDomain\" : {\n" +
                    "    \"columnDomains\" : [ ]\n" +
                    "  }\n" +
                    "}";

            Ticket ticket = new Ticket(split.getBytes(StandardCharsets.UTF_8));

            try (FlightStream stream = client.getStream(ticket, CALL_OPTIONS)) {
                while (stream.next()) {
                    VectorSchemaRoot root = stream.getRoot();
                    int stop = 10;
                }
            }
        }
        catch (Exception e) {
            int stop = 1;
        }
    }

    private static FlightClient createFlightClient(BufferAllocator allocator, int serverPort) throws IOException
    {
        //InputStream trustedCertificate = new ByteArrayInputStream(Files.readAllBytes(Paths.get("src/test/resources/server.crt")));
        //Location location = Location.forGrpcTls("localhost", serverPort);
        //return FlightClient.builder(allocator, location).useTls().trustedCertificates(trustedCertificate).build();
        Location location = Location.forGrpcInsecure("localhost", serverPort);
        return FlightClient.builder(allocator, location).build();
    }
}
