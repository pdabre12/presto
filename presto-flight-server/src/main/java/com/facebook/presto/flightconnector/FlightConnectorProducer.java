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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.plugin.jdbc.JdbcColumnHandle;
import com.facebook.presto.plugin.jdbc.JdbcSplit;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.split.RecordPageSourceProvider;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;

import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FlightConnectorProducer
        extends NoOpFlightProducer
{
    private static final JsonCodec<JdbcSplit> codec = jsonCodec(JdbcSplit.class);
    private final FlightConnectorPluginManager pluginManager;

    @Inject
    public FlightConnectorProducer(FlightConnectorPluginManager pluginManager)
    {
        this.pluginManager = requireNonNull(pluginManager, "pluginManager is null");;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener)
    {
        String ticketString = new String(ticket.getBytes(), StandardCharsets.UTF_8);

        JdbcSplit jdbcSplit = codec.fromJson(ticketString);

        // TODO: need to be in ticket
        //String pluginName = "jmx";
        //String query = "SELECT * FROM java.lang:type=OperatingSystem";

        Connector connector = pluginManager.getConnector(jdbcSplit.getConnectorId());
        if (connector != null) {
            ConnectorRecordSetProvider connectorRecordSetProvider = connector.getRecordSetProvider();

            //RecordPageSourceProvider connectorPageSourceProvider = new RecordPageSourceProvider(connectorRecordSetProvider);
            //ConnectorPageSource source = connectorPageSourceProvider.createPageSource(transactionHandle, SESSION, jdbcSplit, ConnectorTableLayout, );
            // Page page = source.getNextPage();

            // TODO remove
            ColumnHandle columnHandle = new JdbcColumnHandle(
                    jdbcSplit.getConnectorId(),
                    "orderkey",
                    new JdbcTypeHandle(Types.BIGINT, "bigint", 8, 0),
                    BigintType.BIGINT,
                    false,
                    Optional.empty()
            );
            ///////////////

            ConnectorTransactionHandle transactionHandle = connector.beginTransaction(IsolationLevel.READ_COMMITTED, true);

            QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
            Session session = Session.builder(createTestingSessionPropertyManager())
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(new Identity("user", Optional.empty()))
                .setCatalog(jdbcSplit.getCatalogName())
                .setSchema(jdbcSplit.getSchemaName())
                .setTimeZoneKey(DEFAULT_TIME_ZONE_KEY)
                .setLocale(ENGLISH).build();
            ConnectorId connectorId = new ConnectorId(jdbcSplit.getCatalogName());
            ConnectorSession connectorSession = session.toConnectorSession(connectorId);
            RecordSet recordSet = connectorRecordSetProvider.getRecordSet(transactionHandle, connectorSession, jdbcSplit, ImmutableList.of(columnHandle));

            try (RecordCursor cursor = recordSet.cursor()) {

                int stop = 10;

            }

        } else {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Requested connector not loaded: " + jdbcSplit.getConnectorId());
        }
    }
}
