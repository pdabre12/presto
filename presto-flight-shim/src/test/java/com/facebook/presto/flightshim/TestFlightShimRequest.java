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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

public class TestFlightShimRequest
{
    @Test
    public void testJsonRoundTrip()
    {
        FlightShimRequest expected = TestFlightShimProducer.createTpchCustomerRequest();
        String json = TestFlightShimProducer.REQUEST_JSON_CODEC.toJson(expected);
        FlightShimRequest copy = TestFlightShimProducer.REQUEST_JSON_CODEC.fromJson(json);
        assertEquals(copy.getConnectorId(), expected.getConnectorId());
        assertEquals(copy.getSplitBytes(), expected.getSplitBytes());
        assertArrayEquals(copy.getColumnHandlesBytes().toArray(), expected.getColumnHandlesBytes().toArray());
    }
}
