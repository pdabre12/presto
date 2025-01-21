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

#pragma once

#include "presto_cpp/presto_protocol/connector/tpcds/presto_protocol_tpcds.h"
#include "presto_cpp/presto_protocol/core/ConnectorProtocol.h"

namespace facebook::presto::protocol::tpcds {

using TpcdsConnectorProtocol = ConnectorProtocolTemplate<
    TpcdsTableHandle,
    TpcdsTableLayoutHandle,
    TpcdsColumnHandle,
    NotImplemented,
    NotImplemented,
    TpcdsSplit,
    TpcdsPartitioningHandle,
    TpcdsTransactionHandle,
    NotImplemented>;

} // namespace facebook::presto::protocol::tpcds
