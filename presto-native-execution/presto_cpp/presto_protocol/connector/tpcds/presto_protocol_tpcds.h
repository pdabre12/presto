// DO NOT EDIT : This file is generated by chevron
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

// This file is generated DO NOT EDIT @generated

#include <cstdint>
#include <string>

#include "presto_cpp/external/json/nlohmann/json.hpp"
#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"

namespace facebook::presto::protocol::tpcds {
struct TpcdsTransactionHandle : public ConnectorTransactionHandle {
  String instance = {};
};
void to_json(json& j, const TpcdsTransactionHandle& p);

void from_json(const json& j, TpcdsTransactionHandle& p);
} // namespace facebook::presto::protocol::tpcds
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

// TpcdsColumnHandle is special since it needs an implementation of
// operator<().

namespace facebook::presto::protocol::tpcds {
struct TpcdsColumnHandle : public ColumnHandle {
  String columnName = {};
  Type type = {};

  TpcdsColumnHandle() noexcept;

  bool operator<(const ColumnHandle& o) const override {
    return columnName < dynamic_cast<const TpcdsColumnHandle&>(o).columnName;
  }
};
void to_json(json& j, const TpcdsColumnHandle& p);
void from_json(const json& j, TpcdsColumnHandle& p);
} // namespace facebook::presto::protocol::tpcds
namespace facebook::presto::protocol::tpcds {
struct TpcdsPartitioningHandle : public ConnectorPartitioningHandle {
  String table = {};
  int64_t totalRows = {};

  TpcdsPartitioningHandle() noexcept;
};
void to_json(json& j, const TpcdsPartitioningHandle& p);
void from_json(const json& j, TpcdsPartitioningHandle& p);
} // namespace facebook::presto::protocol::tpcds
namespace facebook::presto::protocol::tpcds {
struct TpcdsTableHandle : public ConnectorTableHandle {
  String tableName = {};
  double scaleFactor = {};

  TpcdsTableHandle() noexcept;
};
void to_json(json& j, const TpcdsTableHandle& p);
void from_json(const json& j, TpcdsTableHandle& p);
} // namespace facebook::presto::protocol::tpcds
namespace facebook::presto::protocol::tpcds {
struct TpcdsSplit : public ConnectorSplit {
  TpcdsTableHandle tableHandle = {};
  int partNumber = {};
  int totalParts = {};
  List<HostAddress> addresses = {};
  bool noSexism = {};

  TpcdsSplit() noexcept;
};
void to_json(json& j, const TpcdsSplit& p);
void from_json(const json& j, TpcdsSplit& p);
} // namespace facebook::presto::protocol::tpcds
namespace facebook::presto::protocol::tpcds {
struct TpcdsTableLayoutHandle : public ConnectorTableLayoutHandle {
  TpcdsTableHandle table = {};
  TupleDomain<std::shared_ptr<ColumnHandle>> predicate = {};

  TpcdsTableLayoutHandle() noexcept;
};
void to_json(json& j, const TpcdsTableLayoutHandle& p);
void from_json(const json& j, TpcdsTableLayoutHandle& p);
} // namespace facebook::presto::protocol::tpcds
