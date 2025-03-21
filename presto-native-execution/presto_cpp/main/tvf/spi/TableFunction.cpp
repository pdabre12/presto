/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
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

#include "presto_cpp/main/tvf/spi/TableFunction.h"

#include "velox/expression/FunctionSignature.h"

namespace facebook::presto::tvf {

using namespace facebook::velox;

TableFunctionMap& tableFunctions() {
  static TableFunctionMap functions;
  return functions;
}

namespace {
std::optional<const TableFunctionEntry*> getTableFunctionEntry(
    const std::string& name) {
  auto& functionsMap = tableFunctions();
  auto it = functionsMap.find(name);
  if (it != functionsMap.end()) {
    return &it->second;
  }

  return std::nullopt;
}
} // namespace

bool registerTableFunction(
    const std::string& name,
    TableFunction::Metadata metadata,
    TableFunctionAnalyzer analyzer,
    TableFunctionFactory factory) {
  auto sanitizedName = exec::sanitizeName(name);
  tableFunctions()[sanitizedName] = {
      std::move(analyzer), std::move(factory), std::move(metadata)};
  return true;
}

TableFunction::Metadata getTableFunctionMetadata(const std::string& name) {
  const auto sanitizedName = exec::sanitizeName(name);
  if (auto func = getTableFunctionEntry(sanitizedName)) {
    return func.value()->metadata;
  } else {
    VELOX_USER_FAIL("Table function metadata not found for function: {}", name);
  }
}

std::unique_ptr<TableFunction> TableFunction::create(
    const std::string& name,
    const std::shared_ptr<const TableFunctionHandle>& handle,
    memory::MemoryPool* pool,
    HashStringAllocator* stringAllocator,
    const core::QueryConfig& config) {
  // Lookup the function in the new registry first.
  if (auto func = getTableFunctionEntry(name)) {
    return func.value()->factory(handle, pool, stringAllocator, config);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

std::unique_ptr<TableFunctionAnalysis> TableFunction::analyze(
    const std::string& name,
    const std::unordered_map<std::string, std::shared_ptr<Argument>>& args,
    const core::QueryConfig& config) {
  // Lookup the function in the new registry first.
  if (auto func = getTableFunctionEntry(name)) {
    return func.value()->analyzer(args, config);
  }

  VELOX_USER_FAIL("Table function not registered: {}", name);
}

} // namespace facebook::presto::tvf
