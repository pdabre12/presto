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

#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>

template <typename T>
void thriftRead(const std::string& data, std::shared_ptr<T>& buffer) {
  auto inBuf = folly::IOBuf::wrapBuffer(data.data(), data.size());
  apache::thrift::BinaryProtocolReader reader;
  reader.setInput(inBuf.get());
  buffer->read(&reader);
}

template <typename T>
std::string thriftWrite(T& data) {
  folly::IOBufQueue outQueue;
  apache::thrift::BinaryProtocolWriter writer;
  writer.setOutput(&outQueue);
  data.write(&writer);
  return outQueue.move()->moveToFbString().toStdString();
}
