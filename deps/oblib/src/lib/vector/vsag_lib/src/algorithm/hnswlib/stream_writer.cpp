
// Copyright 2024-present the vsag project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "stream_writer.h"

#include <cstring>

BufferStreamWriter::BufferStreamWriter(char*& buffer) : buffer_(buffer), StreamWriter() {
}

void
BufferStreamWriter::Write(char* data, uint64_t size) {
    memcpy(buffer_, data, size);
    buffer_ += size;
}

IOStreamWriter::IOStreamWriter(std::ostream& ostream) : ostream_(ostream), StreamWriter() {
}

void
IOStreamWriter::Write(char* data, uint64_t size) {
    ostream_.write(data, static_cast<int64_t>(size));
}
