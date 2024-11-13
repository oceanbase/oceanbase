
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

#pragma once
#include "fmt/format.h"
#include "nlohmann/json.hpp"
#include "vsag/constants.h"

namespace vsag {

static const std::string MAGIC_NUM = "43475048";  // means "CGPH"
static const std::string VERSION = "1";
static const int FOOTER_SIZE = 4096;  // 4KB

class SerializationFooter {
public:
    SerializationFooter();

    void
    Clear();

    void
    SetMetadata(const std::string& key, const std::string& value);

    std::string
    GetMetadata(const std::string& key) const;

    void
    Serialize(std::ostream& out_stream) const;

    void
    Deserialize(std::istream& in_stream);

private:
    nlohmann::json json_;
};

}  // namespace vsag
