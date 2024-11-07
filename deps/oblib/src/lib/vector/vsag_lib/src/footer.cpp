
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

#include "footer.h"

namespace vsag {

SerializationFooter::SerializationFooter() {
    this->Clear();
}

void
SerializationFooter::Clear() {
    json_.clear();
    this->SetMetadata(SERIALIZE_MAGIC_NUM, MAGIC_NUM);
    this->SetMetadata(SERIALIZE_VERSION, VERSION);
}

void
SerializationFooter::SetMetadata(const std::string& key, const std::string& value) {
    std::string old_value;
    auto iter = json_.find(key);
    if (iter != json_.end()) {
        old_value = iter.value();
    }

    json_[key] = value;
    std::string new_json_str = json_.dump();

    if (new_json_str.size() >= FOOTER_SIZE - sizeof(uint32_t)) {
        if (iter != json_.end()) {
            json_[key] = old_value;
        } else {
            json_.erase(key);
        }
        throw std::runtime_error("Serialized footer size exceeds 4KB");
    }
}

std::string
SerializationFooter::GetMetadata(const std::string& key) const {
    auto iter = json_.find(key);
    if (iter != json_.end()) {
        return iter->get<std::string>();
    } else {
        throw std::runtime_error(fmt::format("Footer doesn't contain key ({})", key));
    }
}

void
SerializationFooter::Serialize(std::ostream& out_stream) const {
    std::string serialized_data = json_.dump();
    uint32_t serialized_data_size = serialized_data.size();

    out_stream.write(reinterpret_cast<const char*>(&serialized_data_size),
                     sizeof(serialized_data_size));
    out_stream.write(serialized_data.data(), serialized_data_size);

    size_t padding_size = FOOTER_SIZE - sizeof(uint32_t) - serialized_data_size;

    for (size_t i = 0; i < padding_size; ++i) {
        out_stream.put(0);
    }

    out_stream.flush();
}

void
SerializationFooter::Deserialize(std::istream& in_stream) {
    // read json size
    uint32_t serialized_data_size;
    in_stream.read(reinterpret_cast<char*>(&serialized_data_size), sizeof(serialized_data_size));
    if (serialized_data_size > FOOTER_SIZE - sizeof(uint32_t)) {
        throw std::runtime_error("Serialized footer size exceeds 4KB");
    }

    // read footer
    std::vector<char> buffer(FOOTER_SIZE - sizeof(uint32_t));
    in_stream.read(buffer.data(), FOOTER_SIZE - sizeof(uint32_t));
    if (in_stream.fail()) {
        throw std::runtime_error("Failed to read");
    }

    // parse json
    std::string serialized_data(buffer.begin(), buffer.begin() + FOOTER_SIZE - sizeof(uint32_t));
    json_ = nlohmann::json::parse(serialized_data, nullptr, false);
    if (json_.is_discarded()) {
        throw std::runtime_error("Failed to parse JSON data");
    }

    // check
    std::string magic_num = this->GetMetadata(SERIALIZE_MAGIC_NUM);
    if (magic_num != MAGIC_NUM) {
        throw std::runtime_error(fmt::format("Incorrect footer.MAGIC_NUM: {}", magic_num));
    }

    std::string version = this->GetMetadata(SERIALIZE_VERSION);
    if (version != VERSION) {
        throw std::runtime_error(fmt::format("Incorrect footer.VERSION: {}", version));
    }
}

}  // namespace vsag