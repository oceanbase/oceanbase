
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

#include "conjugate_graph.h"

namespace vsag {

ConjugateGraph::ConjugateGraph() {
    clear();
}

tl::expected<bool, Error>
ConjugateGraph::AddNeighbor(int64_t from_tag_id, int64_t to_tag_id) {
    if (from_tag_id == to_tag_id) {
        return false;
    }
    auto& neighbor_set = conjugate_graph_[from_tag_id];
    auto insert_result = neighbor_set.insert(to_tag_id);
    if (!insert_result.second) {
        return false;
    } else {
        if (neighbor_set.size() == 1) {
            memory_usage_ += sizeof(from_tag_id);
            memory_usage_ += sizeof(neighbor_set.size());
        }
        memory_usage_ += sizeof(to_tag_id);
        return true;
    }
}

const std::unordered_set<int64_t>&
ConjugateGraph::get_neighbors(int64_t from_tag_id) const {
    static const std::unordered_set<int64_t> empty_set;
    auto iter = conjugate_graph_.find(from_tag_id);
    if (iter != conjugate_graph_.end()) {
        return iter->second;
    } else {
        return empty_set;
    }
}

tl::expected<uint32_t, Error>
ConjugateGraph::EnhanceResult(std::priority_queue<std::pair<float, size_t>>& results,
                              const std::function<float(int64_t)>& distance_of_tag) const {
    int64_t k = results.size();
    int64_t look_at_k = std::min(LOOK_AT_K, k);
    std::priority_queue<std::pair<float, size_t>> old_results(results);
    std::vector<int64_t> to_be_visited(look_at_k);
    std::unordered_set<int64_t> visited_set;
    uint32_t successfully_enhanced = 0;
    float distance = 0;

    // initialize visited_set
    for (int j = old_results.size() - 1; j >= 0; j--) {
        visited_set.insert(old_results.top().second);
        if (j < look_at_k) {
            to_be_visited[j] = old_results.top().second;
        }
        old_results.pop();
    }

    // add neighbors in conjugate graph to enhance result
    for (int j = 0; j < look_at_k; j++) {
        const std::unordered_set<int64_t>& neighbors = get_neighbors(to_be_visited[j]);

        for (auto neighbor_tag_id : neighbors) {
            if (not visited_set.insert(neighbor_tag_id).second) {
                continue;
            }
            distance = distance_of_tag(neighbor_tag_id);
            // insert into results
            if (distance < results.top().first) {
                results.emplace(distance, neighbor_tag_id);
                results.pop();
                successfully_enhanced++;
            }
        }
    }

    return successfully_enhanced;
}

size_t
ConjugateGraph::GetMemoryUsage() const {
    return memory_usage_;
}

template <typename T>
static void
read_var_from_stream(std::istream& in_stream, uint32_t* offset, T* dest) {
    in_stream.read((char*)dest, sizeof(T));
    *offset += sizeof(T);

    if (in_stream.fail()) {
        throw std::runtime_error("Failed to read from stream.");
    }
}

void
ConjugateGraph::clear() {
    memory_usage_ = sizeof(memory_usage_) + FOOTER_SIZE;
    footer_.Clear();
}

tl::expected<Binary, Error>
ConjugateGraph::Serialize() const {
    std::stringstream out_ss(std::ios::in | std::ios::out | std::ios::binary);
    auto result = this->Serialize(out_ss);
    if (not result) {
        return tl::unexpected(result.error());
    }

    out_ss.seekg(0, std::ios_base::end);
    size_t size = out_ss.tellg();
    out_ss.seekg(0, std::ios_base::beg);

    std::shared_ptr<int8_t[]> data(new int8_t[size]);
    out_ss.read(reinterpret_cast<char*>(data.get()), size);

    Binary binary{.data = data, .size = size};
    return binary;
}

tl::expected<void, Error>
ConjugateGraph::Serialize(std::ostream& out_stream) const {
    out_stream.write((char*)&memory_usage_, sizeof(memory_usage_));

    for (auto item : conjugate_graph_) {
        auto neighbor_set = item.second;
        size_t neighbor_set_size = neighbor_set.size();

        out_stream.write((char*)&item.first, sizeof(item.first));
        out_stream.write((char*)&neighbor_set_size, sizeof(neighbor_set_size));

        for (auto neighbor_tag_id : neighbor_set) {
            out_stream.write((char*)&neighbor_tag_id, sizeof(neighbor_tag_id));
        }
    }

    footer_.Serialize(out_stream);

    return {};
}

tl::expected<void, Error>
ConjugateGraph::Deserialize(const Binary& binary) {
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    ss.write(reinterpret_cast<const char*>(binary.data.get()), binary.size);
    ss.seekg(0, std::ios::beg);
    return this->Deserialize(ss);
}

tl::expected<void, Error>
ConjugateGraph::Deserialize(std::istream& in_stream) {
    try {
        uint32_t offset = 0;
        uint32_t footer_offset = 0;
        size_t neighbor_size = 0;
        int64_t from_tag_id = 0;
        int64_t to_tag_id = 0;

        conjugate_graph_.clear();

        auto cur_pos = in_stream.tellg();

        read_var_from_stream(in_stream, &offset, &memory_usage_);
        if (memory_usage_ <= FOOTER_SIZE) {
            throw std::runtime_error(
                fmt::format("Incorrect header: memory_usage_({})", memory_usage_));
        }
        footer_offset = memory_usage_ - FOOTER_SIZE;
        in_stream.seekg(cur_pos);
        in_stream.seekg(footer_offset, std::ios::cur);
        footer_.Deserialize(in_stream);

        offset = sizeof(memory_usage_);
        in_stream.seekg(cur_pos);
        in_stream.seekg(offset, std::ios::cur);
        while (offset != memory_usage_ - FOOTER_SIZE) {
            read_var_from_stream(in_stream, &offset, &from_tag_id);
            read_var_from_stream(in_stream, &offset, &neighbor_size);
            for (int i = 0; i < neighbor_size; i++) {
                read_var_from_stream(in_stream, &offset, &to_tag_id);
                conjugate_graph_[from_tag_id].insert(to_tag_id);
            }
        }

        return {};
    } catch (const std::runtime_error& e) {
        clear();
        LOG_ERROR_AND_RETURNS(ErrorType::READ_ERROR, "failed to deserialize: ", e.what());
    }
}

}  // namespace vsag