// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <map>
#include <future>

#include "common_includes.h"
#include <vector>
#include "timer.h"
#include "pq_flash_index.h"
#include "cosine_similarity.h"

#ifdef _WINDOWS
#include "windows_aligned_file_reader.h"
#else
#include "local_file_reader.h"
#endif

#define READ_U64(stream, val) stream.read((char *)&val, sizeof(uint64_t))
#define READ_U32(stream, val) stream.read((char *)&val, sizeof(uint32_t))
#define READ_UNSIGNED(stream, val) stream.read((char *)&val, sizeof(unsigned))

// sector # on disk where node_id is present with in the graph part
#define NODE_SECTOR_NO(node_id) (((uint64_t)(node_id)) / nnodes_per_sector + 1)

// obtains region of sector containing node
#define OFFSET_TO_NODE(sector_buf, node_id)                                                                            \
    ((char *)sector_buf + (((uint64_t)node_id) % nnodes_per_sector) * max_node_len)

// returns region of `node_buf` containing [NNBRS][NBR_ID(uint32_t)]
#define OFFSET_TO_NODE_NHOOD(node_buf) (unsigned *)((char *)node_buf + disk_bytes_per_point)

// returns region of `node_buf` containing [COORD(T)]
#define OFFSET_TO_NODE_COORDS(node_buf) (T *)(node_buf)

// sector # beyond the end of graph where data for id is present for reordering
#define VECTOR_SECTOR_NO(id) (((uint64_t)(id)) / nvecs_per_sector + reorder_data_start_sector)

// sector # beyond the end of graph where data for id is present for reordering
#define VECTOR_SECTOR_OFFSET(id) ((((uint64_t)(id)) % nvecs_per_sector) * data_dim * sizeof(float))

namespace diskann
{

template <typename T, typename LabelT>
PQFlashIndex<T, LabelT>::PQFlashIndex(std::shared_ptr<LocalFileReader> &fileReader, diskann::Metric m, size_t sector_len, size_t dim, bool use_bsa)
    : reader(fileReader), metric(m), thread_data(nullptr), sector_len(sector_len), use_bsa(use_bsa), data_dim(dim)
{
    this->dist_cmp_float.reset(new VsagDistanceL2Float(data_dim));
}

template <typename T, typename LabelT> PQFlashIndex<T, LabelT>::~PQFlashIndex()
{
#ifndef EXEC_ENV_OLS
    if (data != nullptr)
    {
        delete[] data;
    }
#endif

    if (tags != nullptr) {
        delete[] tags;
    }

    if (centroid_data != nullptr)
        aligned_free(centroid_data);
    // delete backing bufs for nhood and coord cache
    if (nhood_cache_buf != nullptr)
    {
        delete[] nhood_cache_buf;
        diskann::aligned_free(coord_cache_buf);
    }

    if (load_flag)
    {
        // diskann::cout << "Clearing scratch" << std::endl;
        ScratchStoreManager<SSDThreadData<T>> manager(this->thread_data);
        manager.destroy();
        this->reader->deregister_all_threads();
        reader->close();
    }
    if (_pts_to_label_offsets != nullptr)
    {
        delete[] _pts_to_label_offsets;
    }

    if (_pts_to_labels != nullptr)
    {
        delete[] _pts_to_labels;
    }

    if (medoids != nullptr) {
        delete[] medoids;
    }

}

template <typename T, typename LabelT>
void PQFlashIndex<T, LabelT>::setup_thread_data(uint64_t nthreads, uint64_t visited_reserve)
{
    // diskann::cout << "Setting up thread-specific contexts for nthreads: " << nthreads << std::endl;
// omp parallel for to generate unique thread IDs
    for (int64_t thread = 0; thread < (int64_t)nthreads; thread++)
    {
        {
            SSDThreadData<T> *data = new SSDThreadData<T>(this->max_degree, this->aligned_dim, this->n_chunks);
            this->thread_data.push(data);
        }
    }
    load_flag = true;
}

template <typename T, typename LabelT> void PQFlashIndex<T, LabelT>::load_cache_list(std::vector<uint32_t> &node_list)
{
    diskann::cout << "Loading the cache list into memory.." << std::flush;
    size_t num_cached_nodes = node_list.size();

    // borrow thread data
    ScratchStoreManager<SSDThreadData<T>> manager(this->thread_data);
    auto this_thread_data = manager.scratch_space();


    nhood_cache_buf = new uint32_t[num_cached_nodes * (max_degree + 1)];
    memset(nhood_cache_buf, 0, num_cached_nodes * (max_degree + 1));

    size_t coord_cache_buf_len = num_cached_nodes * aligned_dim;
    diskann::alloc_aligned((void **)&coord_cache_buf, coord_cache_buf_len * sizeof(T), 8 * sizeof(T));
    memset(coord_cache_buf, 0, coord_cache_buf_len * sizeof(T));

    size_t BLOCK_SIZE = 8;
    size_t num_blocks = DIV_ROUND_UP(num_cached_nodes, BLOCK_SIZE);

    for (size_t block = 0; block < num_blocks; block++)
    {
        size_t start_idx = block * BLOCK_SIZE;
        size_t end_idx = (std::min)(num_cached_nodes, (block + 1) * BLOCK_SIZE);
        std::vector<AlignedRead> read_reqs;
        std::vector<std::pair<uint32_t, char *>> nhoods;
        for (size_t node_idx = start_idx; node_idx < end_idx; node_idx++)
        {
            AlignedRead read;
            char *buf = nullptr;
            alloc_aligned((void **)&buf, SECTOR_LEN, SECTOR_LEN);
            nhoods.push_back(std::make_pair(node_list[node_idx], buf));
            read.len = SECTOR_LEN;
            read.buf = buf;
            read.offset = NODE_SECTOR_NO(node_list[node_idx]) * SECTOR_LEN;
            read_reqs.push_back(read);
        }

        reader->read(read_reqs);

        size_t node_idx = start_idx;
        for (uint32_t i = 0; i < read_reqs.size(); i++)
        {
            auto &nhood = nhoods[i];
            char *node_buf = OFFSET_TO_NODE(nhood.second, nhood.first);
            T *node_coords = OFFSET_TO_NODE_COORDS(node_buf);
            T *cached_coords = coord_cache_buf + node_idx * aligned_dim;
            memcpy(cached_coords, node_coords, disk_bytes_per_point);
            coord_cache.insert(std::make_pair(nhood.first, cached_coords));

            // insert node nhood into nhood_cache
            uint32_t *node_nhood = OFFSET_TO_NODE_NHOOD(node_buf);

            auto nnbrs = *node_nhood;
            uint32_t *nbrs = node_nhood + 1;
            std::pair<uint32_t, uint32_t *> cnhood;
            cnhood.first = nnbrs;
            cnhood.second = nhood_cache_buf + node_idx * (max_degree + 1);
            memcpy(cnhood.second, nbrs, nnbrs * sizeof(uint32_t));
            nhood_cache.insert(std::make_pair(nhood.first, cnhood));
            aligned_free(nhood.second);
            node_idx++;
        }
    }
    diskann::cout << "..done." << std::endl;
}

#ifdef EXEC_ENV_OLS
template <typename T, typename LabelT>
void PQFlashIndex<T, LabelT>::generate_cache_list_from_sample_queries(MemoryMappedFiles &files, std::string sample_bin,
                                                                      uint64_t l_search, uint64_t beamwidth,
                                                                      uint64_t num_nodes_to_cache, uint32_t nthreads,
                                                                      std::vector<uint32_t> &node_list)
{
#else
template <typename T, typename LabelT>
void PQFlashIndex<T, LabelT>::generate_cache_list_from_sample_queries(std::string sample_bin, uint64_t l_search,
                                                                      uint64_t beamwidth, uint64_t num_nodes_to_cache,
                                                                      uint32_t nthreads,
                                                                      std::vector<uint32_t> &node_list)
{
#endif
    if (num_nodes_to_cache >= this->num_points)
    {
        // for small num_points and big num_nodes_to_cache, use below way to get the node_list quickly
        node_list.resize(this->num_points);
        for (uint32_t i = 0; i < this->num_points; ++i)
        {
            node_list[i] = i;
        }
        return;
    }

    this->count_visited_nodes = true;
    this->node_visit_counter.clear();
    this->node_visit_counter.resize(this->num_points);
    for (uint32_t i = 0; i < node_visit_counter.size(); i++)
    {
        this->node_visit_counter[i].first = i;
        this->node_visit_counter[i].second = 0;
    }

    uint64_t sample_num, sample_dim, sample_aligned_dim;
    T *samples;

#ifdef EXEC_ENV_OLS
    if (files.fileExists(sample_bin))
    {
        diskann::load_aligned_bin<T>(files, sample_bin, samples, sample_num, sample_dim, sample_aligned_dim);
    }
#else
    if (file_exists(sample_bin))
    {
        diskann::load_aligned_bin<T>(sample_bin, samples, sample_num, sample_dim, sample_aligned_dim);
    }
#endif
    else
    {
        diskann::cerr << "Sample bin file not found. Not generating cache." << std::endl;
        return;
    }

    std::vector<uint64_t> tmp_result_ids_64(sample_num, 0);
    std::vector<float> tmp_result_dists(sample_num, 0);


#pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
    for (int64_t i = 0; i < (int64_t)sample_num; i++)
    {
        // run a search on the sample query with a random label (sampled from base label distribution), and it will
        // concurrently update the node_visit_counter to track most visited nodes. The last false is to not use the
        // "use_reorder_data" option which enables a final reranking if the disk index itself contains only PQ data.
        cached_beam_search(samples + (i * sample_aligned_dim), 1, l_search, tmp_result_ids_64.data() + i,
                           tmp_result_dists.data() + i, beamwidth, nullptr, std::numeric_limits<uint32_t>::max(), false);
    }

    std::sort(this->node_visit_counter.begin(), node_visit_counter.end(),
              [](std::pair<uint32_t, uint32_t> &left, std::pair<uint32_t, uint32_t> &right) {
                  return left.second > right.second;
              });
    node_list.clear();
    node_list.shrink_to_fit();
    num_nodes_to_cache = std::min(num_nodes_to_cache, this->node_visit_counter.size());
    node_list.reserve(num_nodes_to_cache);
    for (uint64_t i = 0; i < num_nodes_to_cache; i++)
    {
        node_list.push_back(this->node_visit_counter[i].first);
    }
    this->count_visited_nodes = false;

    diskann::aligned_free(samples);
}

template <typename T, typename LabelT>
void PQFlashIndex<T, LabelT>::cache_bfs_levels(uint64_t num_nodes_to_cache, std::vector<uint32_t> &node_list,
                                               const bool shuffle)
{
    std::random_device rng;
    std::mt19937 urng(rng());

    tsl::robin_set<uint32_t> node_set;

    // Do not cache more than 10% of the nodes in the index
    uint64_t tenp_nodes = (uint64_t)(std::round(this->num_points * 0.1));
    if (num_nodes_to_cache > tenp_nodes)
    {
        diskann::cout << "Reducing nodes to cache from: " << num_nodes_to_cache << " to: " << tenp_nodes
                      << "(10 percent of total nodes:" << this->num_points << ")" << std::endl;
        num_nodes_to_cache = tenp_nodes == 0 ? 1 : tenp_nodes;
    }
    diskann::cout << "Caching " << num_nodes_to_cache << "..." << std::endl;

    // borrow thread data
    ScratchStoreManager<SSDThreadData<T>> manager(this->thread_data);
    auto this_thread_data = manager.scratch_space();

    std::unique_ptr<tsl::robin_set<uint32_t>> cur_level, prev_level;
    cur_level = std::make_unique<tsl::robin_set<uint32_t>>();
    prev_level = std::make_unique<tsl::robin_set<uint32_t>>();

    for (uint64_t miter = 0; miter < num_medoids && cur_level->size() < num_nodes_to_cache; miter++)
    {
        cur_level->insert(medoids[miter]);
    }

    if ((_filter_to_medoid_ids.size() > 0) && (cur_level->size() < num_nodes_to_cache))
    {
        for (auto &x : _filter_to_medoid_ids)
        {
            for (auto &y : x.second)
            {
                cur_level->insert(y);
                if (cur_level->size() == num_nodes_to_cache)
                    break;
            }
            if (cur_level->size() == num_nodes_to_cache)
                break;
        }
    }

    uint64_t lvl = 1;
    uint64_t prev_node_set_size = 0;
    while ((node_set.size() + cur_level->size() < num_nodes_to_cache) && cur_level->size() != 0)
    {
        // swap prev_level and cur_level
        std::swap(prev_level, cur_level);
        // clear cur_level
        cur_level->clear();

        std::vector<uint32_t> nodes_to_expand;

        for (const uint32_t &id : *prev_level)
        {
            if (node_set.find(id) != node_set.end())
            {
                continue;
            }
            node_set.insert(id);
            nodes_to_expand.push_back(id);
        }

        if (shuffle)
            std::shuffle(nodes_to_expand.begin(), nodes_to_expand.end(), urng);
        else
            std::sort(nodes_to_expand.begin(), nodes_to_expand.end());

        diskann::cout << "Level: " << lvl << std::flush;
        bool finish_flag = false;

        uint64_t BLOCK_SIZE = 1024;
        uint64_t nblocks = DIV_ROUND_UP(nodes_to_expand.size(), BLOCK_SIZE);
        for (size_t block = 0; block < nblocks && !finish_flag; block++)
        {
            diskann::cout << "." << std::flush;
            size_t start = block * BLOCK_SIZE;
            size_t end = (std::min)((block + 1) * BLOCK_SIZE, nodes_to_expand.size());
            std::vector<AlignedRead> read_reqs;
            std::vector<std::pair<uint32_t, char *>> nhoods;
            for (size_t cur_pt = start; cur_pt < end; cur_pt++)
            {
                char *buf = nullptr;
                alloc_aligned((void **)&buf, SECTOR_LEN, SECTOR_LEN);
                nhoods.emplace_back(nodes_to_expand[cur_pt], buf);
                AlignedRead read;
                read.len = SECTOR_LEN;
                read.buf = buf;
                read.offset = NODE_SECTOR_NO(nodes_to_expand[cur_pt]) * SECTOR_LEN;
                read_reqs.push_back(read);
            }

            // issue read requests
            reader->read(read_reqs);

            // process each nhood buf
            for (uint32_t i = 0; i < read_reqs.size(); i++)
            {
                auto &nhood = nhoods[i];

                // insert node coord into coord_cache
                char *node_buf = OFFSET_TO_NODE(nhood.second, nhood.first);
                uint32_t *node_nhood = OFFSET_TO_NODE_NHOOD(node_buf);
                uint64_t nnbrs = (uint64_t)*node_nhood;
                uint32_t *nbrs = node_nhood + 1;
                // explore next level
                for (uint64_t j = 0; j < nnbrs && !finish_flag; j++)
                {
                    if (node_set.find(nbrs[j]) == node_set.end())
                    {
                        cur_level->insert(nbrs[j]);
                    }
                    if (cur_level->size() + node_set.size() >= num_nodes_to_cache)
                    {
                        finish_flag = true;
                    }
                }
                aligned_free(nhood.second);
            }
        }

        diskann::cout << ". #nodes: " << node_set.size() - prev_node_set_size
                      << ", #nodes thus far: " << node_set.size() << std::endl;
        prev_node_set_size = node_set.size();
        lvl++;
    }

    assert(node_set.size() + cur_level->size() == num_nodes_to_cache || cur_level->size() == 0);

    node_list.clear();
    node_list.reserve(node_set.size() + cur_level->size());
    for (auto node : node_set)
        node_list.push_back(node);
    for (auto node : *cur_level)
        node_list.push_back(node);

    diskann::cout << "Level: " << lvl << std::flush;
    diskann::cout << ". #nodes: " << node_list.size() - prev_node_set_size << ", #nodes thus far: " << node_list.size()
                  << std::endl;
    diskann::cout << "done" << std::endl;
}

template <typename T, typename LabelT> void PQFlashIndex<T, LabelT>::use_medoids_data_as_centroids()
{
    if (centroid_data != nullptr)
        aligned_free(centroid_data);
    alloc_aligned(((void **)&centroid_data), num_medoids * aligned_dim * sizeof(float), 32);
    std::memset(centroid_data, 0, num_medoids * aligned_dim * sizeof(float));

    // diskann::cout << "Loading centroid data from medoids vector data of " << num_medoids << " medoid(s)" << std::endl;
    for (uint64_t cur_m = 0; cur_m < num_medoids; cur_m++)
    {
        auto medoid = medoids[cur_m];
        // read medoid nhood
        auto medoid_buf = std::shared_ptr<char[]>(new char[sector_len]);
        std::vector<AlignedRead> medoid_read(1);
        medoid_read[0].len = sector_len;
        medoid_read[0].buf = medoid_buf.get();
        medoid_read[0].offset = NODE_SECTOR_NO(medoid) * sector_len;
        reader->read(medoid_read);

        // all data about medoid
        char *medoid_node_buf = OFFSET_TO_NODE(medoid_buf.get(), medoid);

        // add medoid coords to `coord_cache`
        auto medoid_coords = std::shared_ptr<T[]>(new T[data_dim]);
        T *medoid_disk_coords = OFFSET_TO_NODE_COORDS(medoid_node_buf);
        memcpy(medoid_coords.get(), medoid_disk_coords, disk_bytes_per_point);

        if (!use_disk_index_pq)
        {
            for (uint32_t i = 0; i < data_dim; i++)
                centroid_data[cur_m * aligned_dim + i] = medoid_coords[i];
        }
        else
        {
            disk_pq_table.inflate_vector((uint8_t *)medoid_coords.get(), (centroid_data + cur_m * aligned_dim));
        }
    }
}

template <typename T, typename LabelT>
inline int32_t PQFlashIndex<T, LabelT>::get_filter_number(const LabelT &filter_label)
{
    int idx = -1;
    for (uint32_t i = 0; i < _filter_list.size(); i++)
    {
        if (_filter_list[i] == filter_label)
        {
            idx = i;
            break;
        }
    }
    return idx;
}

template <typename T, typename LabelT>
void PQFlashIndex<T, LabelT>::generate_random_labels(std::vector<LabelT> &labels, const uint32_t num_labels,
                                                     const uint32_t nthreads)
{
    std::random_device rd;
    labels.clear();
    labels.resize(num_labels);

    uint64_t num_total_labels =
        _pts_to_label_offsets[num_points - 1] + _pts_to_labels[_pts_to_label_offsets[num_points - 1]];
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(0, num_total_labels);

    tsl::robin_set<uint64_t> skip_locs;
    for (uint32_t i = 0; i < num_points; i++)
    {
        skip_locs.insert(_pts_to_label_offsets[i]);
    }

#pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
    for (int64_t i = 0; i < num_labels; i++)
    {
        bool found_flag = false;
        while (!found_flag)
        {
            uint64_t rnd_loc = dis(gen);
            if (skip_locs.find(rnd_loc) == skip_locs.end())
            {
                found_flag = true;
                labels[i] = _filter_list[_pts_to_labels[rnd_loc]];
            }
        }
    }
}

template <typename T, typename LabelT>
std::unordered_map<std::string, LabelT> PQFlashIndex<T, LabelT>::load_label_map(const std::string &labels_map_file)
{
    std::unordered_map<std::string, LabelT> string_to_int_mp;
    std::ifstream map_reader(labels_map_file);
    std::string line, token;
    LabelT token_as_num;
    std::string label_str;
    while (std::getline(map_reader, line))
    {
        std::istringstream iss(line);
        getline(iss, token, '\t');
        label_str = token;
        getline(iss, token, '\t');
        token_as_num = (LabelT)std::stoul(token);
        string_to_int_mp[label_str] = token_as_num;
    }
    return string_to_int_mp;
}

template <typename T, typename LabelT>
LabelT PQFlashIndex<T, LabelT>::get_converted_label(const std::string &filter_label)
{
    if (_label_map.find(filter_label) != _label_map.end())
    {
        return _label_map[filter_label];
    }
    std::stringstream stream;
    stream << "Unable to find label in the Label Map";
    diskann::cerr << stream.str() << std::endl;
    throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
}

template <typename T, typename LabelT>
void PQFlashIndex<T, LabelT>::get_label_file_metadata(std::string map_file, uint32_t &num_pts,
                                                      uint32_t &num_total_labels)
{
    std::ifstream infile(map_file);
    std::string line, token;
    num_pts = 0;
    num_total_labels = 0;

    while (std::getline(infile, line))
    {
        std::istringstream iss(line);
        while (getline(iss, token, ','))
        {
            token.erase(std::remove(token.begin(), token.end(), '\n'), token.end());
            token.erase(std::remove(token.begin(), token.end(), '\r'), token.end());
            num_total_labels++;
        }
        num_pts++;
    }

    diskann::cout << "Labels file metadata: num_points: " << num_pts << ", #total_labels: " << num_total_labels
                  << std::endl;
    infile.close();
}

template <typename T, typename LabelT>
inline bool PQFlashIndex<T, LabelT>::point_has_label(uint32_t point_id, uint32_t label_id)
{
    uint32_t start_vec = _pts_to_label_offsets[point_id];
    uint32_t num_lbls = _pts_to_labels[start_vec];
    bool ret_val = false;
    for (uint32_t i = 0; i < num_lbls; i++)
    {
        if (_pts_to_labels[start_vec + 1 + i] == label_id)
        {
            ret_val = true;
            break;
        }
    }
    return ret_val;
}

template <typename T, typename LabelT>
void PQFlashIndex<T, LabelT>::parse_label_file(const std::string &label_file, size_t &num_points_labels)
{
    std::ifstream infile(label_file);
    if (infile.fail())
    {
        throw diskann::ANNException(std::string("Failed to open file ") + label_file, -1);
    }

    std::string line, token;
    uint32_t line_cnt = 0;

    uint32_t num_pts_in_label_file;
    uint32_t num_total_labels;
    get_label_file_metadata(label_file, num_pts_in_label_file, num_total_labels);

    _pts_to_label_offsets = new uint32_t[num_pts_in_label_file];
    _pts_to_labels = new uint32_t[num_pts_in_label_file + num_total_labels];
    uint32_t counter = 0;

    while (std::getline(infile, line))
    {
        std::istringstream iss(line);
        std::vector<uint32_t> lbls(0);

        _pts_to_label_offsets[line_cnt] = counter;
        uint32_t &num_lbls_in_cur_pt = _pts_to_labels[counter];
        num_lbls_in_cur_pt = 0;
        counter++;
        getline(iss, token, '\t');
        std::istringstream new_iss(token);
        while (getline(new_iss, token, ','))
        {
            token.erase(std::remove(token.begin(), token.end(), '\n'), token.end());
            token.erase(std::remove(token.begin(), token.end(), '\r'), token.end());
            LabelT token_as_num = (LabelT)std::stoul(token);
            if (_labels.find(token_as_num) == _labels.end())
            {
                _filter_list.emplace_back(token_as_num);
            }
            int32_t filter_num = get_filter_number(token_as_num);
            if (filter_num == -1)
            {
                diskann::cout << "Error!! " << std::endl;
                exit(-1);
            }
            _pts_to_labels[counter++] = filter_num;
            num_lbls_in_cur_pt++;
            _labels.insert(token_as_num);
        }

        if (num_lbls_in_cur_pt == 0)
        {
            diskann::cout << "No label found for point " << line_cnt << std::endl;
            exit(-1);
        }
        line_cnt++;
    }
    infile.close();
    num_points_labels = line_cnt;
}

template <typename T, typename LabelT> void PQFlashIndex<T, LabelT>::set_universal_label(const LabelT &label)
{
    int32_t temp_filter_num = get_filter_number(label);
    if (temp_filter_num == -1)
    {
        diskann::cout << "Error, could not find universal label." << std::endl;
    }
    else
    {
        _use_universal_label = true;
        _universal_filter_num = (uint32_t)temp_filter_num;
    }
}

#ifdef EXEC_ENV_OLS
template <typename T, typename LabelT>
int PQFlashIndex<T, LabelT>::load(MemoryMappedFiles &files, uint32_t num_threads, const char *index_prefix)
{
#else
template <typename T, typename LabelT> int PQFlashIndex<T, LabelT>::load(uint32_t num_threads, const char *index_prefix)
{
#endif
    std::string pq_table_bin = std::string(index_prefix) + "_pq_pivots.bin";
    std::string pq_compressed_vectors = std::string(index_prefix) + "_pq_compressed.bin";
    std::string disk_index_file = std::string(index_prefix) + "_disk.index";
#ifdef EXEC_ENV_OLS
    return load_from_separate_paths(files, num_threads, disk_index_file.c_str(), pq_table_bin.c_str(),
                                    pq_compressed_vectors.c_str());
#else
    return load_from_separate_paths(num_threads, disk_index_file.c_str(), pq_table_bin.c_str(),
                                    pq_compressed_vectors.c_str());
#endif
}

#ifdef EXEC_ENV_OLS
template <typename T, typename LabelT>
int PQFlashIndex<T, LabelT>::load_from_separate_paths(diskann::MemoryMappedFiles &files, uint32_t num_threads,
                                                      const char *index_filepath, const char *pivots_filepath,
                                                      const char *compressed_filepath)
{
#else
template <typename T, typename LabelT>
int PQFlashIndex<T, LabelT>::load_from_separate_paths(uint32_t num_threads, const char *index_filepath,
                                                      const char *pivots_filepath, const char *compressed_filepath)
{
#endif
    std::string pq_table_bin = pivots_filepath;
    std::string pq_compressed_vectors = compressed_filepath;
    std::string disk_index_file = index_filepath;
    std::string medoids_file = std::string(disk_index_file) + "_medoids.bin";
    std::string centroids_file = std::string(disk_index_file) + "_centroids.bin";

    std::string labels_file = std ::string(disk_index_file) + "_labels.txt";
    std::string labels_to_medoids = std ::string(disk_index_file) + "_labels_to_medoids.txt";
    std::string dummy_map_file = std ::string(disk_index_file) + "_dummy_map.txt";
    std::string labels_map_file = std ::string(disk_index_file) + "_labels_map.txt";
    size_t num_pts_in_label_file = 0;

    size_t pq_file_dim, pq_file_num_centroids;
#ifdef EXEC_ENV_OLS
    get_bin_metadata(files, pq_table_bin, pq_file_num_centroids, pq_file_dim, METADATA_SIZE);
#else
    get_bin_metadata(pq_table_bin, pq_file_num_centroids, pq_file_dim, METADATA_SIZE);
#endif

    this->disk_index_file = disk_index_file;

    if (pq_file_num_centroids != 256)
    {
        diskann::cout << "Error. Number of PQ centroids is not 256. Exiting." << std::endl;
        return -1;
    }

    this->data_dim = pq_file_dim;
    // will reset later if we use PQ on disk
    this->disk_data_dim = this->data_dim;
    // will change later if we use PQ on disk or if we are using
    // inner product without PQ
    this->disk_bytes_per_point = this->data_dim * sizeof(T);
    this->aligned_dim = ROUND_UP(pq_file_dim, 8);

    size_t npts_u64, nchunks_u64;
#ifdef EXEC_ENV_OLS
    diskann::load_bin<uint8_t>(files, pq_compressed_vectors, this->data, npts_u64, nchunks_u64);
#else
    diskann::load_bin<uint8_t>(pq_compressed_vectors, this->data, npts_u64, nchunks_u64);
#endif

    this->num_points = npts_u64;
    this->n_chunks = nchunks_u64;
    if (file_exists(labels_file))
    {
        parse_label_file(labels_file, num_pts_in_label_file);
        assert(num_pts_in_label_file == this->num_points);
        _label_map = load_label_map(labels_map_file);
        if (file_exists(labels_to_medoids))
        {
            std::ifstream medoid_stream(labels_to_medoids);
            assert(medoid_stream.is_open());
            std::string line, token;

            _filter_to_medoid_ids.clear();
            try
            {
                while (std::getline(medoid_stream, line))
                {
                    std::istringstream iss(line);
                    uint32_t cnt = 0;
                    std::vector<uint32_t> medoids;
                    LabelT label;
                    while (std::getline(iss, token, ','))
                    {
                        if (cnt == 0)
                            label = (LabelT)std::stoul(token);
                        else
                            medoids.push_back((uint32_t)stoul(token));
                        cnt++;
                    }
                    _filter_to_medoid_ids[label].swap(medoids);
                }
            }
            catch (std::system_error &e)
            {
                throw FileException(labels_to_medoids, e, __FUNCSIG__, __FILE__, __LINE__);
            }
        }
        std::string univ_label_file = std ::string(disk_index_file) + "_universal_label.txt";
        if (file_exists(univ_label_file))
        {
            std::ifstream universal_label_reader(univ_label_file);
            assert(universal_label_reader.is_open());
            std::string univ_label;
            universal_label_reader >> univ_label;
            universal_label_reader.close();
            LabelT label_as_num = (LabelT)std::stoul(univ_label);
            set_universal_label(label_as_num);
        }
        if (file_exists(dummy_map_file))
        {
            std::ifstream dummy_map_stream(dummy_map_file);
            assert(dummy_map_stream.is_open());
            std::string line, token;

            while (std::getline(dummy_map_stream, line))
            {
                std::istringstream iss(line);
                uint32_t cnt = 0;
                uint32_t dummy_id;
                uint32_t real_id;
                while (std::getline(iss, token, ','))
                {
                    if (cnt == 0)
                        dummy_id = (uint32_t)stoul(token);
                    else
                        real_id = (uint32_t)stoul(token);
                    cnt++;
                }
                _dummy_pts.insert(dummy_id);
                _has_dummy_pts.insert(real_id);
                _dummy_to_real_map[dummy_id] = real_id;

                if (_real_to_dummy_map.find(real_id) == _real_to_dummy_map.end())
                    _real_to_dummy_map[real_id] = std::vector<uint32_t>();

                _real_to_dummy_map[real_id].emplace_back(dummy_id);
            }
            dummy_map_stream.close();
            diskann::cout << "Loaded dummy map" << std::endl;
        }
    }

#ifdef EXEC_ENV_OLS
    pq_table.load_pq_centroid_bin(files, pq_table_bin.c_str(), nchunks_u64);
#else
    pq_table.load_pq_centroid_bin(pq_table_bin.c_str(), nchunks_u64);
#endif

    // diskann::cout << "Loaded PQ centroids and in-memory compressed vectors. #points: " << num_points
    //               << " #dim: " << data_dim << " #aligned_dim: " << aligned_dim << " #chunks: " << n_chunks << std::endl;

    if (n_chunks > MAX_PQ_CHUNKS)
    {
        std::stringstream stream;
        stream << "Error loading index. Ensure that max PQ bytes for in-memory "
                  "PQ data does not exceed "
               << MAX_PQ_CHUNKS << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    std::string disk_pq_pivots_path = this->disk_index_file + "_pq_pivots.bin";
    if (file_exists(disk_pq_pivots_path))
    {
        use_disk_index_pq = true;
#ifdef EXEC_ENV_OLS
        // giving 0 chunks to make the pq_table infer from the
        // chunk_offsets file the correct value
        disk_pq_table.load_pq_centroid_bin(files, disk_pq_pivots_path.c_str(), 0);
#else
        // giving 0 chunks to make the pq_table infer from the
        // chunk_offsets file the correct value
        disk_pq_table.load_pq_centroid_bin(disk_pq_pivots_path.c_str(), 0);
#endif
        disk_pq_n_chunks = disk_pq_table.get_num_chunks();
        disk_bytes_per_point =
            disk_pq_n_chunks * sizeof(uint8_t); // revising disk_bytes_per_point since DISK PQ is used.
        diskann::cout << "Disk index uses PQ data compressed down to " << disk_pq_n_chunks << " bytes per point."
                      << std::endl;
    }

// read index metadata
#ifdef EXEC_ENV_OLS
    // This is a bit tricky. We have to read the header from the
    // disk_index_file. But  this is now exclusively a preserve of the
    // DiskPriorityIO class. So, we need to estimate how many
    // bytes are needed to store the header and read in that many using our
    // 'standard' aligned file reader approach.
    reader->open(disk_index_file);
    this->setup_thread_data(num_threads);
    this->max_nthreads = num_threads;

    char *bytes = getHeaderBytes();
    ContentBuf buf(bytes, HEADER_SIZE);
    std::basic_istream<char> index_metadata(&buf);
#else
    std::ifstream index_metadata(disk_index_file, std::ios::binary);
#endif

    uint32_t nr, nc; // metadata itself is stored as bin format (nr is number of
                     // metadata, nc should be 1)
    READ_U32(index_metadata, nr);
    READ_U32(index_metadata, nc);

    uint64_t disk_nnodes;
    uint64_t disk_ndims; // can be disk PQ dim if disk_PQ is set to true
    READ_U64(index_metadata, disk_nnodes);
    READ_U64(index_metadata, disk_ndims);

    if (disk_nnodes != num_points)
    {
        diskann::cout << "Mismatch in #points for compressed data file and disk "
                         "index file: "
                      << disk_nnodes << " vs " << num_points << std::endl;
        return -1;
    }

    size_t medoid_id_on_file;
    READ_U64(index_metadata, medoid_id_on_file);
    READ_U64(index_metadata, max_node_len);
    READ_U64(index_metadata, nnodes_per_sector);
    max_degree = ((max_node_len - disk_bytes_per_point) / sizeof(uint32_t)) - 1;

    if (max_degree > MAX_GRAPH_DEGREE)
    {
        std::stringstream stream;
        stream << "Error loading index. Ensure that max graph degree (R) does "
                  "not exceed "
               << MAX_GRAPH_DEGREE << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    // setting up concept of frozen points in disk index for streaming-DiskANN
    READ_U64(index_metadata, this->num_frozen_points);
    uint64_t file_frozen_id;
    READ_U64(index_metadata, file_frozen_id);
    if (this->num_frozen_points == 1)
        this->frozen_location = file_frozen_id;
    if (this->num_frozen_points == 1)
    {
        diskann::cout << " Detected frozen point in index at location " << this->frozen_location
                      << ". Will not output it at search time." << std::endl;
    }

    READ_U64(index_metadata, this->reorder_data_exists);
    if (this->reorder_data_exists)
    {
        if (this->use_disk_index_pq == false)
        {
            throw ANNException("Reordering is designed for used with disk PQ "
                               "compression option",
                               -1, __FUNCSIG__, __FILE__, __LINE__);
        }
        READ_U64(index_metadata, this->reorder_data_start_sector);
        READ_U64(index_metadata, this->ndims_reorder_vecs);
        READ_U64(index_metadata, this->nvecs_per_sector);
    }

    // diskann::cout << "Disk-Index File Meta-data: ";
    // diskann::cout << "# nodes per sector: " << nnodes_per_sector;
    // diskann::cout << ", max node len (bytes): " << max_node_len;
    // diskann::cout << ", max node degree: " << max_degree << std::endl;

#ifdef EXEC_ENV_OLS
    delete[] bytes;
#else
    index_metadata.close();
#endif

#ifndef EXEC_ENV_OLS
    // open AlignedFileReader handle to index_file
    std::string index_fname(disk_index_file);
    reader->open(index_fname);
    this->setup_thread_data(num_threads);
    this->max_nthreads = num_threads;

#endif

#ifdef EXEC_ENV_OLS
    if (files.fileExists(medoids_file))
    {
        size_t tmp_dim;
        diskann::load_bin<uint32_t>(files, medoids_file, medoids, num_medoids, tmp_dim);
#else
    if (file_exists(medoids_file))
    {
        size_t tmp_dim;
        diskann::load_bin<uint32_t>(medoids_file, medoids, num_medoids, tmp_dim);
#endif

        if (tmp_dim != 1)
        {
            std::stringstream stream;
            stream << "Error loading medoids file. Expected bin format of m times "
                      "1 vector of uint32_t."
                   << std::endl;
            throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
        }
#ifdef EXEC_ENV_OLS
        if (!files.fileExists(centroids_file))
        {
#else
        if (!file_exists(centroids_file))
        {
#endif
            diskann::cout << "Centroid data file not found. Using corresponding vectors "
                             "for the medoids "
                          << std::endl;
            use_medoids_data_as_centroids();
        }
        else
        {
            size_t num_centroids, aligned_tmp_dim;
#ifdef EXEC_ENV_OLS
            diskann::load_aligned_bin<float>(files, centroids_file, centroid_data, num_centroids, tmp_dim,
                                             aligned_tmp_dim);
#else
            diskann::load_aligned_bin<float>(centroids_file, centroid_data, num_centroids, tmp_dim, aligned_tmp_dim);
#endif
            if (aligned_tmp_dim != aligned_dim || num_centroids != num_medoids)
            {
                std::stringstream stream;
                stream << "Error loading centroids data file. Expected bin format "
                          "of "
                          "m times data_dim vector of float, where m is number of "
                          "medoids "
                          "in medoids file.";
                diskann::cerr << stream.str() << std::endl;
                throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
            }
        }
    }
    else
    {
        num_medoids = 1;
        medoids = new uint32_t[1];
        medoids[0] = (uint32_t)(medoid_id_on_file);
        use_medoids_data_as_centroids();
    }

    std::string norm_file = std::string(disk_index_file) + "_max_base_norm.bin";

    if (file_exists(norm_file) && metric == diskann::Metric::INNER_PRODUCT)
    {
        uint64_t dumr, dumc;
        float *norm_val;
        diskann::load_bin<float>(norm_file, norm_val, dumr, dumc);
        this->max_base_norm = norm_val[0];
        diskann::cout << "Setting re-scaling factor of base vectors to " << this->max_base_norm << std::endl;
        delete[] norm_val;
    }
    // diskann::cout << "done.." << std::endl;
    return 0;
}

template <typename T, typename LabelT>
int PQFlashIndex<T, LabelT>::load_from_separate_paths(uint32_t num_threads, const char *index_filepath,
                                                          std::stringstream &pivots_stream, std::stringstream &compressed_stream)
{

    std::string disk_index_file = index_filepath;
    std::string medoids_file = std::string(disk_index_file) + "_medoids.bin";
    std::string centroids_file = std::string(disk_index_file) + "_centroids.bin";

    std::string labels_file = std ::string(disk_index_file) + "_labels.txt";
    std::string labels_to_medoids = std ::string(disk_index_file) + "_labels_to_medoids.txt";
    std::string dummy_map_file = std ::string(disk_index_file) + "_dummy_map.txt";
    std::string labels_map_file = std ::string(disk_index_file) + "_labels_map.txt";
    size_t num_pts_in_label_file = 0;

    size_t pq_file_dim, pq_file_num_centroids;


    uint64_t nrow, ncol;
    std::unique_ptr<size_t[]> file_offset_data;
    diskann::load_bin<size_t>(pivots_stream, reinterpret_cast<size_t *&>(file_offset_data), nrow, ncol);
    get_bin_metadata(pivots_stream, pq_file_num_centroids, pq_file_dim, file_offset_data[0]);

    this->disk_index_file = disk_index_file;

    if (pq_file_num_centroids != 256)
    {
        diskann::cout << "Error. Number of PQ centroids is not 256. Exiting." << std::endl;
        return -1;
    }

    this->data_dim = pq_file_dim;
    // will reset later if we use PQ on disk
    this->disk_data_dim = this->data_dim;
    // will change later if we use PQ on disk or if we are using
    // inner product without PQ
    this->disk_bytes_per_point = this->data_dim * sizeof(T);
    this->aligned_dim = ROUND_UP(pq_file_dim, 8);

    size_t npts_u64, nchunks_u64;
    diskann::load_bin<uint8_t>(compressed_stream, this->data, npts_u64, nchunks_u64);

    this->num_points = npts_u64;
    this->n_chunks = nchunks_u64;
    if (file_exists(labels_file))
    {
        parse_label_file(labels_file, num_pts_in_label_file);
        assert(num_pts_in_label_file == this->num_points);
        _label_map = load_label_map(labels_map_file);
        if (file_exists(labels_to_medoids))
        {
            std::ifstream medoid_stream(labels_to_medoids);
            assert(medoid_stream.is_open());
            std::string line, token;

            _filter_to_medoid_ids.clear();
            try
            {
                while (std::getline(medoid_stream, line))
                {
                    std::istringstream iss(line);
                    uint32_t cnt = 0;
                    std::vector<uint32_t> medoids;
                    LabelT label;
                    while (std::getline(iss, token, ','))
                    {
                        if (cnt == 0)
                            label = (LabelT)std::stoul(token);
                        else
                            medoids.push_back((uint32_t)stoul(token));
                        cnt++;
                    }
                    _filter_to_medoid_ids[label].swap(medoids);
                }
            }
            catch (std::system_error &e)
            {
                throw FileException(labels_to_medoids, e, __FUNCSIG__, __FILE__, __LINE__);
            }
        }
        std::string univ_label_file = std ::string(disk_index_file) + "_universal_label.txt";
        if (file_exists(univ_label_file))
        {
            std::ifstream universal_label_reader(univ_label_file);
            assert(universal_label_reader.is_open());
            std::string univ_label;
            universal_label_reader >> univ_label;
            universal_label_reader.close();
            LabelT label_as_num = (LabelT)std::stoul(univ_label);
            set_universal_label(label_as_num);
        }
        if (file_exists(dummy_map_file))
        {
            std::ifstream dummy_map_stream(dummy_map_file);
            assert(dummy_map_stream.is_open());
            std::string line, token;

            while (std::getline(dummy_map_stream, line))
            {
                std::istringstream iss(line);
                uint32_t cnt = 0;
                uint32_t dummy_id;
                uint32_t real_id;
                while (std::getline(iss, token, ','))
                {
                    if (cnt == 0)
                        dummy_id = (uint32_t)stoul(token);
                    else
                        real_id = (uint32_t)stoul(token);
                    cnt++;
                }
                _dummy_pts.insert(dummy_id);
                _has_dummy_pts.insert(real_id);
                _dummy_to_real_map[dummy_id] = real_id;

                if (_real_to_dummy_map.find(real_id) == _real_to_dummy_map.end())
                    _real_to_dummy_map[real_id] = std::vector<uint32_t>();

                _real_to_dummy_map[real_id].emplace_back(dummy_id);
            }
            dummy_map_stream.close();
            diskann::cout << "Loaded dummy map" << std::endl;
        }
    }
    pq_table.load_pq_centroid_bin(pivots_stream, nchunks_u64);
    // diskann::cout << "Loaded PQ centroids and in-memory compressed vectors. #points: " << num_points
    //               << " #dim: " << data_dim << " #aligned_dim: " << aligned_dim << " #chunks: " << n_chunks << std::endl;

    if (n_chunks > MAX_PQ_CHUNKS)
    {
        std::stringstream stream;
        stream << "Error loading index. Ensure that max PQ bytes for in-memory "
                  "PQ data does not exceed "
               << MAX_PQ_CHUNKS << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

//    use_disk_index_pq = true;
//    // giving 0 chunks to make the pq_table infer from the
//    // chunk_offsets file the correct value
//    disk_pq_table.load_pq_centroid_bin(pivots_stream, 0);
//    disk_pq_n_chunks = disk_pq_table.get_num_chunks();
//    disk_bytes_per_point =
//            disk_pq_n_chunks * sizeof(uint8_t); // revising disk_bytes_per_point since DISK PQ is used.
//    diskann::cout << "Disk index uses PQ data compressed down to " << disk_pq_n_chunks << " bytes per point."
//                  << std::endl;

// read index metadata
    std::ifstream index_metadata(disk_index_file, std::ios::binary);

    uint32_t nr, nc; // metadata itself is stored as bin format (nr is number of
    // metadata, nc should be 1)
    READ_U32(index_metadata, nr);
    READ_U32(index_metadata, nc);

    uint64_t disk_nnodes;
    uint64_t disk_ndims; // can be disk PQ dim if disk_PQ is set to true
    READ_U64(index_metadata, disk_nnodes);
    READ_U64(index_metadata, disk_ndims);

    if (disk_nnodes != num_points)
    {
        diskann::cout << "Mismatch in #points for compressed data file and disk "
                         "index file: "
                      << disk_nnodes << " vs " << num_points << std::endl;
        return -1;
    }

    size_t medoid_id_on_file;
    READ_U64(index_metadata, medoid_id_on_file);
    READ_U64(index_metadata, max_node_len);
    READ_U64(index_metadata, nnodes_per_sector);
    max_degree = ((max_node_len - disk_bytes_per_point) / sizeof(uint32_t)) - 1;

    if (max_degree > MAX_GRAPH_DEGREE)
    {
        std::stringstream stream;
        stream << "Error loading index. Ensure that max graph degree (R) does "
                  "not exceed "
               << MAX_GRAPH_DEGREE << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    // setting up concept of frozen points in disk index for streaming-DiskANN
    READ_U64(index_metadata, this->num_frozen_points);
    uint64_t file_frozen_id;
    READ_U64(index_metadata, file_frozen_id);
    if (this->num_frozen_points == 1)
        this->frozen_location = file_frozen_id;
    if (this->num_frozen_points == 1)
    {
        diskann::cout << " Detected frozen point in index at location " << this->frozen_location
                      << ". Will not output it at search time." << std::endl;
    }

    READ_U64(index_metadata, this->reorder_data_exists);
    if (this->reorder_data_exists)
    {
        if (this->use_disk_index_pq == false)
        {
            throw ANNException("Reordering is designed for used with disk PQ "
                               "compression option",
                               -1, __FUNCSIG__, __FILE__, __LINE__);
        }
        READ_U64(index_metadata, this->reorder_data_start_sector);
        READ_U64(index_metadata, this->ndims_reorder_vecs);
        READ_U64(index_metadata, this->nvecs_per_sector);
    }

    // diskann::cout << "Disk-Index File Meta-data: ";
    // diskann::cout << "# nodes per sector: " << nnodes_per_sector;
    // diskann::cout << ", max node len (bytes): " << max_node_len;
    // diskann::cout << ", max node degree: " << max_degree << std::endl;
#ifndef EXEC_ENV_OLS
    // open AlignedFileReader handle to index_file
    std::string index_fname(disk_index_file);
    reader->open(index_fname);
    this->setup_thread_data(num_threads);
    this->max_nthreads = num_threads;
#endif
    index_metadata.close();
    if (file_exists(medoids_file))
    {
        size_t tmp_dim;
        diskann::load_bin<uint32_t>(medoids_file, medoids, num_medoids, tmp_dim);

        if (tmp_dim != 1)
        {
            std::stringstream stream;
            stream << "Error loading medoids file. Expected bin format of m times "
                      "1 vector of uint32_t."
                   << std::endl;
            throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
        }
        if (!file_exists(centroids_file))
        {
            diskann::cout << "Centroid data file not found. Using corresponding vectors "
                             "for the medoids "
                          << std::endl;
            use_medoids_data_as_centroids();
        }
        else
        {
            size_t num_centroids, aligned_tmp_dim;
            diskann::load_aligned_bin<float>(centroids_file, centroid_data, num_centroids, tmp_dim, aligned_tmp_dim);
            if (aligned_tmp_dim != aligned_dim || num_centroids != num_medoids)
            {
                std::stringstream stream;
                stream << "Error loading centroids data file. Expected bin format "
                          "of "
                          "m times data_dim vector of float, where m is number of "
                          "medoids "
                          "in medoids file.";
                diskann::cerr << stream.str() << std::endl;
                throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
            }
        }
    }
    else
    {
        num_medoids = 1;
        medoids = new uint32_t[1];
        medoids[0] = (uint32_t)(medoid_id_on_file);
        use_medoids_data_as_centroids();
    }
    std::string norm_file = std::string(disk_index_file) + "_max_base_norm.bin";

    if (file_exists(norm_file) && metric == diskann::Metric::INNER_PRODUCT)
    {
        uint64_t dumr, dumc;
        float *norm_val;
        diskann::load_bin<float>(norm_file, norm_val, dumr, dumc);
        this->max_base_norm = norm_val[0];
        diskann::cout << "Setting re-scaling factor of base vectors to " << this->max_base_norm << std::endl;
        delete[] norm_val;
    }
    // diskann::cout << "done.." << std::endl;
    return 0;
}

template <typename T, typename LabelT>
int PQFlashIndex<T, LabelT>::load_from_separate_paths(std::stringstream &pivots_stream,
                                                      std::stringstream &compressed_stream, std::stringstream& tag_stream)
{

    size_t num_pts_in_label_file = 0;

    size_t pq_file_dim, pq_file_num_centroids;

    uint64_t nrow, ncol;
    std::unique_ptr<size_t[]> file_offset_data;
    diskann::load_bin<size_t>(pivots_stream, reinterpret_cast<size_t *&>(file_offset_data), nrow, ncol);
    get_bin_metadata(pivots_stream, pq_file_num_centroids, pq_file_dim, file_offset_data[0]);


    if (pq_file_num_centroids != 256)
    {
        diskann::cout << "Error. Number of PQ centroids is not 256. Exiting." << std::endl;
        return -1;
    }

    this->data_dim = pq_file_dim;
    // will reset later if we use PQ on disk
    this->disk_data_dim = this->data_dim;
    // will change later if we use PQ on disk or if we are using
    // inner product without PQ
    this->disk_bytes_per_point = this->data_dim * sizeof(T);
    this->aligned_dim = ROUND_UP(pq_file_dim, 8);

    size_t npts_u64, nchunks_u64;
    diskann::load_bin<uint8_t>(compressed_stream, this->data, npts_u64, nchunks_u64);
    if (use_bsa) {
        try {
            errors.reset(new float[npts_u64]);
            compressed_stream.seekg(sizeof(uint32_t) * 2 + npts_u64 * nchunks_u64 * sizeof(uint8_t), compressed_stream.beg);
            compressed_stream.read((char *) errors.get(), npts_u64 * sizeof(float));
        } catch (std::exception e) {
            throw std::runtime_error("get errors for bsa failed:" + std::string(e.what()));
        }
    }



    this->num_points = npts_u64;
    this->n_chunks = nchunks_u64;

    size_t tag_len = 1;
    diskann::load_bin<LabelT>(tag_stream, this->tags, npts_u64, tag_len);

    pq_table.load_pq_centroid_bin(pivots_stream, nchunks_u64);
    // diskann::cout << "Loaded PQ centroids and in-memory compressed vectors. #points: " << num_points
    //               << " #dim: " << data_dim << " #aligned_dim: " << aligned_dim << " #chunks: " << n_chunks << std::endl;

    if (n_chunks > MAX_PQ_CHUNKS)
    {
        std::stringstream stream;
        stream << "Error loading index. Ensure that max PQ bytes for in-memory "
                  "PQ data does not exceed "
               << MAX_PQ_CHUNKS << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

//    use_disk_index_pq = true;
//    // giving 0 chunks to make the pq_table infer from the
//    // chunk_offsets file the correct value
//    disk_pq_table.load_pq_centroid_bin(pivots_stream, 0);
//    disk_pq_n_chunks = disk_pq_table.get_num_chunks();
//    disk_bytes_per_point =
//            disk_pq_n_chunks * sizeof(uint8_t); // revising disk_bytes_per_point since DISK PQ is used.
//    diskann::cout << "Disk index uses PQ data compressed down to " << disk_pq_n_chunks << " bytes per point."
//                  << std::endl;

// read index metadata

    uint32_t nr, nc; // metadata itself is stored as bin format (nr is number of

    uint64_t disk_nnodes;
    uint64_t disk_ndims; // can be disk PQ dim if disk_PQ is set to true

    size_t medoid_id_on_file;
    uint64_t file_frozen_id;
    // metadata, nc should be 1)
    std::vector<AlignedRead> read_reqs;
    uint64_t start = 0;
    read_reqs.emplace_back(0, 4, &nr);
    read_reqs.emplace_back(4, 4, &nc);
    read_reqs.emplace_back(8, 8, &disk_nnodes);
    read_reqs.emplace_back(16, 8, &disk_ndims);
    read_reqs.emplace_back(24, 8, &medoid_id_on_file);
    read_reqs.emplace_back(32, 8, &max_node_len);
    read_reqs.emplace_back(40, 8, &nnodes_per_sector);
    read_reqs.emplace_back(48, 8, &this->num_frozen_points);
    read_reqs.emplace_back(56, 8, &file_frozen_id);
    read_reqs.emplace_back(64, 8, &this->reorder_data_exists);
    reader->read(read_reqs);
//    READ_U32(index_metadata, nr);
//    READ_U32(index_metadata, nc);
//    READ_U64(index_metadata, disk_nnodes);
//    READ_U64(index_metadata, disk_ndims);
//    READ_U64(index_metadata, medoid_id_on_file);
//    READ_U64(index_metadata, max_node_len);
//    READ_U64(index_metadata, nnodes_per_sector);
//    READ_U64(index_metadata, this->num_frozen_points);
//    READ_U64(index_metadata, file_frozen_id);
//    READ_U64(index_metadata, this->reorder_data_exists);

    if (disk_nnodes != num_points)
    {
        diskann::cout << "Mismatch in #points for compressed data file and disk "
                         "index file: "
                      << disk_nnodes << " vs " << num_points << std::endl;
        return -1;
    }
    max_degree = ((max_node_len - disk_bytes_per_point) / sizeof(uint32_t)) - 1;

    if (max_degree > MAX_GRAPH_DEGREE)
    {
        std::stringstream stream;
        stream << "Error loading index. Ensure that max graph degree (R) does "
                  "not exceed "
               << MAX_GRAPH_DEGREE << std::endl;
        throw diskann::ANNException(stream.str(), -1, __FUNCSIG__, __FILE__, __LINE__);
    }

    // setting up concept of frozen points in disk index for streaming-DiskANN
    if (this->num_frozen_points == 1)
        this->frozen_location = file_frozen_id;
    if (this->num_frozen_points == 1)
    {
        diskann::cout << " Detected frozen point in index at location " << this->frozen_location
                      << ". Will not output it at search time." << std::endl;
    }

    // diskann::cout << "Disk-Index File Meta-data: ";
    // diskann::cout << "# nodes per sector: " << nnodes_per_sector;
    // diskann::cout << ", max node len (bytes): " << max_node_len;
    // diskann::cout << ", max node degree: " << max_degree << std::endl;
    num_medoids = 1;
    medoids = new uint32_t[1];
    medoids[0] = (uint32_t)(medoid_id_on_file);

    use_medoids_data_as_centroids();
    // diskann::cout << "done.." << std::endl;
    return 0;
}



template <typename T, typename LabelT>
int64_t PQFlashIndex<T, LabelT>::cached_beam_search(const T *query1, const uint64_t k_search, const uint64_t l_search,
                                                 uint64_t *indices, float *distances, const uint64_t beam_width,
                                                 std::function<bool(int64_t)> filter,
                                                 const uint32_t io_limit, const bool use_reorder_data,
                                                 QueryStats *stats)
{
    std::shared_ptr<float[]> aligned_query_T = std::shared_ptr<float[]>(new float[this->data_dim]);


    for (size_t i = 0; i < this->data_dim; i++)
    {
        aligned_query_T[i] = (float) query1[i];
    }

    // FIXME: alternative instruction on aarch64
#if defined(__i386__) || defined(__x86_64__)
    _mm_prefetch((char *)aligned_query_T.get(), _MM_HINT_T1);
#endif

    // sector scratch
    auto sector_scratch = std::shared_ptr<char[]>(new char[beam_width * sector_len]);

    // query <-> PQ chunk centers distances
    pq_table.preprocess_query(aligned_query_T.get()); // center the query and rotate if
    // we have a rotation matrix
    auto pq_dists = std::shared_ptr<float[]>(new float[NUM_CENTROID * this->n_chunks]);
    pq_table.populate_chunk_distances(aligned_query_T.get(), pq_dists.get());

    // query <-> neighbor list
    auto dist_scratch = std::shared_ptr<float[]>(new float[this->max_degree]);
    auto pq_coord_scratch = std::shared_ptr<uint8_t[]>(new uint8_t[this->max_degree * this->n_chunks]);

    // lambda to batch compute query<-> node distances in PQ space
    auto compute_dists = [this, pq_coord_scratch, pq_dists](const uint32_t *ids, const uint64_t n_ids,
                                                            float *dists_out) {
        diskann::aggregate_coords(ids, n_ids, this->data, this->n_chunks, pq_coord_scratch.get());
        diskann::pq_dist_lookup(pq_coord_scratch.get(), n_ids, this->n_chunks, pq_dists.get(), dists_out);
    };
    Timer query_timer, io_timer, cpu_timer;

    tsl::robin_set<uint64_t> visited;
    std::vector<Neighbor> full_retset;
    NeighborPriorityQueue retset;
    visited.reserve(l_search);
    full_retset.reserve(l_search);
    retset.reserve(l_search);

    uint32_t best_medoid = 0;
    float best_dist = std::numeric_limits<float>::max();

    for (uint64_t cur_m = 0; cur_m < num_medoids; cur_m++)
    {
        float cur_expanded_dist =
            dist_cmp_float->compare(aligned_query_T.get(), centroid_data + aligned_dim * cur_m, (uint32_t)data_dim);
        if (cur_expanded_dist < best_dist)
        {
            best_medoid = medoids[cur_m];
            best_dist = cur_expanded_dist;
        }
    }

    compute_dists(&best_medoid, 1, dist_scratch.get());
    retset.insert(Neighbor(best_medoid, dist_scratch[0]));
    visited.insert(best_medoid);

    uint32_t cmps = 0;
    uint32_t hops = 0;
    uint32_t num_ios = 0;

    // cleared every iteration
    std::vector<uint32_t> frontier;
    frontier.reserve(2 * beam_width);
    std::vector<std::pair<uint32_t, char *>> frontier_nhoods;
    frontier_nhoods.reserve(2 * beam_width);
    std::vector<AlignedRead> frontier_read_reqs;
    frontier_read_reqs.reserve(2 * beam_width);
    std::vector<std::pair<uint32_t, std::pair<uint32_t, uint32_t *>>> cached_nhoods;
    cached_nhoods.reserve(2 * beam_width);

    while (retset.has_unexpanded_node() && num_ios < io_limit)
    {
        // clear iteration state
        frontier.clear();
        frontier_nhoods.clear();
        frontier_read_reqs.clear();
        cached_nhoods.clear();
        uint64_t sector_scratch_idx = 0;
        // find new beam
        uint32_t num_seen = 0;
        while (retset.has_unexpanded_node() && frontier.size() < beam_width && num_seen < beam_width)
        {
            auto nbr = retset.closest_unexpanded();
            num_seen++;
            auto iter = nhood_cache.find(nbr.id);
            if (iter != nhood_cache.end())
            {
                cached_nhoods.push_back(std::make_pair(nbr.id, iter->second));
                if (stats != nullptr)
                {
                    stats->n_cache_hits++;
                }
            }
            else
            {
                frontier.push_back(nbr.id);
            }
            if (this->count_visited_nodes)
            {
                reinterpret_cast<std::atomic<uint32_t> &>(this->node_visit_counter[nbr.id].second).fetch_add(1);
            }
        }

        // read nhoods of frontier ids
        if (!frontier.empty())
        {
            if (stats != nullptr)
                stats->n_hops++;
            for (uint64_t i = 0; i < frontier.size(); i++)
            {
                auto id = frontier[i];
                std::pair<uint32_t, char *> fnhood;
                fnhood.first = id;
                fnhood.second = sector_scratch.get() + sector_scratch_idx * sector_len;
                sector_scratch_idx++;
                frontier_nhoods.push_back(fnhood);
                frontier_read_reqs.emplace_back(NODE_SECTOR_NO(((size_t)id)) * sector_len, sector_len, fnhood.second);
                if (stats != nullptr)
                {
                    stats->n_4k++;
                    stats->n_ios++;
                }
                num_ios++;
            }
            reader->read(frontier_read_reqs);

//            diskann_stream.seekg(13598720);
//            auto x = new char[4096];
//            diskann_stream.read(x, 4096);

//            for (int i = 0; i < frontier_nhoods.size(); ++i) {
//                std::cout << (*(uint32_t *)((char *)frontier_read_reqs[i].buf + data_dim * sizeof (float)));
//                char *node_disk_buf = OFFSET_TO_NODE(frontier_read_reqs[i].buf, frontier_nhoods[i].first);
//                uint32_t *node_buf = OFFSET_TO_NODE_NHOOD(node_disk_buf);
//                long offset1 = node_disk_buf - (char *)frontier_read_reqs[i].buf;
//                long offset2 = (char *)node_buf - (char *)frontier_read_reqs[i].buf;
//                std::cout << offset2 << "    " << data_dim * sizeof (float) << "   " << offset1 << std::endl;
//            }
//            exit(0);
            if (stats != nullptr)
            {
                stats->io_us += (float)io_timer.elapsed();
            }
        }
        // process cached nhoods
        for (auto &cached_nhood : cached_nhoods)
        {
            auto global_cache_iter = coord_cache.find(cached_nhood.first);
            T *node_fp_coords_copy = global_cache_iter->second;
            float cur_expanded_dist;
            cur_expanded_dist = dist_cmp_float->compare(aligned_query_T.get(), (float *)node_fp_coords_copy, (uint32_t)data_dim);
            full_retset.push_back(Neighbor((uint32_t)cached_nhood.first, cur_expanded_dist));

            uint64_t nnbrs = cached_nhood.second.first;
            uint32_t *node_nbrs = cached_nhood.second.second;

            // compute node_nbrs <-> query dists in PQ space
            compute_dists(node_nbrs, nnbrs, dist_scratch.get());

            // process prefetched nhood
            for (uint64_t m = 0; m < nnbrs; ++m)
            {
                uint32_t id = node_nbrs[m];
                if (visited.insert(id).second)
                {
                    cmps++;
                    float dist = dist_scratch[m];
                    Neighbor nn(id, dist);
                    retset.insert(nn);
                }
            }
        }
        for (auto &frontier_nhood : frontier_nhoods)
        {
            char *node_disk_buf = OFFSET_TO_NODE(frontier_nhood.second, frontier_nhood.first);
            uint32_t *node_buf = OFFSET_TO_NODE_NHOOD(node_disk_buf);
            uint64_t nnbrs = (uint64_t)(*node_buf);
            T *node_fp_coords = OFFSET_TO_NODE_COORDS(node_disk_buf);
            float cur_expanded_dist = dist_cmp_float->compare((float *)aligned_query_T.get(), (float *)node_fp_coords, (uint32_t)data_dim);
            full_retset.push_back(Neighbor(frontier_nhood.first, cur_expanded_dist));
            uint32_t *node_nbrs = (node_buf + 1);
            // compute node_nbrs <-> query dist in PQ space
            compute_dists(node_nbrs, nnbrs, dist_scratch.get());
            if (stats != nullptr)
            {
                stats->n_cmps += (uint32_t)nnbrs;
            }
            // process prefetch-ed nhood
            for (uint64_t m = 0; m < nnbrs; ++m)
            {
                uint32_t id = node_nbrs[m];
                if (visited.insert(id).second)
                {
                    cmps++;
                    float dist = dist_scratch[m];

                    Neighbor nn(id, dist);
                    retset.insert(nn);
                }
            }
        }

        hops++;
    }

    // re-sort by distance
    std::sort(full_retset.begin(), full_retset.end());

    // copy k_search values
    int64_t result_size = 0;
    for (uint64_t i = 0; i < full_retset.size(); i++)
    {
        if (filter && filter(tags[full_retset[i].id])) {
            continue;
        }
        if (result_size >= k_search) {
            break;
        }
        indices[result_size] = tags[full_retset[i].id];
        auto key = (uint32_t)indices[result_size];
        if (distances != nullptr)
        {
            distances[result_size] = full_retset[i].distance;
            if (metric == diskann::Metric::INNER_PRODUCT)
            {
                // When using L2 distance to calculate IP distance, the L2
                // distance is exactly twice the IP distance.
                distances[result_size] = distances[result_size] / 2;
            }
        }
        result_size ++;
    }
    return result_size;
}

template <typename T, typename LabelT>
size_t PQFlashIndex<T, LabelT>::load_graph(std::stringstream &in)
{
    size_t expected_file_size;
    size_t file_frozen_pts;

    uint32_t max_observed_degree, start, max_range_of_loaded_graph;

    size_t file_offset = 0; // will need this for single file format support
    in.seekg(0);
    in.read((char *)&expected_file_size, sizeof(expected_file_size));
    in.read((char *)&max_observed_degree, sizeof(max_observed_degree));
    in.read((char *)&start, sizeof(start));
    in.read((char *)&file_frozen_pts, sizeof(file_frozen_pts));
    size_t vamana_metadata_size = sizeof(expected_file_size) + sizeof(max_observed_degree) + sizeof(start) + sizeof(file_frozen_pts);

    size_t bytes_read = vamana_metadata_size;
    size_t cc = 0;
    uint32_t nodes_read = 0;
    final_graph.resize(num_points);
    while (bytes_read < expected_file_size)
    {
        uint32_t k;
        in.read((char *)&k, sizeof(uint32_t));
        graph_size += k;
        if (k == 0)
        {
            throw diskann::ANNException("ERROR: Point found with no out-neighbors.", -1);
        }
        max_degree = std::max(max_degree, (uint64_t)k);
        cc += k;
        ++nodes_read;
        std::vector<uint32_t> tmp(k);
        tmp.reserve(k);
        in.read((char *)tmp.data(), k * sizeof(uint32_t));

        final_graph[nodes_read - 1].swap(tmp);
        bytes_read += sizeof(uint32_t) * ((size_t)k + 1);
        if (k > max_range_of_loaded_graph)
        {
            max_range_of_loaded_graph = k;
        }
    }

    if (bytes_read != expected_file_size)
    {
        throw diskann::ANNException("ERROR: the size of the file being read does not match the expected size.", -1);
    }
    return nodes_read;
}


template <typename T, typename LabelT>
int64_t PQFlashIndex<T, LabelT>::cached_beam_search_memory(const T *query, const uint64_t k_search, const uint64_t l_search,
                                                 uint64_t *indices, float *distances, const uint64_t beam_width,
                                                 std::function<bool(int64_t)> filter,
                                                 const uint32_t io_limit, const bool reorder,
                                                 QueryStats *stats, bool use_for_range)
{
    std::shared_ptr<float[]> aligned_query_T = std::shared_ptr<float[]>(new float[this->data_dim]);

    // if inner product, we also normalize the query and set the last coordinate
    // to 0 (this is the extra coordinate used to convert MIPS to L2 search)

    for (size_t i = 0; i < this->data_dim; i++)
    {
        aligned_query_T[i] = (float) query[i];
    }

    // FIXME: alternative instruction on aarch64
#if defined(__i386__) || defined(__x86_64__)
    _mm_prefetch((char *)aligned_query_T.get(), _MM_HINT_T1);
#endif

    // query <-> PQ chunk centers distances
    pq_table.preprocess_query(aligned_query_T.get()); // center the query and rotate if
    // we have a rotation matrix
    auto pq_dists = std::shared_ptr<float[]>(new float[NUM_CENTROID * this->n_chunks]);
    pq_table.populate_chunk_distances(aligned_query_T.get(), pq_dists.get());

    // query <-> neighbor list
    auto dist_scratch = std::shared_ptr<float[]>(new float[this->max_degree]);
    auto pq_coord_scratch = std::shared_ptr<uint8_t[]>(new uint8_t[this->max_degree * this->n_chunks]);

    // lambda to batch compute query<-> node distances in PQ space
    auto compute_dists = [this, pq_coord_scratch, pq_dists](const uint32_t *ids, const uint64_t n_ids,
                                                            float *dists_out) {
        diskann::aggregate_coords(ids, n_ids, this->data, this->n_chunks, pq_coord_scratch.get());
        diskann::pq_dist_lookup(pq_coord_scratch.get(), n_ids, this->n_chunks, pq_dists.get(), dists_out);
    };

    tsl::robin_set<uint64_t> visited;
    std::vector<Neighbor> full_retset;
    visited.reserve(l_search);
    full_retset.reserve(l_search);
    uint32_t best_medoid = 0;
    float best_dist = std::numeric_limits<float>::max();
    for (uint64_t cur_m = 0; cur_m < num_medoids; cur_m++)
    {
        float cur_expanded_dist =
                dist_cmp_float->compare(aligned_query_T.get(), centroid_data + aligned_dim * cur_m, (uint32_t)data_dim);
        if (cur_expanded_dist < best_dist)
        {
            best_medoid = medoids[cur_m];
            best_dist = cur_expanded_dist;
        }
    }

    compute_dists(&best_medoid, 1, dist_scratch.get());
    visited.insert(best_medoid);

    uint64_t has_searched = 0;
    std::priority_queue<Neighbor> candidate_queue;
    candidate_queue.push(Neighbor(best_medoid, -dist_scratch[0]));

    while (has_searched < l_search)
    {
        if (candidate_queue.empty()) {
            break; // TODO: add logger for the break (the graph is not connective)
        }
        auto nbr = candidate_queue.top();
        full_retset.push_back({nbr.id, -nbr.distance});
        candidate_queue.pop();
        auto nohood_id = nbr.id;

        uint32_t *node_nbrs = final_graph[nohood_id].data();
        size_t nnbrs = final_graph[nohood_id].size();

        std::vector<uint32_t> unseen_ids;
        for (uint64_t m = 0; m < nnbrs; ++m)
        {
            uint32_t id = node_nbrs[m];
            if (visited.insert(id).second)
            {
                unseen_ids.push_back(id);
            }
        }
        if (unseen_ids.size() != 0) {
            compute_dists(unseen_ids.data(), unseen_ids.size(), dist_scratch.get());
            for (uint64_t i = 0; i < unseen_ids.size(); ++i)
            {
                float dist = dist_scratch[i];
                candidate_queue.emplace(unseen_ids[i], -dist);
            }
        }

        has_searched ++;
    }

    if (use_bsa && use_for_range && not reorder) {
        for (int i = 0; i < full_retset.size(); ++i)
        {
            full_retset[i].distance -= this->errors[full_retset[i].id];
        }
    }

    // re-sort by distance
    std::sort(full_retset.begin(), full_retset.end());
    if (reorder) {
        std::vector<Neighbor> reorder_retset;
        std::priority_queue<float> distance_ranks;
        int loc = 0;
        auto sector_scratch = std::shared_ptr<char[]>(new char[beam_width * sector_len]);
        while (loc < io_limit && loc < full_retset.size())
        {
            std::vector<AlignedRead> sorted_read_reqs;
            std::vector<uint32_t> ids;
            int cur_loc = 0;
            while (sorted_read_reqs.size() < beam_width && loc < io_limit && loc < full_retset.size()) {
                auto id = full_retset[loc].id;
                if (not use_bsa || reorder_retset.empty() || reorder_retset.size() < k_search ||
                    distance_ranks.top() + this->errors[id] > full_retset[loc].distance) {
                    ids.push_back(id);
                    sorted_read_reqs.push_back({NODE_SECTOR_NO(((size_t)id)) * sector_len, sector_len,
                                                sector_scratch.get() + cur_loc * sector_len});
                    cur_loc ++;
                }
                loc ++;
            }
#ifndef NDEBUG
            Timer io_times;
#endif
            reader->read(sorted_read_reqs);
#ifndef NDEBUG
            if (stats != nullptr) {
                stats->io_us += (float) io_times.elapsed();
                stats->n_ios += sorted_read_reqs.size();
            }
#endif
            for (int j = 0; j < sorted_read_reqs.size(); j ++)
            {
                uint32_t id = ids[j];
                char *node_disk_buf = OFFSET_TO_NODE(sorted_read_reqs[j].buf, id);
                T *node_fp_coords = OFFSET_TO_NODE_COORDS(node_disk_buf);
                float exact_dist;
                exact_dist = dist_cmp_float->compare((float *)query, (float *)node_fp_coords, (uint32_t)data_dim);
                reorder_retset.push_back(Neighbor(id, exact_dist));
                distance_ranks.push(exact_dist);
                if (distance_ranks.size() > k_search) {
                    distance_ranks.pop();
                }
            }
        }
        std::sort(reorder_retset.begin(), reorder_retset.end());
        full_retset.swap(reorder_retset);
    }

    // copy k_search values
    int64_t result_size = 0;
    for (uint64_t i = 0; i < full_retset.size(); i++)
    {
        if (filter && filter(tags[full_retset[i].id])) {
            continue;
        }
        if (result_size >= k_search) {
            break;
        }
        indices[result_size] = tags[full_retset[i].id];
        if (distances != nullptr)
        {
            distances[result_size] = full_retset[i].distance;
            if (metric == diskann::Metric::INNER_PRODUCT)
            {
                // When using L2 distance to calculate IP distance, the L2
                // distance is exactly twice the IP distance.
                distances[result_size] = distances[result_size] / 2;
            }
        }
        result_size ++;
    }
    return result_size;
}


template <typename T, typename LabelT>
int64_t PQFlashIndex<T, LabelT>::cached_beam_search_async(const T *query, const uint64_t k_search, const uint64_t l_search,
                                                           uint64_t *indices, float *distances, const uint64_t beam_width,
                                                           std::function<bool(int64_t)> filter,
                                                           const uint32_t io_limit, const bool reorder,
                                                           QueryStats *stats)
{
    std::shared_ptr<float[]> aligned_query_T = std::shared_ptr<float[]>(new float[this->data_dim]); // TODO: add support for other data type

    // if inner product, we also normalize the query and set the last coordinate
    // to 0 (this is the extra coordinate used to convert MIPS to L2 search)

    for (size_t i = 0; i < this->data_dim; i++)
    {
        aligned_query_T[i] = (float) query[i];
    }

    // FIXME: alternative instruction on aarch64
#if defined(__i386__) || defined(__x86_64__)
    _mm_prefetch((char *)aligned_query_T.get(), _MM_HINT_T1);
#endif

    // query <-> PQ chunk centers distances
    pq_table.preprocess_query(aligned_query_T.get()); // center the query and rotate if
    // we have a rotation matrix
    auto pq_dists = std::shared_ptr<float[]>(new float[NUM_CENTROID * this->n_chunks]);
    pq_table.populate_chunk_distances(aligned_query_T.get(), pq_dists.get());

    // query <-> neighbor list
    auto dist_scratch = std::shared_ptr<float[]>(new float[this->max_degree]);
    auto pq_coord_scratch = std::shared_ptr<uint8_t[]>(new uint8_t[this->max_degree * this->n_chunks]);

    // lambda to batch compute query<-> node distances in PQ space
    auto compute_dists = [this, pq_coord_scratch, pq_dists](const uint32_t *ids, const uint64_t n_ids,
                                                            float *dists_out) {
        diskann::aggregate_coords(ids, n_ids, this->data, this->n_chunks, pq_coord_scratch.get());
        diskann::pq_dist_lookup(pq_coord_scratch.get(), n_ids, this->n_chunks, pq_dists.get(), dists_out);
    };

    tsl::robin_set<uint64_t> visited;
    std::vector<Neighbor> full_retset;
    visited.reserve(l_search);
    full_retset.reserve(l_search);
    uint32_t best_medoid = 0;
    float best_dist = std::numeric_limits<float>::max();
    for (uint64_t cur_m = 0; cur_m < num_medoids; cur_m++)
    {
        float cur_expanded_dist =
            dist_cmp_float->compare(aligned_query_T.get(), centroid_data + aligned_dim * cur_m, (uint32_t)data_dim);
        if (cur_expanded_dist < best_dist)
        {
            best_medoid = medoids[cur_m];
            best_dist = cur_expanded_dist;
        }
    }

    compute_dists(&best_medoid, 1, dist_scratch.get());
    visited.insert(best_medoid);

    uint64_t has_searched = 0;
    std::priority_queue<Neighbor> candidate_queue;
    candidate_queue.push(Neighbor(best_medoid, -dist_scratch[0]));

    // Async IO
    std::shared_ptr<char[]> cache_sectors;
    std::map<uint32_t, int> cache_loc;
    std::vector<std::future<bool>> futures;
    int max_io_count = std::max(l_search, (const uint64_t) io_limit) / beam_width + 1;
    std::vector<std::promise<bool>> promises(max_io_count);
    if (reorder) {
        cache_sectors.reset(new char[sector_len * l_search]);
    }

    std::vector<AlignedRead> sorted_read_reqs;
    while (has_searched < l_search)
    {
        if (candidate_queue.empty()) {
            break; // TODO: add logger for the break (the graph is not connective)
        }
        auto nbr = candidate_queue.top();
        full_retset.push_back({nbr.id, -nbr.distance});
        candidate_queue.pop();
        auto nohood_id = nbr.id;

        if (reorder) {
            sorted_read_reqs.emplace_back(NODE_SECTOR_NO(((size_t)nohood_id)) * sector_len, sector_len,
                                          cache_sectors.get() + has_searched * sector_len);
            if (sorted_read_reqs.size() >= beam_width || has_searched == l_search - 1) {
                int io_count = has_searched / beam_width;
                if (stats != nullptr) {
                    stats->n_ios += io_count;
                }
                futures.push_back(promises[io_count].get_future());
                auto remaining_ops = std::make_shared<std::atomic<int>>(sorted_read_reqs.size());
                CallBack callBack = [&promises, io_count, remaining_ops] (vsag::IOErrorCode code, const std::string& message){
                    if ((int) code == 0) {
                        if (--(*remaining_ops) == 0) {
                            promises[io_count].set_value(true);
                        }
                    } else {
                        (*remaining_ops) = 0;
                        promises[io_count].set_value(false);
                    }
                };
                reader->read(sorted_read_reqs, true, callBack);
                io_count ++;
                sorted_read_reqs.clear();
            }
            cache_loc[nohood_id] = has_searched;
        }

        uint32_t *node_nbrs = final_graph[nohood_id].data();
        size_t nnbrs = final_graph[nohood_id].size();

        std::vector<uint32_t> unseen_ids;
        for (uint64_t m = 0; m < nnbrs; ++m)
        {
            uint32_t id = node_nbrs[m];
            if (visited.insert(id).second)
            {
                unseen_ids.push_back(id);
            }
        }
        if (unseen_ids.size() != 0) {
            compute_dists(unseen_ids.data(), unseen_ids.size(), dist_scratch.get());
            for (uint64_t i = 0; i < unseen_ids.size(); ++i)
            {
                float dist = dist_scratch[i];
                candidate_queue.emplace(unseen_ids[i], -dist);
            }
        }
        has_searched ++;
    }

    // re-sort by distance
    std::vector<bool> has_result(max_io_count, false);
    std::sort(full_retset.begin(), full_retset.end());
    if (reorder) {
        std::vector<Neighbor> reorder_retset;
        std::priority_queue<float> distance_ranks;
        for (int j = 0; j < full_retset.size(); j ++)
        {
            uint32_t id = full_retset[j].id;
            int loc = cache_loc[id];
            auto& single_future = futures[loc / beam_width];
            if (single_future.valid()) {
                has_result[loc / beam_width] = single_future.get();
            }
            if (not has_result[loc / beam_width]) {
                continue;
            }

            if (not use_bsa || reorder_retset.empty() || reorder_retset.size() < k_search ||
                distance_ranks.top() + this->errors[id] > full_retset[j].distance) {
                char *node_disk_buf = OFFSET_TO_NODE(cache_sectors.get() + loc * sector_len, id);
                T *node_fp_coords = OFFSET_TO_NODE_COORDS(node_disk_buf);
                float exact_dist;
                exact_dist = dist_cmp_float->compare((float *)aligned_query_T.get(), (float *)node_fp_coords, (uint32_t)data_dim);
                if (stats != nullptr)
                {
                    stats->n_cmps += 1;
                }
                reorder_retset.push_back(Neighbor(id, exact_dist));
                distance_ranks.push(exact_dist);
                if (distance_ranks.size() > k_search) {
                    distance_ranks.pop();
                }
            }
        }
        std::sort(reorder_retset.begin(), reorder_retset.end());
        full_retset.swap(reorder_retset);
    }

    // copy k_search values
    int64_t result_size = 0;
    for (uint64_t i = 0; i < full_retset.size(); i++)
    {
        if (filter && filter(tags[full_retset[i].id])) {
            continue;
        }
        if (result_size >= k_search) {
            break;
        }
        indices[result_size] = tags[full_retset[i].id];
        if (distances != nullptr)
        {
            distances[result_size] = full_retset[i].distance;
            if (metric == diskann::Metric::INNER_PRODUCT)
            {
                // When using L2 distance to calculate IP distance, the L2
                // distance is exactly twice the IP distance.
                distances[result_size] = distances[result_size] / 2;
            }
        }
        result_size ++;
    }
    return result_size;
}

const static float THRESHOLD_ERROR = 1e-6;

// range search returns results of all neighbors within distance of range.
// indices and distances need to be pre-allocated of size l_search and the
// return value is the number of matching hits.
template <typename T, typename LabelT>
int64_t PQFlashIndex<T, LabelT>::range_search(const T *query, const double range, const uint64_t min_l_search,
                                              const uint64_t max_l_search, std::vector<uint64_t> &indices,
                                              std::vector<float> &distances, const uint64_t min_beam_width,
                                              uint32_t io_limit, const bool reorder,
                                              std::function<bool(int64_t)> filter, bool memory,
                                              QueryStats *stats)
{
    int64_t res_count = 0;
    bool stop_flag = false;

    uint32_t l_search = (uint32_t) std::min(min_l_search, num_points); // starting size of the candidate list
    while (!stop_flag)
    {
        indices.resize(l_search);
        distances.resize(l_search);
        for (auto &x : distances)
            x = std::numeric_limits<float>::max();
        int64_t result_size = 0;
        if (memory) {
            result_size = this->cached_beam_search_memory(query, l_search, l_search, indices.data(), distances.data(), min_beam_width, filter, io_limit, reorder, stats, true);
        } else {
            result_size = this->cached_beam_search(query, l_search, l_search, indices.data(), distances.data(), min_beam_width, filter, io_limit, false, stats);
        }
        for (uint32_t i = 0; i < result_size; i++)
        {
            if (distances[i] > (float)(range + THRESHOLD_ERROR))
            {
                break;
            } else {
                res_count = i + 1;
            }
        }
        if (res_count < (result_size / 2.0))
            stop_flag = true;
        l_search = l_search * 2;
        io_limit = io_limit * 2;
        if (l_search > max_l_search || l_search > num_points)
            stop_flag = true;
    }
    indices.resize(res_count);
    distances.resize(res_count);
    return res_count;
}

template <typename T, typename LabelT> uint64_t PQFlashIndex<T, LabelT>::get_data_dim()
{
    return data_dim;
}
template <typename T, typename LabelT> uint64_t PQFlashIndex<T, LabelT>::get_data_num()
{
    return num_points;
}

template <typename T, typename LabelT> diskann::Metric PQFlashIndex<T, LabelT>::get_metric()
{
    return this->metric;
}

template <typename T, typename LabelT>  int64_t PQFlashIndex<T, LabelT>::get_memory_usage()
{
    int64_t memory_size = 0;
    memory_size += node_visit_counter.capacity() * (sizeof(uint32_t) + sizeof(uint32_t));
    memory_size += pq_table.get_memory_usage();
    memory_size += disk_pq_table.get_memory_usage();
    memory_size += nhood_cache.size() * (max_degree + 1) * sizeof(uint32_t);
    memory_size += coord_cache.size() * nhood_cache.size() * aligned_dim * sizeof(T);
    memory_size += _labels.size() * sizeof(LabelT);
    memory_size += _filter_list.size() * sizeof(LabelT);
    memory_size += graph_size * sizeof(uint32_t);
    memory_size += num_points * n_chunks * sizeof(uint8_t); // record the memory usage for pq
    memory_size += num_points * sizeof(LabelT); // record memory usage for tags
    return memory_size;
}

// instantiations
template class PQFlashIndex<uint8_t>;
template class PQFlashIndex<int8_t>;
template class PQFlashIndex<float>;
template class PQFlashIndex<uint8_t, uint16_t>;
template class PQFlashIndex<int8_t, uint16_t>;
template class PQFlashIndex<float, uint16_t>;
template class PQFlashIndex<float, int64_t>;

} // namespace diskann
