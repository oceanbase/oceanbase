// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <shared_mutex>
#include <memory>
#include <map>

#include "tsl/robin_map.h"
#include "tsl/robin_set.h"
#include "tsl/sparse_map.h"
// #include "boost/dynamic_bitset.hpp"

#include "abstract_data_store.h"

#include "distance.h"
#include "natural_number_map.h"
#include "natural_number_set.h"

namespace diskann
{
template <typename data_t> class InMemDataStore : public AbstractDataStore<data_t>
{
  public:
    InMemDataStore(const location_t capacity, const size_t dim, std::shared_ptr<Distance<data_t>> distance_fn);
    virtual ~InMemDataStore();

    virtual location_t load(const std::string &filename) override;
    virtual location_t load(std::stringstream &in) override;
    virtual size_t save(const std::string &filename, const location_t num_points) override;
    virtual size_t save(std::stringstream &out, const location_t num_points) override;

    virtual size_t get_aligned_dim() const override;

    // Populate internal data from unaligned data while doing alignment and any
    // normalization that is required.
    virtual void populate_data(const data_t *vectors, const location_t num_pts) override;
    virtual void populate_data(const data_t *vectors, const location_t num_pts, const boost::dynamic_bitset<>& mask) override;
    virtual void populate_data(const std::string &filename, const size_t offset) override;
    virtual void link_data(const data_t *vectors, const location_t num_pts, const boost::dynamic_bitset<>& mask) override;

    virtual void extract_data_to_bin(const std::string &filename, const location_t num_pts) override;

    virtual void get_vector(location_t loc, data_t *target) const override;
    virtual void set_vector(const location_t loc, const data_t *const vector) override;
    virtual void prefetch_vector(location_t loc) override;

    virtual void move_vectors(const location_t old_location_start, const location_t new_location_start,
                              const location_t num_points) override;
    virtual void copy_vectors(const location_t from_loc, const location_t to_loc, const location_t num_points) override;

    virtual float get_distance(const data_t *query, location_t loc) const override;
    virtual float get_distance(location_t loc1, location_t loc2) const override;

    virtual location_t calculate_medoid() const override;

    virtual Distance<data_t> *get_dist_fn() override;

    virtual size_t get_alignment_factor() const override;

  protected:
    virtual location_t expand(const location_t new_size) override;
    virtual location_t shrink(const location_t new_size) override;

    virtual location_t load_impl(const std::string &filename);
    virtual location_t load_impl(std::stringstream &in);
#ifdef EXEC_ENV_OLS
    virtual location_t load_impl(AlignedFileReader &reader);
#endif

  private:
    data_t *_data = nullptr;

    size_t _aligned_dim;

    // It may seem weird to put distance metric along with the data store class,
    // but this gives us perf benefits as the datastore can do distance
    // computations during search and compute norms of vectors internally without
    // have to copy data back and forth.
    std::shared_ptr<Distance<data_t>> _distance_fn;

    // in case we need to save vector norms for optimization
    std::shared_ptr<float[]> _pre_computed_norms;

    bool _use_data_reference = false;
    std::vector<size_t> _loc_to_memory_index;
};

} // namespace diskann