// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include "abstract_graph_store.h"

namespace diskann
{

class InMemGraphStore : public AbstractGraphStore
{
  public:
    InMemGraphStore(const size_t max_pts);

    int load(const std::string &index_path_prefix) override;
    int store(const std::string &index_path_prefix) override;

    void get_adj_list(const location_t i, std::vector<location_t> &neighbors) override;
    void set_adj_list(const location_t i, std::vector<location_t> &neighbors) override;
};

} // namespace diskann
