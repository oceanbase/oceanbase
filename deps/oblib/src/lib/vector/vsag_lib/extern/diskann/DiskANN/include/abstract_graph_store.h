// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string>
#include <vector>

#include "types.h"

namespace diskann
{

class AbstractGraphStore
{
  public:
    AbstractGraphStore(const size_t max_pts) : _capacity(max_pts)
    {
    }

    virtual int load(const std::string &index_path_prefix) = 0;
    virtual int store(const std::string &index_path_prefix) = 0;

    virtual void get_adj_list(const location_t i, std::vector<location_t> &neighbors) = 0;
    virtual void set_adj_list(const location_t i, std::vector<location_t> &neighbors) = 0;

  private:
    size_t _capacity;
};

} // namespace diskann
