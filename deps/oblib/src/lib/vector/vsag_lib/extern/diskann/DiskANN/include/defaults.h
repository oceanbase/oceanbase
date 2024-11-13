// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once
#include <stdint.h>

namespace diskann
{
namespace defaults
{
const float ALPHA = 1.2f;
const uint32_t NUM_THREADS = 0;
const uint32_t MAX_OCCLUSION_SIZE = 750;
const uint32_t FILTER_LIST_SIZE = 0;
const uint32_t NUM_FROZEN_POINTS_STATIC = 0;
const uint32_t NUM_FROZEN_POINTS_DYNAMIC = 1;
// following constants should always be specified, but are useful as a
// sensible default at cli / python boundaries
const uint32_t MAX_DEGREE = 64;
const uint32_t BUILD_LIST_SIZE = 100;
const uint32_t SATURATE_GRAPH = false;
const uint32_t SEARCH_LIST_SIZE = 100;
} // namespace defaults
} // namespace diskann
