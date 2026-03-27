/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/allocator/ob_mem_leak_checker.h"

using namespace oceanbase::lib;
namespace oceanbase
{
namespace common
{
ObSimpleRateLimiter ObMemLeakChecker::rl_{INT64_MAX};
constexpr const char ObMemLeakChecker::MOD_INFO_MAP_STR[];

} // end of namespace common
} // end of namespace oceanbase
