/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_server_object_pool.h"

using namespace oceanbase::common;

ObServerObjectPoolRegistry::PoolPair ObServerObjectPoolRegistry::pool_list_[ObServerObjectPoolRegistry::MAX_POOL_NUM];
int64_t ObServerObjectPoolRegistry::pool_num_ = 0;
