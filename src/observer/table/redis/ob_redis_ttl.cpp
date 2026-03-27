/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER

#include "ob_redis_ttl.h"

namespace oceanbase
{
namespace table
{
void ObRedisTTLCtx::reset()
{
  meta_ = nullptr;
  return_meta_ = false;
  model_ = ObRedisDataModel::MODEL_MAX;
}
}  // namespace table
}  // namespace oceanbase
