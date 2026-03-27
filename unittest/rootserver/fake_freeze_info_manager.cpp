/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "fake_freeze_info_manager.h"

namespace oceanbase
{
namespace rootserver
{
FakeFreezeInfoManager::FakeFreezeInfoManager()
    : is_primary_cluster_(true)
{}

bool FakeFreezeInfoManager::is_primary_cluster() const
{
  return is_primary_cluster_;
}

} // namespace rootserver
} // namespace oceanbase
