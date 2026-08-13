/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX RS

#include "fake_zone_merge_manager.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::share;
using namespace oceanbase::common;

int FakeZoneMergeManager::set_global_merge_info(const ObGlobalMergeInfo &global_merge_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(global_merge_info_.assign(global_merge_info))) {
    LOG_WARN("fail to assign global merge info", K(ret), K(global_merge_info));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase