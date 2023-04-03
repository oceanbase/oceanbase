/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX RS

#include "fake_zone_merge_manager.h"

namespace oceanbase
{
namespace rootserver
{
using namespace oceanbase::share;
using namespace oceanbase::common;

int FakeZoneMergeManager::add_zone_merge_info(const ObZoneMergeInfo& zone_merge_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(zone_merge_infos_[zone_count_].assign(zone_merge_info))) {
    LOG_WARN("fail to assign zone merge info", K(ret), K(zone_merge_info));
  } else {
    ++zone_count_;
  }
  return ret;
}

int FakeZoneMergeManager::update_zone_merge_info(const ObZoneMergeInfo& zone_merge_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool exist = false;
    for (int64_t i = 0; (i < zone_count_) && OB_SUCC(ret); ++i) {
      if ((zone_merge_infos_[i].tenant_id_ == zone_merge_info.tenant_id_)
          && (zone_merge_infos_[i].zone_ == zone_merge_info.zone_)) {
        exist = true;
        if (OB_FAIL(zone_merge_infos_[i].assign(zone_merge_info))) {
          LOG_WARN("fail to assign zone merge info", K(ret), K(i), K(zone_merge_info));
        }
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (!exist) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
  }
  return ret;
}

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