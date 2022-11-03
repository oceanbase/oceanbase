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

#include "ob_gts_info.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
ObGtsInfo::ObGtsInfo()
{
  reset();
}

void ObGtsInfo::reset()
{
  gts_id_ = OB_INVALID_ID;
  gts_name_.reset();
  region_.reset();
  epoch_id_ = OB_INVALID_TIMESTAMP;
  member_list_.reset();
  standby_.reset();
  heartbeat_ts_ = OB_INVALID_TIMESTAMP;
}

bool ObGtsInfo::is_valid() const
{
  // standby cluster cannot guarantee that it will always be a valid value
  return is_valid_gts_id(gts_id_)
         && !gts_name_.is_empty()
         && !region_.is_empty()
         && (OB_INVALID_TIMESTAMP != epoch_id_)
         && member_list_.is_valid()
         && (OB_INVALID_TIMESTAMP != heartbeat_ts_);
}

int ObGtsInfo::assign(const ObGtsInfo &that)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!that.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(that));
  } else {
    gts_id_ = that.gts_id_;
    gts_name_ = that.gts_name_;
    region_ = that.region_;
    epoch_id_ = that.epoch_id_;
    member_list_ = that.member_list_;
    standby_ = that.standby_;
    heartbeat_ts_ = that.heartbeat_ts_;
  }
  return ret;
}

ObGtsTenantInfo::ObGtsTenantInfo()
{
  reset();
}

void ObGtsTenantInfo::reset()
{
  gts_id_ = OB_INVALID_ID;
  tenant_id_ = OB_INVALID_ID;
  member_list_.reset();
}

bool ObGtsTenantInfo::is_valid() const
{
  return is_valid_gts_id(gts_id_) && is_valid_no_sys_tenant_id(tenant_id_)
         && member_list_.is_valid();
}
} // namespace common
} // namespace oceanbase
