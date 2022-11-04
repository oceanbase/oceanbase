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

#include "lib/alloc/alloc_func.h"
#include "storage/tx_storage/ob_tenant_freezer_common.h"

namespace oceanbase
{
using namespace lib;
namespace storage
{

DEF_TO_STRING(ObTenantFreezeArg)
{
  int64_t pos = 0;
  J_KV(K_(freeze_type));
  return pos;
}

OB_SERIALIZE_MEMBER(ObTenantFreezeArg,
                    freeze_type_,
                    try_frozen_scn_);

ObTenantInfo::ObTenantInfo()
  :	tenant_id_(INT64_MAX),
    mem_lower_limit_(0),
    mem_upper_limit_(0),
    mem_memstore_limit_(0),
    is_loaded_(false),
    is_freezing_(false),
    last_freeze_clock_(0),
    frozen_scn_(0),
    freeze_cnt_(0),
    last_halt_ts_(0),
    slow_freeze_(false),
    slow_freeze_timestamp_(0),
    slow_freeze_min_protect_clock_(INT64_MAX)
{
}

void ObTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID; // i64 max as invalid.
  mem_memstore_limit_ = 0;
  mem_lower_limit_ = 0;
  mem_upper_limit_ = 0;
  is_loaded_ = false;
  is_freezing_ = false;
  frozen_scn_ = 0;
  freeze_cnt_ = 0;
  last_halt_ts_ = 0;
  slow_freeze_ = false;
  slow_freeze_timestamp_ = 0;
  slow_freeze_min_protect_clock_ = INT64_MAX;
  slow_tablet_.reset();
}

int ObTenantInfo::update_frozen_scn(int64_t frozen_scn)
{
  int ret = OB_SUCCESS;

  if (frozen_scn > frozen_scn_) {
    frozen_scn_ = frozen_scn;
    freeze_cnt_ = 0;
  }

  return ret;
}

int64_t ObTenantInfo::mem_memstore_left() const
{
  uint64_t memstore_hold = get_tenant_memory_hold(tenant_id_, ObCtxIds::MEMSTORE_CTX_ID);
  return max(0, mem_memstore_limit_ - (int64_t)memstore_hold);
}

} // storage
} // oceanbase
