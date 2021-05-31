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

#include "ob_trans_define.h"
#include "ob_trans_status.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace transaction {
using namespace common;

static inline uint64_t cal_hash(const ObTransID& trans_id)
{
  return trans_id.hash();
}

int ObTransStatusMgr::set_status(const ObTransID& trans_id, const ObTransStatus& status)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const uint64_t hv = cal_hash(trans_id);
    ObSpinLock& lock = lock_[hv % MAX_LOCK];
    ObTransStatus& trans_status = status_array_[hv % MAX_TRANS_STATUS];
    ObSpinLockGuard guard(lock);
    trans_status = status;
  }
  return ret;
}

int ObTransStatusMgr::get_status(const ObTransID& trans_id, ObTransStatus& status)
{
  int ret = OB_SUCCESS;
  if (!trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const uint64_t hv = cal_hash(trans_id);
    ObSpinLock& lock = lock_[hv % MAX_LOCK];
    ObTransStatus& trans_status = status_array_[hv % MAX_TRANS_STATUS];
    ObSpinLockGuard guard(lock);
    if (trans_id == trans_status.get_trans_id()) {
      status = trans_status;
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
