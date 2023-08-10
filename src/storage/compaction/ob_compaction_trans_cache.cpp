/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "storage/compaction/ob_compaction_trans_cache.h"

namespace oceanbase
{
using namespace storage;
namespace compaction
{
/*
 *  ---------------------------------------------ObCachedTransStateMgr----------------------------------------------
 */

int ObCachedTransStateMgr::init(int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCachedTransStateMgr has already been initiated", K(ret));
  } else if (max_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("max count is invalid", K(ret), K(max_cnt));
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(max_cnt * sizeof(ObMergeCachedTransState)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(max_cnt),
          "alloc_size", max_cnt * sizeof(ObMergeCachedTransState));
    } else {
      array_ = new(buf) ObMergeCachedTransState[max_cnt]();
      max_cnt_ = max_cnt;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObCachedTransStateMgr::destroy()
{
  if (OB_NOT_NULL(array_)) {
    allocator_.free(array_);
    array_ = nullptr;
  }
  is_inited_ = false;
}

int ObCachedTransStateMgr::get_trans_state(
  const transaction::ObTransID &trans_id,
  const transaction::ObTxSEQ &sql_seq,
  ObMergeCachedTransState &trans_state)
{
  int ret = OB_SUCCESS;
  ObMergeCachedTransKey key(trans_id, sql_seq);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCachedTransStateMgr is not initialized", K(ret));
  } else if (!key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key", K(ret), K(key));
  } else {
    uint64_t idx = cal_idx(key);
    if (array_[idx].key_ == key && array_[idx].is_valid()) {
      trans_state = array_[cal_idx(key)];
    } else {
      ret = OB_HASH_NOT_EXIST;
    }
  }
  return ret;
}

int ObCachedTransStateMgr::add_trans_state(
  const transaction::ObTransID &trans_id,
  const transaction::ObTxSEQ &sql_seq,
  const int64_t commited_trans_version,
  const int32_t trans_state,
  const int16_t can_read,
  const int16_t is_determined_state)
{
  int ret = OB_SUCCESS;
  ObMergeCachedTransKey key(trans_id, sql_seq);
  ObMergeCachedTransState status(trans_id, sql_seq, commited_trans_version, trans_state, can_read, is_determined_state);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCachedTransStateMgr is not initialized", K(ret));
  } else if (!status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid trans state", K(ret), K(status));
  } else {
    array_[cal_idx(key)] = status;
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase
