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

#include "lib/allocator/ob_hazard_ref.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase;
using namespace oceanbase::lib;
using namespace oceanbase::common;

uint64_t *HazardRef::acquire_ref()
{
  int64_t start_idx = get_itid() * THREAD_REF_COUNT_LIMIT;
  int64_t end_idx = min(start_idx + THREAD_REF_COUNT_LIMIT, TOTAL_REF_COUNT_LIMIT);
  uint64_t *ref = NULL;
  for (int64_t i = start_idx; i < end_idx; i++) {
    if (ref_array_[i] == INVALID_VERSION) {
      ref = ref_array_ + i;
      ATOMIC_STORE(ref, ATOMIC_LOAD(&cur_ver_));
      break;
    }
  }
  if (debug_) {
    if (NULL != ref) {
      _OB_LOG(INFO, "acquire_ref: ref=%p ver=%ld %s", ref, *ref, lbt());
    } else {
      _OB_LOG(INFO, "acquire_ref fail: ref=NULL %s", lbt());
    }
  }
  return ref;
}

void HazardRef::release_ref(uint64_t *ref)
{
  if (NULL != ref) {
    if (debug_) {
      _OB_LOG(INFO, "release_ref: ref=%p ver=%ld %s", ref, *ref, lbt());
    }
    *ref = INVALID_VERSION;
  }
}

void HazardNodeList::push(HazardNode *node)
{
  if (OB_ISNULL(node) || OB_ISNULL(tail_)) {
    _OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid node");
  } else {
    count_++;
    node->next_ = tail_->next_;
    tail_->next_ = node;
    tail_ = node;
  }
}

void RetireList::set_reclaim_version(uint64_t version)
{
  _OB_LOG(DEBUG, "reclaim_version: %ld", version);
  inc_update((int64_t*)&hazard_version_, (int64_t)version);
}

void RetireList::set_retire_version(uint64_t version)
{
  ThreadRetireList *retire_list = NULL;
  _OB_LOG(DEBUG, "retire_version: %ld", version);
  if (NULL != (retire_list = get_thread_retire_list())) {
    retire_list->prepare_list_.set_version(version);
    retire_list->retire_list_.concat(retire_list->prepare_list_);
  }
}
