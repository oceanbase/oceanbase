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

#include "ob_tenant_mutil_allocator.h"
#include "lib/objectpool/ob_concurrency_objpool.h"

namespace oceanbase {
using namespace clog;
using namespace election;
using namespace share;
namespace common {
int ObTenantMutilAllocator::choose_blk_size(int obj_size)
{
  static const int MIN_SLICE_CNT = 64;
  int blk_size = OB_MALLOC_NORMAL_BLOCK_SIZE;  // default blk size is 8KB
  if (obj_size <= 0) {
  } else if (MIN_SLICE_CNT <= (OB_MALLOC_NORMAL_BLOCK_SIZE / obj_size)) {
  } else if (MIN_SLICE_CNT <= (OB_MALLOC_MIDDLE_BLOCK_SIZE / obj_size)) {
    blk_size = OB_MALLOC_MIDDLE_BLOCK_SIZE;
  } else {
    blk_size = OB_MALLOC_BIG_BLOCK_SIZE;
  }
  return blk_size;
}

void* ObTenantMutilAllocator::alloc_log_task_buf()
{
  void* ptr = log_task_alloc_.alloc();
  if (NULL != ptr) {
    ATOMIC_INC(&log_task_alloc_count_);
  } else {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      OB_LOG(WARN,
          "alloc_log_task_buf failed",
          K(tenant_id_),
          K(log_task_alloc_count_),
          "hold",
          log_task_alloc_.hold(),
          "limit",
          log_task_alloc_.limit());
    }
  }
  return ptr;
}

void ObTenantMutilAllocator::free_log_task_buf(void* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ATOMIC_DEC(&log_task_alloc_count_);
    log_task_alloc_.free(ptr);
  }
}

void* ObTenantMutilAllocator::ge_alloc(const int64_t size)
{
  void* ptr = NULL;
  ptr = clog_ge_alloc_.alloc(size);
  return ptr;
}

void ObTenantMutilAllocator::ge_free(void* ptr)
{
  clog_ge_alloc_.free(ptr);
}

ObLogFlushTask* ObTenantMutilAllocator::alloc_log_flush_task()
{
  ObLogFlushTask* ret_ptr = NULL;
  void* ptr = log_flush_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObLogFlushTask();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_log_flush_task(ObLogFlushTask* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObLogFlushTask();
    log_flush_task_alloc_.free(ptr);
  }
}

ObFetchLogTask* ObTenantMutilAllocator::alloc_fetch_log_task()
{
  ObFetchLogTask* ret_ptr = NULL;
  void* ptr = fetch_log_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObFetchLogTask();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_fetch_log_task(ObFetchLogTask* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObFetchLogTask();
    fetch_log_task_alloc_.free(ptr);
  }
}

ObLogStateEventTaskV2* ObTenantMutilAllocator::alloc_log_event_task()
{
  ObLogStateEventTaskV2* ret_ptr = NULL;
  void* ptr = log_event_task_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObLogStateEventTaskV2();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_log_event_task(ObLogStateEventTaskV2* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObLogStateEventTaskV2();
    log_event_task_alloc_.free(ptr);
  }
}

ObTraceProfile* ObTenantMutilAllocator::alloc_trace_profile()
{
  ObTraceProfile* ret_ptr = NULL;
  void* ptr = trace_profile_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObTraceProfile();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_trace_profile(ObTraceProfile* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObTraceProfile();
    trace_profile_alloc_.free(ptr);
  }
}

ObBatchSubmitCtx* ObTenantMutilAllocator::alloc_batch_submit_ctx()
{
  ObBatchSubmitCtx* ret_ptr = NULL;
  void* ptr = batch_submit_ctx_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObBatchSubmitCtx();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_batch_submit_ctx(ObBatchSubmitCtx* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObBatchSubmitCtx();
    batch_submit_ctx_alloc_.free(ptr);
  }
}

ObBatchSubmitDiskTask* ObTenantMutilAllocator::alloc_batch_submit_dtask()
{
  ObBatchSubmitDiskTask* ret_ptr = NULL;
  void* ptr = batch_submit_dtask_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObBatchSubmitDiskTask();
  }
  return ret_ptr;
}

const ObBlockAllocMgr& ObTenantMutilAllocator::get_clog_blk_alloc_mgr() const
{
  return clog_blk_alloc_;
}

void ObTenantMutilAllocator::free_batch_submit_dtask(ObBatchSubmitDiskTask* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObBatchSubmitDiskTask();
    batch_submit_dtask_alloc_.free(ptr);
  }
}

ObElection* ObTenantMutilAllocator::alloc_election()
{
  ObElection* ret_ptr = NULL;
  void* ptr = election_alloc_.alloc();
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObElection();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_election(ObElection* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObElection();
    election_alloc_.free(ptr);
  }
}

ObElectionGroup* ObTenantMutilAllocator::alloc_election_group()
{
  void* ptr = election_group_alloc_.alloc();
  ObElectionGroup* ret_ptr = NULL;
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObElectionGroup();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_election_group(ObElectionGroup* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObElectionGroup();
    election_group_alloc_.free(ptr);
  }
}

ObIPartitionLogService* ObTenantMutilAllocator::alloc_partition_log_service()
{
  void* ptr = partition_log_service_alloc_.alloc();
  ObIPartitionLogService* ret_ptr = NULL;
  if (NULL != ptr) {
    ret_ptr = new (ptr) ObPartitionLogService();
  }
  return ret_ptr;
}

void ObTenantMutilAllocator::free_partition_log_service(ObIPartitionLogService* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ptr->~ObIPartitionLogService();
    partition_log_service_alloc_.free(ptr);
  }
}

void* ObTenantMutilAllocator::alloc_replay_task_buf(const bool is_inner_table, const int64_t size)
{
  void* ptr = NULL;
  ObVSliceAlloc& allocator = is_inner_table ? inner_table_replay_task_alloc_ : user_table_replay_task_alloc_;
  ptr = allocator.alloc(size);
  return ptr;
}

void ObTenantMutilAllocator::free_replay_task(const bool is_inner_table, void* ptr)
{
  if (OB_LIKELY(NULL != ptr)) {
    ObVSliceAlloc& allocator = is_inner_table ? inner_table_replay_task_alloc_ : user_table_replay_task_alloc_;
    allocator.free(ptr);
  }
}

bool ObTenantMutilAllocator::can_alloc_replay_task(const bool is_inner_table, int64_t size) const
{
  const ObVSliceAlloc& allocator = is_inner_table ? inner_table_replay_task_alloc_ : user_table_replay_task_alloc_;
  return allocator.can_alloc_block(size);
}

void ObTenantMutilAllocator::inc_pending_replay_mutator_size(int64_t size)
{
  ATOMIC_AAF(&pending_replay_mutator_size_, size);
}

void ObTenantMutilAllocator::dec_pending_replay_mutator_size(int64_t size)
{
  ATOMIC_SAF(&pending_replay_mutator_size_, size);
}

int64_t ObTenantMutilAllocator::get_pending_replay_mutator_size() const
{
  return ATOMIC_LOAD(&pending_replay_mutator_size_);
}

void ObTenantMutilAllocator::set_nway(const int32_t nway)
{
  if (nway > 0) {
    log_task_alloc_.set_nway(nway);
    log_flush_task_alloc_.set_nway(nway);
    fetch_log_task_alloc_.set_nway(nway);
    log_event_task_alloc_.set_nway(nway);
    //    trace_profile_alloc_.set_nway(nway);;
    batch_submit_ctx_alloc_.set_nway(nway);
    batch_submit_dtask_alloc_.set_nway(nway);
    clog_ge_alloc_.set_nway(nway);
    election_alloc_.set_nway(nway);
    election_group_alloc_.set_nway(nway);
    clog_ge_alloc_.set_nway(nway);
    inner_table_replay_task_alloc_.set_nway(nway);
    user_table_replay_task_alloc_.set_nway(nway);
    OB_LOG(INFO, "finish set nway", K(tenant_id_), K(nway));
  }
}

void ObTenantMutilAllocator::set_limit(const int64_t total_limit)
{
  if (total_limit > 0 && total_limit != ATOMIC_LOAD(&total_limit_)) {
    ATOMIC_STORE(&total_limit_, total_limit);
    const int64_t clog_limit = total_limit / 100 * CLOG_MEM_LIMIT_PERCENT;
    const int64_t replay_limit = std::min(total_limit / 100 * REPLAY_MEM_LIMIT_PERCENT, REPLAY_MEM_LIMIT_THRESHOLD);
    const int64_t inner_table_replay_limit = replay_limit * INNER_TABLE_REPLAY_MEM_PERCENT / 100;
    const int64_t user_table_replay_limit = replay_limit * (100 - INNER_TABLE_REPLAY_MEM_PERCENT) / 100;
    const int64_t common_limit = total_limit - (clog_limit + replay_limit);
    clog_blk_alloc_.set_limit(clog_limit);
    inner_table_replay_blk_alloc_.set_limit(inner_table_replay_limit);
    user_table_replay_blk_alloc_.set_limit(user_table_replay_limit);
    common_blk_alloc_.set_limit(common_limit);
    OB_LOG(INFO,
        "ObTenantMutilAllocator set tenant mem limit finished",
        K(tenant_id_),
        K(total_limit),
        K(clog_limit),
        K(replay_limit),
        K(common_limit),
        K(inner_table_replay_limit),
        K(user_table_replay_limit));
  }
}

int64_t ObTenantMutilAllocator::get_limit() const
{
  return ATOMIC_LOAD(&total_limit_);
}

int64_t ObTenantMutilAllocator::get_hold() const
{
  return clog_blk_alloc_.hold() + inner_table_replay_blk_alloc_.hold() + user_table_replay_blk_alloc_.hold() +
         common_blk_alloc_.hold();
}

#define SLICE_FREE_OBJ(name, cls)                                                                                \
  void ob_slice_free_##name(typeof(cls)* ptr)                                                                    \
  {                                                                                                              \
    if (NULL != ptr) {                                                                                           \
      ObBlockSlicer::Item* item = (ObBlockSlicer::Item*)ptr - 1;                                                 \
      if (NULL != item->host_) {                                                                                 \
        ObTenantMutilAllocator* tma = reinterpret_cast<ObTenantMutilAllocator*>(item->host_->get_tmallocator()); \
        if (NULL != tma) {                                                                                       \
          tma->free_##name(ptr);                                                                                 \
        }                                                                                                        \
      }                                                                                                          \
    }                                                                                                            \
  }

SLICE_FREE_OBJ(election, ObElection);
SLICE_FREE_OBJ(election_group, ObElectionGroup);
SLICE_FREE_OBJ(log_flush_task, ObLogFlushTask);
SLICE_FREE_OBJ(fetch_log_task, ObFetchLogTask);
SLICE_FREE_OBJ(log_event_task, ObLogStateEventTaskV2);
SLICE_FREE_OBJ(trace_profile, ObTraceProfile);
SLICE_FREE_OBJ(batch_submit_ctx, ObBatchSubmitCtx);
SLICE_FREE_OBJ(batch_submit_dtask, ObBatchSubmitDiskTask);
SLICE_FREE_OBJ(partition_log_service, ObIPartitionLogService);

}  // namespace common
}  // namespace oceanbase
