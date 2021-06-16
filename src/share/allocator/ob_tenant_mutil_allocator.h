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

#ifndef _OB_SHARE_TENANT_MUTIL_ALLOCATOR_H_
#define _OB_SHARE_TENANT_MUTIL_ALLOCATOR_H_

#include "common/ob_trace_profile.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/ob_block_alloc_mgr.h"
#include "lib/allocator/ob_slice_alloc.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "lib/queue/ob_link.h"
#include "clog/ob_log_task.h"
#include "clog/ob_log_flush_task.h"
#include "clog/ob_log_event_task_V2.h"
#include "clog/ob_fetch_log_engine.h"
#include "clog/ob_batch_submit_ctx.h"
#include "clog/ob_batch_submit_task.h"
#include "clog/ob_partition_log_service.h"
#include "election/ob_election.h"
#include "election/ob_election_group.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase {
namespace common {
// Interface for Clog module
class ObILogAllocator {
public:
  ObILogAllocator()
  {}
  virtual ~ObILogAllocator()
  {}

public:
  virtual void* alloc_log_task_buf() = 0;
  virtual void free_log_task_buf(void* ptr) = 0;
  virtual void* ge_alloc(const int64_t size) = 0;
  virtual void ge_free(void* ptr) = 0;
  virtual clog::ObLogFlushTask* alloc_log_flush_task() = 0;
  virtual void free_log_flush_task(clog::ObLogFlushTask* ptr) = 0;
  virtual clog::ObFetchLogTask* alloc_fetch_log_task() = 0;
  virtual void free_fetch_log_task(clog::ObFetchLogTask* ptr) = 0;
  virtual clog::ObLogStateEventTaskV2* alloc_log_event_task() = 0;
  virtual void free_log_event_task(clog::ObLogStateEventTaskV2* ptr) = 0;
  virtual common::ObTraceProfile* alloc_trace_profile() = 0;
  virtual void free_trace_profile(common::ObTraceProfile* ptr) = 0;
  virtual clog::ObBatchSubmitCtx* alloc_batch_submit_ctx() = 0;
  virtual void free_batch_submit_ctx(clog::ObBatchSubmitCtx* ptr) = 0;
  virtual clog::ObBatchSubmitDiskTask* alloc_batch_submit_dtask() = 0;
  virtual void free_batch_submit_dtask(clog::ObBatchSubmitDiskTask* ptr) = 0;
  virtual clog::ObIPartitionLogService* alloc_partition_log_service() = 0;
  virtual void free_partition_log_service(clog::ObIPartitionLogService* ptr) = 0;
  virtual const ObBlockAllocMgr& get_clog_blk_alloc_mgr() const = 0;
};

// Interface for ReplayEngine module
class ObIReplayTaskAllocator {
public:
  ObIReplayTaskAllocator()
  {}
  virtual ~ObIReplayTaskAllocator()
  {}

public:
  virtual void* alloc_replay_task_buf(const bool is_inner_table, const int64_t size) = 0;
  virtual void free_replay_task(const bool is_inner_table, void* ptr) = 0;
  virtual bool can_alloc_replay_task(const bool is_inner_table, int64_t size) const = 0;
  virtual void inc_pending_replay_mutator_size(int64_t size) = 0;
  virtual void dec_pending_replay_mutator_size(int64_t size) = 0;
  virtual int64_t get_pending_replay_mutator_size() const = 0;
};

class ObTenantMutilAllocator : public ObILogAllocator, public ObIReplayTaskAllocator, public common::ObLink {
public:
  const int LOG_TASK_SIZE = sizeof(clog::ObLogTask);
  const int LOG_FLUSH_TASK_SIZE = sizeof(clog::ObLogFlushTask);
  const int LOG_FETCH_TASK_SIZE = sizeof(clog::ObFetchLogTask);
  const int LOG_EVENT_TASK_SIZE = sizeof(clog::ObLogStateEventTaskV2);
  const int TRACE_PROFILE_SIZE = sizeof(common::ObTraceProfile);
  const int BATCH_SUBMIT_CTX_SIZE = sizeof(clog::ObBatchSubmitCtx);
  const int BATCH_SUBMIT_DTASK_SIZE = sizeof(clog::ObBatchSubmitDiskTask);
  const int ELECTION_SIZE = sizeof(election::ObElection);
  const int ELECTION_GROUP_SIZE = sizeof(election::ObElectionGroup);
  const int PARTITION_LOG_SERVICE_SIZE = sizeof(clog::ObPartitionLogService);
  // The memory percent of clog
  const int64_t CLOG_MEM_LIMIT_PERCENT = 30;
  // The memory percent of replay engine
  const int64_t REPLAY_MEM_LIMIT_PERCENT = 25;
  // The memory limit of replay engine
  const int64_t REPLAY_MEM_LIMIT_THRESHOLD = 512 * 1024 * 1024ll;
  // The memory percent of replay engine for inner_table
  const int64_t INNER_TABLE_REPLAY_MEM_PERCENT = 20;
  static int choose_blk_size(int obj_size);

public:
  explicit ObTenantMutilAllocator(uint64_t tenant_id)
      : tenant_id_(tenant_id),
        total_limit_(INT64_MAX),
        log_task_alloc_count_(0),
        pending_replay_mutator_size_(0),
        clog_blk_alloc_(),
        inner_table_replay_blk_alloc_(REPLAY_MEM_LIMIT_THRESHOLD * INNER_TABLE_REPLAY_MEM_PERCENT / 100),
        user_table_replay_blk_alloc_(REPLAY_MEM_LIMIT_THRESHOLD * (100 - INNER_TABLE_REPLAY_MEM_PERCENT) / 100),
        common_blk_alloc_(),
        unlimited_blk_alloc_(),
        log_task_alloc_(LOG_TASK_SIZE, ObMemAttr(tenant_id, ObModIds::OB_LOG_TASK), choose_blk_size(LOG_TASK_SIZE),
            clog_blk_alloc_, this),
        log_flush_task_alloc_(LOG_FLUSH_TASK_SIZE, ObMemAttr(tenant_id, ObModIds::OB_LOG_FLUSH_TASK),
            choose_blk_size(LOG_FLUSH_TASK_SIZE), clog_blk_alloc_, this),
        fetch_log_task_alloc_(LOG_FETCH_TASK_SIZE, ObMemAttr(tenant_id, ObModIds::OB_LOG_FETCH_TASK),
            choose_blk_size(LOG_FETCH_TASK_SIZE), clog_blk_alloc_, this),
        log_event_task_alloc_(LOG_EVENT_TASK_SIZE, ObMemAttr(tenant_id, ObModIds::OB_LOG_EVENT_TASK),
            choose_blk_size(LOG_EVENT_TASK_SIZE), common_blk_alloc_, this),
        trace_profile_alloc_(TRACE_PROFILE_SIZE, ObMemAttr(tenant_id, ObModIds::OB_LOG_TRACE_PROFILE),
            choose_blk_size(TRACE_PROFILE_SIZE), clog_blk_alloc_, this),
        batch_submit_ctx_alloc_(BATCH_SUBMIT_CTX_SIZE, ObMemAttr(tenant_id, ObModIds::OB_CLOG_BATCH_SUBMIT_CTX),
            choose_blk_size(BATCH_SUBMIT_CTX_SIZE), clog_blk_alloc_, this),
        batch_submit_dtask_alloc_(BATCH_SUBMIT_DTASK_SIZE,
            ObMemAttr(tenant_id, ObModIds::OB_CLOG_BATCH_SUBMIT_DISK_TASK), choose_blk_size(BATCH_SUBMIT_DTASK_SIZE),
            clog_blk_alloc_, this),
        clog_ge_alloc_(ObMemAttr(tenant_id, ObModIds::OB_CLOG_GE), ObVSliceAlloc::DEFAULT_BLOCK_SIZE, clog_blk_alloc_),
        election_alloc_(ELECTION_SIZE, ObMemAttr(tenant_id, ObModIds::OB_ELECTION), choose_blk_size(ELECTION_SIZE),
            common_blk_alloc_, this),
        election_group_alloc_(ELECTION_GROUP_SIZE, ObMemAttr(tenant_id, ObModIds::OB_ELECTION_GROUP),
            choose_blk_size(ELECTION_GROUP_SIZE), common_blk_alloc_, this),
        inner_table_replay_task_alloc_(ObMemAttr(tenant_id, ObModIds::OB_LOG_REPLAY_ENGINE),
            ObVSliceAlloc::DEFAULT_BLOCK_SIZE, inner_table_replay_blk_alloc_),
        user_table_replay_task_alloc_(ObMemAttr(tenant_id, ObModIds::OB_LOG_REPLAY_ENGINE),
            ObVSliceAlloc::DEFAULT_BLOCK_SIZE, user_table_replay_blk_alloc_),
        partition_log_service_alloc_(PARTITION_LOG_SERVICE_SIZE,
            ObMemAttr(tenant_id, ObModIds::OB_PARTITION_LOG_SERVICE), choose_blk_size(PARTITION_LOG_SERVICE_SIZE),
            unlimited_blk_alloc_, this)
  {
    // set_nway according to tenant's max_cpu
    double min_cpu = 0;
    double max_cpu = 0;
    omt::ObMultiTenant* omt = GCTX.omt_;
    if (NULL == omt) {
    } else if (OB_SUCCESS != omt->get_tenant_cpu(tenant_id, min_cpu, max_cpu)) {
    } else {
      const int32_t nway = (int32_t)max_cpu;
      set_nway(nway);
    }
  }
  ~ObTenantMutilAllocator()
  {}
  // update nway when tenant's max_cpu changed
  void set_nway(const int32_t nway);
  // update limit when tenant's memory_limit changed
  void set_limit(const int64_t total_limit);
  int64_t get_limit() const;
  int64_t get_hold() const;
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  inline ObTenantMutilAllocator*& get_next()
  {
    return reinterpret_cast<ObTenantMutilAllocator*&>(next_);
  }
  // interface for clog
  void* alloc_log_task_buf();
  void free_log_task_buf(void* ptr);
  void* ge_alloc(const int64_t size);
  void ge_free(void* ptr);
  clog::ObLogFlushTask* alloc_log_flush_task();
  void free_log_flush_task(clog::ObLogFlushTask* ptr);
  clog::ObFetchLogTask* alloc_fetch_log_task();
  void free_fetch_log_task(clog::ObFetchLogTask* ptr);
  clog::ObLogStateEventTaskV2* alloc_log_event_task();
  void free_log_event_task(clog::ObLogStateEventTaskV2* ptr);
  common::ObTraceProfile* alloc_trace_profile();
  void free_trace_profile(common::ObTraceProfile* ptr);
  clog::ObBatchSubmitCtx* alloc_batch_submit_ctx();
  void free_batch_submit_ctx(clog::ObBatchSubmitCtx* ptr);
  clog::ObBatchSubmitDiskTask* alloc_batch_submit_dtask();
  void free_batch_submit_dtask(clog::ObBatchSubmitDiskTask* ptr);
  clog::ObIPartitionLogService* alloc_partition_log_service();
  void free_partition_log_service(clog::ObIPartitionLogService* ptr);
  const ObBlockAllocMgr& get_clog_blk_alloc_mgr() const;
  // interface for election
  election::ObElection* alloc_election();
  void free_election(election::ObElection* ptr);
  election::ObElectionGroup* alloc_election_group();
  void free_election_group(election::ObElectionGroup* ptr);
  void* alloc_replay_task_buf(const bool is_inner_table, const int64_t size);
  void free_replay_task(const bool is_inner_table, void* ptr);
  bool can_alloc_replay_task(const bool is_inner_table, int64_t size) const;
  void inc_pending_replay_mutator_size(int64_t size);
  void dec_pending_replay_mutator_size(int64_t size);
  int64_t get_pending_replay_mutator_size() const;

private:
  uint64_t tenant_id_ CACHE_ALIGNED;
  int64_t total_limit_;
  int64_t log_task_alloc_count_;
  int64_t pending_replay_mutator_size_;
  ObBlockAllocMgr clog_blk_alloc_;
  ObBlockAllocMgr inner_table_replay_blk_alloc_;
  ObBlockAllocMgr user_table_replay_blk_alloc_;
  ObBlockAllocMgr common_blk_alloc_;
  ObBlockAllocMgr unlimited_blk_alloc_;
  ObSliceAlloc log_task_alloc_;
  ObSliceAlloc log_flush_task_alloc_;
  ObSliceAlloc fetch_log_task_alloc_;
  ObSliceAlloc log_event_task_alloc_;
  ObSliceAlloc trace_profile_alloc_;
  ObSliceAlloc batch_submit_ctx_alloc_;    // for ObBatchSubmitCtx
  ObSliceAlloc batch_submit_dtask_alloc_;  // for ObBatchSubmitDiskTask
  ObVSliceAlloc clog_ge_alloc_;
  ObSliceAlloc election_alloc_;
  ObSliceAlloc election_group_alloc_;
  ObVSliceAlloc inner_table_replay_task_alloc_;
  ObVSliceAlloc user_table_replay_task_alloc_;
  ObSliceAlloc partition_log_service_alloc_;
};

// Free interface for class-object allocated by slice_alloc
void ob_slice_free_election(election::ObElection* ptr);
void ob_slice_free_election_group(election::ObElectionGroup* ptr);
void ob_slice_free_log_flush_task(clog::ObLogFlushTask* ptr);
void ob_slice_free_fetch_log_task(clog::ObFetchLogTask* ptr);
void ob_slice_free_log_event_task(clog::ObLogStateEventTaskV2* ptr);
void ob_slice_free_trace_profile(common::ObTraceProfile* ptr);
void ob_slice_free_batch_submit_ctx(clog::ObBatchSubmitCtx* ptr);
void ob_slice_free_batch_submit_dtask(clog::ObBatchSubmitDiskTask* ptr);
void ob_slice_free_partition_log_service(clog::ObIPartitionLogService* ptr);

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OB_SHARE_TENANT_MUTIL_ALLOCATOR_H_ */
