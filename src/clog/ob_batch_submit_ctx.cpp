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

#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "ob_batch_submit_ctx.h"
#include "common/ob_clock_generator.h"
#include "storage/ob_partition_service.h"
#include "ob_i_log_engine.h"
#include "ob_partition_log_service.h"

namespace oceanbase {
using namespace common;
using namespace lib;
namespace clog {
int ObBatchSubmitCtx::init(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
    const ObLogInfoArray& log_info_array, const ObLogPersistSizeArray& size_array, const ObISubmitLogCbArray& cb_array,
    const common::ObMemberList& member_list, const int64_t replica_num, const common::ObAddr& leader,
    const common::ObAddr& self, storage::ObPartitionService* partition_service, ObILogEngine* log_engine,
    common::ObILogAllocator* alloc_mgr)
{
  int ret = OB_SUCCESS;

  const int64_t partition_array_count = partition_array.count();
  const int64_t log_info_array_count = log_info_array.count();
  const int64_t log_persist_size_count = size_array.count();
  const int64_t cb_array_count = cb_array.count();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObBatchSubmitCtx init twice", K(ret), K(trans_id));
  } else if (!trans_id.is_valid() || 0 == partition_array_count || partition_array_count != log_info_array_count ||
             log_persist_size_count != log_info_array_count || partition_array_count != cb_array_count ||
             !leader.is_valid() || !self.is_valid() || leader != self || !member_list.is_valid() || replica_num <= 0 ||
             OB_ISNULL(partition_service) || OB_ISNULL(log_engine) || OB_ISNULL(alloc_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(trans_id),
        K(partition_array),
        K(log_info_array),
        K(size_array),
        K(member_list),
        K(replica_num),
        K(leader),
        K(self),
        KP(partition_service),
        KP(log_engine),
        KP(alloc_mgr));
  } else if (OB_FAIL(partition_array_.assign(partition_array))) {
    CLOG_LOG(WARN, "partition_array assign failed", K(ret), K(trans_id), K(partition_array));
  } else if (OB_FAIL(log_info_array_.assign(log_info_array))) {
    CLOG_LOG(WARN, "log_info_array assign failed", K(ret), K(trans_id), K(log_info_array));
  } else if (OB_FAIL(log_persist_size_array_.assign(size_array))) {
    CLOG_LOG(WARN, "persist_size_array assign failed", K(ret), K(trans_id), K(size_array));
  } else if (OB_FAIL(cb_array_.assign(cb_array))) {
    CLOG_LOG(WARN, "cb_array assign failed", K(ret), K(trans_id));
  } else if (OB_FAIL(member_list_.deep_copy(member_list))) {
    CLOG_LOG(WARN, "member_list_ deep_copy failed", K(ret), K(trans_id), K(member_list));
  } else {
    trans_id_ = trans_id;
    replica_num_ = replica_num;
    leader_ = leader;
    self_ = self;
    partition_service_ = partition_service;
    log_engine_ = log_engine;
    alloc_mgr_ = alloc_mgr;

    is_splited_ = false;
    is_flushed_ = false;
    for (int64_t i = 0; (OB_SUCCESS == ret) && i < partition_array_.count(); i++) {
      ObSimpleMemberList member_list;
      if (OB_FAIL(ack_list_array_.push_back(member_list))) {
        CLOG_LOG(WARN, "ack_list_array_ push_back failed", K(ret));
      }
    }
    ack_cnt_ = 0;
    log_cursor_array_.reset();
    create_ctx_ts_ = ObClockGenerator::getClock();

    is_inited_ = true;
  }

  return ret;
}

int ObBatchSubmitCtx::init(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
    const ObLogInfoArray& log_info_array, const ObLogPersistSizeArray& size_array, const common::ObAddr& leader,
    const common::ObAddr& self, storage::ObPartitionService* partition_service, ObILogEngine* log_engine,
    common::ObILogAllocator* alloc_mgr)
{
  int ret = OB_SUCCESS;

  const int64_t partition_array_count = partition_array.count();
  const int64_t log_info_array_count = log_info_array.count();
  const int64_t log_persist_size_count = size_array.count();
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObBatchSubmitCtx init twice", K(ret), K(trans_id));
  } else if (!trans_id.is_valid() || 0 == partition_array_count || partition_array_count != log_info_array_count ||
             log_persist_size_count != log_info_array_count || !leader.is_valid() || !self.is_valid() ||
             leader == self || OB_ISNULL(partition_service) || OB_ISNULL(log_engine) || OB_ISNULL(alloc_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(trans_id),
        K(partition_array),
        K(size_array),
        K(log_info_array),
        K(leader),
        K(self),
        KP(partition_service),
        KP(log_engine),
        KP(alloc_mgr));
  } else if (OB_FAIL(partition_array_.assign(partition_array))) {
    CLOG_LOG(WARN, "partition_array assign failed", K(ret), K(trans_id), K(partition_array));
  } else if (OB_FAIL(log_info_array_.assign(log_info_array))) {
    CLOG_LOG(WARN, "log_info_array assign failed", K(ret), K(trans_id), K(log_info_array));
  } else if (OB_FAIL(log_persist_size_array_.assign(size_array))) {
    CLOG_LOG(WARN, "persist_size_array assign failed", K(ret), K(trans_id), K(size_array));
  } else {
    trans_id_ = trans_id;
    leader_ = leader;
    self_ = self;
    partition_service_ = partition_service;
    log_engine_ = log_engine;
    alloc_mgr_ = alloc_mgr;

    is_splited_ = false;
    is_flushed_ = false;
    for (int64_t i = 0; (OB_SUCCESS == ret) && i < partition_array_.count(); i++) {
      ObSimpleMemberList member_list;
      if (OB_FAIL(ack_list_array_.push_back(member_list))) {
        CLOG_LOG(WARN, "ack_list_array_ push_back failed", K(ret));
      }
    }
    ack_cnt_ = 0;
    log_cursor_array_.reset();
    create_ctx_ts_ = ObClockGenerator::getClock();

    cb_array_.reset();
    member_list_.reset();
    replica_num_ = 0;

    is_inited_ = true;
  }

  return ret;
}

void ObBatchSubmitCtx::reset()
{
  ObMutexGuard guard(lock_);
  is_inited_ = false;
  trans_id_.reset();
  partition_array_.reset();
  log_info_array_.reset();
  log_persist_size_array_.reset();
  cb_array_.reset();
  member_list_.reset();
  replica_num_ = 0;
  leader_.reset();
  self_.reset();
  partition_service_ = NULL;
  log_engine_ = NULL;
  alloc_mgr_ = NULL;

  is_splited_ = false;
  is_flushed_ = false;
  ack_list_array_.reset();
  ack_cnt_ = 0;
  log_cursor_array_.reset();
  create_ctx_ts_ = OB_INVALID_TIMESTAMP;
}

void ObBatchSubmitCtx::destroy()
{
  ObMutexGuard guard(lock_);
  if (is_inited_) {
    is_inited_ = false;
    trans_id_.reset();
    partition_array_.reset();
    log_persist_size_array_.reset();
    for (int64_t index = 0; index < log_info_array_.count(); index++) {
      void* ptr = static_cast<void*>(const_cast<char*>(log_info_array_[index].get_buf()));
      alloc_mgr_->ge_free(ptr);
      ptr = NULL;
    }
    log_info_array_.reset();
    cb_array_.reset();
    member_list_.reset();
    replica_num_ = 0;
    leader_.reset();
    self_.reset();
    partition_service_ = NULL;
    log_engine_ = NULL;
    alloc_mgr_ = NULL;

    is_splited_ = false;
    is_flushed_ = false;
    ack_list_array_.reset();
    ack_cnt_ = 0;
    log_cursor_array_.reset();
    create_ctx_ts_ = OB_INVALID_TIMESTAMP;
  }
}

int ObBatchSubmitCtx::try_split()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObBatchSubmitCtx is not inited", K(ret), K(trans_id_));
    // no need lock
  } else if (need_split_()) {
    if (OB_FAIL(split_with_lock_()) && OB_NO_NEED_BATCH_CTX != ret) {
      CLOG_LOG(ERROR, "split_with_lock_ failed", K(ret), K(trans_id_));
    }
  }

  return ret;
}

int ObBatchSubmitCtx::flush_cb(const ObLogCursor& base_log_cursor)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(ERROR, "ObBatchSubmitCtx is not inited", K(ret), K(trans_id_));
  } else if (!base_log_cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argumets", K(ret), K(trans_id_), K(base_log_cursor));
  } else {
    ObMutexGuard guard(lock_);

    if (is_splited_) {
      // already splited, do nothing, splitting thread will delete ctx later
    } else if (is_majority_() || is_flushed_) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "already majority or flushed, unexpected", K(ret), K(trans_id_));
    } else {
      ObBatchAckArray batch_ack_array;
      if (OB_FAIL(make_cursor_array_(base_log_cursor))) {
        CLOG_LOG(WARN, "make_cursor_array_ failed", K(ret), K(trans_id_), K(base_log_cursor));
      } else if (OB_FAIL(backfill_log_(is_leader_(), batch_ack_array))) {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          CLOG_LOG(WARN, "backfill_log_ failed", K(ret), K(trans_id_), K(is_leader_()));
        }
      }

      if (is_leader_()) {
        if (OB_SUCCESS == ret) {
          is_flushed_ = true;
          if (is_majority_()) {
            if (OB_FAIL(backfill_confirmed_()) && OB_NO_NEED_BATCH_CTX != ret) {
              CLOG_LOG(ERROR, "backfill_confirmed_ failed", K(ret), K(trans_id_));
            }
          }
        } else if (OB_FAIL(split_()) && OB_NO_NEED_BATCH_CTX != ret) {
          CLOG_LOG(ERROR, "split_ failed", K(ret), K(trans_id_));
        }
      } else {
        if (OB_SUCCESS == ret && OB_FAIL(log_engine_->submit_batch_ack(leader_, trans_id_, batch_ack_array))) {
          CLOG_LOG(WARN, "submit_batch_ack failed", K(ret), K(leader_), K(trans_id_), K(batch_ack_array));
        }
        ret = OB_NO_NEED_BATCH_CTX;
      }

      // rewrite ret value, batch submit logic can handle case whether flush_cb succeed or not.
      if (OB_NO_NEED_BATCH_CTX != ret) {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObBatchSubmitCtx::ack_log(const common::ObAddr& server, const ObBatchAckArray& batch_ack_array)
{
  int ret = OB_SUCCESS;

  ObMutexGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObBatchSubmitCtx is not inited", K(ret), K(trans_id_));
  } else if (!server.is_valid() || !member_list_.contains(server) || !is_leader_() ||
             batch_ack_array.count() != partition_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(trans_id_),
        K(member_list_),
        K(server),
        K(leader_),
        K(self_),
        K(batch_ack_array));
  } else if (is_splited_ || is_majority_()) {
    // already splited or majority, just ignore this ack
  } else if (OB_FAIL(handle_ack_array_(server, batch_ack_array))) {
    CLOG_LOG(WARN, "handle_ack_array_ failed", K(ret), K(trans_id_), K(server));
  } else if (is_majority_()) {
    if (OB_FAIL(backfill_confirmed_()) && OB_NO_NEED_BATCH_CTX != ret) {
      CLOG_LOG(ERROR, "backfill_confirmed_ failed", K(ret), K(trans_id_));
    }
  } else if (ack_need_split_()) {
    if (OB_FAIL(split_()) && OB_NO_NEED_BATCH_CTX != ret) {
      CLOG_LOG(ERROR, "split_ failed", K(ret), K(trans_id_));
    }
  }

  return ret;
}

int ObBatchSubmitCtx::make_cursor_array_(const ObLogCursor& base_log_cursor)
{
  int ret = OB_SUCCESS;
  offset_t saved_offset = base_log_cursor.offset_;
  const int64_t log_info_array_count = log_info_array_.count();
  const int64_t log_persist_size_count = log_persist_size_array_.count();
  if (OB_UNLIKELY(log_persist_size_count != log_info_array_count)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "size array is not consistent with log info array",
        K(log_persist_size_array_),
        K(log_info_array_),
        K(ret));
  } else {
    for (int64_t index = 0; OB_SUCC(ret) && index < log_info_array_.count(); index++) {
      const ObLogInfo& log_info = log_info_array_[index];
      const int32_t size = log_info.is_need_flushed() ? static_cast<int32_t>(log_persist_size_array_[index]) : 0;
      ObLogCursor tmp_cursor;

      tmp_cursor.file_id_ = base_log_cursor.file_id_;
      tmp_cursor.offset_ = saved_offset;
      tmp_cursor.size_ = size;

      if (OB_FAIL(log_cursor_array_.push_back(tmp_cursor))) {
        CLOG_LOG(WARN, "log_cursor_array_ push_back failed", K(ret), K(tmp_cursor));
      } else {
        saved_offset += size;
      }
    }
  }

  return ret;
}

int ObBatchSubmitCtx::backfill_log_(const bool is_leader, ObBatchAckArray& batch_ack_array)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  for (int64_t index = 0; index < partition_array_.count(); index++) {
    const common::ObPartitionKey& partition_key = partition_array_[index];
    const ObLogInfo& log_info = log_info_array_[index];
    const ObLogCursor& log_cursor = log_cursor_array_[index];
    ObISubmitLogCb* submit_cb = is_leader ? cb_array_[index] : NULL;
    if (!log_info.is_need_flushed()) {
      tmp_ret = OB_TOO_LARGE_LOG_ID;
    } else if (OB_SUCCESS != (tmp_ret = backfill_log_(partition_key, log_info, log_cursor, is_leader, submit_cb))) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        CLOG_LOG(WARN, "backfill_log_ failed", K(tmp_ret), K(partition_key), K(log_info));
      }
    }
    if (is_leader) {
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_FAIL(batch_ack_array.push_back(OB_SUCCESS == tmp_ret))) {
      CLOG_LOG(WARN, "batch_ack_array push_back failed", K(ret));
      break;
    }
  }

  return ret;
}

int ObBatchSubmitCtx::backfill_log_(const common::ObPartitionKey& partition_key, const ObLogInfo& log_info,
    const ObLogCursor& log_cursor, const bool is_leader, ObISubmitLogCb* submit_cb)
{
  int ret = OB_SUCCESS;

  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
      NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    ret = OB_PARTITION_NOT_EXIST;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(partition_key));
  } else if (!guard.get_partition_group()->is_valid()) {
    ret = OB_INVALID_PARTITION;
    CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key));
  } else if (OB_FAIL(log_service->backfill_log(log_info, log_cursor, is_leader, submit_cb))) {
    if (REACH_TIME_INTERVAL(1000 * 1000)) {
      CLOG_LOG(WARN, "backfill_log failed", K(ret), K(partition_key), K(log_info), K(log_cursor), K(is_leader));
    }
  }

  return ret;
}

bool ObBatchSubmitCtx::is_leader_() const
{
  return self_.is_valid() && leader_ == self_;
}

bool ObBatchSubmitCtx::is_majority_() const
{
  bool ack_bool_ = true;
  for (int64_t i = 0; ack_bool_ && i < ack_list_array_.count(); i++) {
    ack_bool_ = (ack_list_array_[i].get_count() >= replica_num_ / 2);
  }
  return is_flushed_ && ack_bool_;
}

int ObBatchSubmitCtx::backfill_confirmed_()
{
  int ret = OB_NO_NEED_BATCH_CTX;
  int tmp_ret = OB_SUCCESS;

  for (int64_t index = 0; index < partition_array_.count(); index++) {
    const common::ObPartitionKey& partition_key = partition_array_[index];
    const ObLogInfo& log_info = log_info_array_[index];
    const bool batch_first_participant = index == 0;
    if (OB_SUCCESS != (tmp_ret = backfill_confirmed_(partition_key, log_info, batch_first_participant))) {
      CLOG_LOG(WARN, "backfill_confirmed_ failed", K(tmp_ret), K(partition_key), K(log_info));
    }
  }

  return ret;
}

int ObBatchSubmitCtx::backfill_confirmed_(
    const common::ObPartitionKey& partition_key, const ObLogInfo& log_info, const bool batch_first_participant)
{
  int ret = OB_SUCCESS;

  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
      NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    ret = OB_PARTITION_NOT_EXIST;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(partition_key));
  } else if (!guard.get_partition_group()->is_valid()) {
    ret = OB_INVALID_PARTITION;
    CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key));
  } else if (OB_FAIL(log_service->backfill_confirmed(log_info, batch_first_participant))) {
    CLOG_LOG(WARN, "backfill_confirmed failed", K(ret), K(partition_key), K(log_info));
  } else {
    // do nothing
  }

  return ret;
}

int ObBatchSubmitCtx::split_with_lock_()
{
  int ret = OB_SUCCESS;

  ObMutexGuard guard(lock_);
  if (is_splited_ || is_majority_()) {
    // do nothing
  } else if (OB_FAIL(split_()) && OB_NO_NEED_BATCH_CTX != ret) {
    CLOG_LOG(ERROR, "split_ failed", K(ret), K(trans_id_));
  }

  return ret;
}

int ObBatchSubmitCtx::split_()
{
  int ret = OB_NO_NEED_BATCH_CTX;
  int tmp_ret = OB_SUCCESS;

  for (int64_t index = 0; index < partition_array_.count(); index++) {
    const common::ObPartitionKey& partition_key = partition_array_[index];
    const ObLogInfo& log_info = log_info_array_[index];
    ObISubmitLogCb* submit_cb = cb_array_[index];
    if (OB_SUCCESS != (tmp_ret = split_(partition_key, log_info, submit_cb))) {
      CLOG_LOG(WARN, "split_ failed", K(tmp_ret), K(partition_key), K(log_info));
    }
  }
  is_splited_ = true;

  return ret;
}

int ObBatchSubmitCtx::split_(
    const common::ObPartitionKey& partition_key, const ObLogInfo& log_info, ObISubmitLogCb* submit_cb)
{
  int ret = OB_SUCCESS;

  storage::ObIPartitionGroupGuard guard;
  ObIPartitionLogService* log_service = NULL;
  if (OB_FAIL(partition_service_->get_partition(partition_key, guard)) || NULL == guard.get_partition_group() ||
      NULL == (log_service = guard.get_partition_group()->get_log_service())) {
    ret = OB_PARTITION_NOT_EXIST;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(partition_key));
  } else if (!guard.get_partition_group()->is_valid()) {
    ret = OB_INVALID_PARTITION;
    CLOG_LOG(WARN, "partition is invalid", K(ret), K(partition_key));
  } else if (OB_FAIL(log_service->resubmit_log(log_info, submit_cb))) {
    CLOG_LOG(WARN, "resubmit_log failed", K(ret), K(partition_key), K(log_info));
  }

  return ret;
}

bool ObBatchSubmitCtx::need_split_() const
{
  return create_ctx_ts_ != OB_INVALID_TIMESTAMP && (ObClockGenerator::getClock() - create_ctx_ts_ >= SPLIT_INTERVAL) &&
         self_ == leader_;
}

bool ObBatchSubmitCtx::ack_need_split_() const
{
  bool bool_ret = false;

  for (int64_t i = 0; !bool_ret && i < ack_list_array_.count(); i++) {
    // if replica_num = 5, replica_num - replica_num / 2 = 3
    // if replica_num = 4, replica_num - replica_num / 2 = 2
    bool_ret = ((ack_cnt_ - ack_list_array_[i].get_count()) >= replica_num_ - replica_num_ / 2);
  }

  return bool_ret;
}

int ObBatchSubmitCtx::handle_ack_array_(const common::ObAddr& server, const ObBatchAckArray& batch_ack_array)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < batch_ack_array.count(); i++) {
    const bool is_valid_ack = batch_ack_array[i];
    if (is_valid_ack && OB_FAIL(ack_list_array_.at(i).add_server(server))) {
      CLOG_LOG(ERROR, "ack_list_array_ add_server failed", K(ret), K(server));
    }
  }
  ack_cnt_++;
  return ret;
}

int64_t ObBatchSubmitCtxFactory::alloc_cnt_ = 0;
int64_t ObBatchSubmitCtxFactory::free_cnt_ = 0;

ObBatchSubmitCtx* ObBatchSubmitCtxFactory::alloc(common::ObILogAllocator* alloc_mgr)
{
  ObBatchSubmitCtx* ret_val = NULL;
  if (NULL != alloc_mgr) {
    ret_val = alloc_mgr->alloc_batch_submit_ctx();
    ATOMIC_INC(&alloc_cnt_);
  }
  return ret_val;
}

void ObBatchSubmitCtxFactory::free(ObBatchSubmitCtx* ctx)
{
  ATOMIC_INC(&free_cnt_);
  common::ob_slice_free_batch_submit_ctx(ctx);
}

void ObBatchSubmitCtxFactory::statistics()
{
  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    CLOG_LOG(
        INFO, "ObBatchSubmitCtxFactory statistics", K(alloc_cnt_), K(free_cnt_), "using_cnt", alloc_cnt_ - free_cnt_);
  }
}

}  // namespace clog
}  // namespace oceanbase
