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

#include "storage/ob_partition_service.h"
#include "ob_trans_result_info_mgr.h"
#include "ob_trans_factory.h"
#include "ob_trans_define.h"

namespace oceanbase {
using namespace common;
using namespace storage;

namespace transaction {

int ObTransResultInfoLinkNode::connect(ObTransResultInfoLinkNode* next)
{
  int ret = OB_SUCCESS;

  if (NULL == next) {
    this->next_ = next;
  } else if (NULL != next->prev_ || NULL != next->next_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected prev or next node", KP_(prev), KP_(next));
  } else {
    this->next_ = next;
    next->prev_ = this;
  }

  return ret;
}

int ObTransResultInfoLinkNode::connect(ObTransResultInfoLinkNode* prev, ObTransResultInfoLinkNode* next)
{
  int ret = OB_SUCCESS;

  if (NULL != prev_ || NULL != next_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected prev or next node", KP_(prev), KP_(next));
  } else if (OB_ISNULL(prev)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret));
  } else if (NULL == next) {
    this->next_ = next;
    this->prev_ = prev;
    prev->next_ = this;
  } else {
    this->next_ = next;
    this->prev_ = prev;
    next->prev_ = this;
    prev->next_ = this;
  }

  return ret;
}

int ObTransResultInfoLinkNode::del()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(prev_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "del result info error", KR(ret));
  } else if (NULL == next_) {
    prev_->next_ = NULL;
    prev_ = NULL;
  } else {
    prev_->next_ = next_;
    next_->prev_ = prev_;
    prev_ = NULL;
    next_ = NULL;
  }

  return ret;
}

void ObTransResultInfo::reset()
{
  state_ = ObTransResultState::INVALID;
  commit_version_ = 0;
  min_log_id_ = UINT64_MAX;
  trans_id_.reset();
  next_ = NULL;
  prev_ = NULL;
}

int ObTransResultInfo::init(const int state, const int64_t commit_version, const int64_t min_log_id,
    const int64_t min_log_ts, const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ObTransResultState::is_valid(state)) || OB_UNLIKELY(0 >= commit_version) ||
      OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(state), K(commit_version), K(trans_id));
  } else {
    if (0 == min_log_id) {
      TRANS_LOG(ERROR, "unexpected min log id", K(trans_id), K(state), K(commit_version), K(min_log_id));
    }
    state_ = state;
    commit_version_ = commit_version;
    min_log_id_ = min_log_id;
    min_log_ts_ = min_log_ts;
    trans_id_ = trans_id;
  }

  return ret;
}

int ObTransResultInfo::update(const int state, const int64_t commit_version, const int64_t min_log_id,
    const int64_t min_log_ts, const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  // don't update commit version if transaction decided
  if (ObTransResultState::is_decided_state(state_)) {
    TRANS_LOG(INFO,
        "repeated update trans result info, please attention",
        K(state),
        K(commit_version),
        K(min_log_id),
        K(trans_id),
        K(*this));
  } else if (OB_UNLIKELY(trans_id_ != trans_id)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "update trans result info unexpected", KR(ret), K(*this), K(trans_id));
  } else if (OB_UNLIKELY((ObTransResultState::is_commit(state_) && ObTransResultState::is_abort(state)) ||
                         (ObTransResultState::is_abort(state_) && ObTransResultState::is_commit(state)))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "update trans result info unexpected", KR(ret), K(*this), K(state));
  } else {
    if (0 == min_log_id) {
      TRANS_LOG(ERROR, "unexpected min log id", K(trans_id), K(state), K(commit_version), K(min_log_id));
    }
    state_ = state;
    commit_version_ = commit_version;
    min_log_id_ = min_log_id;
    min_log_ts_ = min_log_ts;
    trans_id_ = trans_id;
  }

  return ret;
}

bool ObTransResultInfo::is_valid() const
{
  return ObTransResultState::is_valid(state_) && commit_version_ > 0 && min_log_id_ > 0 && trans_id_.is_valid();
}

bool ObTransResultInfo::is_found(const ObTransID& trans_id) const
{
  return trans_id_ == trans_id;
}

int ObTransResultInfo::set_state(const int state)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!ObTransResultState::is_valid(state))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(state), K_(trans_id));
  } else {
    state_ = state;
  }

  return ret;
}

int ObTransResultInfoMgr::init(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partition_service) || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(pkey));
  } else {
    partition_service_ = partition_service;
    self_ = pkey;
    result_info_count_ = 0;
    is_inited_ = true;
  }

  return ret;
}

void ObTransResultInfoMgr::reset()
{
  is_inited_ = false;
  for (int i = 0; i < TRANS_RESULT_INFO_BUCKET_COUNT; i++) {
    bucket_header_[i].reset();
  }
  partition_service_ = NULL;
  self_.reset();
  result_info_count_ = 0;
}

void ObTransResultInfoMgr::destroy()
{
  is_inited_ = false;
  for (int i = 0; i < TRANS_RESULT_INFO_BUCKET_COUNT; i++) {
    ObTransResultInfoBucketHeader& bucket_header = bucket_header_[i];
    if (NULL != bucket_header.trans_info_.get_next_node()) {
      (void)release_trans_result_info_(static_cast<ObTransResultInfo*>(bucket_header.trans_info_.get_next_node()));
    }
    bucket_header.reset();
  }
  partition_service_ = NULL;
  self_.reset();
  result_info_count_ = 0;
}

int ObTransResultInfoMgr::rdlock(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  const int index = trans_id.hash() & (TRANS_RESULT_INFO_BUCKET_COUNT - 1);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else if (OB_FAIL(bucket_header_[index].lock_.rdlock())) {
    TRANS_LOG(WARN, "rdlock error", KR(ret), K(trans_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransResultInfoMgr::wrlock(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  const int index = trans_id.hash() & (TRANS_RESULT_INFO_BUCKET_COUNT - 1);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else if (OB_FAIL(bucket_header_[index].lock_.wrlock())) {
    TRANS_LOG(WARN, "wrlock error", KR(ret), K(trans_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransResultInfoMgr::unlock(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  const int index = trans_id.hash() & (TRANS_RESULT_INFO_BUCKET_COUNT - 1);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else if (OB_FAIL(bucket_header_[index].lock_.unlock())) {
    TRANS_LOG(WARN, "unlock error", KR(ret), K(trans_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransResultInfoMgr::insert(ObTransResultInfo* info, bool& registered)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransResultInfo* result_info = info;
  const ObTransID& trans_id = info->get_trans_id();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret), K(trans_id));
  } else if (OB_ISNULL(result_info)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "trans result info is null, unexpected error", K(ret), KP(info));
  } else {
    const int index = gen_bucket_index_(trans_id);
    ObTransResultInfo* release_point = NULL;
    {
      SpinWLockGuard guard(bucket_header_[index].lock_);
      if (OB_FAIL(insert_item_in_bucket_(index, result_info))) {
        TRANS_LOG(WARN, "insert item in bucket error", KR(ret), K(result_info));
      } else {
        registered = true;
        ATOMIC_FAA(&result_info_count_, 1);
        if (ObTimeUtility::current_time() > bucket_header_[index].last_gc_ts_) {
          // do gc
          int64_t checkpoint = 0;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = get_partition_checkpoint_version_(checkpoint)))) {
            TRANS_LOG(WARN, "get partition checkpoint version error", K(tmp_ret), K(*result_info));
          } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = gc_by_checkpoint_(checkpoint, result_info, release_point)))) {
            TRANS_LOG(WARN, "gc by checkpoint error", K(tmp_ret), K(*result_info));
          } else {
            // decrease gc frequency in crash recovery
            int64_t delay =
                ((GCTX.status_ == observer::SS_SERVING) ? TRANS_RESULT_INFO_GC_INTERVAL_US : 10 * 1000 * 1000);
            bucket_header_[index].last_gc_ts_ = ObTimeUtility::current_time() + delay;
          }
        }
      }
    }
    if (OB_SUCCESS == ret && OB_SUCCESS == tmp_ret && NULL != release_point &&
        OB_UNLIKELY(OB_SUCCESS != (tmp_ret = release_trans_result_info_(release_point)))) {
      TRANS_LOG(WARN, "release trans result info error", K(tmp_ret), K(*result_info));
    }

    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      TRANS_LOG(INFO, "[statisic] trans result info count : ", K_(result_info_count), K_(self));
    }
  }

  if (OB_FAIL(ret)) {
    ObTransResultInfoFactory::release(result_info);
  }

  return ret;
}

int ObTransResultInfoMgr::update(const int state, const int64_t commit_version, const int64_t min_log_id,
    const int64_t min_log_ts, const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(!trans_id.is_valid() || !ObTransResultState::is_decided_state(state) || 0 >= commit_version ||
                         0 >= min_log_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id), K(state), K(commit_version), K(trans_id));
  } else {
    ObTransResultInfo* result_info = NULL;
    const int index = gen_bucket_index_(trans_id);
    SpinWLockGuard guard(bucket_header_[index].lock_);
    if (OB_FAIL(find_item_in_bucket_(index, trans_id, result_info))) {
      TRANS_LOG(WARN, "find item in bucket error", KR(ret), K(index), K(trans_id));
    } else if (OB_ISNULL(result_info)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "result info is null", KR(ret), K(index), K(trans_id));
    } else if (OB_FAIL(result_info->del())) {
      TRANS_LOG(WARN, "del result info error", KR(ret), K(index), K(*result_info));
    } else if (OB_FAIL(result_info->update(state, commit_version, min_log_id, min_log_ts, trans_id))) {
      TRANS_LOG(WARN, "update result info error", KR(ret), K(index), K(trans_id));
    } else if (OB_FAIL(insert_item_in_bucket_(index, result_info))) {
      TRANS_LOG(WARN, "insert item in bucket error", KR(ret), K(index), K(result_info));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObTransResultInfoMgr::del(const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else {
    ObTransResultInfo* result_info = NULL;
    const int index = gen_bucket_index_(trans_id);
    {
      SpinWLockGuard guard(bucket_header_[index].lock_);
      if (OB_FAIL(find_item_in_bucket_(index, trans_id, result_info))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          TRANS_LOG(WARN, "find item in bucket error", KR(ret), K(index), K(trans_id));
        }
      } else if (OB_ISNULL(result_info)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "result info is null", KR(ret), K(index), K(trans_id));
      } else if (OB_FAIL(result_info->del())) {
        TRANS_LOG(WARN, "del result info error", KR(ret), K(index), K(*result_info));
      } else {
        // do nothing
      }
    }

    if (OB_SUCC(ret)) {
      ObTransResultInfoFactory::release(result_info);
      ATOMIC_FAA(&result_info_count_, -1);
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObTransResultInfoMgr::find_unsafe(const ObTransID& trans_id, ObTransResultInfo*& result_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else if (OB_FAIL(find_item_in_bucket_(gen_bucket_index_(trans_id), trans_id, result_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "find item in bucket error", KR(ret), K(trans_id));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObTransResultInfoMgr::get_state(const ObTransID& trans_id, int& state)
{
  int ret = OB_SUCCESS;
  ObTransResultInfo* result_info = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(trans_id));
  } else {
    const int index = gen_bucket_index_(trans_id);
    SpinRLockGuard guard(bucket_header_[index].lock_);
    if (OB_FAIL(find_item_in_bucket_(index, trans_id, result_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        TRANS_LOG(WARN, "find item in bucket error", KR(ret), K(trans_id));
      }
    } else if (OB_ISNULL(result_info)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected result info NULL", KR(ret), K(trans_id));
    } else {
      state = result_info->get_state();
    }
  }

  return ret;
}

int ObTransResultInfoMgr::try_gc_trans_result_info(const int64_t checkpoint_ts)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < TRANS_RESULT_INFO_BUCKET_COUNT; i++) {
      ObTransResultInfo* release_point_start = NULL;
      {
        ObTransResultInfoBucketHeader& bucket_header = bucket_header_[i];
        common::SpinRLockGuard guard(bucket_header.lock_);

        ObTransResultInfoLinkNode* cur = bucket_header.trans_info_.get_next_node();
        if (NULL != cur) {
          ObTransResultInfo* start = static_cast<ObTransResultInfo*>(cur);
          if (NULL == start->next()) {
            if (start->get_commit_version() < checkpoint_ts - MAX_ELR_TRANS_INTERVAL) {
              bucket_header.trans_info_.set_next_node(NULL);
              release_point_start = start;
            }
          } else if (OB_FAIL(gc_by_checkpoint_(checkpoint_ts, start, release_point_start))) {
            TRANS_LOG(WARN, "gc by checkpoint error", K(ret), K(checkpoint_ts), K(*start));
          } else {
            // do nothing
          }
        }
      }
      if (NULL != release_point_start && (OB_SUCCESS != (tmp_ret = release_trans_result_info_(release_point_start)))) {
        TRANS_LOG(WARN, "release trans result info error", K(tmp_ret), K(*release_point_start));
      }
    }
  }

  return ret;
}

int ObTransResultInfoMgr::get_min_log(uint64_t& min_log_id, int64_t& min_log_ts)
{
  int ret = OB_SUCCESS;
  ObGetMinLogIdFunction function;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransResultInfoMgr not init", KR(ret));
  } else if (OB_FAIL(for_each<ObGetMinLogIdFunction>(function))) {
    TRANS_LOG(WARN, "for each get min log id error", KR(ret));
  } else {
    min_log_id = function.get_min_log_id();
    min_log_ts = function.get_min_log_ts();
  }

  return ret;
}

int ObTransResultInfoMgr::gc_by_checkpoint_(
    const int64_t checkpoint, ObTransResultInfo* info, ObTransResultInfo*& release_point)
{
  int ret = OB_SUCCESS;

  if (NULL != info && 0 != checkpoint) {
    ObTransResultInfo* prev = info;
    ObTransResultInfo* cur = info->next();
    while (NULL != cur) {
      if (cur->get_commit_version() < checkpoint - MAX_ELR_TRANS_INTERVAL) {
        break;
      } else {
        prev = cur;
        cur = cur->next();
      }
    }
    prev->set_next(NULL);
    // record gc position
    release_point = cur;
  }

  return ret;
}

int ObTransResultInfoMgr::release_trans_result_info_(ObTransResultInfo* info)
{
  int ret = OB_SUCCESS;
  ObTransResultInfo* cur = NULL;

  while (NULL != info) {
    cur = info;
    info = info->next();
    ObTransResultInfoFactory::release(cur);
    ATOMIC_FAA(&result_info_count_, -1);
  }

  return ret;
}

int ObTransResultInfoMgr::get_partition_checkpoint_version_(int64_t& checkpoint)
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupGuard pkey_guard;

  if (OB_ISNULL(partition_service_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition service is null", KR(ret), K_(self));
  } else if (OB_FAIL(partition_service_->get_partition(self_, pkey_guard))) {
    TRANS_LOG(WARN, "get partition fail", KR(ret), K_(self));
  } else if (OB_ISNULL(pkey_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "partition is null", K(ret), K_(self));
  } else if (OB_FAIL(pkey_guard.get_partition_group()->get_weak_read_timestamp(checkpoint))) {
    TRANS_LOG(WARN, "get weak read timestamp error", K(ret), K_(self));
  } else {
    // do nothing
  }

  return ret;
}

int ObTransResultInfoMgr::gen_bucket_index_(const ObTransID& trans_id)
{
  return trans_id.hash() & (TRANS_RESULT_INFO_BUCKET_COUNT - 1);
}

int ObTransResultInfoMgr::find_item_in_bucket_(
    const int index, const ObTransID& trans_id, ObTransResultInfo*& result_info)
{
  int ret = OB_SUCCESS;

  ObTransResultInfoLinkNode& dummy_node = bucket_header_[index].trans_info_;
  if (NULL == dummy_node.get_next_node()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ObTransResultInfo* find = static_cast<ObTransResultInfo*>(dummy_node.get_next_node());
    while (NULL != find) {
      if (find->is_found(trans_id)) {
        result_info = find;
        break;
      } else {
        find = find->next();
      }
    }
    if (NULL == find) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }

  return ret;
}

int ObTransResultInfoMgr::insert_item_in_bucket_(const int index, ObTransResultInfo* result_info)
{
  int ret = OB_SUCCESS;
  const int64_t commit_version = result_info->get_commit_version();

  // find dummy node
  ObTransResultInfoLinkNode& dummy_node = bucket_header_[index].trans_info_;
  ObTransResultInfoLinkNode* prev = &dummy_node;
  ObTransResultInfoLinkNode* next = dummy_node.get_next_node();
  while (NULL != next) {
    if (commit_version >= static_cast<ObTransResultInfo*>(next)->get_commit_version()) {
      if (OB_FAIL(result_info->connect(prev, next))) {
        TRANS_LOG(WARN, "connect result info error", KR(ret), K(*result_info));
      }
      break;
    } else {
      prev = next;
      next = next->get_next_node();
    }
  }
  // insert at tail
  if (OB_SUCC(ret) && NULL == next) {
    if (OB_FAIL(prev->connect(result_info))) {
      TRANS_LOG(WARN, "connect result info node error", KR(ret), K(index), K(*result_info));
    }
  }

  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
