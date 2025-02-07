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

#include "storage/tx/ob_tx_ctx_mds.h"

namespace oceanbase
{
namespace transaction
{

void ObTxMDSCache::reset()
{
  // allocator_.reset();
  unsubmitted_size_ = 0;
  mds_list_.reset();
  submitted_iterator_ = mds_list_.end(); // ObTxBufferNodeList::iterator();
  need_retry_submit_mds_ = false;
  max_register_no_ = 0;
  max_submitted_register_no_ = 0;
  max_synced_register_no_ = 0;
}

void ObTxMDSCache::destroy()
{
  unsubmitted_size_ = 0;
  ObTxBufferNode tmp_node;
  while (!mds_list_.empty()) {
    mds_list_.pop_front(tmp_node);
    if (nullptr != tmp_node.data_.ptr()) {
      MultiTxDataFactory::free(tmp_node.data_.ptr());
    }
    tmp_node.get_buffer_ctx_node().destroy_ctx();
  }
}

int ObTxMDSCache::try_recover_max_register_no(const ObTxBufferNodeArray &node_array)
{
  int ret = OB_SUCCESS;
  int64_t array_cnt = node_array.count();
  if (array_cnt > 0) {
    int64_t durable_max_register_no = node_array[array_cnt - 1].get_register_no();
    if (durable_max_register_no > max_register_no_) {
      TRANS_LOG(INFO, "recover the max_register_no from the exec_info", K(ret),
                K(node_array[array_cnt - 1]), KPC(this));
      max_register_no_ = durable_max_register_no;
      max_submitted_register_no_ = durable_max_register_no;
      max_synced_register_no_ = durable_max_register_no;
    }
  }

  return ret;
}

int ObTxMDSCache::insert_mds_node(ObTxBufferNode &buf_node)
{
  int ret = OB_SUCCESS;

  if (!buf_node.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "insert MDS buf node", K(ret));
  } else if (OB_FALSE_IT(max_register_no_++)) {
    // do nothing
  } else if (OB_FAIL(buf_node.set_mds_register_no(max_register_no_))) {
    TRANS_LOG(WARN, "set mds register no failed", K(ret), K(buf_node), KPC(this));
  } else if (!mds_list_.empty()
             && buf_node.get_register_no() <= mds_list_.get_last().get_register_no()) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "we can not insert a new buf_node with the smaller register_no", K(ret),
              K(buf_node), K(mds_list_.get_last()), KPC(this));
  } else if (OB_FAIL(mds_list_.push_back(buf_node))) {
    TRANS_LOG(WARN, "push back MDS buf node", K(ret));
  } else {
    unsubmitted_size_ += buf_node.get_serialize_size();
  }

  return ret;
}

int ObTxMDSCache::rollback_last_mds_node()
{
  int ret = OB_SUCCESS;

  ObTxBufferNode buf_node = mds_list_.get_last();
  if (OB_FAIL(mds_list_.pop_back())) {
    TRANS_LOG(WARN, "pop back last node failed", K(ret));
  } else {
    TRANS_LOG(INFO, "rollback the last mds node", K(ret), K(buf_node), KPC(this));
    MultiTxDataFactory::free(buf_node.get_ptr());
    buf_node.get_buffer_ctx_node().destroy_ctx();
  }

  clear_submitted_iterator();

  return ret;
}

int ObTxMDSCache::fill_mds_log(ObPartTransCtx *ctx,
                               ObTxMultiDataSourceLog &mds_log,
                               ObTxMDSRange &mds_range,
                               logservice::ObReplayBarrierType &barrier_flag,
                               share::SCN &mds_base_scn)
{
  int ret = OB_SUCCESS;

  mds_range.reset();
  mds_base_scn.reset();
  barrier_flag = logservice::ObReplayBarrierType::NO_NEED_BARRIER;

  share::SCN tmp_base_scn;
  tmp_base_scn.reset();
  logservice::ObReplayBarrierType tmp_barrier_type =
      logservice::ObReplayBarrierType::NO_NEED_BARRIER;

  if (OB_FAIL(mds_range.init(ctx))) {
    TRANS_LOG(WARN, "init mds range failed", K(ret));
  } else {
    if (submitted_iterator_ == mds_list_.end()) {
      submitted_iterator_ = mds_list_.begin();
    }
    ObTxBufferNodeList::iterator iter = submitted_iterator_;
    ObTxBufferNodeList::iterator prev_iter = mds_list_.end();
    for (; iter != mds_list_.end() && OB_SUCC(ret); iter++) {
      prev_iter = iter;
      if (iter->is_submitted()) {
        // do nothing
      } else if (OB_FALSE_IT(prev_iter--)) {
        // do nothing
      } else if (mds_range.count() == 0 && prev_iter != mds_list_.end()
                 && !prev_iter->is_submitted()
                 && prev_iter->get_register_no() < iter->get_register_no()) {
        ret = OB_EAGAIN;
        TRANS_LOG(WARN, "we must submit the previous mds node first", K(ret), K(mds_base_scn),
                  KPC(this), K(*prev_iter), K(*iter), KPC(ctx));
        clear_submitted_iterator();
      } else if (iter->get_register_no() <= max_submitted_register_no_
                 || iter->get_register_no() <= max_synced_register_no_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "try to submit a mds node with the smaller register no", K(ret), KPC(this),
                  K(*prev_iter), K(*iter), KPC(ctx));
      } else if (OB_FAIL(mds_log.fill_MDS_data(*iter))) {
        if (ret != OB_SIZE_OVERFLOW) {
          TRANS_LOG(WARN, "fill mds data in log failed", K(ret));
        }
      } else if (OB_FAIL(mds_range.update_range(iter))) {
        TRANS_LOG(WARN, "update mds range failed", K(ret), K(iter));
      } else if (OB_FALSE_IT(
                     tmp_barrier_type = ObTxLogTypeChecker::need_replay_barrier(
                         ObTxLogType::TX_MULTI_DATA_SOURCE_LOG, iter->get_data_source_type()))) {
        // set need_barrier flag
      } else if (OB_FALSE_IT(tmp_base_scn = iter->get_base_scn())) {
        // set base scn
      }

      if (OB_SUCC(ret)) {
        if (!mds_base_scn.is_valid() && tmp_base_scn.is_valid()) {
          mds_base_scn = tmp_base_scn;
        }
        if (logservice::ObReplayBarrierType::NO_NEED_BARRIER == barrier_flag
            && logservice::ObReplayBarrierType::NO_NEED_BARRIER != tmp_barrier_type) {
          barrier_flag = tmp_barrier_type;
        }
        if (mds_base_scn != tmp_base_scn || barrier_flag != tmp_barrier_type) {
          ret = OB_EAGAIN;
          break;
        }
      }
    }
  }

  if (OB_SIZE_OVERFLOW == ret) {
    ret = OB_EAGAIN;
  }

  if (mds_log.count() != mds_range.count()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unmatched mds_range and mds_log", K(mds_log), K(mds_range));
  } else if (0 == mds_range.count() && 0 == mds_log.count()) {
    if (OB_EAGAIN == ret) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected large buf node", K(ret), K(mds_range), K(mds_log), K(this));

    } else {
      ret = OB_EMPTY_RANGE;
    }
  }

  return ret;
}

int ObTxMDSCache::decide_cache_state_log_mds_barrier_type(
    const ObTxLogType state_log_type,
    logservice::ObReplayBarrierType &cache_final_barrier_type)
{
  int ret = OB_SUCCESS;

  cache_final_barrier_type = logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  logservice::ObReplayBarrierType tmp_barrier_type =
      logservice::ObReplayBarrierType::NO_NEED_BARRIER;
  ObTxBufferNodeList::iterator iter = mds_list_.begin();

  for (; iter != mds_list_.end() && OB_SUCC(ret); iter++) {
    tmp_barrier_type =
        ObTxLogTypeChecker::need_replay_barrier(state_log_type, iter->get_data_source_type());
    if (OB_FAIL(ObTxLogTypeChecker::decide_final_barrier_type(tmp_barrier_type,
                                                              cache_final_barrier_type))) {
      TRANS_LOG(WARN, "decide one mds node barrier type failed", K(ret), K(tmp_barrier_type),
                K(cache_final_barrier_type), K(*iter));
    }
  }

  return ret;
}

int ObTxMDSCache::reserve_final_notify_array(const int durable_cnt)
{
  int ret = OB_SUCCESS;
  int64_t size = mds_list_.size() + durable_cnt;
  if (size > 0 && OB_FAIL(final_notify_array_.reserve(size))) {
    TRANS_LOG(WARN, "reserve notify array space failed", K(ret), K(size));
  }

  return ret;
}

int ObTxMDSCache::generate_final_notify_array(const ObTxBufferNodeArray &mds_durable_arr,
                                               bool need_merge_cache,
                                               bool allow_log_overflow)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(final_notify_array_.assign(mds_durable_arr))) {
    TRANS_LOG(WARN, "assign mds_durable_arr failed", K(ret), K(mds_durable_arr.count()),
              K(final_notify_array_.get_capacity()), KPC(this));

  } else if (need_merge_cache) {
    if (OB_FAIL(copy_to_(final_notify_array_))) {
      TRANS_LOG(WARN, "merge mds_cache into final_notify_array failed", K(ret),
                K(mds_durable_arr.count()), K(final_notify_array_.get_capacity()), KPC(this));
    }
  }

  if (!allow_log_overflow) {
    if (final_notify_array_.get_serialize_size() > ObTxMultiDataSourceLog::MAX_MDS_LOG_SIZE) {
      TRANS_LOG(WARN, "MDS array is overflow, use empty MDS array",
                K(final_notify_array_.get_serialize_size()), K(mds_durable_arr.count()), KPC(this));
      final_notify_array_.reuse();
    }
  }

  return ret;
}

int ObTxMDSCache::copy_to_(ObTxBufferNodeArray &tmp_array) const
{
  int ret = OB_SUCCESS;

  ObTxBufferNodeList::const_iterator iter = mds_list_.begin();
  for (; iter != mds_list_.end() && OB_SUCC(ret); iter++) {
    if (OB_FAIL(tmp_array.push_back(*iter))) {
      TRANS_LOG(WARN, "push back failed", K(ret), K(*iter));
    }
  }

  return ret;
}

#define SEARCH_ITER_AFTER_SUBMITTED                                                    \
  int64_t search_count = 0;                                                            \
  do {                                                                                 \
    if (search_iter == mds_list_.end()) {                                              \
      search_iter = mds_list_.begin();                                                 \
    } else {                                                                           \
      search_iter++;                                                                   \
      if (search_iter == mds_list_.end()) {                                            \
        search_iter = mds_list_.begin();                                               \
      }                                                                                \
    }                                                                                  \
    search_count++;                                                                    \
    if (search_count > mds_list_.size()) {                                             \
      if (REACH_TIME_INTERVAL(1000 * 1000)) {                                          \
        TRANS_LOG(ERROR, "unexpected buffer_node in mds_range", K(search_count),       \
                  K(mds_list_.size()), K(range_array[i]), K(*search_iter), KPC(this)); \
      }                                                                                \
    }                                                                                  \
  } while (!((*search_iter) == range_array[i]));

void ObTxMDSCache::update_submitted_iterator(ObTxBufferNodeArray &range_array)
{

  int ret = OB_SUCCESS;
  ObTxBufferNodeList::iterator search_iter = submitted_iterator_;
  for (int i = 0; i < range_array.count() && OB_SUCC(ret); i++) {
    SEARCH_ITER_AFTER_SUBMITTED

    search_iter->set_submitted();
    range_array[i].set_submitted();
    if (range_array[i].get_register_no() > max_submitted_register_no_) {
      max_submitted_register_no_ = range_array[i].get_register_no();
    }

    unsubmitted_size_ = unsubmitted_size_ - search_iter->get_serialize_size();
    submitted_iterator_ = search_iter;
  }
}

void ObTxMDSCache::update_sync_failed_range(ObTxBufferNodeArray &range_array)
{

  int ret = OB_SUCCESS;
  ObTxBufferNodeList::iterator search_iter = submitted_iterator_;
  for (int i = 0; i < range_array.count() && OB_SUCC(ret); i++) {
    SEARCH_ITER_AFTER_SUBMITTED

    search_iter->log_sync_fail();
    range_array[i].log_sync_fail();
  }

  max_submitted_register_no_ = max_synced_register_no_;
  clear_submitted_iterator();
}

bool ObTxMDSCache::is_contain(const ObTxDataSourceType target_type) const
{
  bool contain = false;
  ObTxBufferNodeList::const_iterator iter = mds_list_.begin();
  for (; iter != mds_list_.end(); iter++) {
    if (iter->get_data_source_type() == target_type) {
      contain = true;
      break;
    }
  }
  return contain;
}

void ObTxMDSRange::reset()
{
  tx_ctx_ = nullptr;
  range_array_.reset();
}

// void ObTxMDSRange::clear()
// {
//   list_ptr_ = nullptr;
//   start_iter_ = ObTxBufferNodeList::iterator();
// }

int ObTxMDSRange::init(ObPartTransCtx *tx_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tx_ctx_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(tx_ctx)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    tx_ctx_ = tx_ctx;
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "init MDS range failed", K(ret), KPC(tx_ctx));
  }

  return ret;
}

int ObTxMDSRange::update_range(ObTxBufferNodeList::iterator iter)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_ctx_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "MDS range is not init", K(ret), KPC(tx_ctx_));
  } else if (!(*iter).is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid iter", K(ret), K(*iter));
  } else if (OB_FAIL(range_array_.push_back(*iter))) {
    TRANS_LOG(WARN, "push back into the range array failed", K(ret), K(*iter), KPC(this),
              KPC(tx_ctx_));
  }

  return ret;
}

int ObTxMDSRange::move_from_cache_to_arr(ObTxMDSCache &mds_cache,
                                         ObTxBufferNodeArray &mds_durable_arr)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tx_ctx_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "MDS range is not init", K(ret), KPC(tx_ctx_));
  } else if (range_array_.empty()) {
    TRANS_LOG(WARN, "empty range in move function", K(ret), KPC(tx_ctx_));
  } else {
    for (int64_t i = 0; i < range_array_.count() && OB_SUCC(ret); i++) {
      if (!ObTxBufferNode::is_valid_register_no(range_array_[i].get_register_no())) {
        ret = OB_INVALID_ARGUMENT;
        TRANS_LOG(ERROR, "invalid register no for a mds node in cache", K(ret), K(i),
                  K(range_array_[i]));
      } else if (!mds_durable_arr.empty()
                 && ObTxBufferNode::is_valid_register_no(
                     mds_durable_arr[mds_durable_arr.count() - 1].get_register_no())
                 && range_array_[i].get_register_no()
                        <= mds_durable_arr[mds_durable_arr.count() - 1].get_register_no()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "invalid smaller register no", K(ret), K(i), K(range_array_[i]),
                  K(mds_durable_arr[mds_durable_arr.count() - 1]));
      } else if (OB_FAIL(mds_cache.earse_from_cache(range_array_[i]))) {
        TRANS_LOG(WARN, "earse from mds cache failed", K(ret), K(range_array_[i]), K(mds_cache),
                  K(mds_durable_arr));
      } else if (OB_FALSE_IT(range_array_[i].set_synced())) {
        // do nothing
      } else if (OB_FAIL(mds_durable_arr.push_back(range_array_[i]))) {
        TRANS_LOG(WARN, "push back into mds_durable_arr failed", K(ret), K(range_array_[i]),
                  K(mds_cache), K(mds_durable_arr));
      }
    }
  }

  return ret;
}

int ObTxMDSRange::range_submitted(ObTxMDSCache &cache)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;

  if (OB_ISNULL(tx_ctx_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "MDS range is not init", K(ret), KPC(this), KPC(tx_ctx_));
  } else if (range_array_.empty()) {
    // empty MDS range
    TRANS_LOG(WARN, "use empty mds range when submit range", K(ret), K(cache), KPC(this),
              KPC(tx_ctx_));
  } else {
    cache.update_submitted_iterator(range_array_);
  }

  return ret;
}

void ObTxMDSRange::range_sync_failed(ObTxMDSCache &cache)
{
  cache.update_sync_failed_range(range_array_);
}

int ObTxMDSRange::assign(const ObTxMDSRange &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(range_array_.assign(other.range_array_))) {
    TRANS_LOG(WARN, "assign range array failed", K(ret));
  } else {
    tx_ctx_ = other.tx_ctx_;
  }
  return ret;
}

} // namespace transaction
} // namespace oceanbase
