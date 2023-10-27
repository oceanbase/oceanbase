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

#ifndef OCEANBASE_TRANSACTION_OB_TX_CTX_MDS_
#define OCEANBASE_TRANSACTION_OB_TX_CTX_MDS_

#include "ob_multi_data_source.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_log.h"

namespace oceanbase
{
namespace transaction
{

typedef ObList<ObTxBufferNode, TransModulePageAllocator> ObTxBufferNodeList;

class ObTxMDSRange;

class ObTxMDSCache
{
public:
  ObTxMDSCache(TransModulePageAllocator &allocator) : mds_list_(allocator) { reset(); }
  void reset();
  void destroy();

  int try_recover_max_register_no(const ObTxBufferNodeArray & node_array);
  int insert_mds_node(ObTxBufferNode &buf_node);
  int rollback_last_mds_node();
  int fill_mds_log(ObPartTransCtx *ctx,
                   ObTxMultiDataSourceLog &mds_log,
                   ObTxMDSRange &mds_range,
                   logservice::ObReplayBarrierType &barrier_flag,
                   share::SCN &mds_base_scn);
  int decide_cache_state_log_mds_barrier_type(
      const ObTxLogType state_log_type,
      logservice::ObReplayBarrierType &cache_final_barrier_type);
  int earse_from_cache(const ObTxBufferNode &node) { return mds_list_.erase(node); }
  int copy_to(ObTxBufferNodeArray &tmp_array) const;

  int64_t get_unsubmitted_size() const { return unsubmitted_size_; }
  int64_t count() const { return mds_list_.size(); }
  void update_submitted_iterator(ObTxBufferNodeArray &range_array);
  void update_sync_failed_range(ObTxBufferNodeArray &range_array);
  // {
  //   unsubmitted_size_ = unsubmitted_size_ - iter->get_serialize_size();
  //   submitted_iterator_ = iter;
  // }
  void clear_submitted_iterator() { submitted_iterator_ = mds_list_.end(); }

  bool is_contain(const ObTxDataSourceType target_type) const;

  void set_need_retry_submit_mds(bool need_retry) { need_retry_submit_mds_ = need_retry; };
  bool need_retry_submit_mds() { return need_retry_submit_mds_; }

  TO_STRING_KV(K(unsubmitted_size_), K(mds_list_.size()), K(max_register_no_));

private:
  // TransModulePageAllocator allocator_;
  uint64_t max_register_no_;
  bool need_retry_submit_mds_;
  int64_t unsubmitted_size_;
  ObTxBufferNodeList mds_list_;
  ObTxBufferNodeList::iterator submitted_iterator_;
};

class ObTxMDSRange
{
public:
  ObTxMDSRange() { reset(); }
  void reset();
  // void clear();

  int init(ObPartTransCtx *tx_ctx);
  int update_range(ObTxBufferNodeList::iterator iter);

  int move_from_cache_to_arr(ObTxMDSCache &mds_cache, ObTxBufferNodeArray &mds_durable_arr);
  // int move_to(ObTxBufferNodeArray &tx_buffer_node_arr);
  // int copy_to(ObTxBufferNodeArray &tx_buffer_node_arr) const;

  int range_submitted(ObTxMDSCache &cache);
  void range_sync_failed(ObTxMDSCache &cache);
  int assign(const ObTxMDSRange &other);

  int64_t count() const { return range_array_.count(); };

  const ObTxBufferNodeArray &get_range_array() { return range_array_; }

  TO_STRING_KV(K(range_array_.count()), K(range_array_));

private:
  ObTxBufferNodeArray range_array_;
  ObPartTransCtx *tx_ctx_;

  // ObTxBufferNodeList *list_ptr_;
  // ObTxBufferNodeList::iterator start_iter_;
  // int64_t count_;
};
} // namespace transaction
} // namespace oceanbase

#endif
