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

struct ObMDSMemStat
{
  // uint64_t register_no_;
  uint64_t alloc_cnt_;
  uint64_t free_cnt_;
  uint64_t mem_size_;

  void reset()
  {
    alloc_cnt_ = 0;
    free_cnt_ = 0;
    mem_size_ = 0;
  }

  ObMDSMemStat() { reset(); }

  TO_STRING_KV(K(alloc_cnt_), K(free_cnt_), K(mem_size_));
};

typedef common::hash::ObHashMap<uint64_t, ObMDSMemStat> ObTxMDSMemStatHash;

class ObTxMDSCache
{
public:
  ObTxMDSCache(TransModulePageAllocator &allocator)
      : mds_list_(allocator), final_notify_array_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator)
  {
    reset();
  }
  int init(const int64_t tenant_id, const share::ObLSID ls_id, const ObTransID tx_id);
  void reset();
  void destroy();

  int alloc_mds_node(const ObPartTransCtx *tx_ctx,
                     const char *buf,
                     const int64_t buf_len,
                     common::ObString &data,
                     uint64_t register_no = 0);
  void free_mds_node(common::ObString & data, uint64_t register_no = 0);

  bool is_mem_leak();


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

  int reserve_final_notify_array(const ObTxBufferNodeArray &mds_durable_arr);
  int generate_final_notify_array(const ObTxBufferNodeArray &mds_durable_arr,
                                   bool need_merge_cache,
                                   bool allow_log_overflow);
  ObTxBufferNodeArray &get_final_notify_array() { return final_notify_array_; }

  int64_t get_unsubmitted_size() const { return unsubmitted_size_; }
  int64_t count() const { return mds_list_.size(); }
  void update_submitted_iterator(ObTxBufferNodeArray &range_array);
  void update_sync_failed_range(ObTxBufferNodeArray &range_array);

  void clear_submitted_iterator() { submitted_iterator_ = mds_list_.end(); }

  bool is_contain(const ObTxDataSourceType target_type) const;

  void set_need_retry_submit_mds(bool need_retry) { need_retry_submit_mds_ = need_retry; };
  bool need_retry_submit_mds() { return need_retry_submit_mds_; }

  TO_STRING_KV(K(unsubmitted_size_), K(mds_list_.size()), K(max_register_no_));

private:
  int copy_to_(ObTxBufferNodeArray &tmp_array) const;

private:
  // TransModulePageAllocator allocator_;
  uint64_t max_register_no_;
  bool need_retry_submit_mds_;
  int64_t unsubmitted_size_;
  ObTxBufferNodeList mds_list_;
  ObTxBufferNodeList::iterator submitted_iterator_;
  ObTxBufferNodeArray final_notify_array_;

#ifdef ENABLE_DEBUG_LOG
  int64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTransID  tx_id_;

  int record_mem_ret_;
  ObTxMDSMemStatHash mem_stat_hash_;
#endif
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
