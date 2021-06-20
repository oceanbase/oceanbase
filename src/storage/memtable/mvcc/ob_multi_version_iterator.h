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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MULTI_VERSION_ITERATOR_
#define OCEANBASE_MEMTABLE_MVCC_OB_MULTI_VERSION_ITERATOR_

#include "share/ob_define.h"
#include "common/ob_range.h"

#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "ob_mvcc_iterator.h"

namespace oceanbase {
namespace transaction {
class ObTransSnapInfo;
class ObPartitionTransCtxMgr;
}  // namespace transaction
namespace memtable {

class ObIMvccCtx;
class ObMultiVersionValueIterator : public ObIMvccValueIterator {
public:
  ObMultiVersionValueIterator();
  virtual ~ObMultiVersionValueIterator();
  // for iterating multi version row or uncommitted transaction row
public:
  int init(const ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info, const ObMemtableKey* key,
      ObMvccRow* value, transaction::ObTransStateTableGuard& trans_table_guard);
  virtual int get_next_node(const void*& tnode);
  int get_next_node_for_compact(const void*& tnode);
  int get_next_multi_version_node(const void*& tnode);
  int get_next_uncommitted_node(
      const void*& tnode, transaction::ObTransID& trans_id, int64_t& trans_version, int64_t& sql_sequence);
  int check_next_sql_sequence(
      const transaction::ObTransID& input_trans_id, const int64_t input_sql_sequence, bool& same_sql_sequence_flag);
  void reset();
  bool is_exist() const
  {
    return nullptr != version_iter_;
  }
  virtual enum storage::ObRowDml get_first_dml();
  int64_t get_committed_max_trans_version() const
  {
    return max_committed_trans_version_;
  }
  bool is_cur_multi_version_row_end() const;
  bool is_node_compacted() const
  {
    return is_node_compacted_;
  }
  bool is_multi_version_iter_end() const;
  bool is_trans_node_iter_null() const;
  bool is_compact_iter_end() const;
  storage::ObRowDml get_compacted_row_dml() const
  {
    return compacted_row_dml_;
  }
  void init_multi_version_iter();
  void set_merge_log_ts(const uint64_t merge_log_ts)
  {
    merge_log_ts_ = merge_log_ts;
  }
  int64_t get_merge_log_ts() const
  {
    return merge_log_ts_;
  }
  void set_iter_mode(storage::ObTableIterParam::ObIterTransNodeMode iter_mode)
  {
    iter_mode_ = iter_mode;
  }
  void print_cur_status();
  bool contain_overflow_trans_node();
  void jump_overflow_trans_node();
  TO_STRING_KV(K_(value), KPC_(version_iter), KPC_(multi_version_iter), K_(max_committed_trans_version),
      K_(cur_trans_version), K_(is_node_compacted), K_(merge_log_ts), K_(iter_mode));

private:
  int get_trans_status_with_log_ts(const int64_t log_ts, ObMvccTransNode* trans_node,
      transaction::ObTransTableStatusType& status, int64_t& trans_version_at_merge_log_ts);
  int get_status_of_curr_trans_node(
      transaction::ObTransID& trans_id, transaction::ObTransTableStatusType& trans_status);
  int get_trans_status(const transaction::ObTransID& trans_id, transaction::ObTransTableStatusType& trans_status);
  int get_trans_status_in_determined_status(
      transaction::ObTransID& trans_id, transaction::ObTransTableStatusType& trans_status);
  int get_next_right_node(ObMvccTransNode*& node);
  int should_ignore_cur_node(const ObMvccTransNode& node, bool& ignore_flag);
  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionValueIterator);

private:
  bool is_inited_;
  ObMvccRow* value_;
  ObMvccTransNode* version_iter_;
  ObMvccTransNode* multi_version_iter_;
  const ObIMvccCtx* ctx_;
  int64_t max_committed_trans_version_;
  int64_t cur_trans_version_;
  storage::ObRowDml compacted_row_dml_;  // dml type for compacted row
  bool is_node_compacted_;
  transaction::ObTransStateTableGuard* trans_table_guard_;
  int64_t merge_log_ts_;
  storage::ObTableIterParam::ObIterTransNodeMode iter_mode_;
  bool meet_non_overflow_node_flag_;  // only use in OIM_ITER_OVERFLOW_TO_COMPLEMENT == iter_mode_
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMultiVersionRowIterator {
public:
  ObMultiVersionRowIterator();
  ~ObMultiVersionRowIterator();

public:
  int init(ObQueryEngine& query_engine, const ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info,
      const ObMvccScanRange& range, transaction::ObTransStateTableGuard& trans_table_guard);
  int get_next_row(const ObMemtableKey*& key, ObMultiVersionValueIterator*& value_iter);
  void reset();

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionRowIterator);

private:
  bool is_inited_;
  const ObIMvccCtx* ctx_;
  transaction::ObTransSnapInfo snapshot_info_;
  ObMultiVersionValueIterator value_iter_;
  ObQueryEngine* query_engine_;
  ObIQueryEngineIterator* query_engine_iter_;
  transaction::ObTransStateTableGuard* trans_table_guard_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif /* OCEANBASE_MEMTABLE_MVCC_OB_MULTI_VERSION_ITERATOR_ */
