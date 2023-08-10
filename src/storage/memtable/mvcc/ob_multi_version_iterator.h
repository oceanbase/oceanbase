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


#include "common/ob_range.h"
#include "ob_mvcc_iterator.h"
#include "share/ob_define.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{
struct ObTransNodeDMLStat;
}

namespace memtable
{
class ObMvccAccessCtx;
class ObMultiVersionValueIterator
{
public:
  ObMultiVersionValueIterator();
  virtual ~ObMultiVersionValueIterator();
  //用来迭代冻结memtable多版本和事务未提交的row
public:
  int init(ObMvccAccessCtx *ctx,
           const common::ObVersionRange &version_range,
           const ObMemtableKey *key,
           ObMvccRow *value);
  virtual int get_next_node(const void *&tnode);
  int get_next_node_for_compact(const void *&tnode);
  int get_next_multi_version_node(const void *&tnode);
  int get_next_uncommitted_node(
      const void *&tnode,
      transaction::ObTransID &trans_id,
      share::SCN &trans_version,
      transaction::ObTxSEQ &sql_sequence);
  int check_next_sql_sequence(
      const transaction::ObTransID &input_trans_id,
      const transaction::ObTxSEQ input_sql_sequence,
      bool &same_sql_sequence_flag);
  void reset();
  bool is_exist() const { return nullptr != version_iter_; }
  int64_t get_committed_max_trans_version() const { return max_committed_trans_version_; }
  bool is_cur_multi_version_row_end() const ;
  bool is_node_compacted() const { return is_node_compacted_; }
  bool is_multi_version_iter_end() const;
  bool is_trans_node_iter_null() const;
  bool is_compact_iter_end() const;
  int init_multi_version_iter();
  void set_merge_scn(const share::SCN merge_scn) { merge_scn_ = merge_scn; }
  share::SCN get_merge_scn() const { return merge_scn_; }
  void print_cur_status();
  blocksstable::ObDmlFlag get_row_first_dml_flag() const
  {
    return nullptr != value_ ? value_->get_first_dml_flag() : blocksstable::ObDmlFlag::DF_NOT_EXIST;
  }
  bool has_multi_commit_trans() { return has_multi_commit_trans_; }

  DECLARE_TO_STRING;

private:
  int get_trans_status_with_scn(
    const share::SCN scn,
      ObMvccTransNode *trans_node,
      int64_t &status,
      share::SCN &trans_version_at_merge_scn);
  int get_state_of_curr_trans_node(
      transaction::ObTransID &trans_id,
      int64_t &state,
      uint64_t &cluster_version);
  int get_trans_status(
      const transaction::ObTransID &trans_id,
      int64_t &state,
      uint64_t &cluster_version);
  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionValueIterator);
private:
  bool is_inited_;
  ObMvccAccessCtx *ctx_;
  common::ObVersionRange version_range_;
  ObMvccRow *value_;
  ObMvccTransNode *version_iter_;
  ObMvccTransNode *multi_version_iter_;
  int64_t max_committed_trans_version_;
  share::SCN cur_trans_version_;
  bool is_node_compacted_;
  bool has_multi_commit_trans_;
  share::SCN merge_scn_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMultiVersionRowIterator
{
public:
  ObMultiVersionRowIterator();
  ~ObMultiVersionRowIterator();
public:
  int init(ObQueryEngine &query_engine,
           ObMvccAccessCtx &ctx,
           const common::ObVersionRange &version_range,
           const ObMvccScanRange &range);
  int get_next_row(const ObMemtableKey *&key, ObMultiVersionValueIterator *&value_iter);
  void get_tnode_dml_stat(storage::ObTransNodeDMLStat &mt_stat) const;
  void reset();
private:
  int try_cleanout_mvcc_row_(ObMvccRow *value);
  int try_cleanout_tx_node_(ObMvccRow *value, ObMvccTransNode *tnode);
  DISALLOW_COPY_AND_ASSIGN(ObMultiVersionRowIterator);
private:
  bool is_inited_;
  ObMvccAccessCtx *ctx_;
  common::ObVersionRange version_range_;
  ObMultiVersionValueIterator value_iter_;
  ObQueryEngine *query_engine_;
  ObIQueryEngineIterator *query_engine_iter_;
  int64_t insert_row_count_;
  int64_t update_row_count_;
  int64_t delete_row_count_;
};

}
}

#endif /* OCEANBASE_MEMTABLE_MVCC_OB_MULTI_VERSION_ITERATOR_ */
