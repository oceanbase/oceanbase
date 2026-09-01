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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ITERATOR_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ITERATOR_

#include "common/ob_range.h"
#include "share/ob_define.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/concurrency_control/ob_trans_stat_row.h"

namespace oceanbase
{
namespace storage
{
class ObStoreRowLockState;
}
namespace memtable
{

class ObMvccAccessCtx;

struct ObMvccScanRange
{
  common::ObBorderFlag border_flag_;
  ObMemtableKey *start_key_;
  ObMemtableKey *end_key_;

  ObMvccScanRange()
  {
    reset();
  }

  void reset()
  {
    border_flag_.set_data(0);
    start_key_ = NULL;
    end_key_ = NULL;
  }

  bool is_valid() const
  {
    return (NULL != start_key_
        && NULL != end_key_);
  }

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
    } else {
      if (border_flag_.inclusive_start()) {
        common::databuff_printf(buf, buf_len, pos, "[");
      } else {
        common::databuff_printf(buf, buf_len, pos, "(");
      }

      if (border_flag_.is_min_value()) {
        common::databuff_printf(buf, buf_len, pos, "min,");
      } else {
        common::databuff_printf(buf, buf_len, pos, "%p,", start_key_);
      }

      if (border_flag_.is_max_value()) {
        common::databuff_printf(buf, buf_len, pos, "max");
      } else {
        common::databuff_printf(buf, buf_len, pos, "%p", end_key_);
      }

      if (border_flag_.inclusive_end()) {
        common::databuff_printf(buf, buf_len, pos, "]");
      } else {
        common::databuff_printf(buf, buf_len, pos, ")");
      }
    }
    return pos;
  }
};

////////////////////////////////////////////////////////////////////////////////////////////////////

// Filter for iterating TransNodes in a memtable row after a truncate-tablet MDS operation.
// Truncate cleans up OLD data: nodes committed BEFORE the truncate point are excluded;
// nodes committed at or after the truncate point survive.
//
// Survival rules (committed state must be respected first; SCN is only a follower
// fallback for the case where redo has been replayed but commit has not):
//   - Aborted / lock nodes:                             reject
//   - Compact nodes:    survives if is_committed && trans_version >= snapshot_version_
//   - Normal committed/ELR: survives if trans_version >= snapshot_version_
//   - Normal running on follower: survives if node_scn >= end_scn_
//     (If the node's redo is at or after the truncate redo, the transaction started
//     after the truncate and its data should be preserved.)
//
// Partial validity: snapshot_version_ and end_scn_ may be set independently.
// When snapshot_version_ is valid it is used to filter committed and ELR nodes; when
// it is not valid, end_scn_ is used as a SCN-based fallback for all node types.
// Running nodes are always filtered by end_scn_ (SCN-based) when available.
//
// The truncate-dimension decision lives entirely in is_truncated(). Both the
// read path and multi-version dump iterators call is_truncated() directly so
// both paths agree on exactly what truncate removes.
class ObMemtableTruncateFilter
{
public:
  ObMemtableTruncateFilter() : snapshot_version_(0), end_scn_() {}
  ObMemtableTruncateFilter(const int64_t snapshot_version, const share::SCN &end_scn)
    : snapshot_version_(snapshot_version), end_scn_(end_scn) {}

  void reset()
  {
    snapshot_version_ = 0;
    end_scn_.reset();
  }

  // Returns true if at least one filter dimension is set.
  bool is_valid() const
  {
    return snapshot_version_ > 0 || end_scn_.is_valid();
  }

  // Core truncate-dimension predicate, free of any lock/abort handling.
  // Returns true when the node carries data produced BEFORE the truncate
  // point and therefore must be EXCLUDED (old data cleaned up by truncate).
  // Data committed at or after the truncate point survives.
  // This is the single source of truth used by both the read path and the
  // multi-version dump iterators, so the two paths always agree on what
  // truncate removes.
  bool is_truncated(const ObMvccTransNode &node) const
  {
    bool truncated = false;
    if (NDT_COMPACT == node.type_) {
      // A committed compact node survives only if it was committed at or after
      // the truncate point; an uncommitted compact node cannot exist, so reject it.
      truncated = !(node.is_committed() &&
                    (snapshot_version_ > 0
                        ? node.get_tx_version().get_val_for_tx() >= snapshot_version_
                        : node.get_scn() >= end_scn_));
    } else if (node.is_committed() || node.is_elr()) {
      // Committed and ELR nodes already carry a transaction version. When
      // snapshot_version_ is valid, use it because redo SCN may precede end_scn_
      // even when the transaction version is after the truncate point.
      // Fall back to SCN only when snapshot_version_ is unavailable.
      truncated = !(snapshot_version_ > 0
                        ? node.get_tx_version().get_val_for_tx() >= snapshot_version_
                        : node.get_scn() >= end_scn_);
    } else {
      // running / commit-not-yet-replayed on follower: SCN-based fallback.
      // Uncommitted nodes whose redo SCN precedes the truncate point belong
      // to old transactions and should be excluded.
      truncated = end_scn_.is_valid() && !(node.get_scn() >= end_scn_);
    }
    return truncated;
  }
  TO_STRING_KV(K_(snapshot_version), K_(end_scn));

private:
  int64_t snapshot_version_;    // survives when: trans_version >= snapshot_version_
  share::SCN end_scn_;          // survives when: node scn >= end_scn_
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMvccValueIterator
{
public:
  ObMvccValueIterator()
    : is_inited_(false),
    has_truncate_filter_(false),
    ctx_(NULL),
    value_(NULL),
    memtable_ls_id_(),
    version_iter_(NULL),
    truncate_filter_()
  {
  }
  virtual ~ObMvccValueIterator() {}
public:
  int init(ObMvccAccessCtx &ctx,
           const ObMemtableKey *key,
           ObMvccRow *value,
           const share::ObLSID memtable_ls_id,
           const ObQueryFlag &query_flag);
  OB_INLINE bool is_exist()
  {
    return (NULL != version_iter_);
  }
  virtual int get_next_node(const void *&tnode);
  void reset()
  {
    is_inited_ = false;
    has_truncate_filter_ = false;
    ctx_ = NULL;
    value_ = NULL;
    memtable_ls_id_.reset();
    version_iter_ = NULL;
    truncate_filter_.reset();
  }
  int check_row_locked(storage::ObStoreRowLockState &lock_state);
  const transaction::ObTransID get_trans_id() const { return ctx_->get_tx_id(); }
  share::SCN get_snapshot_version() const { return ctx_->get_snapshot_version(); }
  ObMvccAccessCtx *get_mvcc_acc_ctx() { return ctx_; }
  const ObMvccAccessCtx *get_mvcc_acc_ctx() const { return ctx_; }
  const ObMvccRow *get_mvcc_row() const { return value_; }
  const ObMvccTransNode *get_trans_node() const { return version_iter_; }
  void get_trans_stat_row(concurrency_control::ObTransStatRow &row);

  // The interface returns the reader's reader_tx_id and snapshot_tx_id. Both of
  // the reader_tx_id and snapshot_tx_id is initialized after the first dml and
  // the former one is used for read latest check and the later one is used for
  // the read between statements(including cursor)
  //
  // NB: Be careful with these interface, because it is only for defensive code
  // usage.
  transaction::ObTransID get_reader_tx_id() const { return ctx_->tx_id_; }
  transaction::ObTransID get_snapshot_tx_id() const { return ctx_->snapshot_.tx_id_; }
  int64_t get_major_snapshot() const { return ctx_->major_snapshot_; }

  TO_STRING_KV(KPC_(value), KPC_(version_iter), K_(has_truncate_filter), KPC_(ctx), K_(memtable_ls_id),
               K(get_major_snapshot()), K_(truncate_filter));

private:
  // Build the truncate filter from ctx when the tablet was just truncated.
  // Precondition: ctx.need_memtable_filter_after_truncate_tablet_ is true.
  // The filter is only an extra truncate-dimension predicate applied in
  // get_next_node(); snapshot visibility is still resolved by lock_for_read_().
  int setup_truncate_filter_(ObMvccAccessCtx &ctx);
  int lock_for_read_(const ObQueryFlag &flag);
  int lock_for_read_inner_(const ObQueryFlag &flag, ObMvccTransNode *&iter);
  int try_cleanout_tx_node_(ObMvccTransNode *tnode);
  void move_to_next_node_();
private:
  static const int64_t WAIT_COMMIT_US = 20 * 1000;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccValueIterator);
private:
  bool is_inited_;
  // Cached at init: true only when truncate filter is active for this iterator.
  bool has_truncate_filter_;
  ObMvccAccessCtx *ctx_;
  ObMvccRow *value_;
  share::ObLSID memtable_ls_id_;
  ObMvccTransNode *version_iter_;
  ObMemtableTruncateFilter truncate_filter_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMvccRowIterator
{
public:
  ObMvccRowIterator();
  virtual ~ObMvccRowIterator();
public:
  int init(ObQueryEngine &query_engine,
           ObMvccAccessCtx &ctx,
           const ObMvccScanRange &range,
           const share::ObLSID memtable_ls_id,
           const ObQueryFlag &query_flag);
  int get_next_row(const ObMemtableKey *&key,
                   ObMvccValueIterator *&value_iter,
                   storage::ObStoreRowLockState &lock_state);
  void reset();
  int get_key_val(const ObMemtableKey*& key, ObMvccRow*& row);
private:
  int check_and_purge_row_(const ObMemtableKey *key, ObMvccRow *row, bool &purged);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccRowIterator);
private:
  bool is_inited_;
  ObMvccAccessCtx *ctx_;
  share::ObLSID memtable_ls_id_;
  ObQueryFlag query_flag_;
  ObMvccValueIterator value_iter_;
  ObQueryEngine *query_engine_;
  ObIQueryEngineIterator *query_engine_iter_;
};

}
}

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ITERATOR_
