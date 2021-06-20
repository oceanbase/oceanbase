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

#include "share/ob_define.h"
#include "common/ob_range.h"

#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_row.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace memtable {

class ObIMvccCtx;

struct ObMvccScanRange {
  common::ObBorderFlag border_flag_;
  ObMemtableKey* start_key_;
  ObMemtableKey* end_key_;

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
    return (NULL != start_key_ && NULL != end_key_);
  }

  int64_t to_string(char* buf, const int64_t buf_len) const
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

class ObIMvccValueIterator {
public:
  ObIMvccValueIterator()
  {}
  virtual ~ObIMvccValueIterator()
  {}
  virtual int get_next_node(const void*& tnode) = 0;
  virtual enum storage::ObRowDml get_first_dml() = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMvccValueIterator : public ObIMvccValueIterator {
public:
  ObMvccValueIterator();
  virtual ~ObMvccValueIterator();

public:
  int init(const ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info, const ObMemtableKey* key,
      const ObMvccRow* value, const ObQueryFlag& query_flag, const bool skip_compact);
  bool is_exist();
  virtual int get_next_node(const void*& tnode);
  void reset();
  virtual enum storage::ObRowDml get_first_dml();
  const ObMvccRow* get_mvcc_row() const
  {
    return value_;
  }
  const ObMvccTransNode* get_trans_node() const
  {
    return version_iter_;
  }
  bool read_by_sql_no(const ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info, ObMvccRow* value,
      const ObQueryFlag& query_flag);

private:
  int find_start_pos(const int64_t read_snapshot);
  int find_trans_node_below_version(const int64_t read_snapshot, const bool is_safe_read);
  int check_trans_node_readable(const int64_t read_snapshot);
  void print_conflict_trace_log();
  void mark_trans_node_for_elr(const int64_t read_snapshot, const bool is_prewarm);

  void move_to_next_node();

private:
  static const int64_t WAIT_COMMIT_US = 20 * 1000;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccValueIterator);

private:
  bool is_inited_;
  const ObIMvccCtx* ctx_;
  const ObMvccRow* value_;
  ObMvccTransNode* version_iter_;
  int64_t last_trans_version_;
  bool skip_compact_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMvccRowIterator {
public:
  ObMvccRowIterator();
  virtual ~ObMvccRowIterator();

public:
  int init(ObQueryEngine& query_engine, const ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info,
      const ObMvccScanRange& range, const ObQueryFlag& query_flag);
  int get_next_row(
      const ObMemtableKey*& key, ObMvccValueIterator*& value_iter, uint8_t& iter_flag, const bool skip_compact = false);
  void reset();
  int get_key_val(const ObMemtableKey*& key, ObMvccRow*& row);
  int try_purge(const transaction::ObTransSnapInfo& snapshot_info, const ObMemtableKey* key, ObMvccRow* row);
  int get_end_gap_key(const transaction::ObTransSnapInfo& snapshot_info, const ObStoreRowkey*& key, int64_t& size);
  uint8_t get_iter_flag()
  {
    return query_engine_iter_ ? query_engine_iter_->get_iter_flag() : 0;
  }

private:
  int check_and_purge_row_(const ObMemtableKey* key, ObMvccRow* row, bool& purged);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccRowIterator);

private:
  bool is_inited_;
  const ObIMvccCtx* ctx_;
  transaction::ObTransSnapInfo snapshot_info_;
  ObQueryFlag query_flag_;
  ObMvccValueIterator value_iter_;
  ObQueryEngine* query_engine_;
  ObIQueryEngineIterator* query_engine_iter_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ITERATOR_
