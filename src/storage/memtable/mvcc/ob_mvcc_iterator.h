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

class ObIMvccValueIterator
{
public:
  ObIMvccValueIterator() {}
  virtual ~ObIMvccValueIterator() {}
  virtual int get_next_node(const void *&tnode) = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMvccValueIterator : public ObIMvccValueIterator
{
public:
  ObMvccValueIterator()
      : is_inited_(false),
        ctx_(NULL),
        value_(NULL),
        version_iter_(NULL),
        last_trans_version_(share::SCN::max_scn()),
        skip_compact_(false)
  {
  }
  virtual ~ObMvccValueIterator() {}
public:
  int init(ObMvccAccessCtx &ctx,
           const ObMemtableKey *key,
           ObMvccRow *value,
           const ObQueryFlag &query_flag,
           const bool skip_compact);
  OB_INLINE bool is_exist()
  {
    return (NULL != version_iter_);
  }
  virtual int get_next_node(const void *&tnode);
  void reset()
  {
    is_inited_ = false;
    ctx_ = NULL;
    value_ = NULL;
    version_iter_ = NULL;
    last_trans_version_ = share::SCN::max_scn();
  }
  int check_row_locked(storage::ObStoreRowLockState &lock_state);
  const transaction::ObTransID get_trans_id() const { return ctx_->get_tx_id(); }
  share::SCN get_snapshot_version() const { return ctx_->get_snapshot_version(); }
  ObMvccAccessCtx *get_mvcc_acc_ctx() { return ctx_; }
  const ObMvccAccessCtx *get_mvcc_acc_ctx() const { return ctx_; }
  const ObMvccRow *get_mvcc_row() const { return value_; }
  const ObMvccTransNode *get_trans_node() const { return version_iter_; }
private:
  int lock_for_read_(const ObQueryFlag &flag);
  int lock_for_read_inner_(const ObQueryFlag &flag, ObMvccTransNode *&iter);
  int try_cleanout_tx_node_(ObMvccTransNode *tnode);
  void move_to_next_node_();
  void lock_begin(int64_t &lock_start_time) const;
  void lock_for_read_end(const int64_t lock_start_time, int64_t ret) const;
private:
  static const int64_t WAIT_COMMIT_US = 20 * 1000;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccValueIterator);
private:
  bool is_inited_;
  ObMvccAccessCtx *ctx_;
  ObMvccRow *value_;
  ObMvccTransNode *version_iter_;
  share::SCN last_trans_version_;
  bool skip_compact_;
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
           const ObQueryFlag &query_flag);
  int get_next_row(const ObMemtableKey *&key,
                   ObMvccValueIterator *&value_iter,
                   uint8_t& iter_flag,
                   const bool skip_compact = false);
  void reset();
  int get_key_val(const ObMemtableKey*& key, ObMvccRow*& row);
  int try_purge(const transaction::ObTxSnapshot &snapshot_info,
                const ObMemtableKey* key, ObMvccRow* row);
  uint8_t get_iter_flag()
  {
    return query_engine_iter_? query_engine_iter_->get_iter_flag(): 0;
  }
private:
  int check_and_purge_row_(const ObMemtableKey *key, ObMvccRow *row, bool &purged);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccRowIterator);
private:
  bool is_inited_;
  ObMvccAccessCtx *ctx_;
  ObQueryFlag query_flag_;
  ObMvccValueIterator value_iter_;
  ObQueryEngine *query_engine_;
  ObIQueryEngineIterator *query_engine_iter_;
};

}
}

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ITERATOR_
