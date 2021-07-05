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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ENGINE_
#define OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ENGINE_

#include "share/ob_define.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/ob_row_compactor.h"
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/mvcc/ob_multi_version_iterator.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {

namespace storage {
class ObRowPurgeHandle;
class ObStoreCtx;
}  // namespace storage

namespace memtable {
////////////////////////////////////////////////////////////////////////////////////////////////////
struct ObRowData;
class ObIMvccCtx;
class RowHeaderGetter;
class ObMemtableData;
class ObMemtableDataHeader;
class ObMemtable;
class ObMTKVBuilder;

class ObMvccEngine {
public:
  ObMvccEngine();
  virtual ~ObMvccEngine();

public:
  virtual int init(
      common::ObIAllocator* allocator, ObMTKVBuilder* kv_builder, ObQueryEngine* query_engine, ObMemtable* memtable);
  virtual void destroy();

public:
  int get_max_trans_version(const ObMemtableKey* key, bool& locked, int64_t& max_trans_version);
  int create_kv(const ObMemtableKey* key, ObMemtableKey* stored_key, ObMvccRow*& value);
  int store_data(ObMvccRow& value, const ObMemtableData* data, const int64_t version, const int64_t timestamp);

  int get_trans_version(ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info,
      const ObQueryFlag& query_flag, const ObMemtableKey* key, ObMvccRow* row, int64_t& trans_version);
  virtual int get(ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info, const ObQueryFlag& query_flag,
      const bool skip_compact, const ObMemtableKey* parameter_key, ObMemtableKey* internal_key,
      ObMvccValueIterator& value_iter);
  virtual int scan(ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info, const ObQueryFlag& query_flag,
      const ObMvccScanRange& range, ObMvccRowIterator& row_iter);
  virtual int prefix_exist(ObIMvccCtx& ctx, const ObMemtableKey* parameter_key, bool& may_exist);
  // scan method for multi version mvcc engine
  virtual int scan(ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info, const ObMvccScanRange& range,
      transaction::ObTransStateTableGuard& trans_table_guard, ObMultiVersionRowIterator& row_iter);
  virtual int replay(ObIMvccCtx& ctx, const ObMemtableKey* key, const ObMemtableData* data, const uint32_t modify_count,
      const uint32_t acc_checksum, const int64_t version, const int32_t sql_no, const int64_t log_timestamp);
  virtual int store_data(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMvccRow& value, const ObMemtableData* data,
      const ObRowData* old_row, const int64_t version, const int32_t sql_no);
  virtual int check_row_locked(ObIMvccCtx& ctx, const ObMemtableKey* key, bool& is_locked, uint32_t& lock_descriptor,
      int64_t& max_trans_version);
  virtual int create_kv(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMemtableKey* stored_key, ObMvccRow*& value,
      RowHeaderGetter& getter, bool& is_new_add);
  virtual int lock(
      ObIMvccCtx& ctx, const ObMemtableKey* stored_key, const ObMvccRow* value, const bool is_replay, bool& new_locked);
  virtual int relocate_lock(ObIMvccCtx& ctx, const ObMemtableKey* stored_key, const ObMvccRow* value,
      const bool is_replay, const bool need_lock_for_write, const bool is_sequential_relocate, bool& new_locked);
  virtual int unlock(
      ObIMvccCtx& ctx, const ObMemtableKey* key, const ObMvccRow* value, const bool is_replay, const bool new_locked);
  virtual int append_kv(ObIMvccCtx& ctx, const ObMemtableKey* stored_key, ObMvccRow* value, const bool is_replay,
      const bool new_locked, const int32_t sql_no, const bool is_sequential_relocate);

  int estimate_scan_row_count(const ObMvccScanRange& range, storage::ObPartitionEst& part_est);
  int relocate_data(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMvccRow& value, const ObMvccTransNode* trans_node,
      const bool is_replay, const bool need_fill_redo, const ObRowData* old_row, const int64_t version,
      const bool is_stmt_committed, const int32_t sql_no, const int64_t log_ts, const bool is_sequential_relocate);

private:
  int m_prepare_kv(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMemtableKey* stored_key, ObMvccRow*& value,
      const bool is_replay, const int32_t sql_no, bool& is_new_add);
  int m_store_data(ObIMvccCtx& ctx, const ObMemtableKey* key, ObMvccRow& value, const ObMemtableData* data,
      const ObRowData* old_row, const bool is_replay, const uint32_t modify_count, const uint32_t acc_checksum,
      const int64_t version, const int32_t sql_no, const int64_t log_timestamp);
  int try_compact_row(ObIMvccCtx& ctx, const transaction::ObTransSnapInfo& snapshot_info, ObMvccRow& row);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccEngine);
  bool is_inited_;
  ObMTKVBuilder* kv_builder_;
  ObQueryEngine* query_engine_;
  common::ObIAllocator* engine_allocator_;
  ObMemtable* memtable_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ENGINE_
