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
#include "storage/memtable/mvcc/ob_mvcc_define.h"
#include "storage/memtable/mvcc/ob_multi_version_iterator.h"
#include "storage/memtable/mvcc/ob_mvcc_iterator.h"
#include "storage/memtable/mvcc/ob_query_engine.h"
#include "storage/memtable/ob_row_compactor.h"

namespace oceanbase
{
namespace storage
{
class ObRowPurgeHandle;
struct ObPartitionEst;
}

namespace memtable
{
////////////////////////////////////////////////////////////////////////////////////////////////////
struct ObRowData;
class ObIMemtableCtx;
class ObMvccAccessCtx;
class RowHeaderGetter;
class ObMemtableData;
class ObMemtableDataHeader;
class ObMemtable;
class ObMTKVBuilder;

// class for concurrent control
class ObMvccEngine
{
public:
  ObMvccEngine();
  virtual ~ObMvccEngine();
  virtual int init(common::ObIAllocator *allocator,
                   ObMTKVBuilder *kv_builder,
                   ObQueryEngine *query_engine,
                   ObMemtable *memtable);
  virtual void destroy();
public:
  // Mvcc engine write interface

  // Return the ObMvccRow according to the memtable key or create
  // the new one if the memtable key is not exist.
  int create_kv(const ObMemtableKey *key,
                ObMemtableKey *stored_key,
                ObMvccRow *&value,
                RowHeaderGetter &getter,
                bool &is_new_add);

  // mvcc_write builds the ObMvccTransNode according to the arg and write
  // into the head of the value. It will return OB_SUCCESS if successfully written,
  // OB_TRY_LOCK_ROW_CONFLICT if encountering write-write conflict or
  // OB_TRANSACTION_SET_VIOLATION if encountering lost update. The interesting
  // implementation about mvcc_write is located in ob_mvcc_row.cpp/.h
  int mvcc_write(ObIMemtableCtx &ctx,
                 const concurrent_control::ObWriteFlag write_flag,
                 const transaction::ObTxSnapshot &snapshot,
                 ObMvccRow &value,
                 const ObTxNodeArg &arg,
                 ObMvccWriteResult &res);

  // mvcc_undo removes the newly written tx node. It never returns error
  // and always succeed.
  void mvcc_undo(ObMvccRow *value);

  // mvcc_replay builds the ObMvccTransNode according to the arg and replay
  // into the ascending ordering of the value based on the scn recorded in ctx.
  int mvcc_replay(ObIMemtableCtx &ctx,
                  const ObMemtableKey *stored_key,
                  ObMvccRow &value,
                  const ObTxNodeArg &arg,
                  ObMvccReplayResult &res);

  // ensure_kv is used to make sure b-tree is no longer broken by the deleted
  // row.
  int ensure_kv(const ObMemtableKey *stored_key,
                ObMvccRow *value);

  // Mvcc engine read interface
  int get(ObMvccAccessCtx &ctx,
          const ObQueryFlag &query_flag,
          const ObMemtableKey *parameter_key,
          ObMemtableKey *internal_key,
          ObMvccValueIterator &value_iter,
          storage::ObStoreRowLockState &lock_state);
  int scan(ObMvccAccessCtx &ctx,
           const ObQueryFlag &query_flag,
           const ObMvccScanRange &range,
           ObMvccRowIterator &row_iter);
  int scan(ObMvccAccessCtx &ctx,
           const ObMvccScanRange &range,
           const common::ObVersionRange &version_range,
           ObMultiVersionRowIterator &row_iter);
  int prefix_exist(const ObMemtableKey *parameter_key, bool &may_exist);
  // check_row_locked check the status of the tx node for forzen memtable. It
  // also returns tx_id for same txn write and max_trans_version for TSC check
  int check_row_locked(ObMvccAccessCtx &ctx,
                       const ObMemtableKey *key,
                       storage::ObStoreRowLockState &lock_state);
  // estimate_scan_row_count estimate the row count for the range
  int estimate_scan_row_count(const transaction::ObTransID &tx_id,
                              const ObMvccScanRange &range,
                              storage::ObPartitionEst &part_est) const;
private:
  int try_compact_row_when_mvcc_read_(const share::SCN &snapshot_version,
                                      ObMvccRow &row);

  int build_tx_node_(ObIMemtableCtx &ctx,
                     const ObTxNodeArg &arg,
                     ObMvccTransNode *&node);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMvccEngine);
  bool is_inited_;
  ObMTKVBuilder *kv_builder_;
  ObQueryEngine *query_engine_;
  common::ObIAllocator *engine_allocator_;
  ObMemtable *memtable_;
};
}
}

#endif //OCEANBASE_MEMTABLE_MVCC_OB_MVCC_ENGINE_
