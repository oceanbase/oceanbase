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

#ifndef OCEANBASE_STORAGE_OB_TABLE_SCAN_ITERATOR_
#define OCEANBASE_STORAGE_OB_TABLE_SCAN_ITERATOR_

#include "common/row/ob_row_iterator.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_col_map.h"
#include "storage/ob_i_store.h"
#include "ob_multiple_get_merge.h"
#include "ob_multiple_merge.h"
#include "ob_multiple_multi_scan_merge.h"
#include "ob_multiple_scan_merge.h"
#include "ob_multiple_skip_scan_merge.h"
#include "ob_multiple_multi_skip_scan_merge.h"
#include "ob_single_merge.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx/ob_trans_service.h"
#include "ob_table_scan_range.h"
#include "ob_global_iterator_pool.h"

namespace oceanbase
{
namespace storage
{
class ObTableScanParam;
class ObTableReadInfo;
class ObISampleIterator;
class ObMemtableRowSampleIterator;
class ObRowSampleIterator;
class ObBlockSampleIterator;

class ObTableScanIterator : public common::ObNewRowIterator
{
public:
  ObTableScanIterator();
  virtual ~ObTableScanIterator();
  int init(ObTableScanParam &scan_param, const ObTabletHandle &tablet_handle);
  int switch_param(ObTableScanParam &scan_param, const ObTabletHandle &tablet_handle);
  int get_next_row(blocksstable::ObDatumRow *&row);
  virtual int get_next_row(common::ObNewRow *&row) override;
  virtual int get_next_row() override { blocksstable::ObDatumRow *r = nullptr; return get_next_row(r); }
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  int rescan(ObTableScanParam &scan_param);
  void reuse();
  void reset_for_switch();
  virtual void reset();
  ObStoreCtxGuard &get_ctx_guard() { return ctx_guard_; }

  // A offline ls will disable replay status and kill all part_ctx on the follower.
  // We can not read the uncommitted data which has not replay commit log yet.
  int check_ls_offline_after_read();
public:
  static constexpr int64_t RP_MAX_FREE_LIST_NUM = 1024;
  static constexpr const char LABEL[] = "RPTableScanIter";
private:
  static const int64_t LOOP_RESCAN_BUFFER_SIZE = 8 * 1024; // 8K
  int prepare_table_param(const ObTabletHandle &tablet_handle);
  int prepare_table_context();
  bool can_use_global_iter_pool(const ObQRIterType iter_type) const;
  int prepare_cached_iter_node();
  void try_release_cached_iter_node(const ObQRIterType rescan_iter_type);
  template<typename T> int init_scan_iter(T *&iter);
  template<typename T> void reset_scan_iter(T *&iter);
  int switch_scan_param(ObMultipleMerge &iter);
  void reuse_row_iters();
  int rescan_for_iter();
  int switch_param_for_iter();
  int open_iter();

  // if need retire to row sample, sample_memtable_ranges must not be null
  int can_retire_to_memtable_row_sample_(bool &retire, ObIArray<blocksstable::ObDatumRange> &sample_memtable_ranges);
  int get_memtable_sample_ranges(const ObIArray<ObITable *> &memtables,
                                 ObIArray<blocksstable::ObDatumRange> &sample_memtable_ranges);

  // for read uncommitted data, txn possible rollbacked before iterate
  // check txn status after read rows to ensure read result is correct
  int check_txn_status_if_read_uncommitted_();
  int init_and_open_get_merge_iter_();
  int init_and_open_scan_merge_iter_();
  int init_and_open_block_sample_iter_();
  int init_and_open_row_sample_iter_();
  int init_and_open_memtable_row_sample_iter_(const ObIArray<blocksstable::ObDatumRange> &scan_ranges);
  int sort_sample_ranges();

private:
  bool is_inited_;
  ObQRIterType current_iter_type_;
  ObSingleMerge *single_merge_;
  ObMultipleGetMerge *get_merge_;
  ObMultipleScanMerge *scan_merge_;
  ObMultipleMultiScanMerge *multi_scan_merge_;
  ObMultipleSkipScanMerge *skip_scan_merge_;
  ObMemtableRowSampleIterator *memtable_row_sample_iterator_;
  ObRowSampleIterator *row_sample_iterator_;
  ObBlockSampleIterator *block_sample_iterator_; // TODO: @yuanzhe refactor
  // we should consider the constructor cost
  ObTableAccessParam main_table_param_;
  ObTableAccessContext main_table_ctx_;
  ObGetTableParam get_table_param_;

  ObStoreCtxGuard ctx_guard_;
  ObTableScanParam *scan_param_;
  ObTableScanRange table_scan_range_;
  ObQueryRowIterator *main_iter_;
  ObSEArray<ObDatumRange, 1> sample_ranges_;
  CachedIteratorNode *cached_iter_node_;
  ObQueryRowIterator **cached_iter_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableScanIterator);
};

} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLE_SCAN_ITERATOR_
