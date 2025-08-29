/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_BLOCK_STAT_ITER_H_
#define OB_BLOCK_STAT_ITER_H_

#include "storage/blocksstable/index_block/ob_sstable_index_scanner.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/tx_storage/ob_access_service.h"
#include "ob_block_stat_collector.h"

namespace oceanbase
{
namespace storage
{

class ObBlockStatScanParam
{
public:
  ObBlockStatScanParam();
  virtual ~ObBlockStatScanParam() = default;
  void reset();
  bool is_valid() const;
  int init(
      const ObIArray<ObSkipIndexColMeta> &stat_cols,
      const ObIArray<uint32_t> &stat_projectors,
      ObTableScanParam &scan_param,
      bool scan_single_major_only = false,
      bool scan_max_sstable_block_granule = false);

  const ObIArray<ObSkipIndexColMeta> *get_stat_cols() const { return stat_cols_; }
  const ObIArray<uint32_t> *get_stat_projectors() const { return stat_projectors_; }
  ObTableScanParam *get_scan_param() const { return scan_param_; }
  bool is_scan_single_major_only() const { return scan_single_major_only_; }
  bool is_scan_max_sstable_block_granule() const { return scan_max_sstable_block_granule_; }
  TO_STRING_KV(KPC_(stat_cols), KPC_(stat_projectors), KPC_(scan_param),
      K_(scan_single_major_only), K_(scan_max_sstable_block_granule));
private:
  const ObIArray<ObSkipIndexColMeta> *stat_cols_;
  const ObIArray<uint32_t> *stat_projectors_;
  ObTableScanParam *scan_param_; // TODO: maybe we do not need table scan param when scan major only
  bool scan_single_major_only_;
  bool scan_max_sstable_block_granule_;
};

// Iterator for block-level statistics in one single tablet
// "Block" here means a collection of contunious stored data in a specific key range.
class ObBlockStatIterator
{
public:
  ObBlockStatIterator();
  virtual ~ObBlockStatIterator() { reset(); }
  void reset();
  ObStoreCtxGuard &get_ctx_guard() { return ctx_guard_; }

  int init(const ObTabletHandle &tablet_handle, ObBlockStatScanParam &scan_param);
  int advance_to(const ObDatumRowkey &advance_key, const bool inclusive);
  int get_next(const ObDatumRow *&agg_row, const ObDatumRowkey *&endkey);
private:
  class SSTableIter
  {
  public:
    SSTableIter() : idx_scanner_(nullptr), idx_row_(nullptr), iter_end_(false) {}
    SSTableIter(ObSSTableIndexScanner *idx_scanner)
      : idx_scanner_(idx_scanner), idx_row_(nullptr), iter_end_(false) {}
    const ObSSTableIndexRow *get_curr_index_row() const { return idx_row_; }
    bool is_iter_end() const { return iter_end_; }
    int next();
    inline int advance_to(const ObDatumRowkey &advance_key, const bool inclusive)
    {
      OB_ASSERT(nullptr != idx_scanner_);
      return idx_scanner_->advance_to(advance_key, inclusive);
    }
    void reset(ObIAllocator *allocator)
    {
      if (nullptr != idx_scanner_) {
        idx_scanner_->reset();
        idx_scanner_->~ObSSTableIndexScanner();
        if (nullptr != allocator) {
          allocator->free(idx_scanner_);
        }
        idx_scanner_ = nullptr;
      }
      idx_row_ = nullptr;
      iter_end_ = false;
    }
    TO_STRING_KV(KP_(idx_scanner));
  private:
    ObSSTableIndexScanner *idx_scanner_;
    const ObSSTableIndexRow *idx_row_;
    bool iter_end_;
  };
  class MemTableIter
  {
  public:
    MemTableIter() : memtable_scanner_(nullptr), row_(nullptr), iter_end_(false) {}
    MemTableIter(ObStoreRowIterator *memtable_scanner)
      : memtable_scanner_(memtable_scanner), row_(nullptr), iter_end_(false) {}
    const blocksstable::ObDatumRow *get_curr_row() const { return row_; }
    bool is_iter_end() const { return iter_end_; }
    int next();
    void reset(ObIAllocator *allocator)
    {
      if (nullptr != memtable_scanner_) {
        memtable_scanner_->reset();
        memtable_scanner_->~ObStoreRowIterator();
        if (nullptr != allocator) {
          allocator->free(memtable_scanner_);
        }
        memtable_scanner_ = nullptr;
      }
      row_ = nullptr;
    }
    TO_STRING_KV(KP_(memtable_scanner), K_(iter_end));
  private:
    ObStoreRowIterator *memtable_scanner_;
    const blocksstable::ObDatumRow *row_;
    bool iter_end_;
  };
private:
  int init_scan_range(const ObTabletHandle &tablet_handle, ObTableScanParam &scan_param);
  int init_memtable_access_param(const ObTabletHandle &tablet_handle, ObTableScanParam &scan_param);
  int refresh_scan_table_on_demand();
  int refresh_tablet_iter();
  int prepare_scan_tables();
  int construct_iters();
  void reset_iters();
  int next_baseline_range(bool &beyond_range);
  int collect_sstable_idx_rows(const bool drain_all_iters);
  int collect_memtable_scan_rows(const bool drain_all_iters);
  int advance_sstable_iters(const ObDatumRowkey &advance_key, const bool inclusive);
  int advance_memtable_iters(const ObDatumRowkey &advance_key, const bool inclusive);
  int check_rowkey_in_range(const ObDatumRow &row, bool &rowkey_in_range) const;
  int check_rowkey_in_range(const ObDatumRowkey &rowkey, bool &rowkey_in_range) const;
  int shrink_scan_range(const ObDatumRowkey &start_key);
  SSTableIter &get_baseline_block_iter() { return sstable_iters_.at(0); }
  bool is_all_iter_end() const;
private:
  const ObBlockStatScanParam *scan_param_;
  ObArenaAllocator allocator_;
  ObBlockStatCollector stat_collector_;
  ObTableScanRange scan_range_;
  ObGetTableParam get_table_param_;
  ObStoreCtxGuard ctx_guard_;
  // ObTableAccessParam and ObTableAccessContext for memtable scan
  ObTableAccessParam main_table_param_;
  ObTableAccessContext main_table_ctx_;
  ObSSTableIndexScanParam sstable_idx_scan_param_;
  common::ObSEArray<ObITable *, 4> scan_tables_;
  common::ObSEArray<MemTableIter, 4> memtable_iters_;
  common::ObSEArray<SSTableIter, 4>sstable_iters_;
  const ObITableReadInfo *rowkey_read_info_;
  const ObDatumRowkey *curr_endkey_;
  ObDatumRange curr_scan_range_;
  ObDatumRowkey curr_scan_start_key_;
  ObIAllocator *iter_allocator_;
  bool iter_end_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObBlockStatIterator);
};

} // namespace storage
} // namespace oceanbase

#endif