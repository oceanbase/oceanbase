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

#ifndef OB_STORAGE_OB_SSTABLE_ROW_SCANNER_H_
#define OB_STORAGE_OB_SSTABLE_ROW_SCANNER_H_

#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/column_store/ob_column_store_util.h"
#include "storage/column_store/ob_co_prefetcher.h"
#include "ob_index_tree_prefetcher.h"

namespace oceanbase {
using namespace blocksstable;
namespace storage {
template<typename PrefetchType = ObIndexTreeMultiPassPrefetcher<>>
class ObSSTableRowScanner : public ObStoreRowIterator
{
public:
  ObSSTableRowScanner() :
      ObStoreRowIterator(),
      is_opened_(false),
      sstable_(nullptr),
      iter_param_(nullptr),
      access_ctx_(nullptr),
      prefetcher_(),
      macro_block_reader_(),
      micro_scanner_(nullptr),
      micro_data_scanner_(nullptr),
      mv_micro_data_scanner_(nullptr),
      mv_di_micro_data_scanner_(nullptr),
      skip_scanner_(nullptr),
      skip_state_(),
      range_idx_(0),
      is_di_base_iter_(false),
      cur_range_idx_(-1)
  {
    type_ = ObStoreRowIterator::IteratorScan;
  }
  virtual ~ObSSTableRowScanner();
  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
  virtual bool can_blockscan() const override;
  virtual bool can_batch_scan() const override;
  OB_INLINE bool is_di_base_iter() { return is_di_base_iter_; }
  virtual int get_next_rowkey(int64_t &curr_scan_index,
                              blocksstable::ObDatumRowkey& rowkey,
                              common::ObIAllocator &allocator) final;
  OB_INLINE bool is_end_of_scan() const
  {
    return prefetcher_.is_prefetch_end_ &&
        prefetcher_.cur_range_fetch_idx_ >= prefetcher_.cur_range_prefetch_idx_;
  }
  TO_STRING_KV(K_(is_opened), K_(range_idx), K_(is_di_base_iter), K_(cur_range_idx),
               KP_(micro_scanner), KP_(micro_data_scanner), KP_(mv_micro_data_scanner), KP_(mv_di_micro_data_scanner),
               KP_(skip_scanner), K_(skip_state), KP_(sstable), KP_(iter_param), KP_(access_ctx), K_(prefetcher));
protected:
  int inner_open(
      const ObTableIterParam &iter_param,
      ObTableAccessContext &access_ctx,
      ObITable *table,
      const void *query_range);
  int inner_get_next_row_with_row_id(const ObDatumRow *&store_row, ObCSRowId &row_id);
  virtual int inner_get_next_row(const ObDatumRow *&store_row) override;
  virtual int fetch_row(ObSSTableReadHandle &read_handle, const ObDatumRow *&store_row);
  virtual int refresh_blockscan_checker(const blocksstable::ObDatumRowkey &rowkey) override final;
  virtual int get_blockscan_border_rowkey(blocksstable::ObDatumRowkey &border_rowkey) override final
  {
    int ret = OB_SUCCESS;
    border_rowkey = prefetcher_.get_border_rowkey();
    return ret;
  }
  virtual int get_next_rows() override;
  // for column store
  int get_blockscan_start(ObCSRowId &start, int32_t &range_idx, BlockScanState &block_scan_state);
  int forward_blockscan(ObCSRowId &end, BlockScanState &block_scan_state, const ObCSRowId begin);
  int try_skip_deleted_row(ObCSRowId &co_current);
  virtual bool is_multi_get() const { return false; }

private:
  int init_micro_scanner();
  int open_cur_data_block(ObSSTableReadHandle &read_handle);
  int fetch_rows(ObSSTableReadHandle &read_handle);
  // For columnar store
  int update_border_rowid_for_column_store();
  int update_start_rowid_for_column_store();
  int prepare_micro_scanner_for_column_store(ObSSTableReadHandle& read_handle);
  int detect_border_rowid_for_column_store();
  int try_refreshing_blockscan_checker_for_column_store(
      const int64_t start_offset,
      const int64_t end_offset);
  OB_INLINE bool has_skip_scanner() const
  {
    return nullptr != skip_scanner_ && skip_scanner_->should_skip();
  }
  OB_INLINE bool has_skip_scanner_and_not_skipped(const ObMicroIndexInfo &index_info, const bool ignore_disabled = false) const
  {
    return nullptr != skip_scanner_ &&
           (ignore_disabled || !skip_scanner_->is_disabled()) &&
           !index_info.skip_state_.is_skipped();
  }
  OB_INLINE void preprocess_skip_scanner(ObMicroIndexInfo &index_info)
  {
    if (nullptr != skip_scanner_ && skip_scanner_->is_disabled()) {
      index_info.skip_state_.set_state(0, ObIndexSkipNodeState::PREFIX_SKIPPED_LEFT);
    }
  }

protected:
  bool is_opened_;
  ObSSTable *sstable_;
  const ObTableIterParam *iter_param_;
  ObTableAccessContext *access_ctx_;
  PrefetchType prefetcher_;
  ObMacroBlockReader macro_block_reader_;
  ObIMicroBlockRowScanner *micro_scanner_;
  ObMicroBlockRowScanner *micro_data_scanner_;
  ObMultiVersionMicroBlockRowScanner *mv_micro_data_scanner_;
  ObMultiVersionDIMicroBlockRowScanner *mv_di_micro_data_scanner_;
  ObIndexSkipScanner *skip_scanner_;
  ObIndexSkipState skip_state_;
  int64_t range_idx_;
private:
  bool is_di_base_iter_;
  int64_t cur_range_idx_;
  friend class ObCOSSTableRowScanner;
};

}
}
#endif //OB_STORAGE_OB_SSTABLE_ROW_SCANNER_H_
