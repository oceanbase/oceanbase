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

#ifndef OB_SSTABLE_ROW_WHOLE_SCANNER_H_
#define OB_SSTABLE_ROW_WHOLE_SCANNER_H_

#include "storage/ob_sstable_row_iterator.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"

namespace oceanbase {
namespace storage {

class ObSSTableRowWholeScanner : public ObISSTableRowIterator {
private:
  struct MacroScanHandle {
  public:
    MacroScanHandle() : macro_io_handle_(), meta_(), is_left_border_(false), is_right_border_(false)
    {}
    ~MacroScanHandle()
    {}
    void reset();
    int get_block_data(const int64_t cur_micro_cursor, blocksstable::ObMicroBlockData& block_data);

    blocksstable::MacroBlockId macro_block_id_;
    blocksstable::ObMacroBlockHandle macro_io_handle_;
    common::ObSEArray<blocksstable::ObMicroBlockInfo, 512> micro_infos_;
    blocksstable::ObFullMacroBlockMeta meta_;
    blocksstable::ObMacroBlockReader macro_reader_;
    bool is_left_border_;
    bool is_right_border_;

  private:
    DISALLOW_COPY_AND_ASSIGN(MacroScanHandle);
  };

public:
  ObSSTableRowWholeScanner()
      : iter_param_(NULL),
        access_ctx_(NULL),
        sstable_(NULL),
        allocator_(common::ObModIds::OB_SSTABLE_READER),
        prefetch_macro_cursor_(0),
        cur_macro_cursor_(0),
        cur_micro_cursor_(0),
        micro_scanner_(NULL),
        is_opened_(false),
        file_handle_()
  {}

  virtual ~ObSSTableRowWholeScanner();
  int open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx,
      const common::ObExtStoreRange* query_range, const blocksstable::ObMacroBlockCtx& macro_block_ctx,
      ObSSTable* sstable);

protected:
  virtual int inner_open(const ObTableIterParam& iter_param, ObTableAccessContext& access_ctx, ObITable* table,
      const void* query_range) override;
  virtual int inner_get_next_row(const ObStoreRow*& row) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual const common::ObIArray<ObRowkeyObjComparer*>* get_rowkey_cmp_funcs();

private:
  int open_macro_block();
  int prefetch();
  int open_micro_block(const bool is_first_open);
  bool is_multi_version_range(const common::ObExtStoreRange* ext_range, const int64_t multi_version_rowkey_col_cnt)
  {
    const common::ObStoreRange& range = ext_range->get_range();
    return range.is_whole_range() ||
           (range.get_start_key().is_min() && range.get_end_key().get_obj_cnt() == multi_version_rowkey_col_cnt) ||
           (range.get_end_key().is_max() && range.get_start_key().get_obj_cnt() == multi_version_rowkey_col_cnt) ||
           (range.get_end_key().get_obj_cnt() == multi_version_rowkey_col_cnt &&
               range.get_start_key().get_obj_cnt() == multi_version_rowkey_col_cnt);
  }

private:
  static const int64_t DEFAULT_MACRO_BLOCK_CTX = 1024;

  static const int64_t PREFETCH_DEPTH = 2;
  const ObTableIterParam* iter_param_;
  ObTableAccessContext* access_ctx_;
  ObSSTable* sstable_;
  common::ObExtStoreRange query_range_;
  common::ObArenaAllocator allocator_;

  //
  int64_t prefetch_macro_cursor_;
  int64_t cur_macro_cursor_;
  int64_t cur_micro_cursor_;
  common::ObSEArray<blocksstable::ObMacroBlockCtx, DEFAULT_MACRO_BLOCK_CTX> macro_blocks_;
  //
  MacroScanHandle scan_handles_[PREFETCH_DEPTH];
  blocksstable::ObIMicroBlockRowScanner* micro_scanner_;
  bool is_opened_;
  blocksstable::ObStorageFileHandle file_handle_;
};

}  // namespace storage
}  // namespace oceanbase

#endif
