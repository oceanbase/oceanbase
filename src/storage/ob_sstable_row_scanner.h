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

#ifndef OB_SSTABLE_ROW_SCANNER_H_
#define OB_SSTABLE_ROW_SCANNER_H_
#include "blocksstable/ob_micro_block_row_scanner.h"
#include "ob_sstable_row_iterator.h"

namespace oceanbase {
namespace storage {

class ObSSTableRowScanner : public ObSSTableRowIterator {
public:
  ObSSTableRowScanner();
  virtual ~ObSSTableRowScanner();
  virtual void reset();
  virtual void reuse() override;
  virtual int skip_range(int64_t range_idx, const common::ObStoreRowkey* gap_key, const bool include_gap_key) override;
  int skip_range_impl(const int64_t range_idx, const ObExtStoreRange& org_range, const ObExtStoreRange& new_range);
  int generate_new_range(const int64_t range_idx, const ObStoreRowkey& gap_key, const bool include_gap_key,
      const ObExtStoreRange& org_range, ObExtStoreRange*& new_range);
  virtual int get_skip_range_ctx(
      ObSSTableReadHandle& read_handle, const int64_t cur_micro_idx, ObSSTableSkipRangeCtx*& ctx) override;
  int get_gap_range_idx(int64_t& range_idx);
  int get_gap_end_impl(const ObExtStoreRange& org_range, ObStoreRowkey& gap_key, int64_t& gap_size);
  int get_row_iter_flag_impl(uint8_t& flag);

protected:
  virtual int get_handle_cnt(const void* query_range, int64_t& read_handle_cnt, int64_t& micro_handle_cnt);
  virtual int prefetch_read_handle(ObSSTableReadHandle& read_handle);
  virtual int fetch_row(ObSSTableReadHandle& read_handle, const ObStoreRow*& store_row);
  int prefetch_range(
      const int64_t range_idx, const common::ObExtStoreRange& ext_range, ObSSTableReadHandle& read_handle);
  int check_can_skip_range(const int64_t range_idx, const common::ObStoreRowkey& gap_key, bool& can_skip);
  int prefetch_block_index(const uint64_t table_id, const blocksstable::ObMacroBlockCtx& block_ctx,
      ObMicroBlockIndexHandle& block_index_handle);
  virtual int get_range_count(const void* query_range, int64_t& range_count) const;

private:
  int skip_batch_rows(
      const int64_t range_idx, const ObStoreRowkey& gap_key, const bool include_gap_key, bool& need_actual_skip);

protected:
  static const int64_t SCAN_READ_HANDLE_CNT = 4;
  static const int64_t SCAN_MICRO_HANDLE_CNT = 32;
  static const int64_t SCAN_DEFAULT_MACRO_BLOCK_CNT = 2;
  bool has_find_macro_;
  int64_t prefetch_macro_idx_;
  int64_t macro_block_cnt_;

private:
  int64_t prefetch_macro_order_;
  ObExtStoreRange last_range_;
  ObExtStoreRange new_range_;
  int64_t last_gap_range_idx_;
  int64_t last_gap_macro_idx_;
  int64_t last_gap_micro_idx_;
  const ObStoreRow* curr_row_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_SSTABLE_ROW_SCANNER_H_ */
