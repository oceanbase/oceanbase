/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_ACCESS_OB_DI_BASE_SSTABLE_ROW_SCANNER_H_
#define OB_STORAGE_ACCESS_OB_DI_BASE_SSTABLE_ROW_SCANNER_H_

#include "share/schema/ob_table_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/blocksstable/ob_sstable.h"

namespace oceanbase
{
namespace storage
{
class ObDIBaseSSTableRowScanner final : public ObStoreRowIterator
{

public:
  ObDIBaseSSTableRowScanner();
  virtual ~ObDIBaseSSTableRowScanner();
  virtual int init(const ObTableIterParam &param,
                   ObTableAccessContext &context,
                   ObITable *table,
                   const void *query_range) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual void reset() override;
  virtual void reuse() override;
  virtual int get_blockscan_border_rowkey(blocksstable::ObDatumRowkey &border_rowkey) override;
  virtual bool can_blockscan() const override { return true; }
  virtual bool can_batch_scan() const override { return true; }
  virtual int get_next_row(const blocksstable::ObDatumRow *&row) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int get_next_rows() override;
  virtual int refresh_blockscan_checker(const blocksstable::ObDatumRowkey &border_rowkey) override;
  int switch_param(ObTableAccessParam *access_param,
                   ObTableAccessContext *access_ctx,
                   const common::ObIArray<ObITable *> &tables);
  int prepare_ranges(const blocksstable::ObDatumRange &range);
  int prepare_cow_ranges();
  int prepare_ranges(const common::ObIArray<blocksstable::ObDatumRange> &ranges);
  int prepare_cow_multi_ranges();
  int construct_iters(const bool is_multi_scan);
  int save_curr_rowkey();
  int prepare_di_base_blockscan(bool di_base_only, ObDatumRow *row = nullptr);
  int check_di_base_changed(const common::ObIArray<ObITable *> &tables);
  void reset_iter_array();
  void reuse_iter_array();
  void reclaim_iter_array();
  OB_INLINE int64_t get_di_base_table_cnt() const
  {
    return di_base_table_keys_.count();
  }
  OB_INLINE int64_t get_di_base_iter_cnt() const
  {
    return di_base_iters_.count();
  }
  OB_INLINE const blocksstable::ObDatumRowkey &get_di_base_curr_rowkey() const
  {
    return di_base_curr_rowkey_;
  }
  OB_INLINE int64_t get_di_base_curr_scan_index() const
  {
    return di_base_curr_scan_index_;
  }
  OB_INLINE const blocksstable::ObDatumRange *&get_di_base_range()
  {
    return di_base_range_;
  }
  OB_INLINE blocksstable::ObDatumRange &get_di_base_cow_range()
  {
    return di_base_cow_range_;
  }
  OB_INLINE const common::ObIArray<blocksstable::ObDatumRange> *&get_di_base_multi_range()
  {
    return di_base_multi_range_;
  }
  OB_INLINE common::ObIArray<blocksstable::ObDatumRange> &get_di_base_cow_multi_range()
  {
    return di_base_cow_multi_range_;
  }
  INHERIT_TO_STRING_KV("ObStoreRowIterator", ObStoreRowIterator, KP(access_param_), KP(access_ctx_),
                       KP(tables_), K(di_base_table_keys_),
                       K(is_di_base_iter_end_), K(curr_di_base_idx_), K(di_base_iters_.count()),
                       K(di_base_curr_rowkey_), K(di_base_curr_scan_index_),
                       KPC(di_base_range_), KPC(di_base_multi_range_));
private:
  OB_INLINE int64_t get_table_cnt() const
  {
    return tables_->count();
  }
  OB_INLINE const ObITable::TableKey &get_di_base_table_key(const int64_t idx) const
  {
    OB_ASSERT_MSG(idx >= 0 && idx < di_base_table_keys_.count(), "idx is out of range");
    return di_base_table_keys_.at(idx);
  }
  OB_INLINE ObStoreRowIterator *get_di_base_iter(const int64_t idx) const
  {
    OB_ASSERT_MSG(idx >=0 && idx < di_base_iters_.count(), "idx is out of range");
    return di_base_iters_[idx];
  }

private:
  const ObTableAccessParam *access_param_;
  ObTableAccessContext *access_ctx_;
  const common::ObIArray<ObITable *> *tables_;
  bool is_di_base_iter_end_; // whether di base iters before curr_di_base_idx_ are all iter end
  int64_t curr_di_base_idx_; // current di base iter index, di base iters before curr_di_base_idx_ are scanned in this blockscan
  common::ObSEArray<ObITable::TableKey, DEFAULT_STORE_CNT_IN_STORAGE> di_base_table_keys_;
  common::ObSEArray<ObStoreRowIterator *, DEFAULT_STORE_CNT_IN_STORAGE> di_base_iters_;
  blocksstable::ObDatumRowkey di_base_curr_rowkey_;
  int64_t di_base_curr_scan_index_;
  const blocksstable::ObDatumRange *di_base_range_;
  blocksstable::ObDatumRange di_base_cow_range_;
  const common::ObIArray<blocksstable::ObDatumRange> *di_base_multi_range_;
  common::ObSEArray<blocksstable::ObDatumRange, 32> di_base_cow_multi_range_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDIBaseSSTableRowScanner);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_ACCESS_OB_DI_BASE_SSTABLE_ROW_SCANNER_H_