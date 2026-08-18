/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  virtual int probe_next_rowkey() const final;
  int switch_param(ObTableAccessParam *access_param,
                   ObTableAccessContext *access_ctx,
                   const common::ObIArray<ObITable *> &tables,
                   const bool is_multi_scan);
  // after table refreshed, ranges maybe point to upper cow_ranges, so we need to copy the ranges to own cow_ranges
  int prepare_ranges(const blocksstable::ObDatumRange &range, const bool copy_ranges = false);
  int prepare_ranges(const common::ObIArray<blocksstable::ObDatumRange> &ranges, const bool copy_ranges = false);
  int calc_scan_range();
  bool can_calc_scan_range() const
  {
    return di_base_curr_rowkey_.is_valid();
  }
  int construct_iters();
  int save_curr_rowkey(const bool need_scan_di_base);
  int prepare_di_base_blockscan(bool di_base_only, ObDatumRow *row = nullptr);
  int check_di_base_changed(const common::ObIArray<ObITable *> &tables, bool &is_changed);
  void reset_iter_array();
  void reuse_iter_array();
  void reclaim_iter_array();
  int stash_tablet_iter(const ObTabletTableIterator &tablet_iter, const common::ObIArray<ObITable *> &tables);
  int clone_and_switch_tables(const blocksstable::ObDatumRowkey &border_rowkey);
  void clear_stash();
  const ObITableReadInfo *get_rowkey_read_info() const;
  OB_INLINE int64_t get_di_base_table_cnt() const
  {
    return di_base_table_keys_.count();
  }
  OB_INLINE int64_t get_di_base_iter_cnt() const
  {
    return di_base_iters_.count();
  }
  INHERIT_TO_STRING_KV("ObStoreRowIterator", ObStoreRowIterator, KP(access_param_), KP(access_ctx_),
                       KP(tables_), K(di_base_table_keys_), K(is_multi_scan_),
                       K(is_di_base_iter_end_), K(curr_di_base_idx_), K(di_base_iters_.count()),
                       K(di_base_curr_rowkey_), K(di_base_curr_scan_index_),
                       KPC(di_base_range_), KPC(di_base_multi_range_), KPC(stash_info_));
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
  struct StashInfo
  {
  public:
    StashInfo();
    ~StashInfo();
    void reset();
    int deep_copy_sstables(const ObIArray<ObITable::TableKey> &table_keys,
                           const int64_t start_pos);
    int deep_copy_rowkey_read_info(const ObRowkeyReadInfo &rowkey_read_info);
    int prepare_ranges(const bool is_multi_scan, const int64_t size);
    TO_STRING_KV(K(tablet_iter_),
                 K(tables_),
                 K(tables_handle_),
                 KP(rowkey_read_info_),
                 KP(read_info_),
                 K(ranges_.count()),
                 K(multi_ranges_.count()));
  public:
    ObArenaAllocator allocator_;
    ObTabletTableIterator tablet_iter_;
    common::ObArray<ObITable *> tables_;
    ObTablesHandleArray tables_handle_;
    ObRowkeyReadInfo *rowkey_read_info_;
    const ObITableReadInfo *read_info_;
    common::ObArray<blocksstable::ObDatumRange> ranges_;
    common::ObArray<common::ObArray<blocksstable::ObDatumRange> *> multi_ranges_;
  };

private:
  ObTableAccessParam *access_param_;
  ObTableAccessContext *access_ctx_;
  const common::ObIArray<ObITable *> *tables_;
  bool is_multi_scan_;
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
  StashInfo *stash_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDIBaseSSTableRowScanner);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_ACCESS_OB_DI_BASE_SSTABLE_ROW_SCANNER_H_
