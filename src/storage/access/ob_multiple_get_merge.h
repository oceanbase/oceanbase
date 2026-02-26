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

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_GET_MERGE_
#define OCEANBASE_STORAGE_OB_MULTIPLE_GET_MERGE_

#include "ob_multiple_merge.h"
#include "ob_fuse_row_cache_fetcher.h"
#include "storage/blocksstable/ob_fuse_row_cache.h"
#include "storage/ob_storage_struct.h"

namespace oceanbase
{
namespace storage
{

enum class ObMultiGetRowState //FARM COMPAT WHITELIST
{
  INVALID = 0,
  IN_MEMTABLE,
  IN_FUSE_ROW_CACHE,
  IN_SSTABLE_AND_FUSE_ROW_CACHE,
  IN_SSTABLE
};

struct ObQueryRowInfo final
{
public:
  ObQueryRowInfo()
    : row_(), nop_pos_(), has_uncommitted_row_(false), end_iter_idx_(-1)
  {}
  ~ObQueryRowInfo()
  {
    reset();
  }
  OB_INLINE void reset()
  {
    row_.reset();
    nop_pos_.reset();
    has_uncommitted_row_ = false;
    end_iter_idx_ = -1;
  }
  OB_INLINE void reuse()
  {
    row_.row_flag_.reset();
    row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    row_.snapshot_version_ = 0L;
    nop_pos_.reset();
    has_uncommitted_row_ = false;
    end_iter_idx_ = -1;
  }
  OB_INLINE void reclaim()
  {
    row_.count_ = 0;
    row_.row_flag_.reset();
    row_.row_flag_.set_flag(ObDmlFlag::DF_NOT_EXIST);
    row_.snapshot_version_ = 0L;
    row_.trans_info_ = nullptr;
    nop_pos_.reset();
    has_uncommitted_row_ = false;
    end_iter_idx_ = -1;
  }
  OB_INLINE bool need_update_cache(const bool enable_put_fuse_row_cache) const
  {
    return end_iter_idx_ > 0 &&
           row_.row_flag_.is_exist_without_delete() &&
           !has_uncommitted_row_ &&
           enable_put_fuse_row_cache;
  }
  OB_INLINE bool is_valid() { return end_iter_idx_ > -1; }
  TO_STRING_KV(K_(row), K_(nop_pos), K_(has_uncommitted_row), K_(end_iter_idx));
  blocksstable::ObDatumRow row_;
  ObNopPos nop_pos_;
  bool has_uncommitted_row_;
  int64_t end_iter_idx_;
};

class ObMultipleGetMerge : public ObMultipleMerge
{
public:
  ObMultipleGetMerge();
  virtual ~ObMultipleGetMerge();
  int open(const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys);
  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
  virtual int pause(bool& do_pause) override final { do_pause = false; return OB_SUCCESS; }
protected:
  virtual int prepare() override;
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row);
  virtual int get_range_count() const override
  { return rowkeys_->count(); }
private:
  int alloc_resource();
  int init_resource();
  int construct_specified_iters(int64_t &table_start_idx, const bool need_stop_at_first_sstable = false);
  int try_get_fuse_row_cache(
    const int64_t &read_snapshot_version,
    const int64_t &multi_version_start,
    ObDatumRowkey &rowkey,
    ObFuseRowValueHandle &handle,
    ObQueryRowInfo &row_info);
  int iter_fuse_row_from_memtable(
    ObDatumRowkey &cur_rowkey,
    ObDatumRow &row,
    ObNopPos &nop_pos,
    bool &has_uncommitted_row);
  int iter_fuse_row_from_memtable(
    ObDatumRowkey &cur_rowkey,
    ObDatumRow &row,
    ObNopPos &nop_pos);
  int iter_fuse_row_from_sstable(ObQueryRowInfo &row_info);
  int fuse_cache_row(const ObFuseRowValueHandle &handle, ObQueryRowInfo &fuse_row_info);
  int check_final_row(ObDatumRow &fuse_row, bool &is_valid_row);
  int project_final_row(const ObDatumRow &fuse_row, ObDatumRow &row);
  int get_rows_from_memory();
  int prepare_prefetch_next_rowkey(const int64_t &multi_version_start, const int64_t &read_snapshot_version);
  int try_get_next_row(ObQueryRowInfo &row_info, ObFuseRowValueHandle &handle);
  int inner_get_next_row_for_memtables_only(ObDatumRow &row);
  int inner_get_next_row_for_sstables_exist(ObDatumRow &row);
private:
  common::ObIArray<blocksstable::ObDatumRowkey> *rowkeys_;
  ObFuseRowCacheFetcher fuse_row_cache_fetcher_;
  ObFuseRowValueHandle cache_handles_[common::OB_MULTI_GET_OPEN_ROWKEY_NUM];
  ObQueryRowInfo *full_rows_;
  int64_t prefetch_row_range_idx_;
  int64_t get_row_range_idx_;
  bool enable_fuse_row_cache_;
  bool all_in_memory_;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleGetMerge);
};

}
}

#endif // OCEANBASE_STORAGE_OB_MULTIPLE_GET_MERGE_
