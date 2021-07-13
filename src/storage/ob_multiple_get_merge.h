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

#include "storage/ob_multiple_merge.h"
#include "storage/ob_fuse_row_cache_fetcher.h"
#include "blocksstable/ob_fuse_row_cache.h"

namespace oceanbase {
namespace storage {

enum class ObMultiGetRowState {
  INVALID = 0,
  IN_FUSE_ROW_CACHE,
  IN_FUSE_ROW_CACHE_AND_SSTABLE,
  IN_MEMTABLE,
  IN_SSTABLE
};

struct ObQueryRowInfo final {
public:
  ObQueryRowInfo()
      : row_(),
        nop_pos_(),
        final_result_(false),
        state_(ObMultiGetRowState::INVALID),
        end_iter_idx_(0),
        sstable_end_log_ts_(0)
  {}
  ~ObQueryRowInfo() = default;
  TO_STRING_KV(K_(row), K_(final_result), K_(final_result), K_(end_iter_idx), K_(sstable_end_log_ts));
  ObStoreRow row_;
  ObNopPos nop_pos_;
  bool final_result_;
  ObMultiGetRowState state_;
  int64_t end_iter_idx_;
  int64_t sstable_end_log_ts_;
};

class ObMultipleGetMerge : public ObMultipleMerge {
public:
  ObMultipleGetMerge();
  virtual ~ObMultipleGetMerge();
  int open(const common::ObIArray<common::ObExtStoreRowkey>& rowkeys);
  static int estimate_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, const common::ObIArray<ObITable*>& tables,
      ObPartitionEst& part_estimate);
  static int to_collation_free_rowkey_on_demand(
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, common::ObIAllocator& allocator);
  virtual void reset() override;
  virtual void reuse() override;
  virtual int is_range_valid() const override;

protected:
  virtual int prepare() override;
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int inner_get_next_row(ObStoreRow& row);
  virtual void collect_merge_stat(ObTableStoreStat& stat) const override;
  virtual int skip_to_range(const int64_t range_idx) override;

private:
  int construct_iters_with_fuse_row_cache();
  int construct_iters_without_fuse_row_cache();
  int inner_get_next_row_with_fuse_row_cache(ObStoreRow& row);
  int inner_get_next_row_without_fuse_row_cache(ObStoreRow& row);
  int get_table_row(const int64_t table_idx, const int64_t rowkey_idx, bool& stop_reading);
  int try_get_fuse_row_cache(int64_t& end_table_idx);
  int try_put_fuse_row_cache(ObQueryRowInfo& row_info);
  int construct_sstable_iter();
  int prefetch();
  void reuse_row(const int64_t rowkey_idx, ObQueryRowInfo& row);
  int alloc_resource();
  void reset_with_fuse_row_cache();

private:
  static const int64_t MAX_PREFETCH_CNT = 300;
  static const int64_t MAX_MULTI_GET_FUSE_ROW_CACHE_GET_COUNT = 100;
  static const int64_t MAX_MULTI_GET_FUSE_ROW_CACHE_PUT_COUNT;
  const common::ObIArray<common::ObExtStoreRowkey>* rowkeys_;
  ObArray<common::ObExtStoreRowkey> sstable_rowkeys_;
  GetRowkeyArray cow_rowkeys_;
  int64_t prefetch_range_idx_;
  int64_t get_row_range_idx_;
  int64_t prefetch_cnt_;
  ObQueryRowInfo* rows_;
  blocksstable::ObFuseRowValueHandle* handles_;
  ObFuseRowCacheFetcher fuse_row_cache_fetcher_;
  bool has_frozen_memtable_;
  bool can_prefetch_all_;
  int64_t end_memtable_idx_;
  int64_t sstable_begin_iter_idx_;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleGetMerge);
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_MULTIPLE_GET_MERGE_
