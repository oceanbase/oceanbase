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

enum class ObMultiGetRowState
{
  INVALID = 0,
  IN_FUSE_ROW_CACHE,
  IN_FUSE_ROW_CACHE_AND_SSTABLE,
  IN_MEMTABLE,
  IN_SSTABLE
};

struct ObQueryRowInfo final
{
public:
  ObQueryRowInfo()
    : row_(), nop_pos_(), final_result_(false), state_(ObMultiGetRowState::INVALID),
    end_iter_idx_(0), sstable_end_log_ts_(0)
  {}
  ~ObQueryRowInfo() = default;
  TO_STRING_KV(K_(row), K_(final_result), K_(final_result), K_(end_iter_idx), K_(sstable_end_log_ts));
  blocksstable::ObDatumRow row_;
  ObNopPos nop_pos_;
  bool final_result_;
  ObMultiGetRowState state_;
  int64_t end_iter_idx_;
  int64_t sstable_end_log_ts_;
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
  virtual int is_range_valid() const override;
protected:
  virtual int prepare() override;
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row);
private:
  const common::ObIArray<blocksstable::ObDatumRowkey> *rowkeys_;
  common::ObSEArray<blocksstable::ObDatumRowkey, common::OB_DEFAULT_MULTI_GET_ROWKEY_NUM> cow_rowkeys_;
  int64_t get_row_range_idx_;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleGetMerge);
};

}
}

#endif // OCEANBASE_STORAGE_OB_MULTIPLE_GET_MERGE_
