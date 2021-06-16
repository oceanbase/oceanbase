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

#ifndef OB_SINGLE_MERGE_H_
#define OB_SINGLE_MERGE_H_
#include "ob_multiple_merge.h"
#include "storage/ob_fuse_row_cache_fetcher.h"
#include "blocksstable/ob_fuse_row_cache.h"

namespace oceanbase {
namespace storage {

class ObSingleMerge : public ObMultipleMerge {
public:
  ObSingleMerge();
  virtual ~ObSingleMerge();
  int open(const common::ObExtStoreRowkey& rowkey);
  virtual void reset();
  virtual void reuse() override;
  static int estimate_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObExtStoreRowkey& rowkey, const common::ObIArray<ObITable*>& stores, ObPartitionEst& part_estimate);

protected:
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int is_range_valid() const override;
  virtual int inner_get_next_row(ObStoreRow& row);
  virtual void collect_merge_stat(ObTableStoreStat& stat) const override;

private:
  virtual int get_table_row(const int64_t table_idx, const ObIArray<ObITable*>& tables, const ObStoreRow*& prow,
      ObStoreRow& fuse_row, bool& final_result, int64_t& sstable_end_log_ts, bool& stop_reading);

private:
  const common::ObExtStoreRowkey* rowkey_;
  blocksstable::ObFuseRowValueHandle handle_;

private:
  static const int64_t SINGLE_GET_FUSE_ROW_CACHE_PUT_COUNT_THRESHOLD = 50;
  ObFuseRowCacheFetcher fuse_row_cache_fetcher_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSingleMerge);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OB_SINGLE_MERGE_H_ */
