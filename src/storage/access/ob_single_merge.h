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
#include "ob_fuse_row_cache_fetcher.h"
#include "storage/blocksstable/ob_fuse_row_cache.h"

namespace oceanbase
{
namespace storage
{
class ObSingleMerge : public ObMultipleMerge
{
public:
  ObSingleMerge();
  virtual ~ObSingleMerge();
  int open(const blocksstable::ObDatumRowkey &rowkey);
  virtual void reset();
  virtual void reuse() override;
protected:
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int is_range_valid() const override;
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row);
  virtual void collect_merge_stat(ObTableStoreStat &stat) const override;
private:
  virtual int get_table_row(const int64_t table_idx,
                            const common::ObIArray<ObITable *> &tables,
                            blocksstable::ObDatumRow &fuse_row,
                            bool &final_result,
                            bool &has_uncommited_row);
  virtual int get_and_fuse_cache_row(const int64_t read_snapshot_version,
                                     const int64_t multi_version_start,
                                     blocksstable::ObDatumRow &fuse_row,
                                     bool &final_result,
                                     bool &have_uncommited_row,
                                     bool &need_update_fuse_cache);
private:
  static const int64_t SINGLE_GET_FUSE_ROW_CACHE_PUT_COUNT_THRESHOLD = 50;
  const blocksstable::ObDatumRowkey *rowkey_;
  blocksstable::ObDatumRow full_row_;
  blocksstable::ObFuseRowValueHandle handle_;
  ObFuseRowCacheFetcher fuse_row_cache_fetcher_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObSingleMerge);
};
} /* namespace storage */
} /* namespace oceanbase */

#endif /* OB_SINGLE_MERGE_H_ */
