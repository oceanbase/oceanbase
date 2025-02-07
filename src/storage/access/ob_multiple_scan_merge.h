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

#ifndef OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_
#define OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_

#include "lib/container/ob_loser_tree.h"
#include "storage/ob_i_store.h"
#include "storage/ob_row_fuse.h"
#include "ob_multiple_merge.h"
#include "ob_scan_merge_loser_tree.h"
#include "ob_simple_rows_merger.h"
#include "storage/ob_i_store.h"
#include "lib/statistic_event/ob_stat_event.h"

namespace oceanbase
{
namespace storage
{
class ObMultipleScanMerge : public ObMultipleMerge
{
public:
  ObMultipleScanMerge();
  virtual ~ObMultipleScanMerge();
public:
  int open(const blocksstable::ObDatumRange &range);
  virtual int init(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param);
  virtual int switch_table(
    ObTableAccessParam &param,
    ObTableAccessContext &context,
    ObGetTableParam &get_table_param) override;
  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
protected:
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row);
  virtual int inner_get_next_rows() override;
  virtual int can_batch_scan(bool &can_batch) override;
  virtual int is_range_valid() const override;
  virtual int prepare() override;
  virtual int supply_consume();
  virtual int inner_merge_row(blocksstable::ObDatumRow &row);
  int set_rows_merger(const int64_t table_cnt);
  int locate_blockscan_border();
private:
  int prepare_blockscan(ObStoreRowIterator &iter);
protected:
  ObScanMergeLoserTreeCmp tree_cmp_;
  ObScanSimpleMerger *simple_merge_;
  ObScanMergeLoserTree *loser_tree_;
  common::ObRowsMerger<ObScanMergeLoserTreeItem, ObScanMergeLoserTreeCmp> *rows_merger_;
  int64_t consumers_[common::MAX_TABLE_CNT_IN_STORAGE];
  int64_t consumer_cnt_;
private:
  int64_t filt_del_count_;
  const blocksstable::ObDatumRange *range_;
  blocksstable::ObDatumRange cow_range_;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMultipleScanMerge);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MULTIPLE_SCAN_MERGE_
