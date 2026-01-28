/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_LEVEL_ORDER_SCAN_MERGE_H
#define OCEANBASE_STORAGE_OB_LEVEL_ORDER_SCAN_MERGE_H

#include "ob_multiple_merge.h"

namespace oceanbase
{
namespace storage
{

class ObLevelOrderScanMerge : public ObMultipleMerge
{
public:
  ObLevelOrderScanMerge() : ObMultipleMerge(), curr_iter_idx_(0), range_(nullptr), cow_range_()
  {
    type_ = ObQRIterType::T_LEVEL_ORDER_SCAN;
  }
  virtual ~ObLevelOrderScanMerge() = default;

  int open(const blocksstable::ObDatumRange &range);

  virtual void reset() override;
  virtual void reuse() override;
  virtual void reclaim() override;
  virtual int pause(bool& do_pause) override;

  TO_STRING_KV(K_(curr_iter_idx), K_(range), K_(cow_range));

protected:
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int inner_get_next_row(blocksstable::ObDatumRow &row) override;
  virtual int inner_get_next_rows() override;
  virtual int can_batch_scan(bool &can_batch) override;
  virtual int prepare() override;

  virtual int set_all_blockscan(ObStoreRowIterator &iter);

  virtual int build_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *&iter) override;
  virtual int init_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *iter) override;

  int64_t curr_iter_idx_;

private:
  const ObDatumRange *range_;
  ObDatumRange cow_range_;
};

class ObLevelOrderMultiScanMerge : public ObLevelOrderScanMerge
{
public:
  ObLevelOrderMultiScanMerge() : ObLevelOrderScanMerge(), ranges_(nullptr), cow_ranges_()
  {
    type_ = ObQRIterType::T_LEVEL_ORDER_MULTI_SCAN;
  }
  virtual ~ObLevelOrderMultiScanMerge() = default;

public:
  int open(const ObIArray<ObDatumRange> &ranges);
  virtual void reset();

protected:
  virtual int calc_scan_range() override;
  virtual int construct_iters() override;
  virtual int inner_get_next_row(ObDatumRow &row) override;
  virtual int get_range_count() const override { return ranges_->count(); }

  virtual int build_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *&iter) override;
  virtual int init_iter(ObITable *table, const ObTableIterParam *iter_param, ObStoreRowIterator *iter) override;

private:
  const ObIArray<ObDatumRange> *ranges_;
  ObSEArray<ObDatumRange, 8> cow_ranges_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLevelOrderMultiScanMerge);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_LEVEL_ORDER_SCAN_MERGE_H
