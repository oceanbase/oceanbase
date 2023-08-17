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

#ifndef OCEANBASE_STORAGE_OB_SAMPLE_ITER_HELPER_H
#define OCEANBASE_STORAGE_OB_SAMPLE_ITER_HELPER_H

#include "storage/access/ob_block_sample_iterator.h"
#include "storage/access/ob_row_sample_iterator.h"

namespace oceanbase {
namespace storage {
class ObMultipleMultiScanMerge;

class ObGetSampleIterHelper {
public:
  ObGetSampleIterHelper(const ObTableScanRange &table_scan_range,
                        ObTableAccessContext &main_table_ctx,
                        ObTableScanParam &scan_param,
                        ObGetTableParam &get_table_param)
      : table_scan_range_(table_scan_range),
        main_table_ctx_(main_table_ctx),
        scan_param_(scan_param),
        get_table_param_(get_table_param)
  {}

  int check_scan_range_count(bool &res, ObIArray<blocksstable::ObDatumRange> &sample_ranges);

  // int get_sample_iter(ObISampleIterator *&i_sample_iter,
  //                     ObQueryRowIterator *&main_iter,
  //                     ObMultipleScanMerge *scan_merge);
  int get_sample_iter(ObMemtableRowSampleIterator *&sample_iter,
                      ObQueryRowIterator *&main_iter,
                      ObMultipleScanMerge *scan_merge);
  int get_sample_iter(ObRowSampleIterator *&sample_iter,
                      ObQueryRowIterator *&main_iter,
                      ObMultipleScanMerge *scan_merge);

  int get_sample_iter(ObBlockSampleIterator *&sample_iter,
                      ObQueryRowIterator *&main_iter,
                      ObMultipleScanMerge *scan_merge);

private:
  // if need retire to row sample, sample_memtable_ranges must not be null
  int can_retire_to_memtable_row_sample_(bool &retire, ObIArray<blocksstable::ObDatumRange> &sample_ranges);

  int get_memtable_sample_ranges_(const ObIArray<ObITable *> &memtables,
                                  ObIArray<blocksstable::ObDatumRange> &sample_ranges);

private:
  const ObTableScanRange &table_scan_range_;
  ObTableAccessContext &main_table_ctx_;
  ObTableScanParam &scan_param_;
  ObGetTableParam &get_table_param_;
  bool need_scan_multiple_range_;
};

}  // namespace storage
}  // namespace oceanbase

#endif