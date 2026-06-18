/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SAMPLE_ITER_HELPER_H
#define OCEANBASE_STORAGE_OB_SAMPLE_ITER_HELPER_H

#include "storage/access/ob_block_sample_iterator.h"
#include "storage/access/ob_row_sample_iterator.h"
#include "storage/access/ob_ddl_block_sample_iterator.h"

namespace oceanbase {
namespace storage {
class ObMultipleMultiScanMerge;
struct ObTabletReadTables;

class ObGetSampleIterHelper {
public:
  ObGetSampleIterHelper(const ObTableScanRange &table_scan_range,
                        ObTableAccessContext &main_table_ctx,
                        ObTableScanParam &scan_param,
                        ObTabletReadTables &tablet_read_tables)
      : table_scan_range_(table_scan_range),
        main_table_ctx_(main_table_ctx),
        scan_param_(scan_param),
        tablet_read_tables_(tablet_read_tables)
  {}

  int check_scan_range_count(bool &res, ObIArray<blocksstable::ObDatumRange> &sample_ranges);

  // int get_sample_iter(ObISampleIterator *&i_sample_iter,
  //                     ObQueryRowIterator *&main_iter,
  //                     ObMultipleScanMerge *scan_merge);
  int get_sample_iter(ObMemtableRowSampleIterator *&sample_iter,
                      ObQueryRowIterator *&main_iter,
                      ObMultipleScanMerge *scan_merge);
  int get_sample_iter(ObBlockSampleIterator *&sample_iter,
                      ObQueryRowIterator *&main_iter,
                      ObMultipleScanMerge *scan_merge);
  int get_sample_iter(ObDDLBlockSampleIterator *&sample_iter,
                      ObQueryRowIterator *&main_iter,
                      ObMultipleScanMerge *scan_merge);

private:
  // if need retire to row sample, sample_memtable_ranges must not be null
  int can_retire_to_memtable_row_sample_(bool &retire, ObIArray<blocksstable::ObDatumRange> &sample_ranges);

  int get_memtable_sample_ranges_(const ObIArray<memtable::ObMemtable *> &memtables,
                                  ObIArray<blocksstable::ObDatumRange> &sample_ranges);

private:
  const ObTableScanRange &table_scan_range_;
  ObTableAccessContext &main_table_ctx_;
  ObTableScanParam &scan_param_;
  ObTabletReadTables &tablet_read_tables_;
  bool need_scan_multiple_range_;
};

}  // namespace storage
}  // namespace oceanbase

#endif