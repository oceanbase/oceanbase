/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_QUERY_ITERATOR_FACTORY_
#define OCEANBASE_STORAGE_OB_QUERY_ITERATOR_FACTORY_

#include <stdint.h>
#include "common/row/ob_row_iterator.h"

namespace oceanbase
{
namespace blocksstable
{
class ObDatumRowIterator;
}
namespace storage
{
class ObMultipleScanMerge;
class ObMultipleGetMerge;
class ObTableScanIterator;
class ObQueryRowIterator;
class ObValueRowIterator;
class ObColMap;
class ObStoreRow;
class ObQueryIteratorFactory
{
public:
  static ObMultipleScanMerge *get_multi_scan_merge_iter();
  static ObMultipleGetMerge *get_multi_get_merge_iter();
  static ObTableScanIterator *get_table_scan_iter();
  static ObValueRowIterator *get_insert_dup_iter();
  static ObColMap *get_col_map();

  static void free_table_scan_iter(common::ObNewRowIterator *iter);
  static void free_insert_dup_iter(blocksstable::ObDatumRowIterator *iter);
  static void free_merge_iter(ObQueryRowIterator *iter);
  static void free_col_map(ObColMap *col_map);
  static void free_work_row(ObStoreRow *row);
private:
  static void print_count();
private:
  static int64_t single_row_merge_alloc_count_;
  static int64_t single_row_merge_release_count_;
  static int64_t multi_scan_merge_alloc_count_;
  static int64_t multi_scan_merge_release_count_;
  static int64_t multi_get_merge_alloc_count_;
  static int64_t multi_get_merge_release_count_;
  static int64_t table_scan_alloc_count_;
  static int64_t table_scan_release_count_;
  static int64_t insert_dup_alloc_count_;
  static int64_t insert_dup_release_count_;
  static int64_t col_map_alloc_count_;
  static int64_t col_map_release_count_;
  static int64_t work_row_alloc_count_;
  static int64_t work_row_release_count_;
};

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_QUERY_ITERATOR_FACTORY_
