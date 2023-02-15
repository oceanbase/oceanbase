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

#include "ob_query_iterator_factory.h"
#include "lib/time/ob_time_utility.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/access/ob_multiple_get_merge.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "ob_value_row_iterator.h"
#include "ob_col_map.h"

namespace oceanbase
{
using namespace oceanbase::common;
namespace storage
{

int64_t ObQueryIteratorFactory::multi_scan_merge_alloc_count_ = 0;
int64_t ObQueryIteratorFactory::multi_scan_merge_release_count_ = 0;
int64_t ObQueryIteratorFactory::multi_get_merge_alloc_count_ = 0;
int64_t ObQueryIteratorFactory::multi_get_merge_release_count_ = 0;
int64_t ObQueryIteratorFactory::table_scan_alloc_count_ = 0;
int64_t ObQueryIteratorFactory::table_scan_release_count_ = 0;
int64_t ObQueryIteratorFactory::insert_dup_alloc_count_ = 0;
int64_t ObQueryIteratorFactory::insert_dup_release_count_ = 0;
int64_t ObQueryIteratorFactory::col_map_alloc_count_ = 0;
int64_t ObQueryIteratorFactory::col_map_release_count_ = 0;

void ObQueryIteratorFactory::print_count()
{
  static int64_t last_stat_time = 0;
  int64_t stat_time = ObTimeUtility::current_time();
  const int64_t stat_interval = 10000000;
  if (stat_time - ATOMIC_LOAD(&last_stat_time) > stat_interval) {
    STORAGE_LOG(INFO, "ObQueryIteratorFactory statistics",
        K_(multi_scan_merge_alloc_count), K_(multi_scan_merge_release_count),
        K_(multi_get_merge_alloc_count), K_(multi_get_merge_release_count),
        K_(table_scan_alloc_count), K_(table_scan_release_count),
        K_(insert_dup_alloc_count), K_(insert_dup_release_count),
        K_(col_map_alloc_count), K_(col_map_release_count));
    ATOMIC_STORE(&last_stat_time, stat_time);
  }
}

ObMultipleScanMerge *ObQueryIteratorFactory::get_multi_scan_merge_iter()
{
  print_count();
  ObMultipleScanMerge *iter = rp_alloc(ObMultipleScanMerge, ObModIds::OB_PARTITION_SCAN_MERGE);
  if (NULL != iter) {
    (void)ATOMIC_FAA(&multi_scan_merge_alloc_count_, 1);
  }
  return iter;
}

ObMultipleGetMerge *ObQueryIteratorFactory::get_multi_get_merge_iter()
{
  print_count();
  ObMultipleGetMerge *iter = rp_alloc(ObMultipleGetMerge, ObModIds::OB_PARTITION_GET_MERGE);
  if (NULL != iter) {
    (void)ATOMIC_FAA(&multi_get_merge_alloc_count_, 1);
  }
  return iter;
}

ObTableScanIterator *ObQueryIteratorFactory::get_table_scan_iter()
{
  print_count();
  ObTableScanIterator *iter = rp_alloc(ObTableScanIterator, ObModIds::OB_TABLE_SCAN_ITER);
  if (NULL != iter) {
    (void)ATOMIC_FAA(&table_scan_alloc_count_, 1);
  }
  return iter;
}

ObValueRowIterator *ObQueryIteratorFactory::get_insert_dup_iter()
{
  print_count();
  ObValueRowIterator *iter = rp_alloc(ObValueRowIterator, ObModIds::OB_VALUE_ROW_ITER);
  if (NULL != iter) {
    (void)ATOMIC_FAA(&insert_dup_alloc_count_, 1);
  }
  return iter;
}

ObColMap *ObQueryIteratorFactory::get_col_map()
{
  print_count();
  ObColMap *col_map = rp_alloc(ObColMap, ObModIds::OB_COL_MAP);
  if (NULL != col_map) {
    (void)ATOMIC_FAA(&col_map_alloc_count_, 1);
  }
  return col_map;
}

// no need to invoked reset() in the following free_XXX,
// since rp_free() will invoke reset() for the corresponding object
void ObQueryIteratorFactory::free_table_scan_iter(common::ObNewRowIterator *iter)
{
  if (OB_LIKELY(NULL != iter)) {
    (void)ATOMIC_FAA(&table_scan_release_count_, 1);
    rp_free(static_cast<ObTableScanIterator *>(iter), ObModIds::OB_TABLE_SCAN_ITER);
    iter = NULL;
  }
}

void ObQueryIteratorFactory::free_insert_dup_iter(common::ObNewRowIterator *iter)
{
  if (OB_LIKELY(NULL != iter)) {
    (void)ATOMIC_FAA(&insert_dup_release_count_, 1);
    rp_free(static_cast<ObValueRowIterator *>(iter), ObModIds::OB_VALUE_ROW_ITER);
    iter = NULL;
  }
}

void ObQueryIteratorFactory::free_merge_iter(ObQueryRowIterator *iter)
{
  if (OB_LIKELY(NULL != iter)) {
    switch (iter->get_type()) {
    case T_MULTI_SCAN:
      rp_free(static_cast<ObMultipleScanMerge *>(iter), ObModIds::OB_PARTITION_SCAN_MERGE);
      iter = NULL;
      (void)ATOMIC_FAA(&multi_scan_merge_release_count_, 1);
      break;
    case T_MULTI_GET:
      rp_free(static_cast<ObMultipleGetMerge *>(iter), ObModIds::OB_PARTITION_GET_MERGE);
      iter = NULL;
      (void)ATOMIC_FAA(&multi_get_merge_release_count_, 1);
      break;
    default:
      STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "invalid iterator type", K(iter->get_type()));
      break;
    }
  }
}

void ObQueryIteratorFactory::free_col_map(ObColMap *col_map)
{
  if (OB_LIKELY(NULL != col_map)) {
    (void)ATOMIC_FAA(&col_map_release_count_, 1);
    rp_free(col_map, ObModIds::OB_COL_MAP);
    col_map = NULL;
  }
}

} // namespace storage
} // namespace oceanbase
