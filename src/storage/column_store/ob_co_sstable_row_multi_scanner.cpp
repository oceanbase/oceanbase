/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX STORAGE
#include "ob_co_sstable_row_multi_scanner.h"
#include "storage/access/ob_sstable_row_multi_scanner.h"

namespace oceanbase
{
namespace storage
{

void ObCOSSTableRowMultiScanner::reset()
{
  ObCOSSTableRowScanner::reset();
  ranges_ = nullptr;
}

void ObCOSSTableRowMultiScanner::reuse()
{
  ObCOSSTableRowScanner::reuse();
  ranges_ = nullptr;
}

int ObCOSSTableRowMultiScanner::init_row_scanner(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    ObITable *table,
    const void *query_range)
{
  int ret = OB_SUCCESS;
  if (nullptr == row_scanner_) {
    if (OB_ISNULL(row_scanner_ = OB_NEWx(ObSSTableRowMultiScanner<ObCOPrefetcher>, context.stmt_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc row scanner", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_scanner_->init(param, context, table, query_range))) {
    LOG_WARN("Fail to init row scanner", K(ret), K(param), KPC(table));
  } else {
    ranges_ = static_cast<const common::ObIArray<blocksstable::ObDatumRange> *>(query_range);
  }
  return ret;
}

int ObCOSSTableRowMultiScanner::get_group_idx(int64_t &group_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ranges_ || range_idx_ >= ranges_->count() )) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected ranges", K(ret), K(range_idx_), KPC(ranges_));
  } else {
    group_idx = ranges_->at(range_idx_).get_group_idx();
  }
  return ret;
}

}
}
