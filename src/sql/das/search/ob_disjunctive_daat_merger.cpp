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

#define USING_LOG_PREFIX SQL_DAS

#include "ob_disjunctive_daat_merger.h"

namespace oceanbase
{
namespace sql
{

int ObIDisjunctiveDAATMerger::alloc_merger(
    const ObIArray<ObDAATMergeIter *> &merge_iters,
    const ObIArray<ObExpr *> &id_exprs,
    const RowMeta &id_meta,
    const ObDASRowIDType &rowid_type,
    const bool use_max_score,
    ObIAllocator &allocator,
    ObIDisjunctiveDAATMerger *&merger)
{
  int ret = OB_SUCCESS;
  const int64_t dim_cnt = merge_iters.count();
  const bool use_simple_merger = dim_cnt <= ObIDisjunctiveDAATMerger::SIMPLE_MERGE_MAX_DIM_CNT;
  merger = nullptr;
  if (rowid_type == ObDASRowIDType::DAS_ROWID_TYPE_UINT64) {
    if (use_simple_merger) {
      using RowMergerType = ObDisjunctiveDAATMerger<ObDASRowIDType::DAS_ROWID_TYPE_UINT64, true>;
      merger = OB_NEWx(RowMergerType, &allocator);
    } else {
      using RowMergerType = ObDisjunctiveDAATMerger<ObDASRowIDType::DAS_ROWID_TYPE_UINT64, false>;
      merger = OB_NEWx(RowMergerType, &allocator);
    }
  } else if (rowid_type == ObDASRowIDType::DAS_ROWID_TYPE_COMPACT) {
    if (use_simple_merger) {
      using RowMergerType = ObDisjunctiveDAATMerger<ObDASRowIDType::DAS_ROWID_TYPE_COMPACT, true>;
      merger = OB_NEWx(RowMergerType, &allocator);
    } else {
      using RowMergerType = ObDisjunctiveDAATMerger<ObDASRowIDType::DAS_ROWID_TYPE_COMPACT, false>;
      merger = OB_NEWx(RowMergerType, &allocator);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowid type", K(ret), K(rowid_type));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(merger)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for row merger", K(ret));
  } else if (OB_FAIL(merger->init(
      merge_iters,
      id_exprs,
      id_meta,
      use_max_score,
      allocator))) {
    LOG_WARN("failed to init daat merger", K(ret));
  }
  return ret;
}

}
}
