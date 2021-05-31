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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_hash_partitioning_infrastructure_op.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

bool ObHashPartCols::equal(const ObHashPartCols& other, const ObIArray<ObSortFieldCollation>* sort_collations,
    const ObIArray<ObCmpFunc>* cmp_funcs) const
{
  bool result = true;
  const ObObj* lcell = NULL;
  const ObObj* rcell = NULL;
  if (OB_ISNULL(sort_collations) || OB_ISNULL(cmp_funcs)) {
    result = false;
  } else if (nullptr == store_row_ || nullptr == other.store_row_) {
    result = false;
  } else {
    int cmp_result = 0;
    ObDatum* l_cells = store_row_->cells();
    ObDatum* r_cells = other.store_row_->cells();
    for (int64_t i = 0; i < sort_collations->count() && 0 == cmp_result; ++i) {
      int64_t idx = sort_collations->at(i).field_idx_;
      cmp_result = cmp_funcs->at(i).cmp_func_(l_cells[idx], r_cells[idx]);
    }
    result = (0 == cmp_result);
  }
  return result;
}

int ObHashPartCols::equal_temp(const ObTempHashPartCols& other, const ObIArray<ObSortFieldCollation>* sort_collations,
    const ObIArray<ObCmpFunc>* cmp_funcs, ObEvalCtx* eval_ctx, bool& result) const
{
  int ret = OB_SUCCESS;
  result = true;
  const ObObj* lcell = NULL;
  const ObObj* rcell = NULL;
  if (OB_ISNULL(sort_collations) || OB_ISNULL(cmp_funcs) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: compare info is null", K(sort_collations), K(cmp_funcs), K(eval_ctx), K(ret));
  } else if (nullptr == store_row_ || nullptr == other.exprs_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: row is null", K(store_row_), K(other.exprs_), K(ret));
  } else {
    int cmp_result = 0;
    ObDatum* l_cells = store_row_->cells();
    ObDatum* r_cell = nullptr;
    for (int64_t i = 0; i < sort_collations->count() && 0 == cmp_result; ++i) {
      int64_t idx = sort_collations->at(i).field_idx_;
      if (OB_FAIL(other.exprs_->at(idx)->eval(*eval_ctx, r_cell))) {
        LOG_WARN("failed to eval datum", K(ret), K(i));
      } else {
        cmp_result = cmp_funcs->at(i).cmp_func_(l_cells[idx], *r_cell);
      }
    }
    result = (0 == cmp_result);
  }
  return ret;
}
