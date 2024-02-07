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
#ifndef OB_STORAGE_COLUMN_STORE_OB_CG_ITER_PARAM_POOL_H_
#define OB_STORAGE_COLUMN_STORE_OB_CG_ITER_PARAM_POOL_H_
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_allocator.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace storage
{
struct ObTableIterParam;
class ObCGIterParamPool
{
public:
  static const int64_t DEFAULT_ITER_PARAM_CNT = 2;
  ObCGIterParamPool(common::ObIAllocator &alloc);
  ~ObCGIterParamPool() { reset(); }
  void reset();
  int get_iter_param(
    const int32_t cg_idx,
    const ObTableIterParam &row_param,
    sql::ObExpr *expr,
    ObTableIterParam *&iter_param);
  int get_iter_param(
    const int32_t cg_idx,
    const ObTableIterParam &row_param,
    const common::ObIArray<sql::ObExpr*> &exprs,
    ObTableIterParam *&iter_param,
    const bool is_aggregate = false);
private:
  int new_iter_param(
      const int32_t cg_idx,
      const ObTableIterParam &row_param,
      const common::ObIArray<sql::ObExpr*> &exprs,
      ObTableIterParam *&iter_param,
      const bool is_aggregate = false);
  int fill_cg_iter_param(
      const ObTableIterParam &row_param,
      const int32_t cg_idx,
      const common::ObIArray<sql::ObExpr*> &exprs,
      ObTableIterParam &cg_param);
  int fill_virtual_cg_iter_param(
      const ObTableIterParam &row_param,
      const int32_t cg_idx,
      const common::ObIArray<sql::ObExpr*> &exprs,
      ObTableIterParam &cg_param);
  int generate_for_column_store(
      const ObTableIterParam &row_param,
      const sql::ExprFixedArray *exprs,
      const ObIArray<int32_t> *out_cols_project,
      const int32_t cg_idx,
      ObTableIterParam &cg_param);
  int put_iter_param(ObTableIterParam *iter_param);
  void free_iter_param(ObTableIterParam *iter_param);
  common::ObIAllocator &alloc_;
  common::ObSEArray<ObTableIterParam*, DEFAULT_ITER_PARAM_CNT> iter_params_;
};

}
}

#endif
