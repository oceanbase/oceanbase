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

#ifndef _OB_COLUMN_INDEX_PROVIDER_H
#define _OB_COLUMN_INDEX_PROVIDER_H
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_mod_define.h"
#include "objit/expr/ob_column_index_provider.h"
#include "objit/expr/ob_iraw_expr.h"

namespace oceanbase
{
namespace sql
{

class ObRawExpr;
class RowDesc: public jit::expr::ObColumnIndexProvider
{
public:
  RowDesc() {}
  virtual ~RowDesc() {}
  int init();
  void reset();
  int assign(const RowDesc &other);
  int append(const RowDesc &other);
  /**
   * @brief add column to row descriptor
   */
  int add_column(ObRawExpr *raw_expr);
  int replace_column(ObRawExpr *old_expr, ObRawExpr *new_expr);
  int swap_position(const ObRawExpr *expr1, const ObRawExpr *expr2);
  int64_t get_column_num() const;
  ObRawExpr *get_column(int64_t idx) const;
  const common::ObIArray<ObRawExpr*> &get_columns() const;
  int get_column(int64_t idx, ObRawExpr *&raw_expr) const;
  int get_idx(const jit::expr::ObIRawExpr *raw_expr, int64_t &idx) const override;
  TO_STRING_KV(N_EXPR, exprs_);
private:
  typedef common::hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> ExprIdxMap;
  ExprIdxMap expr_idx_map_;
  common::ObSEArray<ObRawExpr*, 64> exprs_;
  DISALLOW_COPY_AND_ASSIGN(RowDesc);
};

} // end namespace sql
} // end namespace oceanbase
#endif
