/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_COLUMN_INDEX_PROVIDER_H
#define _OB_COLUMN_INDEX_PROVIDER_H
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_mod_define.h"

namespace oceanbase
{
namespace sql
{

class ObRawExpr;
class RowDesc
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
  int get_idx(const ObRawExpr *raw_expr, int64_t &idx) const;
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
