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

#ifndef OBJIT_EXPR_OB_COLUMN_INDEX_PROVIDER_H
#define OBJIT_EXPR_OB_COLUMN_INDEX_PROVIDER_H

#include "sql/resolver/expr/ob_expr.h"

namespace oceanbase {
namespace jit {
namespace expr {

class ObColumnIndexProvider {
public:
  ObColumnIndexProvider()
  {}
  /**
   * get expr index in array
   *
   * @param raw_expr
   * @param index
   *
   * @return OB_ENTRY_NOT_EXIST for raw_expr not found.
   */
  virtual int get_idx(const ObExpr* raw_expr, int64_t& index) const = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(ObColumnIndexProvider);
};

}  // namespace expr
}  // namespace jit
}  // namespace oceanbase

#endif
