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

#ifndef OCEANBASE_EXPR_CMP_FUNC_H_
#define OCEANBASE_EXPR_CMP_FUNC_H_

#include "sql/engine/expr/ob_expr.h"
#include "common/object/ob_object.h"
#include "common/object/ob_obj_compare.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase {
namespace common {
class ObDatum;
}
namespace sql {
typedef int (*DatumCmpFunc)(const common::ObDatum& datum1, const common::ObDatum& datum2);
class ObExprCmpFuncsHelper {
public:
  static sql::ObExpr::EvalFunc get_eval_expr_cmp_func(const common::ObObjType type1, const common::ObObjType type2,
      const common::ObCmpOp cmp_op, const bool is_oracle_mode, const common::ObCollationType cs_type);

  static DatumCmpFunc get_datum_expr_cmp_func(const common::ObObjType type1, const common::ObObjType type2,
      const bool is_oracle_mode, const common::ObCollationType cs_type);
};
}  // namespace sql
}  // end namespace oceanbase
#endif  // !OCEANBASE_EXPR_CMP_FUNC_H_
