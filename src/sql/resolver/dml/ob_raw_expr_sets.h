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

#ifndef OB_RAW_EXPR_SETS_H
#define OB_RAW_EXPR_SETS_H

#include "share/ob_define.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
typedef common::ObFixedArray<ObRawExpr*, common::ObIAllocator> ObRawExprSet;
// NOTE: do not reuse on ObRawExprSets
// reuse will not de-construct existed ObFixedArray
// A ObFixedArray can not be reused because its size can not be adjusted.
typedef common::ObSEArray<ObRawExprSet*, 8, common::ModulePageAllocator, true> ObRawExprSets;
typedef ObRawExprSet EqualSet;
typedef ObRawExprSets EqualSets;

class ObRawExprSetUtils
{
public:

  static int to_expr_set(common::ObIAllocator *allocator,
                         const common::ObIArray<ObRawExpr *> &exprs,
                         ObRawExprSet &expr_set);

  static int add_expr_set(common::ObIAllocator *allocator,
                          const common::ObIArray<ObRawExpr *> &exprs,
                          ObRawExprSets &expr_sets);
};



}
}


#endif // OB_RAW_EXPR_SETS_H
