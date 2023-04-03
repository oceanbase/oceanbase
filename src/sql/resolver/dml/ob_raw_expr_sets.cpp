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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_raw_expr_sets.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/expr/ob_raw_expr.h"

using namespace oceanbase;
using namespace oceanbase::sql;

int ObRawExprSetUtils::to_expr_set(common::ObIAllocator *allocator,
                                   const common::ObIArray<ObRawExpr *> &exprs,
                                   ObRawExprSet &expr_set)
{
  int ret = OB_SUCCESS;
  if (exprs.count() <= 0 || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(allocator), K(exprs.count()));
  } else {
    expr_set.set_allocator(allocator);
    if (OB_FAIL(expr_set.init(exprs.count()))) {
      LOG_WARN("failed to init expr set", K(ret));
    } else if (OB_FAIL(common::append(expr_set, exprs))) {
      LOG_WARN("faield to append expr", K(ret));
    }
  }
  return ret;
}

int ObRawExprSetUtils::add_expr_set(common::ObIAllocator *allocator,
                                    const common::ObIArray<ObRawExpr *> &exprs,
                                    ObRawExprSets &expr_sets)
{
  int ret = OB_SUCCESS;
  ObRawExprSet *expr_set = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(allocator));
  } else if (exprs.count() <= 0) {
    /*do nothing*/
  } else if (OB_ISNULL(ptr = allocator->alloc(sizeof(ObRawExprSet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory to create ObRawExprSet", K(ret));
  } else {
    expr_set = new(ptr) ObRawExprSet();
    if (OB_FAIL(to_expr_set(allocator, exprs, *expr_set))) {
      LOG_WARN("failed to convert array to expr set", K(ret));
    } else if (OB_FAIL(expr_sets.push_back(expr_set))) {
      LOG_WARN("failed to push back expr set", K(ret));
    }
  }
  if (OB_FAIL(ret) && expr_set != NULL) {
    expr_set->~ObRawExprSet();
  }
  return ret;
}
