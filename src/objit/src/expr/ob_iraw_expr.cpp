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

#define USING_LOG_PREFIX SQL
#include "objit/expr/ob_iraw_expr.h"
#include "lib/oblog/ob_log.h"
namespace oceanbase {
namespace jit {
namespace expr {

using namespace ::oceanbase::common;

//// member function definitions
//////////////////////////////////////////////////////////////////////
int ObIRawExpr::preorder_accept(Visitor &v) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(accept(v))) {
    LOG_WARN("accept fail", K(ret));
  } else {
    ExprArray exprs;
    if (OB_FAIL(get_children(exprs))) {
      LOG_WARN("fail to get children", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      if (OB_FAIL(exprs.at(i)->preorder_accept(v))) {
        LOG_WARN("preorder_accept fail", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObIRawExpr::postorder_accept(Visitor &v) const
{
  int ret = OB_SUCCESS;
  ExprArray exprs;
  if (OB_FAIL(get_children(exprs))) {
    LOG_WARN("fail to get children", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_FAIL(exprs.at(i)->postorder_accept(v))) {
      LOG_WARN("postorder_accept fail", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(accept(v))) {
      LOG_WARN("accept fail", K(ret));
    }
  }
  return ret;
}

}  // expr
}  // jit
}  // oceanbase
