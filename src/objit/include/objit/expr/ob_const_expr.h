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

#ifndef OB_CONST_EXPR_H
#define OB_CONST_EXPR_H

#include <stdint.h>
#include "common/object/ob_object.h"
#include "objit/expr/ob_iraw_expr.h"

namespace oceanbase {
namespace jit {
namespace expr {

class ObExprVisitor;

// OceanBase constant expression.
class ObConstExpr
    : virtual public ObIRawExpr {
public:
  ObConstExpr() { set_expr_class(EXPR_CONST); }
  virtual ~ObConstExpr() {}

  common::ObObj &get_value();
  const common::ObObj &get_value() const;

  virtual int accept(ObExprVisitor &v) const override;

protected:
  common::ObObj value_;
};

inline common::ObObj &ObConstExpr::get_value()
{
  return value_;
}

inline const common::ObObj &ObConstExpr::get_value() const
{
  return value_;
}

}  // expr
}  // jit
}  // oceanbase

#endif /* OB_CONST_EXPR_H */
