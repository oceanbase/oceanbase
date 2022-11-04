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

#ifndef OB_EXPR_VISITOR_H
#define OB_EXPR_VISITOR_H

namespace oceanbase {
namespace jit {
namespace expr {

class ObIRawExpr;
class ObConstExpr;
class ObVarExpr;
class ObOpExpr;
class ObCaseOpExpr;
class ObColumnRefExpr;

//对应于server中ObRawExprVisitor
class ObExprVisitor {
public:
  virtual int visit(const ObIRawExpr &expr) = 0;
  virtual int visit(const ObConstExpr &expr) = 0;
  virtual int visit(const ObVarExpr &expr) = 0;
  virtual int visit(const ObOpExpr &expr) = 0;
  virtual int visit(const ObColumnRefExpr &expr) = 0;
  virtual int visit(const ObCaseOpExpr &expr) = 0;

  virtual bool skip_child(const ObConstExpr &expr) { return skip_child_internal(reinterpret_cast<const ObIRawExpr &>(expr)); }
  virtual bool skip_child(const ObVarExpr &expr) { return skip_child_internal(reinterpret_cast<const ObIRawExpr &>(expr)); }
  virtual bool skip_child(const ObOpExpr &expr) { return skip_child_internal(reinterpret_cast<const ObIRawExpr &>(expr)); }
  virtual bool skip_child(const ObColumnRefExpr &expr) { return skip_child_internal(reinterpret_cast<const ObIRawExpr &>(expr)); }
  virtual bool skip_child(const ObCaseOpExpr &expr) { return skip_child_internal(reinterpret_cast<const ObIRawExpr &>(expr)); }

  virtual bool skip_child_internal(const ObIRawExpr &expr) { UNUSED(expr); return false; }

protected:
  bool skip_;
};

}  // expr
}  // jit
}  // oceanbase

#endif /* OB_EXPR_VISITOR_H */
