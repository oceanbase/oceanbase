/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STMT_EXPR_VISITOR_H
#define OB_STMT_EXPR_VISITOR_H
#include "sql/ob_sql_utils.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"
#include "sql/resolver/expr/ob_raw_expr_replacer.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

enum DmlStmtScope {
  SCOPE_BASIC_TABLE = 0,
  SCOPE_FROM,
  SCOPE_JOINED_TABLE,
  SCOPE_SEMI_INFO,
  SCOPE_WHERE,
  SCOPE_GROUPBY,
  SCOPE_HAVING,
  SCOPE_SELECT,
  SCOPE_SELECT_INTO,
  SCOPE_ORDERBY,
  SCOPE_LIMIT,
  SCOPE_START_WITH,
  SCOPE_CONNECT_BY,
  SCOPE_PIVOT,
  SCOPE_DMLINFOS,
  SCOPE_DML_COLUMN,
  SCOPE_DML_VALUE,
  SCOPE_SHADOW_COLUMN,
  SCOPE_DML_CONSTRAINT,
  SCOPE_INSERT_DESC,
  SCOPE_INSERT_VECTOR,
  SCOPE_RETURNING,
  SCOPE_DICT_FIELDS // 16
};

class ObStmtExprVisitor
{
public:
  ObStmtExprVisitor() : flags_(0xffffffff),
    is_recursive_(false)
  {}

  template <typename T>
  int visit(T *&expr, const DmlStmtScope my_scope);

  template <typename T>
  int visit(common::ObIArray<T *> &exprs,
                           const DmlStmtScope my_scope);

  virtual int do_visit(ObRawExpr *&expr) = 0;

  void add_all()
  {
    flags_ = 0xffffffff;
  }

  void set_relation_scope()
  {
    remove_scope(SCOPE_BASIC_TABLE);
    remove_scope(SCOPE_DICT_FIELDS);
    remove_scope(SCOPE_SHADOW_COLUMN);
    remove_scope(SCOPE_INSERT_DESC);
  }

  void remove_all()
  {
    flags_ = 0;
  }

  void add_scope(DmlStmtScope scope)
  {
    flags_ = static_cast<uint32_t>((flags_ | (1L << scope)));
  }

  void add_scope(const ObIArray<DmlStmtScope> &scopes);

  void remove_scope(DmlStmtScope scope)
  {
    flags_ = (flags_ & (~(1L << scope)));
  }

  void remove_scope(const ObIArray<DmlStmtScope> &scopes);

  bool is_required(const DmlStmtScope scope)
  {
    return 0 != (flags_ & (1L << scope));
  }

  bool is_recursive() const { return is_recursive_; }
  void set_recursive(bool flag) { is_recursive_ = flag; }

private:
  uint32_t flags_;
  bool is_recursive_;
};

template <typename T>
int ObStmtExprVisitor::visit(T *&expr,
                             const DmlStmtScope my_scope)
{
  int ret = common::OB_SUCCESS;
  if (is_required(my_scope) && NULL != expr) {
    ObRawExpr *tmp = expr;
    if (OB_FAIL(do_visit(reinterpret_cast<ObRawExpr *&>(expr)))) {
      SQL_RESV_LOG(WARN, "failed to visit tmp expr", K(ret));
    } else if (tmp == expr) {
      // do nothing
    } else if (OB_ISNULL(expr) ||
               (!std::is_same<T, ObRawExpr>::value &&
                OB_UNLIKELY(tmp->get_expr_class() != expr->get_expr_class()))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "expr class is changed", K(tmp->get_expr_class()), K(expr->get_expr_class()));
    }
  }
  return ret;
}

template <typename T>
int ObStmtExprVisitor::visit(common::ObIArray<T *> &exprs,
                             const DmlStmtScope my_scope)
{
  int ret = common::OB_SUCCESS;
  if (is_required(my_scope)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
      if (NULL != exprs.at(i)) {
        ObRawExpr *tmp = exprs.at(i);
        if (OB_FAIL(do_visit(reinterpret_cast<ObRawExpr *&>(exprs.at(i))))) {
          SQL_RESV_LOG(WARN, "failed to visit tmp expr", K(ret));
        } else if (tmp == exprs.at(i)) {
          // continue
        } else if (OB_ISNULL(exprs.at(i)) ||
                   (!std::is_same<T, ObRawExpr>::value &&
                    OB_UNLIKELY(tmp->get_expr_class() != exprs.at(i)->get_expr_class()))) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "expr class is changed", K(tmp->get_expr_class()), K(exprs.at(i)->get_expr_class()));
        }
      }
    }
  }
  return ret;
}

class ObStmtExprGetter : public ObStmtExprVisitor
{
public:
  ObStmtExprGetter() : checker_(NULL)
  {}
  ObStmtExprGetter(const ObIArray<DmlStmtScope> &scopes) : checker_(NULL)
  {
    remove_all();
    add_scope(scopes);
  }

  int do_visit(ObRawExpr *&expr) override;

  RelExprCheckerBase *checker_;
};

class ObStmtExprReplacer : public ObStmtExprVisitor
{
public:
  ObStmtExprReplacer() {}

  ObStmtExprReplacer(const ObIArray<DmlStmtScope> &scopes)
  {
    remove_all();
    add_scope(scopes);
  }

  virtual int do_visit(ObRawExpr *&expr) override;
  int add_replace_exprs(const ObIArray<ObRawExpr *> &from_exprs,
                        const ObIArray<ObRawExpr *> &to_exprs,
                        const ObIArray<ObRawExpr *> *skip_exprs = NULL);
  void set_skip_bool_param_mysql(bool skip) { replacer_.set_skip_bool_param_mysql(skip); }
  bool is_skip_bool_param_mysql() { return replacer_.is_skip_bool_param_mysql(); }
private:
  int add_skip_expr(const ObRawExpr *skip_expr);
  int check_expr_need_skip(const ObRawExpr *skip_expr, bool &need_skip);
private:
  ObRawExprReplacer replacer_;
  hash::ObHashSet<uint64_t> skip_exprs_;
};

class ObStmtExprCopier : public ObStmtExprVisitor
{
public:
  ObStmtExprCopier(ObRawExprCopier &copier) : copier_(copier)
  {}

  virtual int do_visit(ObRawExpr *&expr) override;

private:
  ObRawExprCopier &copier_;
};

class ObSharedExprChecker : public ObStmtExprVisitor
{
public:
  ObSharedExprChecker() : stmt_expr_set_(NULL),
                          shared_expr_set_() {}

  virtual int do_visit(ObRawExpr *&expr) override;

  int init(ObDMLStmt &stmt);
  int destroy();
  int is_shared_expr(const ObRawExpr *expr, bool &is_shared) const;
private:
  hash::ObHashSet<uint64_t> *stmt_expr_set_;
  hash::ObHashSet<uint64_t> shared_expr_set_;
};

class ObStmtExecParamFormatter : public ObStmtExprVisitor
{
public:
  ObStmtExecParamFormatter() {}

  virtual int do_visit(ObRawExpr *&expr) override;

  int do_formalize_exec_param(ObRawExpr *&expr, bool &is_happened);

};

}
}

#endif // OB_STMT_EXPR_VISITOR_H
