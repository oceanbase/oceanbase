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

#ifndef _OB_RAW_EXPR_REPLACER_H
#define _OB_RAW_EXPR_REPLACER_H 1
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace sql
{

/**
 * difference between ObRawExprReplacer::replace and ObTransformUtils::replace_expr:
 * 1. ObColumnRefRawExpr::dependant_expr_ is not replaced when visit ObColumnRefRawExpr;
 * 2. ObExecParamRawExpr::outer_expr is not replaced when visit ObExecParamRawExpr,
 *    it is replaced when visit ObQueryRefRawExpr;
 * 3. the replaced expr and its children will be skipped when traverse the expr tree, for example
 *    with replace rule [{c1 -> c1+c2+1}, {c2 -> c1+c2+2}]
 *    select c1, c2 from t1 will be transformed into select c1+c2+1, c1+c2+2 from t1
 *    In contrast, ObTransformUtils::replace_expr will return OB_SIZE_OVERFLOW unexpectedly;
 */
class ObRawExprReplacer: public ObRawExprVisitor
{
public:
  ObRawExprReplacer();
  virtual ~ObRawExprReplacer();

  void destroy();
  int replace(ObRawExpr *&expr);
  virtual int visit(ObConstRawExpr &expr) override;
  virtual int visit(ObExecParamRawExpr &expr) override;
  virtual int visit(ObVarRawExpr &expr) override;
  virtual int visit(ObOpPseudoColumnRawExpr &expr) override;
  virtual int visit(ObQueryRefRawExpr &expr) override;
  virtual int visit(ObColumnRefRawExpr &expr) override;
  virtual int visit(ObOpRawExpr &expr) override;
  virtual int visit(ObCaseOpRawExpr &expr) override;
  virtual int visit(ObAggFunRawExpr &expr) override;
  virtual int visit(ObSysFunRawExpr &expr) override;
  virtual int visit(ObSetOpRawExpr &expr) override;
  virtual int visit(ObWinFunRawExpr &expr) override;
  virtual int visit(ObPlQueryRefRawExpr &expr) override;
  virtual int visit(ObAliasRefRawExpr &expr) override;
  virtual int visit(ObPseudoColumnRawExpr &expr) override;

  virtual bool skip_child(ObRawExpr &expr) override;
  bool get_replace_happened() const { return replace_happened_; }
  void set_skip_bool_param_mysql(bool skip) { skip_bool_param_mysql_ = skip; }
  bool is_skip_bool_param_mysql() { return skip_bool_param_mysql_; }
  bool empty() const { return !expr_replace_map_.created(); }
  int add_replace_expr(ObRawExpr *from_expr,
                       ObRawExpr *to_expr,
                       bool overwrite = false);
  int add_replace_exprs(const ObIArray<ObRawExpr *> &from_exprs,
                        const ObIArray<ObRawExpr *> &to_exprs);
  int add_replace_exprs(const ObIArray<std::pair<ObRawExpr *, ObRawExpr *>> &to_replace_exprs);
  int append_replace_exprs(const ObRawExprReplacer &other);

private:
  // types and constants
  static const int64_t DEFAULT_BUCKET_SIZE = 64;
private:
  int try_init_expr_map(int64_t bucket_size);
  int check_from_expr_existed(const ObRawExpr *from_expr,
                              const ObRawExpr *to_expr,
                              const bool overwrite,
                              bool &is_existed);
  int check_skip_expr(const ObRawExpr &expr, bool &skip_expr);
  int check_need_replace(const ObRawExpr *old_expr,
                         ObRawExpr *&new_expr,
                         bool &need_replace);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprReplacer);
  // function members
private:
  hash::ObHashSet<uint64_t> to_exprs_;
  hash::ObHashMap<uint64_t, uint64_t> expr_replace_map_;

  bool replace_happened_;
   //If true, skip param exprs which are resolved as flags instead of exprs in mysql.
   //(e.g. the second param of IS expr)
  bool skip_bool_param_mysql_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RAW_EXPR_REPLACER_H */
