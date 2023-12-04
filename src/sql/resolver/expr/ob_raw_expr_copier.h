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

#ifndef OB_RAW_EXPR_COPIER_H
#define OB_RAW_EXPR_COPIER_H
#include "sql/resolver/expr/ob_raw_expr.h"
namespace oceanbase {
namespace sql {

class ObIRawExprCopier
{
public:

  ObIRawExprCopier(ObRawExprFactory &expr_factory) : expr_factory_(expr_factory) {}

  virtual ~ObIRawExprCopier() {}

  virtual int check_need_copy(const ObRawExpr *old_expr, ObRawExpr *&new_expr) = 0;

  // check if old expr has already been copied, and if it has been copied, takes it directly.
  virtual int find_in_copy_context(const ObRawExpr *old_expr, ObRawExpr *&new_expr) = 0;

  virtual int do_copy_expr(const ObRawExpr *old_expr, ObRawExpr *&new_expr) = 0;

  virtual bool deep_copy_attributes() const { return false; }

  int copy(ObRawExpr *&expr);

  int copy(const ObRawExpr *expr, ObRawExpr *&new_expr);

  template <typename T>
  int copy(const ObIArray<T> &input_exprs, ObIArray<T> &output_exprs)
  {
    int ret = common::OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); ++i) {
      ObRawExpr *temp_expr = NULL;
      if (OB_FAIL(copy(input_exprs.at(i), temp_expr))) {
        SQL_RESV_LOG(WARN, "failed to copy expr", K(ret));
      } else if (OB_ISNULL(temp_expr)) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "null expr", K(ret));
      } else {
        T cast_expr = static_cast<T>(temp_expr);
        if (OB_FAIL(output_exprs.push_back(cast_expr))) {
          SQL_RESV_LOG(WARN, "failed to push back expr", K(ret));
        } else { /*do nothing*/ }
      }
    }
    return ret;
  }

  ObRawExprFactory& get_expr_factory() { return expr_factory_; }
protected:
  ObRawExprFactory &expr_factory_;
};

class ObPLExprCopier : public ObIRawExprCopier
{
public:
  ObPLExprCopier(ObRawExprFactory &expr_factory) :
    ObIRawExprCopier(expr_factory)
  {}

  static int copy_expr(ObRawExprFactory &expr_factory,
                       const ObRawExpr *old_expr,
                       ObRawExpr *&new_expr);

  int check_need_copy(const ObRawExpr *old_expr, ObRawExpr *&new_expr) override;

  int do_copy_expr(const ObRawExpr *old_expr, ObRawExpr *&new_expr) override;

  bool deep_copy_attributes() const { return true; }

  int find_in_copy_context(const ObRawExpr *old_expr, ObRawExpr *&new_expr) override;
};

/**
 * @brief The ObIRawExprReplacer class
 *  Tell the expr copier how to generate a new expr for an old one,
 *  and then the copier would replace the old with the new one.
 */
class ObIRawExprReplacer
{
public:
  virtual int generate_new_expr(ObRawExprFactory &expr_factory,
                                ObRawExpr *old_expr,
                                ObRawExpr *&new_expr) = 0;
};

class ObRawExprCopier : public ObIRawExprCopier
{
public:
  ObRawExprCopier(ObRawExprFactory &expr_factory) :
    ObIRawExprCopier(expr_factory)
  {}

  int check_need_copy(const ObRawExpr *old_expr, ObRawExpr *&new_expr) override;

  int copy_expr_node(const ObRawExpr *old_expr,
                     ObRawExpr *&new_expr);

  static int copy_expr_node(ObRawExprFactory &expr_factory,
                            const ObRawExpr *old_expr,
                            ObRawExpr *&new_expr);

  static int copy_expr(ObRawExprFactory &expr_factory,
                       const ObRawExpr *old_expr,
                       ObRawExpr *&new_expr);

  /**
   * @brief add_skipped_expr
   *  skip the target expr tree or expr node during copying/copy_on_replace
   * @param target
   * @param include_child
   *     include_child = true, skip the whole expr tree
   *     include_child = false, skip the expr node itself only, its children still
   *                            need to be copied or replaced
   * @return
   */
  int add_skipped_expr(const ObRawExpr *target, bool include_child = true);

  template <typename T>
  int add_skipped_expr(const ObIArray<T *> &targets, bool include_child = true);

  /**
   * @brief add_replaced_expr
   *  replace the from_expr with the new_expr when copy a expr tree
   * @param from_expr
   * @param new_expr
   * @return
   */
  int add_replaced_expr(const ObRawExpr *from_expr,
                        const ObRawExpr *new_expr);

  int add_replaced_expr(const ObIArray<ObRawExpr *> &from_exprs,
                        const ObIArray<ObRawExpr *> &to_exprs);

  int copy_on_replace(ObRawExpr *from,
                      ObRawExpr *&to,
                      ObIRawExprReplacer *replacer = NULL);
  
  template <typename T>
  int copy_on_replace(const ObIArray<T *> &from_exprs,
                      ObIArray<T *> &to_exprs,
                      ObIRawExprReplacer *replacer = NULL);

  bool is_existed(const ObRawExpr *from) const;

  int do_copy_expr(const ObRawExpr *old_expr, ObRawExpr *&new_expr) override;

  int get_copied_exprs(ObIArray<std::pair<ObRawExpr *, ObRawExpr *>> &from_to_exprs);

  int find_in_copy_context(const ObRawExpr *old_expr, ObRawExpr *&new_expr) override;

private:

  int add_expr(const ObRawExpr *from, const ObRawExpr *to);

  int add_expr(const ObIArray<ObRawExpr *> &from_exprs,
               const ObIArray<ObRawExpr *> &to_exprs);

private:
  hash::ObHashSet<uint64_t> new_exprs_;
  hash::ObHashMap<uint64_t, uint64_t> copied_exprs_;

  // these exprs can be modified directly,
  // there is no need to create a copy
  // and do replacement on its copy.
  ObArray<const ObRawExpr *> uncopy_expr_nodes_;
};

template <typename T>
int ObRawExprCopier::copy_on_replace(const common::ObIArray<T *> &from_exprs,
                                     common::ObIArray<T *> &to_exprs,
                                     ObIRawExprReplacer *replacer)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<T *, 4> tmp_arr;
  for (int64_t i = 0; OB_SUCC(ret) && i < from_exprs.count(); ++i) {
    ObRawExpr *tmp = from_exprs.at(i);
    ObRawExpr *to_expr = NULL;
    if (OB_FAIL(copy_on_replace(from_exprs.at(i),
                                to_expr,
                                replacer))) {
      SQL_RESV_LOG(WARN, "failed to replace expr", K(ret));
    } else if (tmp == to_expr || std::is_same<T, ObRawExpr>::value) {
      // do nothing
    } else if (OB_ISNULL(to_expr) ||
               OB_UNLIKELY(tmp->get_expr_class() != to_expr->get_expr_class())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "expr class is changed", KPC(tmp), KPC(to_expr));
    }
    if (OB_SUCC(ret) && OB_FAIL(tmp_arr.push_back(static_cast<T *>(to_expr)))) {
      SQL_RESV_LOG(WARN, "failed to push back to expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(to_exprs.assign(tmp_arr))) {
    SQL_RESV_LOG(WARN, "failed to assign replaced results", K(ret));
  }
  return ret;
}

template <typename T>
int ObRawExprCopier::add_skipped_expr(const ObIArray<T *> &targets, bool include_child)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < targets.count(); ++i) {
    ret = add_skipped_expr(targets.at(i), include_child);
  }
  return ret;
}

}
}

#endif // OB_RAW_EXPR_COPIER_H
