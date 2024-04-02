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

#ifndef OCEANBASE_SQL_OPTIMIZER_OB_SEL_ESTIMATOR_
#define OCEANBASE_SQL_OPTIMIZER_OB_SEL_ESTIMATOR_

#include "sql/optimizer/ob_opt_selectivity.h"

namespace oceanbase
{
namespace sql
{

enum class ObSelEstType
{
  INVALID = 0,
  DEFAULT,
  CONST,
  IN,
  COLUMN,
  BTW,
  IS,
  CMP,
  AGG,
  EQUAL,
  LIKE,
  BOOL_OP,
  RANGE,
  SIMPLE_JOIN,
  INEQUAL_JOIN,
};

class ObSelEstimatorFactory;
class ObSelEstimator
{
public:
  ObSelEstimator(ObSelEstType type) : type_(type) {}
  virtual ~ObSelEstimator() = default;

  static int append_estimators(ObIArray<ObSelEstimator *> &sel_estimators, ObSelEstimator *new_estimator);

  // Check whether it is related to other ObSelEstimator, and if so, merge them
  virtual int merge(const ObSelEstimator &other, bool &is_success) = 0;
  // check whether it is independent of any other ObSelEstimator
  virtual bool is_independent() const = 0;
  // Calculate the selectivity
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) = 0;
  // Check whether we tend to use dynamic sampling for this estimator
  virtual bool tend_to_use_ds() = 0;
  inline ObSelEstType get_type() const { return type_; }

  VIRTUAL_TO_STRING_KV(K_(type));

protected:
  ObSelEstType type_;

private:
  DISABLE_COPY_ASSIGN(ObSelEstimator);
};

class ObSelEstimatorFactory
{
public:
  explicit ObSelEstimatorFactory(common::ObIAllocator &alloc)
    : allocator_(alloc),
      estimator_store_(alloc)
  {}

  ~ObSelEstimatorFactory() {
    destory();
  }

  inline common::ObIAllocator &get_allocator() { return allocator_; }
  inline void destory()
  {
    DLIST_FOREACH_NORET(node, estimator_store_.get_obj_list()) {
      if (node != NULL && node->get_obj() != NULL) {
        node->get_obj()->~ObSelEstimator();
      }
    }
    estimator_store_.destroy();
  }

  int create_estimator(const OptSelectivityCtx &ctx,
                       const ObRawExpr *expr,
                       ObSelEstimator *&new_estimator);

  template <typename EstimatorType>
  inline int create_estimator_inner(EstimatorType *&new_estimator)
  {
    int ret = common::OB_SUCCESS;
    void *ptr = allocator_.alloc(sizeof(EstimatorType));
    new_estimator = NULL;
    if (OB_ISNULL(ptr)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SQL_OPT_LOG(ERROR, "no more memory to create estimator");
    } else {
      new_estimator = new (ptr) EstimatorType();
      if (OB_FAIL(estimator_store_.store_obj(new_estimator))) {
        SQL_OPT_LOG(WARN, "store estimator failed", K(ret));
        new_estimator->~EstimatorType();
        new_estimator = NULL;
      }
    }
    return ret;
  }

  typedef int (*CreateEstimatorFunc) (ObSelEstimatorFactory &, const OptSelectivityCtx &,
                                      const ObRawExpr &, ObSelEstimator *&);

private:
  common::ObIAllocator &allocator_;
  common::ObObjStore<ObSelEstimator *, common::ObIAllocator&, true> estimator_store_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSelEstimatorFactory);
};

template<typename ObTemplateEstimator>
int create_simple_estimator(ObSelEstimatorFactory &factory,
                            const OptSelectivityCtx &ctx,
                            const ObRawExpr &expr,
                            ObSelEstimator *&estimator)
{
  int ret = OB_SUCCESS;
  estimator = NULL;
  ObTemplateEstimator *temp_estimator = NULL;
  if (!ObTemplateEstimator::check_expr_valid(expr)) {
    // do nothing
  } else if (OB_FAIL(factory.create_estimator_inner(temp_estimator))) {
    LOG_WARN("failed to create estimator ", K(ret));
  } else {
    temp_estimator->set_expr(&expr);
    estimator = temp_estimator;
  }
  return ret;
}

/**
 * Virtual class which estimate selectivity for filters that are independent of others
*/
class ObIndependentSelEstimator : public ObSelEstimator
{
public:
  ObIndependentSelEstimator(ObSelEstType type) : ObSelEstimator(type), expr_(NULL) {}
  virtual ~ObIndependentSelEstimator() = default;

  virtual int merge(const ObSelEstimator &other, bool &is_success) override {
    int ret = OB_SUCCESS;
    is_success = false;
    return ret;
  }

  virtual bool is_independent() const override { return true; }
  inline void set_expr(const ObRawExpr *expr) { expr_ = expr; }

  VIRTUAL_TO_STRING_KV(K_(type), KPC_(expr));

protected:
  const ObRawExpr *expr_;

private:
  DISABLE_COPY_ASSIGN(ObIndependentSelEstimator);
};

/**
 * Estimate default selectivity
*/
class ObDefaultSelEstimator : public ObIndependentSelEstimator
{
public:
  ObDefaultSelEstimator() : ObIndependentSelEstimator(ObSelEstType::DEFAULT) {}
  virtual ~ObDefaultSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObDefaultSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return true; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override;
  inline static bool check_expr_valid(const ObRawExpr &expr) { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObDefaultSelEstimator);
};

/**
 * Estimate selectivity for preds which contain agg function
 * such as : `max(c1) < 10`
*/
class ObAggSelEstimator : public ObIndependentSelEstimator
{
public:
  ObAggSelEstimator() : ObIndependentSelEstimator(ObSelEstType::AGG) {}
  virtual ~ObAggSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObAggSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return false; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override;
  inline static bool check_expr_valid(const ObRawExpr &expr) { return expr.has_flag(CNT_AGG); }
private:
  static int get_agg_sel(const OptTableMetas &table_metas,
                         const OptSelectivityCtx &ctx,
                         const ObRawExpr &qual,
                         double &selectivity);

  static int get_agg_sel_with_minmax(const OptTableMetas &table_metas,
                                     const OptSelectivityCtx &ctx,
                                     const ObRawExpr &aggr_expr,
                                     const ObRawExpr *const_expr1,
                                     const ObRawExpr *const_expr2,
                                     const ObItemType type,
                                     double &selectivity,
                                     const double rows_per_group);

  static double get_agg_eq_sel(const ObObj &maxobj,
                               const ObObj &minobj,
                               const ObObj &constobj,
                               const double distinct_sel,
                               const double rows_per_group,
                               const bool is_eq,
                               const bool is_sum);

  static double get_agg_range_sel(const ObObj &maxobj,
                                  const ObObj &minobj,
                                  const ObObj &constobj,
                                  const double rows_per_group,
                                  const ObItemType type,
                                  const bool is_sum);

  static double get_agg_btw_sel(const ObObj &maxobj,
                                const ObObj &minobj,
                                const ObObj &constobj1,
                                const ObObj &constobj2,
                                const double rows_per_group,
                                const ObItemType type,
                                const bool is_sum);

  static int is_valid_agg_qual(const ObRawExpr &qual,
                               bool &is_valid,
                               const ObRawExpr *&aggr_expr,
                               const ObRawExpr *&const_expr1,
                               const ObRawExpr *&const_expr2);
private:
  DISABLE_COPY_ASSIGN(ObAggSelEstimator);
};

/**
 * calculate const or calculable expr selectivity.
 * e.g. `1`, `1 = 1`, `1 + 1`, `1 = 0`
 * if expr is always true, selectivity = 1.0
 * if expr is always false, selectivity = 0.0
 * if expr can't get actual value, like exec_param, selectivity = 0.5
 */
class ObConstSelEstimator : public ObIndependentSelEstimator
{
public:
  ObConstSelEstimator() : ObIndependentSelEstimator(ObSelEstType::CONST) {}
  virtual ~ObConstSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObConstSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return false; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KPC(this));
    } else {
      ret = get_const_sel(ctx, *expr_, selectivity);
    }
    return ret;
  }
  inline static bool check_expr_valid(const ObRawExpr &expr) { return expr.is_const_expr(); }
private:
  static int get_const_sel(const OptSelectivityCtx &ctx,
                           const ObRawExpr &qual,
                           double &selectivity);
private:
  DISABLE_COPY_ASSIGN(ObConstSelEstimator);
};

/**
 * calculate column expr selectivity.
 * e.g. `c1`, `t1.c1`
 * selectity = 1.0 - sel(t1.c1 = 0) - sel(t1.c1 is NULL)
 */
class ObColumnSelEstimator : public ObIndependentSelEstimator
{
public:
  ObColumnSelEstimator() : ObIndependentSelEstimator(ObSelEstType::COLUMN) {}
  virtual ~ObColumnSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObColumnSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return false; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KPC(this));
    } else {
      ret = get_column_sel(table_metas, ctx, *expr_, selectivity);
    }
    return ret;
  }
  inline static bool check_expr_valid(const ObRawExpr &expr) { return expr.is_column_ref_expr(); }
private:
  static int get_column_sel(const OptTableMetas &table_metas,
                            const OptSelectivityCtx &ctx,
                            const ObRawExpr &qual,
                            double &selectivity);
private:
  DISABLE_COPY_ASSIGN(ObColumnSelEstimator);
};

/**
 * calculate [not] in predicate selectivity
 * e.g. `c1 in (1, 2, 3)`, `1 in (c1, c2, c3)`
 * The most commonly format `column in (const1, const2, const3)`
 *    selectivity = sum(selectivity(column = const_i))
 * otherwise, `var in (var1, var2, var3)
 *    selectivity = sum(selectivity(var = var_i))
 * not_in_selectivity = 1.0 - in_selectivity
 */
class ObInSelEstimator : public ObIndependentSelEstimator
{
public:
  ObInSelEstimator() : ObIndependentSelEstimator(ObSelEstType::IN) {}
  virtual ~ObInSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObInSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return false; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KPC(this));
    } else {
      ret = get_in_sel(table_metas, ctx, *expr_, selectivity);
    }
    return ret;
  }
  inline static bool check_expr_valid(const ObRawExpr &expr) {
    return T_OP_IN == expr.get_expr_type() || T_OP_NOT_IN == expr.get_expr_type();
  }
private:
  static int get_in_sel(const OptTableMetas &table_metas,
                        const OptSelectivityCtx &ctx,
                        const ObRawExpr &qual,
                        double &selectivity);
private:
  DISABLE_COPY_ASSIGN(ObInSelEstimator);
};

// get var is[not] NULL\true\false selectivity
// for var is column:
//   var is NULL: selectivity = null_sel(get_var_basic_sel)
//   var is true: selectivity = 1 - distinct_sel(var = 0) - null_sel
//   var is false: selectivity = distinct_sel(var = 0)
// others:
//   DEFAULT_SEL
// for var is not NULL\true\false: selectivity = 1.0 - is_sel
/**
 * calculate is [not] predicate selectivity
 * e.g. `c1 is null`， `c1 is ture`(mysql only)
 */
class ObIsSelEstimator : public ObIndependentSelEstimator
{
public:
  ObIsSelEstimator() : ObIndependentSelEstimator(ObSelEstType::IS) {}
  virtual ~ObIsSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObIsSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return false; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KPC(this));
    } else {
      ret = get_is_sel(table_metas, ctx, *expr_, selectivity);
    }
    return ret;
  }
  inline static bool check_expr_valid(const ObRawExpr &expr) {
    return T_OP_IS == expr.get_expr_type() || T_OP_IS_NOT == expr.get_expr_type();
  }
private:
  static int get_is_sel(const OptTableMetas &table_metas,
                        const OptSelectivityCtx &ctx,
                        const ObRawExpr &qual,
                        double &selectivity);
private:
  DISABLE_COPY_ASSIGN(ObIsSelEstimator);
};

//c1 between $val1 and $val2     -> equal with [$val2 - $val1] range sel
//c1 not between $val1 and $val2 -> equal with (min, $val1) or ($val2, max) range sel
class ObBtwSelEstimator : public ObIndependentSelEstimator
{
public:
  ObBtwSelEstimator() : ObIndependentSelEstimator(ObSelEstType::BTW) {}
  virtual ~ObBtwSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObBtwSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return false; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KPC(this));
    } else {
      ret = get_btw_sel(table_metas, ctx, *expr_, selectivity);
    }
    return ret;
  }
  inline static bool check_expr_valid(const ObRawExpr &expr) {
    return T_OP_BTW == expr.get_expr_type() || T_OP_NOT_BTW == expr.get_expr_type();
  }
private:
  static int get_btw_sel(const OptTableMetas &table_metas,
                         const OptSelectivityCtx &ctx,
                         const ObRawExpr &qual,
                         double &selectivity);
private:
  DISABLE_COPY_ASSIGN(ObBtwSelEstimator);
};

// col RANGE_CMP const, column_range_sel
// (c1, c2) RANGE_CMP (c3, c4)
// func(col) RANGE_CMP const, DEFAULT_INEQ_SEL
class ObCmpSelEstimator : public ObIndependentSelEstimator
{
public:
  ObCmpSelEstimator() : ObIndependentSelEstimator(ObSelEstType::CMP) {}
  virtual ~ObCmpSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObCmpSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return false; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", KPC(this));
    } else {
      ret = get_range_cmp_sel(table_metas, ctx, *expr_, selectivity);
    }
    return ret;
  }
  inline static bool check_expr_valid(const ObRawExpr &expr) {
    return IS_RANGE_CMP_OP(expr.get_expr_type());
  }
private:
  static int get_range_cmp_sel(const OptTableMetas &table_metas,
                               const OptSelectivityCtx &ctx,
                               const ObRawExpr &qual,
                               double &selectivity);
private:
  DISABLE_COPY_ASSIGN(ObCmpSelEstimator);
};

// Estimate selectivity for equal preds
// such as: `c1 = 1`, `c1 <=> c2`, `c1 != 3`
class ObEqualSelEstimator : public ObIndependentSelEstimator
{
public:
  ObEqualSelEstimator() :
    ObIndependentSelEstimator(ObSelEstType::EQUAL) {}
  virtual ~ObEqualSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator)
  {
    return create_simple_estimator<ObEqualSelEstimator>(factory, ctx, expr, estimator);
  }
  virtual bool tend_to_use_ds() override { return false; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override;

  inline static bool check_expr_valid(const ObRawExpr &expr) {
    return  T_OP_EQ == expr.get_expr_type() ||
            T_OP_NSEQ == expr.get_expr_type() ||
            T_OP_NE == expr.get_expr_type();
  }

  //1. var = | <=> const, get_simple_predicate_sel
  //2. func(var) = | <=> const,
  //       only simple op(+,-,*,/), get_simple_predicate_sel,
  //       mod(cnt_var, mod_num),  distinct_sel * mod_num
  //       else sqrt(distinct_sel)
  //3. cnt(var) = |<=> cnt(var) get_cntcol_eq_cntcol_sel
  static int get_equal_sel(const OptTableMetas &table_metas,
                           const OptSelectivityCtx &ctx,
                           const ObRawExpr &qual,
                           double &selectivity);
  static int get_equal_sel(const OptTableMetas &table_metas,
                           const OptSelectivityCtx &ctx,
                           const ObRawExpr &left_expr,
                           const ObRawExpr &right_expr,
                           const bool null_safe,
                           double &selectivity);
private:
  // col or (col +-* 2) != 1, 1.0 - distinct_sel - null_sel
  // col or (col +-* 2) != NULL -> 0.0
  // otherwise DEFAULT_SEL;
  static int get_ne_sel(const OptTableMetas &table_metas,
                        const OptSelectivityCtx &ctx,
                        const ObRawExpr &l_expr,
                        const ObRawExpr &r_expr,
                        double &selectivity);

  //  Get simple predicate selectivity
   //  (col) | (col +-* num) = const, sel = distinct_sel
   //  (col) | (col +-* num) = null, sel = 0
   //  (col) | (col +-* num) <=> const, sel = distinct_sel
   //  (col) | (col +-* num) <=> null, sel = null_sel
   //  multi_col | func(col) =|<=> null, sel DEFAULT_EQ_SEL 0.005
   // @param partition_id only used in base table
  /**
   * calculate equal predicate with format `contain_column_expr = not_contain_column_expr` by ndv
   * e.g. `c1 = 1`, `c1 + 1 = 2`, `c1 + c2 = 10`
   * if contain_column_expr contain not monotonic operator or has more than one column,
   *    selectivity = DEFAULT_EQ_SEL
   * if contain_column_expr contain only one column and contain only monotonic operator,
   *    selectivity = 1 / ndv
   */
  static int get_simple_equal_sel(const OptTableMetas &table_metas,
                                  const OptSelectivityCtx &ctx,
                                  const ObRawExpr &cnt_col_expr,
                                  const ObRawExpr *calculable_expr,
                                  const bool null_safe,
                                  double &selectivity);

  static int get_cntcol_op_cntcol_sel(const OptTableMetas &table_metas,
                                      const OptSelectivityCtx &ctx,
                                      const ObRawExpr &input_left_expr,
                                      const ObRawExpr &input_right_expr,
                                      ObItemType op_type,
                                      double &selectivity);
private:
  DISABLE_COPY_ASSIGN(ObEqualSelEstimator);
};

/**
 * Estimate selectivity for like preds
 * such as: `c1 like 'xx%'`, `c1 like '%xx'`
 *           c1 like 'xx%', use query range selectivity
 *           c1 like '%xx', use DEFAULT_INEQ_SEL 1.0 / 3.0
*/
class ObLikeSelEstimator : public ObIndependentSelEstimator
{
public:
  ObLikeSelEstimator() :
    ObIndependentSelEstimator(ObSelEstType::LIKE),
    variable_(NULL),
    pattern_(NULL),
    escape_(NULL),
    can_calc_sel_(false),
    match_all_str_(false) {}
  virtual ~ObLikeSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator);
  virtual bool tend_to_use_ds() override { return !can_calc_sel_; }
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override;
  static int can_calc_like_sel(const OptSelectivityCtx &ctx, const ObRawExpr &expr, bool &can_calc_sel);

private:
  const ObRawExpr *variable_;
  const ObRawExpr *pattern_;
  const ObRawExpr *escape_;
  bool can_calc_sel_;
  bool match_all_str_;
private:
  DISABLE_COPY_ASSIGN(ObLikeSelEstimator);
};

/**
 * Estimate selectivity for bool op preds
 * such as: `c1 > 1 or c2 > 1`, `lnnvl(c1 > 1)`
*/
class ObBoolOpSelEstimator : public ObIndependentSelEstimator
{
public:
  ObBoolOpSelEstimator() : ObIndependentSelEstimator(ObSelEstType::BOOL_OP) {}
  virtual ~ObBoolOpSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator);
  virtual bool tend_to_use_ds() override;
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override;

private:
  common::ObSEArray<ObSelEstimator *, 4, common::ModulePageAllocator, true> child_estimators_;
private:
  DISABLE_COPY_ASSIGN(ObBoolOpSelEstimator);
};

/**
 * Estimate selectivity for range preds which contain the same column
 * such as: `c1 > 1 and (c1 < 5 or c1 > 7)`
*/
class ObRangeSelEstimator : public ObSelEstimator
{
public:
  ObRangeSelEstimator() : ObSelEstimator(ObSelEstType::RANGE), column_expr_(NULL) {}
  virtual ~ObRangeSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator);
  virtual int merge(const ObSelEstimator &other, bool &is_success) override;
  virtual bool is_independent() const override { return false; }

  // 计算选择率
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override;

  virtual bool tend_to_use_ds() override { return false; }

  VIRTUAL_TO_STRING_KV(K_(type), KPC_(column_expr), K_(range_exprs));

  inline int get_min_max(const OptSelectivityCtx &ctx,
                         ObObj &obj_min,
                         ObObj &obj_max) {
    return ObOptSelectivity::get_column_range_min_max(ctx, column_expr_, range_exprs_, obj_min, obj_max);
  }

  const ObColumnRefRawExpr *get_column_expr() const { return column_expr_; }
  ObIArray<ObRawExpr *> &get_range_exprs() { return range_exprs_; }

private:
  const ObColumnRefRawExpr *column_expr_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> range_exprs_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRangeSelEstimator);
};

/**
 * Estimate selectivity for equal join filter which join ctx.get_left_rel_ids() and ctx.get_right_rel_ids()
 * such as: `t1.c1 = t2.c1 and t1.c2 = t2.c2`
*/
class ObSimpleJoinSelEstimator : public ObSelEstimator
{
public:
  ObSimpleJoinSelEstimator() : ObSelEstimator(ObSelEstType::SIMPLE_JOIN) {}
  virtual ~ObSimpleJoinSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator);
  virtual int merge(const ObSelEstimator &other, bool &is_success) override;
  virtual bool is_independent() const override { return false; }

  // 计算选择率
  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override;

  virtual bool tend_to_use_ds() override { return false; }

  VIRTUAL_TO_STRING_KV(K_(type), KPC_(left_rel_ids), KPC_(right_rel_ids), K_(join_conditions));
private:
  static int is_simple_join_condition(const ObRawExpr &qual,
                                      const ObRelIds *left_rel_ids,
                                      const ObRelIds *right_rel_ids,
                                      bool &is_valid);
  static int get_multi_equal_sel(const OptTableMetas &table_metas,
                                 const OptSelectivityCtx &ctx,
                                 ObIArray<ObRawExpr *> &quals,
                                 double &selectivity);
  static int extract_join_exprs(ObIArray<ObRawExpr *> &quals,
                                const ObRelIds &left_rel_ids,
                                const ObRelIds &right_rel_ids,
                                ObIArray<ObRawExpr *> &left_exprs,
                                ObIArray<ObRawExpr *> &right_exprs,
                                ObIArray<bool> &null_safes);
  static int get_cntcols_eq_cntcols_sel(const OptTableMetas &table_metas,
                                        const OptSelectivityCtx &ctx,
                                        const ObIArray<ObRawExpr *> &left_exprs,
                                        const ObIArray<ObRawExpr *> &right_exprs,
                                        const ObIArray<bool> &null_safes,
                                        double &selectivity);
  /**
  * 判断多列连接是否只涉及到两个表
  */
  static int is_valid_multi_join(ObIArray<ObRawExpr *> &quals,
                                 bool &is_valid);

  const ObRelIds *left_rel_ids_;
  const ObRelIds *right_rel_ids_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> join_conditions_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSimpleJoinSelEstimator);
};

/**
 * Estimate selectivity for inequal join filter which contains the same term
 * such as: `t1.c1 - t2.c1 < 2 and t1.c1 > t2.c1 - 3`
*/
class ObInequalJoinSelEstimator : public ObSelEstimator
{
public:
  struct Term {
    Term() : col1_(NULL), col2_(NULL), coefficient1_(1.0), coefficient2_(1.0) {}
    bool is_valid() { return col1_ != NULL && col2_ != NULL; }

    VIRTUAL_TO_STRING_KV(K_(col1), KPC_(col2),
                         K_(coefficient1), K_(coefficient2));

    const ObColumnRefRawExpr *col1_;
    const ObColumnRefRawExpr *col2_;
    double coefficient1_;
    double coefficient2_;
  };

public:
  ObInequalJoinSelEstimator() :
    ObSelEstimator(ObSelEstType::INEQUAL_JOIN),
    has_lower_bound_(false),
    has_upper_bound_(false),
    include_lower_bound_(false),
    include_upper_bound_(false),
    lower_bound_(0),
    upper_bound_(0) {}
  virtual ~ObInequalJoinSelEstimator() = default;

  static int create_estimator(ObSelEstimatorFactory &factory,
                              const OptSelectivityCtx &ctx,
                              const ObRawExpr &expr,
                              ObSelEstimator *&estimator);
  virtual int merge(const ObSelEstimator &other, bool &is_success) override;
  virtual bool is_independent() const override { return false; }

  virtual int get_sel(const OptTableMetas &table_metas,
                      const OptSelectivityCtx &ctx,
                      double &selectivity,
                      ObIArray<ObExprSelPair> &all_predicate_sel) override;

  virtual bool tend_to_use_ds() override { return false; }

  VIRTUAL_TO_STRING_KV(K_(type), K_(term),
                       K_(has_lower_bound), K_(has_upper_bound),
                       K_(include_lower_bound), K_(include_upper_bound),
                       K_(lower_bound), K_(upper_bound));

private:

  static void cmp_term(const Term &t1, const Term &t2, bool &equal, bool &need_reverse);

  static int extract_ineq_qual(const OptSelectivityCtx &ctx,
                               const ObRawExpr &qual,
                               bool &is_valid);

  static int extract_column_offset(const OptSelectivityCtx &ctx,
                                   const ObRawExpr *expr,
                                   bool is_minus,
                                   bool &is_valid,
                                   Term &term,
                                   double &offset);

  static bool is_higher_lower_bound(double bound1, bool include1, double bound2, bool include2)
  {
    return bound1 > bound2 || (bound1 == bound2 && !include1 && include2);
  }
  static bool is_higher_upper_bound(double bound1, bool include1, double bound2, bool include2)
  {
    return bound1 > bound2 || (bound1 == bound2 && include1 && !include2);
  }
  // c1 in [min1, max1], c2 in [min2, max2]
  // calc the sel of `c1 + c2 > offset`;
  static double get_gt_sel(double min1,
                           double max1,
                           double min2,
                           double max2,
                           double offset);

  static double get_any_gt_sel(double min1,
                               double max1,
                               double min2,
                               double max2,
                               double offset);

  static double get_all_gt_sel(double min1,
                               double max1,
                               double min2,
                               double max2,
                               double offset);

  // c1 in [min1, max1], c2 in [min2, max2]
  // calc the sel of `c1 + c2 = offset`;
  static double get_equal_sel(double min1,
                              double max1,
                              double ndv1,
                              double min2,
                              double max2,
                              double ndv2,
                              double offset,
                              bool is_semi);

  double get_sel_for_point(double point1, double point2);

  void reverse();
  void update_lower_bound(double bound, bool include);
  void update_upper_bound(double bound, bool include);
  void set_bound(ObItemType item_type, double bound);

  Term term_;
  bool has_lower_bound_;
  bool has_upper_bound_;
  bool include_lower_bound_;
  bool include_upper_bound_;
  double lower_bound_;
  double upper_bound_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObInequalJoinSelEstimator);
};

}
}

#endif