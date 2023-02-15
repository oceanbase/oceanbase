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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_SQL_EXPRESSION_H_
#define OCEANBASE_SQL_ENGINE_EXPR_SQL_EXPRESSION_H_

#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "share/ob_i_sql_expression.h"

#include "sql/engine/expr/ob_postfix_expression.h"
#include "sql/engine/expr/ob_infix_expression.h"
#include "sql/engine/expr/ob_sql_expression_factory.h"
#include "sql/engine/sort/ob_base_sort.h"
#include "sql/engine/ob_operator.h"         // ObOpSchemaObj
class ObAggregateFunctionTest;

// 这个宏由于expr的构造需要传参，暂时没统一
#define OB_UNIS_DECODE_EXPR_DLIST(Type, expr_list, phy_plan) \
  do { \
    int64_t list_count = 0; \
    OB_UNIS_DECODE(list_count); \
    if (OB_UNLIKELY(list_count < 0)) { \
    } else { \
      for (int64_t i = 0; OB_SUCC(ret) && i < list_count; ++i) { \
        Type *p = NULL; \
        if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr((phy_plan), p))) { \
          LOG_WARN("make sql expr failed", K(i), K(list_count), K(ret)); \
        } else if (OB_ISNULL(p)) { \
          ret = OB_BAD_NULL_ERROR; \
          LOG_WARN("sql expr is null", K(i), K(list_count), K(ret)); \
        } else if (OB_FAIL(p->deserialize(buf, data_len, pos))) { \
          LOG_WARN("deserialize expr failed", K(i), K(list_count), K(ret)); \
        } else { \
          if (!(expr_list).add_last(p)) { \
            ret = OB_ERR_UNEXPECTED; \
            LOG_WARN("add expr list failed", K(i), K(list_count), K(ret)); \
          } \
        } \
      } \
    } \
  } while(0)

namespace oceanbase
{
namespace sql
{
class ObPhysicalPlan;

class ObSqlExpression: public common::ObISqlExpression, public common::ObDLinkBase<ObSqlExpression>
{
  OB_UNIS_VERSION_V(1);
public:
  enum ExpressionType
  {
    EXPR_TYPE_SQL = 0,
    EXPR_TYPE_COLUMN,
    EXPR_TYPE_AGGREGATE,
    EXPR_TYPE_MAX
  };
public:
  ObSqlExpression(common::ObIAllocator &alloc, int64_t item_count = 0);
  virtual ~ObSqlExpression();

  int assign(const ObSqlExpression &other);

  int set_item_count(int64_t count);
  inline void set_need_construct_binding_array(bool need_construct) { need_construct_binding_array_ = need_construct; }
  inline bool need_construct_binding_array() const { return need_construct_binding_array_; }
  inline int64_t get_array_param_index() const { return array_param_index_; }
  /**
    common::ObIAllocator &alloc, * 设置表达式
   * @param expr [in] 表达式，表达方式与实现相关，目前定义为后缀表达式
   *
   * @return error code
   */
  virtual int add_expr_item(const ObPostExprItem &item, const ObRawExpr *raw_expr = nullptr);
  inline const ObSqlFixedArray<ObInfixExprItem> &get_expr_items() const
  {
    return infix_expr_.get_exprs();
  }
  virtual void reset();

  virtual void start_gen_infix_exr() { gen_infix_expr_ = true; }
  bool is_gen_infix_expr() const { return gen_infix_expr_; }
  void set_gen_infix_expr(const bool is_gen_infix_expr ) { gen_infix_expr_ = is_gen_infix_expr; }

  ObInfixExpression &get_infix_expr() { return infix_expr_; }

  bool is_equijoin_cond(int64_t &c1, int64_t &c2,
      common::ObObjType &cmp_type, common::ObCollationType &cmp_cs_type,
      bool &is_null_safe) const;
  /**
   * 根据表达式语义对row的值进行计算
   *
   * @param row [in] 输入行
   * @param result [out] 计算结果
   *
   * @return error code
   */
  virtual int calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
                   common::ObObj &result) const;
  virtual int calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row1,
                   const common::ObNewRow &row2,
                   common::ObObj &result) const;
  TO_STRING_KV(K_(infix_expr), K_(post_expr), K_(gen_infix_expr), KP(expr_));

  // check expression type
  int generate_idx_for_regexp_ops(int16_t &cur_regexp_op_count);
  inline bool is_empty() const;
  virtual int32_t get_type() const { return ObSqlExpression::EXPR_TYPE_SQL; }

  inline void set_fast_expr(ObFastExprOperator *fast_expr) { fast_expr_ = fast_expr; }

  bool is_use_jitted_expr() const
  {
    return false;
  }

  void set_is_pl_mock_default_expr(bool v) { is_pl_mock_default_expr_ = v; }
  bool get_is_pl_mock_default_expr() const { return is_pl_mock_default_expr_; }

  void set_expr(const ObExpr *expr) { expr_ = expr; }

  virtual const ObExpr *get_expr() const override { return expr_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlExpression);
protected:
  friend class ::ObAggregateFunctionTest;
  common::ObIAllocator &inner_alloc_;
  // data members
  ObPostfixExpression post_expr_;
  //fast expr目前不序列化，不在远端执行
  ObFastExprOperator *fast_expr_;
  ObInfixExpression infix_expr_;
  int32_t array_param_index_;
  bool need_construct_binding_array_;
   // used for infix generation, no need to serialize
  bool gen_infix_expr_;

  // expression for static engine, only used in PL
  bool is_pl_mock_default_expr_;
  const ObExpr *expr_;
};

inline void ObSqlExpression::reset(void)
{
  common::ObDLinkBase<ObSqlExpression>::reset();
  post_expr_.reset();
  fast_expr_ = NULL;
  gen_infix_expr_ = false;
  infix_expr_.reset();
  need_construct_binding_array_ = false;
  array_param_index_ = common::OB_INVALID_INDEX;
}

inline bool ObSqlExpression::is_empty() const
{
  return infix_expr_.is_empty() && post_expr_.is_empty();
}

class ObColumnExpression: public ObSqlExpression, public common::ObIColumnExpression
{
  OB_UNIS_VERSION_V(1);
public:
  ObColumnExpression(common::ObIAllocator &alloc, int64_t item_count = 0);
  virtual ~ObColumnExpression();

  int assign(const ObColumnExpression &other);
  void set_result_index(int64_t index) { result_index_ = index; }
  inline int64_t get_result_index() const { return result_index_; }

  inline void set_collation_type(common::ObCollationType cs_type) { cs_type_ = cs_type; }
  inline common::ObCollationType get_collation_type() const { return cs_type_; }
  inline void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_.set_accuracy(accuracy); }
  inline const common::ObAccuracy &get_accuracy() const { return accuracy_; }
  int calc_and_project(common::ObExprCtx &expr_ctx, common::ObNewRow &row) const;
  virtual void reset();
  virtual int32_t get_type() const { return ObSqlExpression::EXPR_TYPE_COLUMN; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ObColumnExpression);
protected:
  int64_t result_index_; //result's index in output row
  common::ObCollationType cs_type_;
  common::ObAccuracy accuracy_;
};

inline void ObColumnExpression::reset()
{
  ObSqlExpression::reset();
  cs_type_ = common::CS_TYPE_INVALID;
}

class ObAggregateExpression: public ObColumnExpression
{
  OB_UNIS_VERSION_V(1);
public:
  ObAggregateExpression(common::ObIAllocator &alloc, int64_t item_count = 0);
  virtual ~ObAggregateExpression();

  int assign(const ObAggregateExpression &other);

  virtual void reset();
  virtual int32_t get_type() const { return ObSqlExpression::EXPR_TYPE_AGGREGATE; }
  virtual int calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
                   common::ObObj &result) const;
  virtual int calc(common::ObExprCtx &expr_ctx,
                   const common::ObNewRow &row1,
                   const common::ObNewRow &row2,
                   common::ObObj &result) const;
  int calc_result_row(common::ObExprCtx &expr_ctx,
                      const common::ObNewRow &row,
                      common::ObNewRow &result_row) const;

  int64_t to_string(char *buf, const int64_t buf_len) const;

  inline void set_aggr_func(ObItemType aggr_fun, bool is_distinct);
  inline int get_aggr_column(ObItemType &aggr_fun, bool &is_distinct,
                             common::ObCollationType &cs_type) const;
  inline int add_separator_param_expr_item(const ObPostExprItem &item, const ObRawExpr *raw_expr)
           { return separator_param_expr_.add_expr_item(item, raw_expr); }
  inline const ObSqlExpression &get_separator_param_expr() const { return separator_param_expr_; }
  // 不包括sort column的真正param的列数
  inline void set_real_param_col_count(int64_t real_param_col_count) { real_param_col_count_ = real_param_col_count; }
  inline int64_t get_real_param_col_count() const { return real_param_col_count_; }
  // 包括sort column的所有param的列数
  inline void set_all_param_col_count(int64_t all_param_col_count) { output_column_count_ = all_param_col_count; }
  inline int64_t get_all_param_col_count() const { return output_column_count_; }

  int init_sort_column_count(int64_t count) { return sort_columns_.init(count); }
  int init_sort_extra_infos_(int64_t count) { return extra_infos_.init(count); }
  int add_sort_column(const int64_t index,
                      common::ObCollationType cs_type,
                      bool is_ascending);
  inline const common::ObIArray<ObSortColumn> &get_sort_columns() const { return sort_columns_; }
  inline const common::ObIArray<ObOpSchemaObj> &get_sort_extra_infos() const { return extra_infos_; }
  inline common::ObIArray<ObOpSchemaObj> &get_sort_extra_infos() { return extra_infos_; }
  inline ObItemType get_aggr_func() const { return aggr_func_; }

  int init_aggr_cs_type_count(int64_t count) { return aggr_cs_types_.init(count); }
  int add_aggr_cs_type(common::ObCollationType cs_type) { return aggr_cs_types_.push_back(cs_type); }
  const common::ObIArray<common::ObCollationType> *get_aggr_cs_types() const { return &aggr_cs_types_; }
  virtual void start_gen_infix_exr() {
    gen_infix_expr_ = true;
    separator_param_expr_.start_gen_infix_exr();
    window_size_param_expr_.start_gen_infix_exr();
    item_size_param_expr_.start_gen_infix_exr();
    bucket_num_expr_.start_gen_infix_exr();
  }
  inline int add_window_size_param_expr_item(const ObPostExprItem &item,
                                             const ObRawExpr *raw_expr) {
    return window_size_param_expr_.add_expr_item(item, raw_expr); }
  inline const ObSqlExpression &get_window_size_param_expr() const {
    return window_size_param_expr_; }
  inline int add_item_size_param_expr_item(const ObPostExprItem &item, const ObRawExpr *raw_expr) {
    return item_size_param_expr_.add_expr_item(item, raw_expr); }
  inline const ObSqlExpression &get_item_size_param_expr() const {
    return item_size_param_expr_; }
  void set_is_need_deserialize_row(bool is_need) {
    is_need_deserialize_row_ = is_need;
  }
  bool is_need_deserialize_row() const {
    return is_need_deserialize_row_;
  }

  void set_pl_agg_udf_type_id(int64_t type_id) { pl_agg_udf_type_id_ = type_id; }
  int64_t get_pl_agg_udf_type_id() const { return pl_agg_udf_type_id_; }

  inline void set_result_type(ObExprResType result_type) { result_type_ = result_type; }
  inline ObExprResType get_result_type() const { return result_type_; }

  int init_pl_agg_udf_params_type_count(int64_t count) {
    return pl_agg_udf_params_type_.init(count); }
  int add_pl_agg_udf_param_type(ObExprResType type) {
    return pl_agg_udf_params_type_.push_back(type); }
  const common::ObIArray<ObExprResType> &get_pl_agg_udf_params_type() const {
    return pl_agg_udf_params_type_; }
  inline int add_bucket_num_expr_item(const ObPostExprItem &item, const ObRawExpr *raw_expr) {
    return bucket_num_expr_.add_expr_item(item, raw_expr); }
  inline const ObSqlExpression &get_bucket_num_expr() const {
    return bucket_num_expr_; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObAggregateExpression);
protected:
  ObItemType aggr_func_;
  bool is_distinct_;
  common::ObFixedArray<ObSortColumn, common::ObIAllocator> sort_columns_;
  ObSqlExpression separator_param_expr_;
  int64_t real_param_col_count_;
public:
  common::ObFixedArray<common::ObCollationType, common::ObIAllocator> aggr_cs_types_;
protected:
  int64_t output_column_count_;
  common::ObFixedArray<ObOpSchemaObj, common::ObIAllocator> extra_infos_;//支持NULLs FIRST
protected:
  ObSqlExpression window_size_param_expr_;
  ObSqlExpression item_size_param_expr_;
  bool is_need_deserialize_row_;
protected:
  int64_t pl_agg_udf_type_id_;
  common::ObFixedArray<ObExprResType, common::ObIAllocator> pl_agg_udf_params_type_;
  ObExprResType result_type_;
  ObSqlExpression bucket_num_expr_;
};

inline void ObAggregateExpression::reset()
{
  ObColumnExpression::reset();
  aggr_func_ = ::T_INVALID;
  is_distinct_ = false;
  sort_columns_.reset();
  //separator_param_idx_ = common::OB_INVALID_INDEX;
  separator_param_expr_.reset();
  real_param_col_count_ = 0;
  aggr_cs_types_.reset();
  window_size_param_expr_.reset();
  item_size_param_expr_.reset();
  is_need_deserialize_row_ = false;
  pl_agg_udf_type_id_ = common::OB_INVALID_ID;
  pl_agg_udf_params_type_.reset();
  result_type_.reset();
  bucket_num_expr_.reset();
}

inline void ObAggregateExpression::set_aggr_func(ObItemType aggr_func, bool is_distinct)
{
  aggr_func_ = aggr_func;
  is_distinct_ = is_distinct;
}

inline int ObAggregateExpression::get_aggr_column(ObItemType &aggr_fun,
                                                  bool &is_distinct,
                                                  common::ObCollationType &cs_type) const
{
  int ret = common::OB_SUCCESS;
  aggr_fun = aggr_func_;
  is_distinct = is_distinct_;
  cs_type = cs_type_;
  return ret;
}

// ObExprOperatorFetcher is used fetch the ObExprOperator of ObExprGeneratorImpl.
// only override the add_expr_item.
struct ObExprOperatorFetcher: public ObSqlExpression
{
public:
  ObExprOperatorFetcher()
      //  the allocator is never used
      : ObSqlExpression(*lib::ObMallocAllocator::get_instance(), 0), op_(NULL)
  {}

  virtual int add_expr_item(const ObPostExprItem &item, const ObRawExpr *raw_expr) override
   {
    UNUSED(raw_expr);
    int ret = common::OB_SUCCESS;
    if (IS_EXPR_OP(item.get_item_type())) {
      if (NULL != op_) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "only one expr operator expected", K(ret));
      } else {
        op_ = item.get_expr_operator();
      }
    } else {
      op_ = NULL;
    }
    return ret;
  }

  void reset() { op_ = NULL; }

  const ObExprOperator *op_;
};

class ObSqlExpressionUtil
{
public:
  static int make_sql_expr(ObPhysicalPlan *physical_plan, ObSqlExpression  *&expr);
  static int make_sql_expr(ObPhysicalPlan *physical_plan, ObColumnExpression  *&expr);
  static int make_sql_expr(ObPhysicalPlan *physical_plan, ObAggregateExpression  *&expr);
  static int add_expr_to_list(common::ObDList<ObSqlExpression> &list, ObSqlExpression *expr);
  static int copy_sql_expression(ObSqlExpressionFactory &sql_expression_factory,
                                 const ObSqlExpression *src_expr,
                                 ObSqlExpression *&dst_expr);
  static int copy_sql_expressions(ObSqlExpressionFactory &sql_expression_factory,
                                  const common::ObIArray<ObSqlExpression *> &src_exprs,
                                  common::ObIArray<ObSqlExpression *> &dst_exprs);
  static int expand_array_params(common::ObExprCtx &expr_ctx,
                                 const common::ObObj &src_param,
                                 const common::ObObj *&result);
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlExpressionUtil);
  ObSqlExpressionUtil();
  ~ObSqlExpressionUtil();
  template <class T>
  static int make_expr(ObPhysicalPlan *physical_plan, T *&expr);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_EXPR_SQL_EXPRESSION_H_ */
