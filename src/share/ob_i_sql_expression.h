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

#ifndef OCEANBASE_COMMON_EXPRESSION_OB_I_SQL_EXPRESSION_
#define OCEANBASE_COMMON_EXPRESSION_OB_I_SQL_EXPRESSION_

#include "common/object/ob_object.h"
#include "share/object/ob_obj_cast.h"
namespace oceanbase
{

namespace sql
{
class ObPhysicalPlanCtx;
class ObSQLSessionInfo;
class ObExecContext;
class ObBasicSessionInfo;
class ObInfixExpression;
class ObRawExpr;
struct ObExpr;
}
namespace share
{
class ObAutoincrementService;
namespace schema
{
class ObUDFMeta;
}
}

namespace common
{
class ObNewRowIterator;
class ObIAllocator;
class ObNewRow;
//The row-level context of the expression, the context content will be cleared each time the row is iterated
struct ObRowCtx
{
  ObRowCtx()
      : is_uk_checked_(false),
        is_uk_cnt_null_(false)
  {
  }

  inline void reset()
  {
    is_uk_checked_ = false;
    is_uk_cnt_null_ = false;
  }

  bool is_uk_checked_; //Mark whether the null value of the unique key has been checked
  bool is_uk_cnt_null_;
};

struct ObExprCtx
{
  ObExprCtx()
      : phy_plan_ctx_(NULL),
        my_session_(NULL),
        exec_ctx_(NULL),
        calc_buf_(NULL),
        subplan_iters_(NULL),
        cast_mode_(0),
        err_col_idx_(0),
        tz_offset_(INVALID_TZ_OFF),
        is_pre_calculation_(false),
        phy_operator_ctx_id_ (-1),
        row_ctx_(),
        sys_connect_by_path_id_(-1),
        infix_expr_(NULL),
        stack_(NULL),
        row1_(NULL),
        row2_(NULL),
        cur_array_index_(0),
        pdml_partition_id_(OB_INVALID_INDEX_INT64)
  {}
  ObExprCtx(sql::ObPhysicalPlanCtx *phy_plan_ctx,
            sql::ObSQLSessionInfo *my_session,
            sql::ObExecContext *exec_ctx,
            ObIAllocator *calc_buf,
            ObIArray<ObNewRowIterator*> *subplan_iters = NULL)
      : phy_plan_ctx_(phy_plan_ctx),
        my_session_(my_session),
        exec_ctx_(exec_ctx),
        calc_buf_(calc_buf),
        subplan_iters_(subplan_iters),
        cast_mode_(0),
        err_col_idx_(0),
        tz_offset_(INVALID_TZ_OFF),
        is_pre_calculation_(false),
        phy_operator_ctx_id_ (-1),
        row_ctx_(),
        sys_connect_by_path_id_(-1),
        infix_expr_(NULL),
        stack_(NULL),
        row1_(NULL),
        row2_(NULL),
        cur_array_index_(0),
        pdml_partition_id_(OB_INVALID_INDEX_INT64)
  {}
  sql::ObPhysicalPlanCtx *phy_plan_ctx_;
  sql::ObSQLSessionInfo *my_session_;
  sql::ObExecContext *exec_ctx_;
  ObIAllocator *calc_buf_;
  //When an expression involves sub-query calculation, sub_row_iters_ stores the row iterators iterated from all sub-queries in the expression
  ObIArray<ObNewRowIterator*> *subplan_iters_;
  ObCastMode cast_mode_;
  common::ObObjCastParams column_conv_ctx_;
  int64_t err_col_idx_;
  int64_t tz_offset_;
  bool is_pre_calculation_;
  int64_t phy_operator_ctx_id_;
  ObRowCtx row_ctx_; //Row-level context of the expression
  // Use to store current connect by path expression id.
  int64_t sys_connect_by_path_id_;

  // for expr parameters lazy evaluation
  const sql::ObInfixExpression *infix_expr_;
  // stack for infix expr evaluation
  common::ObObj *stack_;
  // row parameters for expr evaluation (see: ObSqlExpression::calc() interface)
  const common::ObNewRow *row1_;
  const common::ObNewRow *row2_;
  //cur_array_index_ use to switch the param in array binding
  //@TODO: yuchen.wyc in the multi query scheme, it will be removed
  int64_t cur_array_index_;

  int64_t pdml_partition_id_; //In pdml, used to pass the partition id
};

//class sql::ObBasicSessionInfo;
class ObExprTypeCtx
{
public:
  ObExprTypeCtx()
     : coll_type_(CS_TYPE_INVALID),
     div_precision_increment_(OB_INVALID_COUNT),
     ob_max_allowed_packet_(OB_INVALID_COUNT),
     session_(NULL),
     udf_meta_(NULL),
     cast_mode_(CM_NONE),
     raw_expr_(NULL)
  {}

  inline ObCollationType get_coll_type() const {
    return coll_type_;
  }
  inline void set_coll_type(ObCollationType coll_type) {
    coll_type_ = coll_type;
  }

  inline int64_t get_div_precision_increment() const {
    return div_precision_increment_;
  }

  inline void set_div_precision_increment(int64_t div_precision_increment) {
    div_precision_increment_ = div_precision_increment;
  }

  inline int64_t get_max_allowed_packet() const {
    return ob_max_allowed_packet_;
  }

  inline void set_max_allowed_packet(int64_t ob_max_allowed_packet) {
    ob_max_allowed_packet_ = ob_max_allowed_packet;
  }

  inline const sql::ObSQLSessionInfo *get_session() {
    return session_;
  }

  inline void set_session(const sql::ObSQLSessionInfo *session) {
    session_ = session;
  }

  inline const share::schema::ObUDFMeta* get_udf_meta() {
    return udf_meta_;
  }

  inline void set_udf_meta(const share::schema::ObUDFMeta *udf_meta) {
    udf_meta_ = udf_meta;
  }

  inline common::ObCastMode get_cast_mode() { return cast_mode_; }
  inline void set_cast_mode(const common::ObCastMode &cast_mode) { cast_mode_ = cast_mode; }

  void set_raw_expr(sql::ObRawExpr *expr) { raw_expr_ = expr; }
  sql::ObRawExpr *get_raw_expr() { return raw_expr_; }

  TO_STRING_KV(K_(coll_type),
               K_(div_precision_increment),
               K_(ob_max_allowed_packet),
               KP_(session),
               KP_(udf_meta),
               K_(cast_mode));
private:
//  const sql::ObSQLSessionInfo *my_session_;
   ObCollationType coll_type_;
   int64_t div_precision_increment_;
   int64_t ob_max_allowed_packet_; // Carefull!!! mysql.h defined a macro call "max_allowed_packet"
   const sql::ObSQLSessionInfo *session_;
   const share::schema::ObUDFMeta *udf_meta_; /* for udf */
   // In the type inference, the expression can set cast_mode to control the behavior of each parameter for type conversion
   // Just add your own special cast mode, some regular modes will be automatically added before putting in the cast expr
   // Set cast_mode of type_ctx when calc_result_type works for static engine.
   // Usually need to override get_cast_mode() of ObExprOperator which works for non_static engine
   common::ObCastMode cast_mode_;
   sql::ObRawExpr *raw_expr_;
   //used to switch params in subquery comparison operators
   int64_t cur_row_idx_;
};

class ObISqlExpression
{
public:

  /**
   * 根据表达式语义对row的值进行计算
   *
   * @param row [in] 输入行
   * @param result [out] 计算结果
   *
   * @return error code
   */
  virtual int calc(ObExprCtx &expr_ctx, const common::ObNewRow &row, common::ObObj &result) const = 0;
  virtual int calc(ObExprCtx &expr_ctx, const common::ObNewRow &row1, const common::ObNewRow &row2,
                   common::ObObj &result) const = 0;
  /// Print expression
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;

  // Whether to call the expression compiled by jit
  virtual bool is_use_jitted_expr() const = 0;

  virtual const sql::ObExpr *get_expr() const = 0;
};

class ObIColumnExpression
{
public:
  virtual int64_t get_result_index() const = 0;
  virtual int calc_and_project(ObExprCtx &expr_ctx, common::ObNewRow &row) const = 0;
  /// Print expression
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
};

}
}
#endif /* OCEANBASE_COMMON_EXPRESSION_OB_I_SQL_EXPRESSION_ */

