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

#ifndef OCEANBASE_SQL_OB_EXPR_OPERATOR_H_
#define OCEANBASE_SQL_OB_EXPR_OPERATOR_H_

#include "share/ob_define.h"
#include "share/ob_errno.h"
#include <math.h>
#include "lib/objectpool/ob_tc_factory.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/timezone/ob_timezone_info.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_i_sql_expression.h"
#include "share/config/ob_server_config.h"
#include "share/datum/ob_datum_funcs.h"
#include "common/expression/ob_expr_string_buf.h"
#include "share/object/ob_obj_cast.h"
#include "common/object/ob_obj_compare.h"
#include "common/ob_accuracy.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "sql/parser/ob_item_type.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_cmp_func.h"

#define GET_EXPR_CTX(ClassType, ctx, id) static_cast<ClassType*>((ctx).get_expr_op_ctx(id))

namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObExprCGCtx;
class ObSubQueryIterator;

enum CollectionPredRes {
  COLL_PRED_INVALID = -1,
  COLL_PRED_FALSE,
  COLL_PRED_TRUE,
  COLL_PRED_NULL,
  COLL_PRED_FIRST_COLL_ZERO,
  COLL_PRED_SECOND_COLL_ZERO,
  COLL_PRED_BOTH_COLL_ZERO,
};
class ObFuncInputType {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObFuncInputType() : calc_meta_(), max_length_(0), flag_(0)
  {}
  ObFuncInputType(common::ObObjMeta calc_meta, common::ObLength max_length, uint32_t flag)
      : calc_meta_(calc_meta), max_length_(max_length), flag_(flag)
  {
    if (OB_UNLIKELY(calc_meta.get_type() >= common::ObMaxType)) {
      SQL_LOG(ERROR, "the wrong type");
    }
  }

  common::ObObjType get_calc_type() const
  {
    return calc_meta_.get_type();
  }
  common::ObLength get_length() const
  {
    return max_length_;
  }
  const common::ObObjMeta& get_calc_meta() const
  {
    return calc_meta_;
  }
  common::ObLength get_max_length() const
  {
    return max_length_;
  }
  bool is_zerofill() const
  {
    return flag_ & OB_MYSQL_ZEROFILL_FLAG;
  }

  TO_STRING_KV(N_CALC_META, calc_meta_, N_LENGTH, max_length_, N_FLAG, flag_);

private:
  // connvert ObObj to this type
  common::ObObjMeta calc_meta_;
  // for zerofill
  common::ObLength max_length_;
  uint32_t flag_;
};

class ObExprOperatorCtx {
public:
  ObExprOperatorCtx()
  {}
  virtual ~ObExprOperatorCtx()
  {}
  virtual int64_t to_string(char* buffer, int64_t buf_len) const
  {
    UNUSED(buffer);
    UNUSED(buf_len);
    return 0;
  }
};

typedef common::ObIArray<common::ObNewRowIterator*> RowIterIArray;

class ObIterExprCtx {
public:
  ObIterExprCtx(ObExecContext& ctx, common::ObIAllocator& allocator)
      : iter_expr_ctxs_(allocator), index_scan_iters_(NULL), ctx_(ctx), allocator_(allocator), cur_row_(NULL)
  {}

  int init(uint64_t expr_size, RowIterIArray* iters)
  {
    index_scan_iters_ = iters;
    return iter_expr_ctxs_.prepare_allocate(expr_size);
  }

  RowIterIArray* get_index_scan_iters()
  {
    return index_scan_iters_;
  }

  template <typename T>
  int create_expr_op_ctx(uint64_t expr_id, T*& op_ctx)
  {
    int ret = common::OB_SUCCESS;
    if (expr_id >= iter_expr_ctxs_.count()) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid expr id", K(expr_id), K(iter_expr_ctxs_.count()));
    } else {
      void* ptr = allocator_.alloc(sizeof(T));
      if (NULL == ptr) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "allocate memory for expr op ctx failed");
      } else {
        op_ctx = new (ptr) T();
        iter_expr_ctxs_.at(expr_id) = op_ctx;
      }
    }
    return ret;
  }
  template <typename T>
  T* get_expr_op_ctx(uint64_t expr_id)
  {
    T* ret = NULL;
    if (expr_id < iter_expr_ctxs_.count()) {
      ret = static_cast<T*>(iter_expr_ctxs_.at(expr_id));
    }
    return ret;
  }
  ObExecContext& get_exec_context()
  {
    return ctx_;
  }
  void set_cur_row(common::ObNewRow* cur_row)
  {
    cur_row_ = cur_row;
  }
  common::ObNewRow* get_cur_row() const
  {
    return cur_row_;
  }

private:
  common::ObFixedArray<ObExprOperatorCtx*, common::ObIAllocator> iter_expr_ctxs_;
  RowIterIArray* index_scan_iters_;
  ObExecContext& ctx_;
  common::ObIAllocator& allocator_;
  common::ObNewRow* cur_row_;
};

class ObFastExprOperator {
public:
  ObFastExprOperator(ObExprOperatorType operator_type) : op_type_(operator_type)
  {}
  virtual ~ObFastExprOperator()
  {}
  virtual int calc(common::ObExprCtx& expr_ctx, const common::ObNewRow& row, common::ObObj& result) const = 0;
  inline ObExprOperatorType get_op_type() const
  {
    return op_type_;
  }
  virtual int assign(const ObFastExprOperator& other) = 0;

protected:
  ObExprOperatorType op_type_;
};

class ObIterExprOperator {
  OB_UNIS_VERSION_V(1);

public:
  ObIterExprOperator() : expr_id_(common::OB_INVALID_ID), expr_type_(T_INVALID)
  {}
  virtual ~ObIterExprOperator()
  {}

  virtual int get_next_row(ObIterExprCtx& expr_ctx, const common::ObNewRow*& result) const = 0;
  inline uint64_t get_expr_id() const
  {
    return expr_id_;
  }
  inline void set_expr_id(uint64_t expr_id)
  {
    expr_id_ = expr_id;
  }
  inline void set_expr_type(ObExprOperatorType expr_type)
  {
    expr_type_ = expr_type;
  }
  inline ObExprOperatorType get_expr_type() const
  {
    return expr_type_;
  }

  TO_STRING_KV(K_(expr_id), K_(expr_type));

protected:
  uint64_t expr_id_;
  ObExprOperatorType expr_type_;
};

class ObExprOperator : public common::ObDLinkBase<ObExprOperator> {
  OB_UNIS_VERSION_V(1);

public:
  friend class ObRawExpr;

  enum ObSqlParamNumFlag {
    PARAM_NUM_UNKNOWN = -1,
    MORE_THAN_ZERO = -2,
    MORE_THAN_ONE = -3,
    MORE_THAN_TWO = -4,
    ZERO_OR_ONE = -5,
    ONE_OR_TWO = -6,
    TWO_OR_THREE = -7,
    OCCUR_AS_PAIR = -8,
  };

  static const int32_t NOT_ROW_DIMENSION = -1;
  ObExprOperator(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num,
      int32_t row_dimension = NOT_ROW_DIMENSION);

  virtual ~ObExprOperator()
  {}
  virtual int assign(const ObExprOperator& other);
  // initailize every expr operater's traits
  // including eval_func_, inner_eval_func_, extra_
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;
  // whether needs expression level's context. default is not needs;
  // if need_rt_ctx, it should be put in corresponding child
  // expression's implemention and it should define as following:
  //      virtual bool need_rt_ctx() const override { return true; }
  virtual bool need_rt_ctx() const
  {
    return false;
  }

  // TODO : remove after all expr implemented in new engine.
  // Check expr is default cg_expr() implement to detect the expr is supported in new engine.
  bool is_default_expr_cg() const;

  inline int32_t get_param_num() const
  {
    return param_num_;
  }
  inline int32_t get_real_param_num() const
  {
    return real_param_num_;
  }
  inline int32_t get_row_dimension() const
  {
    return row_dimension_;
  }
  inline const char* get_name() const
  {
    return name_;
  };
  virtual inline ObExprOperatorType get_type() const
  {
    return type_;
  }
  virtual inline void reset();
  inline int32_t get_magic() const
  {
    return magic_;
  };
  inline ObRawExpr* get_raw_expr() const
  {
    return raw_expr_;
  }
  inline void set_magic(int32_t magic)
  {
    magic_ = magic;
  };
  inline bool is_called_in_sql() const
  {
    return is_called_in_sql_;
  };
  inline void set_is_called_in_sql(bool is_called_in_sql)
  {
    is_called_in_sql_ = is_called_in_sql;
  };
  inline uint64_t get_id() const
  {
    return id_;
  };
  inline void set_id(uint64_t id)
  {
    id_ = id;
  };

  inline const ObExprResType& get_result_type() const
  {
    return result_type_;
  }
  inline const common::ObIArray<ObFuncInputType>& get_input_types() const
  {
    return input_types_;
  }
  int set_input_types(const ObIExprResTypes& input_types);  // convert ExprResTyp=>FuncInputType
  inline void set_result_type(const ObExprResType& type)
  {
    result_type_ = type;
  }
  inline void set_result_type(const common::ObObjType& type)
  {
    result_type_.set_type(type);
  }
  inline void set_row_dimension(const int32_t row_dimension)
  {
    row_dimension_ = row_dimension;
  }
  inline void set_real_param_num(const int32_t param_num)
  {
    real_param_num_ = param_num;
  };
  inline void set_raw_expr(ObRawExpr* expr)
  {
    raw_expr_ = expr;
  }
  virtual int calc_result_type0(ObExprResType& type, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;

  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;

  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const;

  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_array, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

  // define derive type from expr argument value
  virtual int calc_result_type0(
      ObExprResType& type, common::ObExprTypeCtx& type_ctx, common::ObIArray<common::ObObj*>& arg_arrs) const;
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx,
      common::ObIArray<common::ObObj*>& arg_arrs) const;
  virtual int calc_result_type2(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
      common::ObExprTypeCtx& type_ctx, common::ObIArray<common::ObObj*>& arg_arrs) const;
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx, common::ObIArray<common::ObObj*>& arg_arrs) const;
  virtual int calc_result_typeN(ObExprResType& type, ObExprResType* types_array, int64_t param_num,
      common::ObExprTypeCtx& type_ctx, common::ObIArray<common::ObObj*>& arg_arrs) const;
  // end define derive type from expr argment value

  virtual int calc_result0(common::ObObj& result, common::ObExprCtx& expr_ctx) const;
  virtual int calc_result1(common::ObObj& result, const common::ObObj& obj, common::ObExprCtx& expr_ctx) const;
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;
  virtual int calc_result3(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObExprCtx& expr_ctx) const;
  /**
   * cal value
   * @param[out] result
   * @param[in]  objs        array ob input arguments
   * @param[in]  param_num   number of obobj(value) const
   */
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_array, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  /**
   * call the operation on the stack
   * pop the input argument from the stack and push the result into it
   * @param stack [in/out]
   * @param stack_size [in/out]
   *
   * @return ob error
   */
  virtual int call(common::ObObj* stack, int64_t& stack_size, common::ObExprCtx& expr_ctx) const;

  // call() interface is used for postfix expression evaluation, will be discard.
  // eval() is called in infix expression evaluation, support lazy evaluate.
  virtual int eval(common::ObExprCtx& expr_ctx, common::ObObj& val, common::ObObj* params, int64_t param_num) const;

  // parameter evaluation.
  // NOTICE: %param must in %params array which passed by eval().
  int param_eval(common::ObExprCtx& expr_ctx, const common::ObObj& param, const int64_t param_index) const;

  static bool is_valid_nls_param(const common::ObString& nls_param_str);
  inline bool is_param_lazy_eval() const
  {
    return param_lazy_eval_;
  }

  inline static bool is_type_valid(const common::ObObjType& type);
  virtual bool need_charset_convert() const
  {
    return need_charset_convert_;
  }
  virtual int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(N_EXPR_TYPE,
        get_type_name(type_),
        N_EXPR_NAME,
        name_,
        N_PARAM_NUM,
        param_num_,
        N_DIM,
        row_dimension_,
        N_REAL_PARAM_NUM,
        real_param_num_,
        N_RESULT_TYPE,
        result_type_,
        N_INPUT_TYPE,
        input_types_);
    J_OBJ_END();
    return pos;
  }

public:
  /*
    Aggregate arguments for comparison, e.g: a=b, a LIKE b, a RLIKE b
    - don't convert to @@character_set_connection if all arguments are numbers
    - don't allow DERIVATION_NONE
  */
  static int aggregate_charsets_for_comparison(common::ObObjMeta& type, const common::ObObjMeta* types,
      int64_t param_num, const common::ObCollationType conn_coll_type);
  static int aggregate_charsets_for_comparison(
      ObExprResType& type, const ObExprResType* types, int64_t param_num, const common::ObCollationType conn_coll_type);

  /*
    Aggregate arguments for string result, e.g: CONCAT(a,b)
    - convert to @@character_set_connection if all arguments are numbers
    - allow DERIVATION_NONE
  */
  static int aggregate_charsets_for_string_result(common::ObObjMeta& type, const common::ObObjMeta* types,
      int64_t param_num, const common::ObCollationType conn_coll_type);
  static int aggregate_charsets_for_string_result(
      ObExprResType& type, const ObExprResType* types, int64_t param_num, const common::ObCollationType conn_coll_type);

  static int aggregate_max_length_for_string_result(ObExprResType& type, const ObExprResType* types, int64_t param_num,
      bool is_oracle_mode, const common::ObLengthSemantics default_length_semantics, bool need_merge_type = TRUE,
      bool skip_null = FALSE);

  /*
    Aggregate arguments for string result, whe`.fn some comparison
    is involved internally, only for: REPLACE(a,b,c) and SubStringIndex()
    - convert to @@character_set_connection if all arguments are numbers
    - disallow DERIVATION_NONE
  */
  static int aggregate_charsets_for_string_result_with_comparison(common::ObObjMeta& type,
      const common::ObObjMeta* types, int64_t param_num, const common::ObCollationType conn_coll_type);
  static int aggregate_charsets_for_string_result_with_comparison(common::ObObjMeta& type, const ObExprResType* types,
      int64_t param_num, const common::ObCollationType conn_coll_type);
  static int aggregate_result_type_for_merge(ObExprResType& type, const ObExprResType* types, int64_t param_num,
      const common::ObCollationType conn_coll_type, bool is_oracle_mode,
      const common::ObLengthSemantics default_length_semantics, const sql::ObSQLSessionInfo* session,
      bool need_merge_type = TRUE, bool skip_null = FALSE);
  static int aggregate_result_type_for_case(ObExprResType& type, const ObExprResType* types, int64_t param_num,
      const common::ObCollationType conn_coll_type, bool is_oracle_mode,
      const common::ObLengthSemantics default_length_semantics, const sql::ObSQLSessionInfo* session,
      bool need_merge_type = TRUE, bool skip_null = FALSE);

  // oracle string's type deduce
  static int aggregate_string_type_and_charset_oracle(const ObBasicSessionInfo& session,
      const common::ObIArray<ObExprResType*>& params, ObExprResType& result, bool prefer_var_len_char = false);
  static int deduce_string_param_calc_type_and_charset(
      const ObBasicSessionInfo& session, const ObExprResType& result, common::ObIArray<ObExprResType*>& params);
  typedef int (*calc_result_len_func)(const common::ObLengthSemantics param_ls, const common::ObLength param_len,
      const bool byte_reach_limits, const common::ObLengthSemantics res_ls, common::ObLength& res_len);
  static int sum_result_len(common::ObLength& res_len, common::ObLength delta_len)
  {
    res_len += delta_len;
    return common::OB_SUCCESS;
  }
  static int max_result_len(common::ObLength& res_len, common::ObLength delta_len)
  {
    res_len = std::max(delta_len, res_len);
    return common::OB_SUCCESS;
  }

  static common::ObCollationType get_default_collation_type(
      common::ObObjType type, const ObBasicSessionInfo& session_info);

  static int is_same_kind_type_for_case(const ObExprResType& type1, const ObExprResType& type2, bool& match);
  static int aggregate_numeric_accuracy_for_merge(
      ObExprResType& type, const ObExprResType* types, int64_t param_num, bool is_oracle_mode);

  static int aggregate_temporal_accuracy_for_merge(ObExprResType& type, const ObExprResType* types, int64_t param_num);

  static int aggregate_accuracy_for_merge(ObExprResType& type, const ObExprResType* types, int64_t param_num);

  int calc_cmp_type2(ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2,
      const common::ObCollationType coll_type) const;

  int calc_cmp_type3(ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2,
      const ObExprResType& type3, const common::ObCollationType coll_type) const;
  int calc_trig_function_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;
  int calc_trig_function_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;

public:
  virtual common::ObCastMode get_cast_mode() const;

protected:
  ObExpr* get_rt_expr(const ObRawExpr& raw_expr) const;

  inline static void calc_result_flag1(ObExprResType& type, const ObExprResType& type1);
  inline static void calc_result_flag2(ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2);
  inline static void calc_result_flag3(
      ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2, const ObExprResType& type3);
  inline static void calc_result_flagN(ObExprResType& type, const ObExprResType* types, int64_t param_num);
  static common::ObObjType get_calc_cast_type(common::ObObjType param_type, common::ObObjType calc_type);
  static common::ObObjType enumset_calc_types_[common::ObMaxTC];

  void disable_operand_auto_cast()
  {
    operand_auto_cast_ = false;
  }

private:
  // computation framework internally provided a universal datatype's converting
  // method, which is able to convert param's type to input_types_. this may
  // not be a expected behavior for some expression. If you want to prohibit
  // this, please explictly call disable_operand_auto_cast() in constructor.
  //
  // @output: result should be store in stack
  int cast_operand_type(common::ObObj* params, const int64_t param_num, common::ObExprCtx& expr_ctx) const;

  int OB_INLINE cast_operand_type(common::ObObj& param, const ObFuncInputType& type, common::ObExprCtx& expr_ctx) const;

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprOperator);
  // types and constants

  static const uint32_t OB_COLL_DISALLOW_NONE = 1;
  static const uint32_t OB_COLL_ALLOW_NUMERIC_CONV = 2;

protected:
  static int aggregate_collations(common::ObObjMeta& type, const common::ObObjMeta* types_array, int64_t param_num,
      uint32_t flag, const common::ObCollationType conn_coll_type);

  static int aggregate_charsets(common::ObObjMeta& type, const common::ObObjMeta* types, int64_t param_num,
      uint32_t flags, const common::ObCollationType conn_coll_type);

  static int aggregate_charsets(common::ObObjMeta& type, const ObExprResType* types, int64_t param_num, uint32_t flags,
      const common::ObCollationType conn_coll_type);

  // data members
  int32_t magic_;
  uint64_t id_;
  const ObExprOperatorType type_; /* type defined in sql/ob_item_type.h */
  const char* const name_;
  const int32_t param_num_; /* expected param number*/
  int32_t row_dimension_;   /* if row, it is the number of row items, else -1 */
  int32_t real_param_num_;  /* if param is ObRowType, param_num_ mean num of row  */
  // is operand auto cast enabled, no need to serialize.
  bool operand_auto_cast_;
  // is param lazy evaluate supported, no need to serialize.
  bool param_lazy_eval_;
  ObExprResType result_type_;
  // If you want to determine the meta information of each operand
  // in the resolve phase (such as target type, zerofill, length, etc.),
  // then you need to add @input_types_ and record the target type of
  // the operand to it in the calc_result_type phase
  //
  // @details: In the calc_result_type stage, you can know all the information
  // of the operator, especially the type of each operand to be converted
  // during the operation. In the postfix expression calculation phase,
  // the calculation framework will convert ObObj according to the
  // type specified in @input_types_, and then call calc_result.
  //
  // No longer need to pay attention to the type of operand in calc_result, just assert the type.
  //
  typedef common::ObFixedArray<ObFuncInputType, common::ObIAllocator> ObExprOperatorInputTypeArray;
  ObExprOperatorInputTypeArray input_types_;
  // Indicates whether to perform automatic character set conversion, used in type inference, without serialization
  bool need_charset_convert_;
  ObRawExpr* raw_expr_;
  bool is_called_in_sql_;
  int64_t extra_serialize_;
};

class ObSQLSessionInfo;
const common::ObTimeZoneInfo* get_timezone_info(const ObSQLSessionInfo* session);
const common::ObObjPrintParams get_obj_print_params(const ObSQLSessionInfo* session);
const common::ObString* get_nls_formats(const ObSQLSessionInfo* session);
class ObPhysicalPlanCtx;
int64_t get_cur_time(ObPhysicalPlanCtx* phy_plan_ctx);
int get_tz_offset(const common::ObTimeZoneInfo* tz_info, int64_t& offset);

inline ObExprOperator::ObExprOperator(
    common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t row_dimension)
    : magic_(0),
      id_(common::OB_INVALID_ID),
      type_(type),
      name_(name),
      param_num_(param_num),
      row_dimension_(row_dimension),
      real_param_num_(param_num),
      operand_auto_cast_(true),
      param_lazy_eval_(false),
      result_type_(alloc),
      input_types_(alloc),
      need_charset_convert_(true),
      raw_expr_(NULL),
      is_called_in_sql_(true),
      extra_serialize_(0)
{}

inline int ObExprOperator::calc_result_type0(
    ObExprResType& type, common::ObExprTypeCtx& type_ctx, common::ObIArray<common::ObObj*>& arg_arrs) const
{
  UNUSED(type);
  UNUSED(type_ctx);
  UNUSED(arg_arrs);
  SQL_LOG(WARN, "not implement");
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx,
    common::ObIArray<common::ObObj*>& arg_arrs) const
{
  UNUSED(type);
  UNUSED(type1);
  UNUSED(type_ctx);
  UNUSED(arg_arrs);
  SQL_LOG(WARN, "not implement");
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_type2(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    common::ObExprTypeCtx& type_ctx, common::ObIArray<common::ObObj*>& arg_arrs) const
{
  UNUSED(type);
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(type_ctx);
  UNUSED(arg_arrs);
  SQL_LOG(WARN, "not implement");
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    ObExprResType& type3, common::ObExprTypeCtx& type_ctx, common::ObIArray<common::ObObj*>& arg_arrs) const
{
  UNUSED(type);
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(type3);
  UNUSED(type_ctx);
  UNUSED(arg_arrs);
  SQL_LOG(WARN, "not implement");
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_typeN(ObExprResType& type, ObExprResType* types, int64_t param_num,
    common::ObExprTypeCtx& type_ctx, common::ObIArray<common::ObObj*>& arg_arrs) const
{
  UNUSED(type);
  UNUSED(types);
  UNUSED(param_num);
  UNUSED(type_ctx);
  UNUSED(arg_arrs);
  SQL_LOG(ERROR, "not implement", K(type_), K(get_type_name(type_)));
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_type0(ObExprResType& type, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type);
  UNUSED(type_ctx);
  SQL_LOG(WARN, "not implement");
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_type1(
    ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type);
  UNUSED(type1);
  UNUSED(type_ctx);
  SQL_LOG(WARN, "not implement");
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type);
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(type_ctx);
  SQL_LOG(WARN, "not implement");
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2,
    ObExprResType& type3, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type);
  UNUSED(type1);
  UNUSED(type2);
  UNUSED(type3);
  UNUSED(type_ctx);
  SQL_LOG(WARN, "not implement");
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type);
  UNUSED(types);
  UNUSED(param_num);
  UNUSED(type_ctx);
  SQL_LOG(ERROR, "not implement", K(type_), K(get_type_name(type_)));
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result0(common::ObObj& result, common::ObExprCtx& expr_ctx) const
{
  UNUSED(result);
  UNUSED(expr_ctx);
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result1(
    common::ObObj& result, const common::ObObj& obj, common::ObExprCtx& expr_ctx) const
{
  UNUSED(result);
  UNUSED(obj);
  UNUSED(expr_ctx);
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result2(
    common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const
{
  UNUSED(result);
  UNUSED(obj1);
  UNUSED(obj2);
  UNUSED(expr_ctx);
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_result3(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
    const common::ObObj& obj3, common::ObExprCtx& expr_ctx) const
{
  UNUSED(result);
  UNUSED(obj1);
  UNUSED(obj2);
  UNUSED(obj3);
  UNUSED(expr_ctx);
  return common::OB_NOT_IMPLEMENT;
}

inline int ObExprOperator::calc_resultN(
    common::ObObj& result, const common::ObObj* objs, int64_t param_num, common::ObExprCtx& expr_ctx) const
{
  UNUSED(result);
  UNUSED(objs);
  UNUSED(param_num);
  UNUSED(expr_ctx);
  return common::OB_NOT_IMPLEMENT;
}

inline void ObExprOperator::reset()
{
  common::ObDLinkBase<ObExprOperator>::reset();
  row_dimension_ = NOT_ROW_DIMENSION;
  result_type_.set_type(common::ObMaxType);
  result_type_.reset();
  input_types_.reset();
  is_called_in_sql_ = true;
}

// may be a new mod can be defined for it
inline bool ObExprOperator::is_type_valid(const common::ObObjType& type)
{
  common::ObObjTypeClass type_class = ob_obj_type_class(type);
  return (common::ob_is_castable_type_class(type_class) || common::ObUnknownTC == type_class);
}

inline void ObExprOperator::calc_result_flag1(ObExprResType& type, const ObExprResType& type1)
{
  if (type1.has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
    type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  }
}

inline void ObExprOperator::calc_result_flag2(
    ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2)
{
  if (type1.has_result_flag(OB_MYSQL_NOT_NULL_FLAG) && type2.has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
    type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  }
}

inline void ObExprOperator::calc_result_flag3(
    ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2, const ObExprResType& type3)
{
  if (type1.has_result_flag(OB_MYSQL_NOT_NULL_FLAG) && type2.has_result_flag(OB_MYSQL_NOT_NULL_FLAG) &&
      type3.has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
    type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
  }
}

inline void ObExprOperator::calc_result_flagN(ObExprResType& type, const ObExprResType* types, int64_t param_num)
{

  bool not_null = true;
  if (OB_ISNULL(types) || OB_UNLIKELY(param_num < 0)) {
    SQL_LOG(ERROR, "null types or the wrong param_num");
  } else {
    for (int64_t i = 0; i < param_num; ++i) {
      if (!types[i].has_result_flag(OB_MYSQL_NOT_NULL_FLAG)) {
        not_null = false;
        break;
      }
    }
    if (not_null) {
      type.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
    }
  }
}

////////////////////////////////////////////////////////////////
class ObFuncExprOperator : public ObExprOperator {
public:
  ObFuncExprOperator(
      common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
      : ObExprOperator(alloc, type, name, param_num, dimension){};

  virtual ~ObFuncExprOperator(){};
};

//
// In ObRelationalExprOperator, there are three concepts: res_type, cmp_type, calc_type
// The first two are relative to the expression; the latter one is relative to the parameters of the expression.
//
class ObRelationalExprOperator : public ObExprOperator {
public:
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) override;

public:
  ObRelationalExprOperator(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num,
      int32_t dimension = NOT_ROW_DIMENSION)
      : ObExprOperator(alloc, type, name, param_num, dimension), cmp_op_func2_(NULL)
  {}

  virtual ~ObRelationalExprOperator()
  {}
  /**
   * general compare.
   * use this func if you are NOT SURE the compare need cast or not.
   * @param[out] result: true / false / -1 / 0 / 1 / null.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cmp_ctx
   * @param[in] cast_ctx
   * @param[in] cmp_op: CO_EQ / CO_LE / CO_LT / CO_GE / CO_GT / CO_NE / CO_CMP.
   * @return ob error code.
   */
  static int compare(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObCompareCtx& cmp_ctx, common::ObCastCtx& cast_ctx, common::ObCmpOp cmp_op);
  /**
   * compare with no cast.
   * use this func if you are SURE the compare NEED NOT cast.
   * @param[out] result: true / false / -1 / 0 / 1 / null.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cmp_ctx
   * @param[in] cmp_op: CO_EQ / CO_LE / CO_LT / CO_GE / CO_GT / CO_NE / CO_CMP.
   * @param[out] need_cast: set to true if can't compare without cast, otherwise false.
   * @return ob error code.
   */
  OB_INLINE static int compare_nocast(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObCompareCtx& cmp_ctx, common::ObCmpOp cmp_op, bool& need_cast)
  {
    return common::ObObjCmpFuncs::compare(result, obj1, obj2, cmp_ctx, cmp_op, need_cast);
  }

  /**
   * compare with cast.
   * use this func if you are SURE the compare always NEED cast.
   * like ObExprBetween / ObExprNotBetween / ObExprField / ObExprStrcmp / ObExprOracleDecode.
   * @param[out] result: true / false / -1 / 0 / 1 / null.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cmp_ctx
   * @param[in] cast_ctx
   * @param[in] cmp_op: CO_EQ / CO_LE / CO_LT / CO_GE / CO_GT / CO_NE / CO_CMP.
   * @return ob error code.
   */
  static int compare_cast(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObCompareCtx& cmp_ctx, common::ObCastCtx& cast_ctx, common::ObCmpOp cmp_op);
  /**
   * general compare, null safe.
   * use this func if you are NOT SURE the compare need cast or not.
   * @param[out] result: -1 / 0 / 1.
   * @param[in] obj1
   * @param[in] obj2
   * @param[in] cast_ctx
   * @param[in] cmp_type
   * @param[in] cmp_cs_type
   * @return ob error code.
   */
  static int compare_nullsafe(int64_t& result, const common::ObObj& obj1, const common::ObObj& obj2,
      common::ObCastCtx& cast_ctx, common::ObObjType cmp_type, common::ObCollationType cmp_cs_type);
  // determine the type used for comparison of the two types
  // binary comparison

  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const override;

  // deduce binary comparison result type and parameters types
  static int deduce_cmp_type(const ObExprOperator& expr, ObExprResType& type, ObExprResType& type1,
      ObExprResType& type2, common::ObExprTypeCtx& type_ctx);

  // for between...and, not between...and etc.
  // @todo need refactor, ....yzf....Thu,  6 Aug 2015....16:00....
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const override;
  virtual int calc_calc_type3(ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx, const common::ObObjType cmp_type) const;
  int get_cmp_result_type3(ObExprResType& type, bool& need_no_cast, const ObExprResType* types, const int64_t param_num,
      const sql::ObSQLSessionInfo& my_session);

  // vector comparison, e.g. (a,b,c) > (1,2,3)
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const override;
  virtual int calc_result2(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      common::ObExprCtx& expr_ctx, bool is_null_safe, common::ObCmpOp cmp_op) const;
  virtual int calc_resultN(common::ObObj& result, const common::ObObj* objs_array, int64_t param_num,
      common::ObExprCtx& expr_ctx, bool is_null_safe, common::ObCmpOp cmp_op) const;

  static int is_equivalent(
      const common::ObObjMeta& meta1, const common::ObObjMeta& meta2, const common::ObObjMeta& meta3, bool& result);
  int assign(const ObExprOperator& other) override;
  int set_cmp_func(const common::ObObjType type1, const common::ObObjType type2);
  common::obj_cmp_func get_cmp_fun() const
  {
    return cmp_op_func2_;
  }

  // pure virtual but implemented, derived classes can use this implement.
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override = 0;

  static int cg_row_cmp_expr(const int row_dim, common::ObIAllocator& allocator, const ObRawExpr& raw_expr,
      const ObExprOperatorInputTypeArray& input_types, ObExpr& rt_expr);
  static int cg_datum_cmp_expr(
      const ObRawExpr& raw_expr, const ObExprOperatorInputTypeArray& input_types, ObExpr& rt_expr);

  static int is_row_cmp(const ObRawExpr&, int& row_dim);
  static int row_eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datm);

  // row compare
  // CAUTION: null safe equal row compare is not included.
  static int row_cmp(
      const ObExpr& expr, ObDatum& expr_datum, ObExpr** l_row, ObEvalCtx& l_ctx, ObExpr** r_row, ObEvalCtx& r_ctx);

  OB_INLINE static int get_comparator_operands(const ObExpr& expr, ObEvalCtx& ctx, common::ObDatum*& left,
      common::ObDatum*& right, ObDatum& result, bool& is_finish)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
      SQL_LOG(WARN, "left eval failed", K(ret));
    } else if (left->is_null()) {
      result.set_null();
      is_finish = true;
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, right))) {
      SQL_LOG(WARN, "right eval failed", K(ret));
    } else if (right->is_null()) {
      result.set_null();
      is_finish = true;
    } else { /* do nothing */
    }
    return ret;
  }
  static int get_equal_meta(common::ObObjMeta& meta, const common::ObObjMeta& meta1, const common::ObObjMeta& meta2);

  OB_INLINE static bool can_cmp_without_cast(
      ObExprResType type1, ObExprResType type2, common::ObCmpOp cmp_op, common::obj_cmp_func& cmp_func)
  {
    bool need_no_cast = false;
    // Special processing shows that compare is called (for example: c1> c2),
    // at this time enum/set should be converted to string processing.
    // Internal comparison (order by), enum/set does not need to be converted.
    if (GCONF.enable_static_engine_for_query()) {
      if (common::ObDatumFuncs::is_string_type(type1.get_type()) &&
          common::ObDatumFuncs::is_string_type(type2.get_type())) {
        need_no_cast = common::ObCharset::charset_type_by_coll(type1.get_collation_type()) ==
                       common::ObCharset::charset_type_by_coll(type2.get_collation_type());
      } else {
        auto func_ptr = ObExprCmpFuncsHelper::get_eval_expr_cmp_func(
            type1.get_type(), type2.get_type(), cmp_op, lib::is_oracle_mode(), common::CS_TYPE_MAX);
        need_no_cast = (func_ptr != nullptr);
      }
    } else if (common::ob_is_enum_or_set_type(type1.get_type())

               && common::ob_is_enum_or_set_type(type2.get_type())) {
      need_no_cast = false;
    } else {
      need_no_cast = common::ObObjCmpFuncs::can_cmp_without_cast(type1, type2, cmp_op, cmp_func);
    }
    return need_no_cast;
  }

protected:
  static bool is_int_cmp_const_str(const ObExprResType* type1, const ObExprResType* type2, common::ObObjType& cmp_type);
  OB_INLINE static common::ObCmpOp get_cmp_op(const ObExprOperatorType type)
  {
    /*
     * maybe we can use associative array(table lookup) to get a better
     * performance here. Yeah, just maybe, not absolutely. If you are free, you
     * can have a profiling.
     */
    common::ObCmpOp cmp_op = common::CO_MAX;
    switch (type) {
      case T_FUN_SYS_NULLIF:  // nullif(e1, e2) check if e1 = e2
      case T_OP_EQ:           // fall through
      case T_OP_NSEQ:         // fall through
      case T_OP_SQ_EQ:        // fall through
      case T_OP_SQ_NSEQ: {
        cmp_op = common::CO_EQ;
        break;
      }
      case T_OP_BTW:  // a between b and c <==> b <= a and a <= c
                      // fall through
      case T_OP_LE:   // fall through
      case T_OP_SQ_LE: {
        cmp_op = common::CO_LE;
        break;
      }
      case T_OP_NOT_BTW:  // a not between b and c <==> a < b or c < a
                          // fall through
      case T_OP_LT:       // fall through
      case T_OP_SQ_LT: {
        cmp_op = common::CO_LT;
        break;
      }
      case T_OP_GE:  // fall through
      case T_OP_SQ_GE: {
        cmp_op = common::CO_GE;
        break;
      }
      case T_OP_GT:  // fall through
      case T_OP_SQ_GT: {
        cmp_op = common::CO_GT;
        break;
      }
      case T_OP_NE:  // fall through
      case T_OP_SQ_NE: {
        cmp_op = common::CO_NE;
        break;
      }
      case T_FUN_SYS_STRCMP: {
        cmp_op = common::CO_CMP;
      }
      default: {
        // do nothing
        break;
      }
    }
    return cmp_op;
  }

  OB_INLINE static bool is_expected_cmp_ret(const common::ObCmpOp cmp_op, const int cmp_ret)
  {
    bool ret_bool = false;
    switch (cmp_ret) {
      case 0: {
        ret_bool = (cmp_op == common::CO_EQ || cmp_op == common::CO_GE || cmp_op == common::CO_LE);
      } break;
      case 1: {
        ret_bool = (cmp_op == common::CO_GT || cmp_op == common::CO_GE || cmp_op == common::CO_NE);
      } break;
      case -1: {
        ret_bool = (cmp_op == common::CO_LT || cmp_op == common::CO_LE || cmp_op == common::CO_NE);
      } break;
      default: {
        ret_bool = false;
      }
    }
    return ret_bool;
  }

  /**
   * fast path. cmp_func should not be NULL.
   */
  OB_INLINE static int compare_nocast(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObCompareCtx& cmp_ctx, common::ObCmpOp cmp_op, const common::obj_cmp_func cmp_func)
  {
    return common::ObObjCmpFuncs::compare(result, obj1, obj2, cmp_ctx, cmp_op, cmp_func);
  }

  static bool can_cmp_without_cast(
      ObExprResType type1, ObExprResType type2, common::ObCmpOp cmp_op, const ObSQLSessionInfo& session);

protected:
  // only use for comparison with 2 operands(calc_result2)
  // if cmp_op_func2_ is not NULL, that means we can compare the 2 objs directly without any casts
  // otherwise, compare_cast is necessary.
  // It is used for performance optimization.
  common::obj_cmp_func cmp_op_func2_;
};

class ObSubQueryRelationalExpr : public ObExprOperator {
  OB_UNIS_VERSION(1);

public:
  // extra info stored in ObExpr::extra_
  struct ExtraInfo : public ObExprExtraInfoAccess<ExtraInfo> {
    ObSubQueryKey subquery_key_;
    // compare operator's left operator is row_iterator
    bool left_is_iter_;
    // compare operator's right operator is row_iterator
    bool right_is_iter_;

    TO_STRING_KV(K(subquery_key_), K(left_is_iter_), K(right_is_iter_));
  } __attribute__((packed));
  static_assert(sizeof(ExtraInfo) <= sizeof(uint64_t), "too big extra info");

  ObSubQueryRelationalExpr(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num,
      int32_t dimension = NOT_ROW_DIMENSION)
      : ObExprOperator(alloc, type, name, param_num, dimension),
        subquery_key_(T_WITH_NONE),
        left_is_iter_(false),
        right_is_iter_(false)
  {}
  virtual ~ObSubQueryRelationalExpr()
  {}

  virtual int assign(const ObExprOperator& other);

  void set_subquery_key(ObSubQueryKey key)
  {
    subquery_key_ = key;
  }
  void set_left_is_iter(bool is_iter)
  {
    left_is_iter_ = is_iter;
  }
  void set_right_is_iter(bool is_iter)
  {
    right_is_iter_ = is_iter;
  }
  virtual void reset() override
  {
    subquery_key_ = T_WITH_NONE;
    left_is_iter_ = false;
    right_is_iter_ = false;
  }
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const override;
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const override;
  int calc_result2(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      common::ObExprCtx& expr_ctx) const override;
  int calc_resultN(common::ObObj& result, const common::ObObj* param_array, int64_t param_num,
      common::ObExprCtx& expr_ctx) const override;
  virtual int call(common::ObObj* stack, int64_t& stack_size, common::ObExprCtx& expr_ctx) const override;
  virtual int eval(
      common::ObExprCtx& expr_ctx, common::ObObj& val, common::ObObj* params, int64_t param_num) const override;

  static int subquery_cmp_eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

  VIRTUAL_TO_STRING_KV(N_EXPR_TYPE, get_type_name(type_), N_REAL_PARAM_NUM, real_param_num_, N_RESULT_TYPE,
      result_type_, K_(subquery_key), K_(left_is_iter), K_(right_is_iter));

protected:
  // The result of processing the subquery is a vector. In this case, the result
  // of the subquery has at most one row of data, and multiple rows of data are not allowed.
  // According to the characteristics of the vector, the result of the subquery
  // cannot be a single column of data. The result of a single row and single
  // column subquery is a scalar.
  int calc_result_with_none(
      common::ObObj& result, const common::ObNewRow& left_row, int64_t subquery_idx, common::ObExprCtx& expr_ctx) const;
  // In this case, the result of the subquery is a set, the comparison result
  // of all elements in the set is true, and the result of the entire expression is true
  // Otherwise, expression's result is false
  int calc_result_with_all(
      common::ObObj& result, const common::ObNewRow& left_row, int64_t subquery_idx, common::ObExprCtx& expr_ctx) const;
  // In this case, the result of the subquery is a collection,
  // and the comparison result of at least one element in the collection is true,
  // and the result of the entire expression is TRUE
  // Otherwise expression's result is false
  int calc_result_with_any(
      common::ObObj& result, const common::ObNewRow& left_row, int64_t subquery_idx, common::ObExprCtx& expr_ctx) const;
  virtual int compare_single_row(const common::ObNewRow& left_row, const common::ObNewRow& right_row,
      common::ObExprCtx& expr_ctx, common::ObObj& result) const;
  int compare_obj(common::ObExprCtx& expr_ctx, const common::ObObj& obj1, const common::ObObj& obj2,
      const ObExprCalcType& cmp_type, bool is_null_safe, common::ObObj& result) const;
  virtual int calc_result_type2_(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;

  // pure virtual but implemented, derived classes can use this implement.
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override = 0;

  int get_param_types(const ObRawExpr& param, const bool is_iter, common::ObIArray<common::ObObjMeta>& types) const;

  static int setup_row(ObExpr** expr, ObEvalCtx& ctx, const bool is_iter, const int64_t cmp_func_cnt,
      ObSubQueryIterator*& iter, ObExpr**& row);

  static int subquery_cmp_eval_with_none(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res, ObExpr** l_row, ObExpr** r_row, ObSubQueryIterator* r_iter);
  static int subquery_cmp_eval_with_any(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res, ObExpr** l_row, ObExpr** r_row, ObSubQueryIterator* r_iter);
  static int subquery_cmp_eval_with_all(
      const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res, ObExpr** l_row, ObExpr** r_row, ObSubQueryIterator* r_iter);

  static int cmp_one_row(
      const ObExpr& expr, ObDatum& res, ObExpr** l_row, ObEvalCtx& l_ctx, ObExpr** r_row, ObEvalCtx& r_ctx);

  static int check_exists(const ObExpr& expr, ObEvalCtx& ctx, bool& exists);

protected:
  ObSubQueryKey subquery_key_;
  bool left_is_iter_;
  bool right_is_iter_;
};

typedef int (*ObResultTypeFunc)(ObExprResType& type, const ObExprResType& type1, const ObExprResType& type2);
typedef int (*ObCalcTypeFunc)(common::ObObjType& calc_type, common::ObObjType& calc_ob1_type,
    common::ObObjType& calc_ob2_type, const common::ObObjType type1, const common::ObObjType type2);
typedef int (*ObArithFunc)(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
    common::ObIAllocator* allocator, common::ObScale scale);

class ObArithExprOperator : public ObExprOperator {
public:
  ObArithExprOperator(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num,
      int32_t dimension, ObResultTypeFunc result_type_func, ObCalcTypeFunc calc_type_func,
      const ObArithFunc* arith_funcs)
      : ObExprOperator(alloc, type, name, param_num, dimension),
        result_type_func_(result_type_func),
        calc_type_func_(calc_type_func),
        arith_funcs_(arith_funcs){};

  virtual ~ObArithExprOperator(){};

  virtual int assign(const ObExprOperator& other);
  OB_INLINE static bool is_float_out_of_range(float res)
  {
    return (0 != isinff(res));
  }
  OB_INLINE static bool is_double_out_of_range(double res)
  {
    return (0 != ::isinf(res));
  }

protected:
  // temporary used, remove after all expr converted
  OB_INLINE static int get_arith_operand(const ObExpr& expr, ObEvalCtx& ctx, common::ObDatum*& left,
      common::ObDatum*& right, ObDatum& result, bool& is_finish)
  {
    int ret = common::OB_SUCCESS;
    is_finish = false;
    if (lib::is_oracle_mode()) {
      if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
        SQL_LOG(WARN, "left eval failed", K(ret));
      } else if (left->is_null()) {
        result.set_null();
        is_finish = true;
      } else if (OB_FAIL(expr.args_[1]->eval(ctx, right))) {
        SQL_LOG(WARN, "right eval failed", K(ret));
      } else if (right->is_null()) {
        result.set_null();
        is_finish = true;
      }
    } else {
      if (OB_FAIL(expr.args_[0]->eval(ctx, left))) {
        SQL_LOG(WARN, "left eval failed", K(ret));
      } else if (OB_FAIL(expr.args_[1]->eval(ctx, right))) {
        SQL_LOG(WARN, "right eval failed", K(ret));
      } else if (left->is_null() || right->is_null()) {
        result.set_null();
        is_finish = true;
      }
    }
    SQL_LOG(DEBUG, "finish get_arith_operand", KPC(expr.args_[0]), KPC(expr.args_[1]), K(result), K(is_finish));
    return ret;
  }

  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& left, const common::ObObj& right, common::ObExprCtx& expr_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj& left, const common::ObObj& right,
      common::ObIAllocator* allocator, common::ObScale scale, ObCalcTypeFunc calc_type_func,
      const ObArithFunc* arith_funcs);
  static int calc(common::ObObj& result, const common::ObObj& left, const common::ObObj& right,
      common::ObExprCtx& expr_ctx, common::ObScale calc_scale, ObCalcTypeFunc calc_type_func,
      const ObArithFunc* arith_funcs);

  ObResultTypeFunc result_type_func_;
  ObCalcTypeFunc calc_type_func_;
  const ObArithFunc* arith_funcs_;

protected:
  static int calc_(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObExprCtx& expr_ctx, common::ObScale calc_scale, common::ObObjType calc_type,
      const ObArithFunc* arith_func);
  static bool is_datetime_add_minus_calc(const common::ObObjType calc_type, const ObArithFunc* arith_funcs);
  static int interval_add_minus(common::ObObj& res, const common::ObObj& left, const common::ObObj& right,
      common::ObExprCtx& expr_ctx, common::ObScale scale, bool is_minus = false);
};

class ObVectorExprOperator : public ObExprOperator {
public:
  ObVectorExprOperator(
      common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
      : ObExprOperator(alloc, type, name, param_num, dimension)
  {}
  virtual ~ObVectorExprOperator()
  {}

  virtual int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

private:
  int calc_result_type2_(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
};

class ObLogicalExprOperator : public ObExprOperator {
public:
  ObLogicalExprOperator(
      common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
      : ObExprOperator(alloc, type, name, param_num, dimension)
  {}

  virtual ~ObLogicalExprOperator()
  {}
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

protected:
  static int is_true(const common::ObObj& obj, common::ObCastMode cast_mode, bool& result);
};

//
// Const ObIArray interface of c array, call the mutable method will got run time error.
// It used to adapt interface which need the ObIArray arguments. e.g:
//
//    int foo(const ObIArray<int> &);
//    int foo(int v) { return foo(make_const_carray(v); }
//
template <int64_t N, typename T>
class ObConstCArray : public common::ObIArray<T> {
public:
  using common::ObIArray<T>::at;
  using common::ObIArray<T>::count;

  template <typename... TS>
  ObConstCArray(const TS&... args) : common::ObIArray<T>(local_data_, N), local_data_{args...}
  {
    static_assert(N > 0 && N == sizeof...(TS), "wrong argument count");
  }

  virtual int push_back(const T&) override
  {
    return common::OB_NOT_SUPPORTED;
  }
  virtual void pop_back() override
  {}
  virtual int pop_back(T&) override
  {
    return common::OB_NOT_SUPPORTED;
  }
  virtual int remove(int64_t) override
  {
    return common::OB_NOT_SUPPORTED;
  }

  virtual int at(int64_t idx, T& obj) const override
  {
    int ret = common::OB_SUCCESS;
    if (idx >= 0 && idx < count()) {
      obj = at(idx);
    } else {
      ret = common::OB_INDEX_OUT_OF_RANGE;
    }
    return ret;
  }

  virtual void reset() override
  {}
  virtual void reuse() override
  {}
  virtual void destroy() override
  {}
  virtual int reserve(int64_t) override
  {
    return common::OB_NOT_SUPPORTED;
  }
  virtual int assign(const common::ObIArray<T>&) override
  {
    return common::OB_NOT_SUPPORTED;
  }
  virtual int prepare_allocate(int64_t) override
  {
    return common::OB_NOT_SUPPORTED;
  }
  virtual void extra_access_check() const override
  {}

protected:
  T local_data_[N];
  using common::ObIArray<T>::data_;
  using common::ObIArray<T>::count_;
};

template <typename T, typename... TS>
const ObConstCArray<1 + sizeof...(TS), T> make_const_carray(const T& t, const TS&... args)
{
  return ObConstCArray<1 + sizeof...(TS), T>(t, args...);
}

// functions who's result type is string
class ObStringExprOperator : public ObExprOperator {
public:
  ObStringExprOperator(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num)
      : ObExprOperator(alloc, type, name, param_num, NOT_ROW_DIMENSION)
  {}
  virtual ~ObStringExprOperator()
  {}
  static int convert_result_collation(
      const ObExprResType& result_type, common::ObObj& result, common::ObIAllocator* allocator);

protected:
  common::ObObjType get_result_type_mysql(int64_t char_length) const;
  static const int64_t MAX_CHAR_LENGTH_FOR_VARCAHR_RESULT = 512;
  static const int64_t MAX_CHAR_LENGTH_FOR_TEXT_RESULT = 65535;

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObStringExprOperator);
  // function members
private:
  // data members
};

class ObBitwiseExprOperator : public ObExprOperator {
public:
  ObBitwiseExprOperator(
      common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
      : ObExprOperator(alloc, type, name, param_num, dimension)
  {}
  virtual ~ObBitwiseExprOperator()
  {}
  // for static_typing_engine
  static int set_calc_type(ObExprResType& type);
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  // for static engine
  // retrive int64/uint64 from datum param, and do bit operations according to extra_ fields
  // the difference between mysql mode and oracle mode is:
  //  mysql mode use get_uint64
  //  oracle mode use get_int64() and will calculate param's value even if former param is null
  static int calc_result2_oracle(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
  static int calc_result2_mysql(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);

protected:
  enum BitOperator {
    BIT_AND,
    BIT_OR,
    BIT_XOR,
    BIT_LEFT_SHIFT,
    BIT_RIGHT_SHIFT,
    BIT_NEG,
    BIT_COUNT,
    BIT_MAX,
  };
  int calc_(common::ObObj& res, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx,
      BitOperator op) const;
  static int get_uint64(const common::ObObj& obj, common::ObExprCtx& expr_ctx, bool is_round, uint64_t& out);
  static int get_int64(const common::ObObj& obj, common::ObExprCtx& expr_ctx, bool is_round, int64_t& out);
  // initailize eval_func_, inner_functions_, extra_ fields
  static int cg_bitwise_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr, const BitOperator op);
  // according to param's type, from 4 get_int/get_uint method
  // to choose a suitable get_int64/get_uint64 function
  static int choose_get_int_func(const common::ObObjType type, void*& out_func);
  // get int64/uint64 from datum. For numbser should do some round/trunc operations;
  // For int tc, it will directly retrive int value
  typedef int (*GetIntFunc)(const common::ObDatum&, bool, int64_t&, common::ObCastMode&);
  typedef int (*GetUIntFunc)(const common::ObDatum&, bool, uint64_t&, common::ObCastMode&);
  static int get_int64_from_int_tc(
      const common::ObDatum& datum, bool is_round, int64_t& out, const common::ObCastMode& cast_mode);
  static int get_uint64_from_int_tc(
      const common::ObDatum& datum, bool is_round, uint64_t& out, const common::ObCastMode& cast_mode);
  static int get_int64_from_number_type(
      const common::ObDatum& datum, bool is_round, int64_t& out, const common::ObCastMode& cast_mode);
  static int get_uint64_from_number_type(
      const common::ObDatum& datum, bool is_round, uint64_t& out, const common::ObCastMode& cast_mode);
};

class ObMinMaxExprOperator : public ObExprOperator {
public:
  // constructor and destructor
  ObMinMaxExprOperator(
      common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
      : ObExprOperator(alloc, type, name, param_num, dimension), need_cast_(true)
  {}

  virtual ~ObMinMaxExprOperator()
  {}
  virtual int assign(const ObExprOperator& other);

public:
  // serialize and deserialize
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
  virtual int64_t get_serialize_size() const;

public:
  OB_INLINE void set_need_cast(bool need_cast)
  {
    need_cast_ = need_cast;
  }

protected:
  /*
     Aggregate  result type for comparison
     is involved by greatest, least
     */
  int aggregate_result_type_for_comparison(
      ObExprResType& type, const ObExprResType* types_stack, int64_t param_num) const;

  /*
     Aggregate cmp type for comparison
     is involved by greatest, least
     */
  int aggregate_cmp_type_for_comparison(ObExprResType& type, const ObExprResType* types, int64_t param_num) const;

protected:
  // calculate greatest/least's result type
  int calc_result_meta_for_comparison(ObExprResType& type, ObExprResType* types, int64_t param_num,
      const common::ObCollationType coll_type, const common::ObLengthSemantics default_length_semantics) const;

protected:
  // least should set cmp_op to CO_LT.
  // greatest should set cmp_op to CO_GT.
  static int calc_(common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num,
      const ObExprResType& result_type, common::ObExprCtx& expr_ctx, common::ObCmpOp cmp_op, bool need_cast);
  OB_INLINE static int calc_without_cast(common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num,
      const ObExprResType& result_type, common::ObExprCtx& expr_ctx, common::ObCmpOp cmp_op);
  OB_INLINE static int calc_with_cast(common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num,
      const ObExprResType& result_type, common::ObExprCtx& expr_ctx, common::ObCmpOp cmp_op);

protected:
  // if all params are of same types
  // or if all params are numeric
  // need_no_cast_ will be ture and no casts are necessary during calculation
  bool need_cast_;
};

////////////////////////////////////////////////////////////////
// locate instr position
class ObLocationExprOperator : public ObFuncExprOperator {
public:
  ObLocationExprOperator(
      common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num, int32_t dimension)
      : ObFuncExprOperator(alloc, type, name, param_num, dimension){};

  virtual ~ObLocationExprOperator(){};
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;
  virtual int calc_result3(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      const common::ObObj& obj3, common::ObExprCtx& expr_ctx) const;
  // for sql engine 3.0
  static int calc_(
      const ObExpr& expr, const ObExpr& sub_arg, const ObExpr& ori_arg, ObEvalCtx& ctx, ObDatum& res_datum);
  static int calc_location_expr(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& res_datum);
  static int get_calc_cs_type(const ObExpr& expr, common::ObCollationType& calc_cs_type);
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const;

private:
  OB_INLINE static int get_pos_int64(const common::ObObj& obj, common::ObExprCtx& expr_ctx, int64_t& out);
};

class ObExprTRDateFormat {
public:
  // http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions230.htm#i1002084
  // http://www.techonthenet.com/oracle/functions/trunc_date.php
  enum FORMAT_ID {
    SYYYY = 0,
    YYYY = 1,
    YEAR = 2,
    SYEAR = 3,
    YYY = 4,
    YY = 5,
    Y = 6,
    IYYY = 7,
    IY = 8,
    I = 9,
    Q = 10,
    MONTH = 11,
    MON = 12,
    MM = 13,
    RM = 14,
    WW = 15,
    IW = 16,
    W = 17,
    DDD = 18,
    DD = 19,
    J = 20,
    DAY = 21,
    DY = 22,
    D = 23,
    HH = 24,
    HH12 = 25,
    HH24 = 26,
    MI = 27,
    CC = 28,
    SCC = 29,
    FORMAT_MAX_TYPE
  };

  static int init();
  static int calc_hash(const char* p, int64_t len, uint64_t& hash);
  static int trunc_new_obtime(common::ObTime& ob_time, const common::ObString& fmt);
  static int round_new_obtime(common::ObTime& ob_time, const common::ObString& fmt);
  inline static void get_format_id(const uint64_t fmt_hash, int64_t& fmt_id)
  {
    fmt_id = SYYYY;
    while (FORMATS_HASH[fmt_id] != fmt_hash && ++fmt_id < FORMAT_MAX_TYPE) {};
  }
  inline static void set_time_part_to_zero(common::ObTime& ob_time)
  {
    ob_time.parts_[DT_HOUR] = 0;
    ob_time.parts_[DT_MIN] = 0;
    ob_time.parts_[DT_SEC] = 0;
    ob_time.parts_[DT_USEC] = 0;
  }

public:
  static const char* FORMATS_TEXT[FORMAT_MAX_TYPE];
  static uint64_t FORMATS_HASH[FORMAT_MAX_TYPE];
};

}  // end namespace sql
}  // end namespace oceanbase

#define REGISTER_EXPR_OPERATOR(OP, OP_TYPE) \
  REGISTER_CREATOR(oceanbase::sql::ObExprOperatorGFactory, ObExprOperator, OP, OP_TYPE)

////////////////////////////////////////////////////////////////
// macros that use new cast function.

#define EXPR_DEFINE_CMP_CTX(calc_type, is_null_safe, expr_ctx) \
  ObCompareCtx cmp_ctx(                                        \
      calc_type.get_type(), calc_type.get_collation_type(), is_null_safe, expr_ctx.tz_offset_, default_null_pos())
#define EXPR_SET_CAST_CTX_MODE(expr_ctx) \
  ObSQLUtils::set_compatible_cast_mode((expr_ctx).my_session_, (expr_ctx).cast_mode_)
// external variables: expr_ctx.
#define EXPR_DEFINE_CAST_CTX(expr_ctx, cast_mode) EXPR_DEFINE_CAST_CTX_ZF(expr_ctx, cast_mode, NULL)

#define EXPR_DEFINE_CAST_CTX_ZF(expr_ctx, cast_mode, zf_info)                                                \
  ObCollationType cast_coll_type = CS_TYPE_INVALID;                                                          \
  ObCastMode cp_cast_mode_ = (expr_ctx).cast_mode_ | (cast_mode);                                            \
  if (NULL != (expr_ctx).my_session_) {                                                                      \
    if (lib::is_oracle_mode()) {                                                                             \
      if (common::OB_SUCCESS != (expr_ctx).my_session_->get_collation_server(cast_coll_type)) {              \
        SQL_LOG(ERROR, "fail to get server collation");                                                      \
        cast_coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());                 \
      }                                                                                                      \
    } else if (lib::is_mysql_mode()) {                                                                       \
      if (common::OB_SUCCESS != (expr_ctx).my_session_->get_collation_connection(cast_coll_type)) {          \
        SQL_LOG(ERROR,                                                                                       \
            "fail to get collation_connection, "                                                             \
            "set it to default collation");                                                                  \
        cast_coll_type = ObCharset::get_default_collation(ObCharset::get_default_charset());                 \
      } else {                                                                                               \
      }                                                                                                      \
    }                                                                                                        \
    if (common::OB_SUCCESS != ObSQLUtils::set_compatible_cast_mode((expr_ctx).my_session_, cp_cast_mode_)) { \
      SQL_LOG(ERROR, "fail to get compatible mode for cast_mode");                                           \
    }                                                                                                        \
  } else {                                                                                                   \
    SQL_LOG(WARN, "session is null");                                                                        \
    cast_coll_type = ObCharset::get_system_collation();                                                      \
  }                                                                                                          \
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params((expr_ctx).my_session_);     \
  ObCastCtx cast_ctx((expr_ctx).calc_buf_,                                                                   \
      &dtc_params,                                                                                           \
      get_cur_time((expr_ctx).phy_plan_ctx_),                                                                \
      cp_cast_mode_,                                                                                         \
      cast_coll_type,                                                                                        \
      (zf_info));

// external variables: ret, cast_ctx.
// can not use do ... while(0), because the buf obj will be freed.
#define EXPR_CAST_OBJ_V2(obj_type, obj, res_obj)                                                      \
  common::ObObj tmp_out_obj;                                                                          \
  if (OB_SUCC(ret) && OB_FAIL(ObObjCaster::to_type(obj_type, cast_ctx, obj, tmp_out_obj, res_obj))) { \
    SQL_LOG(WARN, "failed to cast object to " #obj_type, K(ret), K(obj), K(obj_type));                \
  }

// external variables: ret, cast_ctx.
#define EXPR_GET_VAL_V2(type, obj, val, func)                                                    \
  do {                                                                                           \
    const common::ObObj* res_obj = NULL;                                                         \
    EXPR_CAST_OBJ_V2(type, obj, res_obj);                                                        \
    if (OB_SUCC(ret)) {                                                                          \
      if (OB_ISNULL(res_obj)) {                                                                  \
        ret = OB_ERR_UNEXPECTED;                                                                 \
        SQL_LOG(WARN, "unexpected error. res_obj is null", K(ret), K(type), K(obj), K(type));    \
      } else if (OB_FAIL(res_obj->get_##func(val))) {                                            \
        SQL_LOG(WARN, "unexpected error. get val failed", K(ret), K(obj), K(type), K(*res_obj)); \
      }                                                                                          \
    }                                                                                            \
  } while (0)

#define EXPR_GET_TINYINT_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObTinyIntType, arg_obj, arg_val, tinyint)
#define EXPR_GET_INT32_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObInt32Type, arg_obj, arg_val, int32)
#define EXPR_GET_UINT32_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObUInt32Type, arg_obj, arg_val, uint32)
#define EXPR_GET_INT64_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObIntType, arg_obj, arg_val, int)
#define EXPR_GET_UINT64_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObUInt64Type, arg_obj, arg_val, uint64)
#define EXPR_GET_FLOAT_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObFloatType, arg_obj, arg_val, float)
#define EXPR_GET_DOUBLE_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObDoubleType, arg_obj, arg_val, double)
#define EXPR_GET_NUMBER_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObNumberType, arg_obj, arg_val, number)
#define EXPR_GET_DATETIME_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObDateTimeType, arg_obj, arg_val, datetime)
#define EXPR_GET_TIMESTAMP_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObTimestampType, arg_obj, arg_val, timestamp)
#define EXPR_GET_DATE_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObDateType, arg_obj, arg_val, date)
#define EXPR_GET_TIME_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObTimeType, arg_obj, arg_val, time)
#define EXPR_GET_VARCHAR_V2(arg_obj, arg_val) EXPR_GET_VAL_V2(ObVarcharType, arg_obj, arg_val, varchar)

#define TYPE_CHECK(obj_to_check, expected_type)                                                               \
  do {                                                                                                        \
    if (OB_UNLIKELY(obj_to_check.get_type() != expected_type)) {                                              \
      ret = OB_INVALID_ARGUMENT;                                                                              \
      LOG_WARN("invalid argument. unexpected obj type", K(obj_to_check), K(expected_type), K(common::lbt())); \
      return ret;                                                                                             \
    }                                                                                                         \
  } while (0)

#define GET_EXEC_ALLOCATOR(expr_ctx) \
  ((nullptr == expr_ctx.exec_ctx_) ? nullptr : &(expr_ctx.exec_ctx_->get_allocator()));

#endif  // OCEANBASE_SQL_OB_EXPR_OPERATOR_H_
