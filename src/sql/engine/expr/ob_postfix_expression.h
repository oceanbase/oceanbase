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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_POSTFIX_EXPRESSION_
#define OCEANBASE_SQL_ENGINE_EXPR_POSTFIX_EXPRESSION_

#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_fixed_array.h"
#include "share/ob_errno.h"
#include "common/ob_string_buf.h"
#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "share/ob_i_sql_expression.h"
#include "objit/common/ob_item_type.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "share/system_variable/ob_system_variable.h"
namespace oceanbase
{
namespace common
{
class ObRowkeyInfo;
}
namespace sql
{
class ObResultSet;
class ObExprOperator;

template<typename T>
class ObSqlFixedArray
{
public:
  ObSqlFixedArray():
    data_(NULL), cap_(0), count_(0) {}
  int init(int64_t cap, common::ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    void *buf = NULL;
    if (cap < 0 || cap > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(cap));
    } else if (0 == cap) {
      // do nothing
    } else if (OB_ISNULL(buf = allocator.alloc(sizeof(T) * cap))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret), K(cap), K(sizeof(T)));
    } else {
      data_ = static_cast<T *>(buf);
      for (int32_t i = 0; i < cap; i++) {
        new (data_ + i)T();
      }
      cap_ = static_cast<int32_t>(cap);
      count_ = 0;
    }
    return ret;
  }

  int push_back(const T& obj)
  {
    int ret = OB_SUCCESS;
    if (NULL == data_) {
      ret = OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "array not init", K(ret), K(data_));
    } else if (count_ >= cap_) {
      ret = OB_SIZE_OVERFLOW;
      SQL_ENG_LOG(WARN, "sql fixed array size overflow", K(ret), K(cap_), K(count_));
    } else if (OB_FAIL(copy_assign(data_[count_], obj))) {
      SQL_ENG_LOG(WARN, "failed to copy assign data", K(ret));
    } else {
      count_++;
    }
    return ret;
  }
  inline T& at(const int64_t idx)
  {
    OB_ASSERT(idx >= 0 && idx < cap_);
    return data_[idx];
  }
  inline const T& at(const int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < cap_);
    return data_[idx];
  }
  inline T& operator[](const int64_t idx) { return at(idx); }
  inline const T& operator[](const int64_t idx) const { return at(idx); }
  inline bool empty() const { return 0 == count_; }
  inline int64_t count() const { return static_cast<int64_t>(count_); }

  int reserve(const int64_t cap, common::ObIAllocator &allocator)
  {
    int ret = OB_SUCCESS;
    if (cap < 0 || cap > INT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(cap));
    } else if (NULL == data_) {
      if (OB_FAIL(init(cap, allocator))) {
        SQL_ENG_LOG(WARN, "failed to init array", K(ret));
      }
    } else {
      if (cap > cap_) {
        ret = OB_SIZE_OVERFLOW;
        SQL_ENG_LOG(WARN, "failed to reserve capacity", K(ret), K(cap), K(cap_));
      }
    }
    return ret;
  }

  inline void reset(common::ObIAllocator &alloc) { destroy(alloc); }
  inline void reuse(common::ObIAllocator &alloc) { destroy(alloc); }
  inline void destroy(common::ObIAllocator &allocator)
  {
    if (NULL != data_) {
      for (int32_t i = 0; i < cap_; i++) {
        data_[i].~T();
      }
      allocator.free(data_);
      data_ = NULL;
    }
    cap_ = 0;
    count_ = 0;
  }

  int64_t to_string(char *buf, int64_t buf_len) const
  {
    int64_t pos = 0;
    J_ARRAY_START();
    int64_t N = count();
    for (int64_t i = 0; i < N - 1; i++) {
      BUF_PRINTO(at(i));
      J_COMMA();
    }
    if (N > 0) {
      BUF_PRINTO(at(N-1));
    }
    J_ARRAY_END();
    return pos;
  }

  const T* get_ptr() const { return data_; }

private:
  T *data_;
  int32_t cap_;
  int32_t count_;
};

class ObPostExprItem
{
public:
  ObPostExprItem()
      : v2_(),
        accuracy_(),
        item_type_(::T_INVALID)
  {}
  // setters
  OB_INLINE void set_item_type(ObItemType item_type) { item_type_ = item_type; }
  OB_INLINE void set_accuracy(const common::ObAccuracy &accuracy) { accuracy_ = accuracy; }
  int assign(const common::ObObj &obj);
  int assign(ObExprOperator *op);
  int assign(ObItemType item_type);
  int set_column(int64_t index);
  // getters
  inline ObItemType get_item_type() const {return item_type_;}
  inline const common::ObObj &get_obj() const { return *reinterpret_cast<const common::ObObj *>(&(v2_.v1_)); }
  OB_INLINE bool can_get_value_directly() const;
  OB_INLINE int get_item_value_directly(const ObPhysicalPlanCtx &plan_ctx,
                                        const common::ObNewRow &row,
                                        const common::ObObj *&value) const;
  OB_INLINE int get_indirect_const(const ObPhysicalPlanCtx &phy_plan_ctx, const common::ObObj *&obj) const;
  OB_INLINE int64_t get_column() const { return v2_.cell_index_; }
  inline ObExprOperator *get_expr_operator() {return v2_.op_;}
  inline const ObExprOperator *get_expr_operator() const {return v2_.op_;}
  const common::ObAccuracy &get_accuracy() const { return accuracy_; }

  int64_t to_string(char *buf, const int64_t buf_len) const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &alloc,
                  const char *buf,
                  const int64_t data_len,
                  int64_t &pos);
  int64_t get_serialize_size() const;
  // for testing
  void set_int(int64_t v) { common::ObObj obj; obj.set_int(v); assign(obj); }
  /* for unittest only */
  int set_op(common::ObIAllocator &alloc, const char *op_name, ObExprOperator *&op);
  ///* for unittest only */
  void set_op(common::ObIAllocator &alloc, const char *op_name, int32_t real_param_num);

  uint16_t get_param_idx() const { return 0; }
  uint16_t get_param_num() const { return 0; }

protected:
  //common::ObObj v1_;
  //为了省内存，考虑到v1_和union中的成员是互斥使用的，，将v1_移动到union内部
  //但是union中的成员只能是简单数据类型，所以只能通过一个buf空间来构造一个obobj;
  //要保证v1_的内存是对齐的。
  union
  {
    char v1_[sizeof(common::ObObj)]; //用来保存obobj
    int64_t query_id_;
    int64_t cell_index_;  // column reference, aka cell index in ObRow
    ObExprOperator *op_;  // expression operator
  } v2_;
  common::ObAccuracy accuracy_;  // for const, column, questionmark
  ObItemType  item_type_;
private:
  // we need push ObPostExprItem object into ObIArray, so must allow copy and assign.
  // DISALLOW_COPY_AND_ASSIGN(ObPostExprItem);
};

OB_INLINE bool ObPostExprItem::can_get_value_directly() const
{
  return IS_DATATYPE_OR_QUESTIONMARK_OP(item_type_) || T_REF_COLUMN == item_type_;
}

//当item时column或者常量(包括param常量)的时候，可以直接获取值，避免冗余的参数检查
OB_INLINE int ObPostExprItem::get_item_value_directly(const ObPhysicalPlanCtx &plan_ctx,
                                                      const common::ObNewRow &row,
                                                      const common::ObObj *&value) const
{
  int ret = common::OB_SUCCESS;
  switch (item_type_) {
    case T_REF_COLUMN: {
      //后缀计算使用的是row的真实位置，没有使用通过投影后的逻辑位置，这里跟后缀计算的寻址方式保持一致
      //直接使用真实位置减少一次间接寻址的逻辑
      if (OB_UNLIKELY(get_column() >= row.count_) || OB_UNLIKELY(get_column() < 0) || OB_ISNULL(row.cells_)) {
        ret = common::OB_INVALID_ARGUMENT;
        SQL_ENG_LOG(WARN, "value index is invalid", K(ret), K(get_column()), K_(row.count), K_(row.cells));
      } else {
        value = &row.cells_[get_column()];
        if (OB_UNLIKELY(ObBitType == value->get_type())) {
        //use object scale to store bit length
        const_cast<common::ObObj*>(value)->set_scale(get_accuracy().get_precision());
        } else if (-1 != get_accuracy().get_scale()) {
        const_cast<common::ObObj*>(value)->set_scale(get_accuracy().get_scale());
        }
      }
      break;
    }
    case T_QUESTIONMARK: {
      //column convert的值来自于param store后者本身
      if (OB_FAIL(get_indirect_const(plan_ctx, value))) {
        SQL_ENG_LOG(WARN, "get indirect const from value item failed", K(ret), K(*this));
      }
      break;
    }
    default: {
      if (IS_DATATYPE_OP(item_type_)) {
        value = &get_obj();
      } else {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "invalid item type", K(item_type_));
      }
      break;
    }
  }
  return ret;
}

OB_INLINE int ObPostExprItem::get_indirect_const(const ObPhysicalPlanCtx &plan_ctx, const common::ObObj *&val) const
{
  int ret = common::OB_SUCCESS;
  switch (item_type_) {
    case T_QUESTIONMARK: {
      int64_t param_idx = -1;
      const auto &param_store = plan_ctx.get_param_store();
      if (OB_FAIL(get_obj().get_unknown(param_idx))) {
        SQL_ENG_LOG(WARN, "fail to get unknown", K(ret), K(get_obj()));
      } else if (OB_UNLIKELY(param_idx < 0 || param_idx >= param_store.count())) {
        ret = common::OB_ARRAY_OUT_OF_RANGE;
        SQL_ENG_LOG(WARN, "wrong index of question mark position", K(param_idx), "param_count", param_store.count());
      } else {
        val = &param_store.at(param_idx);
      }
      break;
    }
    default: {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalid type", K(ret), K_(item_type));
      break;
    }
  }
  return ret;
}

enum ObWriteExprItemFlag
{
  NEW_OP_WHEN_COPY,
  NO_NEW_OP_WHEN_COPY
};

enum ObSqlParamNumFlag
{
  TWO_OR_THREE = -3,
  OCCUR_AS_PAIR = -2,
  MORE_THAN_ZERO = -1,
};

struct ObPostfixExpressionCalcStack
{
  // should be larger than MAX_SQL_EXPRESSION_SYMBOL_COUNT(=256)
  static const int64_t STACK_SIZE = 1024 * 8;
  common::ObObj stack_[STACK_SIZE];
};

class ObPostfixExpression
{
public:
  ObPostfixExpression(common::ObIAllocator &alloc, int64_t item_count);
  ~ObPostfixExpression();
  int assign(const ObPostfixExpression &other);

  inline void set_output_column_count(int64_t output_column_count) { output_column_count_ = output_column_count; }
  inline int64_t get_output_column_count() const { return output_column_count_; }

  int set_item_count(int64_t count) { return post_exprs_.init(count, str_buf_); }
  // add expression object into array directly,
  // user assure objects of postfix expr sequence.
  // int add_expr_obj(const common::ObObj &obj);
  int add_expr_item(const ObPostExprItem &item);
  inline const ObSqlFixedArray<ObPostExprItem> &get_expr_items() const { return post_exprs_; }
  void reset();
  /* 将row中的值代入到expr计算结果 */
  int calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row, common::ObObj &result_val) const;
  int calc(common::ObExprCtx &expr_ctx, const common::ObNewRow &row1, const common::ObNewRow &row2,
           common::ObObj &result_val) const;
  int uk_fast_project(common::ObExprCtx &expr_ctx, const common::ObNewRow &row, common::ObObj &result_val) const;
  int calc_result_row(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
                      common::ObNewRow &result_row) const;
  int calc_result_row(common::ObExprCtx &expr_ctx, const common::ObNewRow &row1,
                      const common::ObNewRow &row2, common::ObNewRow &result_row) const;
  int calc_ref_column(const common::ObNewRow &row,
                      common::ObNewRow &result_row, const ObPostExprItem &item,
                      common::ObObj *stack, int64_t &stack_top) const;
  int calc_ref_column(const common::ObNewRow &row1, const common::ObNewRow &row2,
                      common::ObNewRow &result_row, const ObPostExprItem &item,
                      common::ObObj *stack, int64_t &stack_top) const;
  int calc_question_mark(common::ObExprCtx &expr_ctx,
                         common::ObNewRow &result_row, const ObPostExprItem &item,
                         common::ObObj *stack, int64_t &stack_top) const;
  int calc_other_op(common::ObExprCtx &expr_ctx, const ObPostExprItem &item,
                    common::ObObj *stack, int64_t &stack_top) const;
  int calc_agg_param_list(common::ObNewRow &result_row, const ObPostExprItem &item,
                          common::ObObj *stack, int64_t stack_top) const;
  template <ObItemType item_type>
  int calc_row(common::ObExprCtx &expr_ctx, const common::ObNewRow &row,
               common::ObNewRow &result_row, const ObPostExprItem &item,
               common::ObObj *stack, int64_t stack_top) const;
  int generate_idx_for_regexp_ops(int16_t &cur_regexp_op_count);
  bool is_empty() const;
  bool is_equijoin_cond(int64_t &c1, int64_t &c2,
                        common::ObObjType &cmp_type, common::ObCollationType &cmp_cs_type,
                        bool &is_null_safe) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  // 辅助函数，检查表达式是否表示const或者column index
  int check_expr_type(const int64_t type_val, bool &is_type, const int64_t stack_len) const;
private:
  static const int64_t DEF_STRING_BUF_SIZE = 64 * 1024L;
  static const int64_t BASIC_SYMBOL_COUNT = 64;
  typedef ObSqlFixedArray<ObPostExprItem> PostfixExprArray;
  void data_clear();
private:
  PostfixExprArray post_exprs_;
  common::ObIAllocator &str_buf_;
  int64_t output_column_count_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPostfixExpression);
}; // class ObPostfixExpression

inline bool ObPostfixExpression::is_empty() const
{
  return (0 == post_exprs_.count());
}

} // namespace sql
}// namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_EXPR_POSTFIX_EXPRESSION_H_
