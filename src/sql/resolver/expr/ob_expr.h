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

#ifndef OB_EXPR_H
#define OB_EXPR_H

#include <stdint.h>
#include "lib/ob_errno.h"
#include "sql/parser/ob_item_type.h"
#include "lib/container/ob_se_array.h"
#include "sql/engine/expr/ob_expr_res_type.h"
namespace oceanbase {
namespace jit {
namespace expr {

class ObExpr;

typedef ::oceanbase::common::ObSEArray<ObExpr*, 2> ExprArray;
// The interface of OceanBase raw expression structure.
class ObExpr {
public:
  enum ExprClass {
    EXPR_INVALID_CLASS = 0,
    EXPR_CONST,
    EXPR_QUERY_REF,
    EXPR_COLUMN_REF,
    EXPR_OPERATOR,
    EXPR_CASE_OPERATOR,
    EXPR_AGGR,
    EXPR_SYS_FUNC,
    EXPR_DOMAIN_INDEX,
    EXPR_WINDOW,
    EXPR_PSEUDO_COLUMN,
    EXPR_ALIAS_REF,
    EXPR_UDF,
    EXPR_VAR,
    EXPR_SET_OP
  };

  ObExpr(ObItemType expr_type = T_INVALID) : type_(expr_type), expr_class_(EXPR_INVALID_CLASS), result_type_()
  {
    set_data_type(common::ObMaxType);
  }

  ObExpr(common::ObIAllocator& alloc, ObItemType expr_type = T_INVALID)
      : type_(expr_type), expr_class_(EXPR_INVALID_CLASS), result_type_(alloc)
  {
    set_data_type(common::ObMaxType);
  }

  virtual ~ObExpr()
  {}

  /* set/get expr class type */
  inline void set_expr_class(ExprClass expr_class)
  {
    expr_class_ = expr_class;
  }
  inline ExprClass get_expr_class() const
  {
    return expr_class_;
  }
  inline bool is_const_expr() const
  {
    return EXPR_CONST == expr_class_;
  }
  inline bool is_var_expr() const
  {
    return EXPR_VAR == expr_class_;
  }
  inline bool is_query_ref_expr() const
  {
    return EXPR_QUERY_REF == expr_class_;
  }
  inline bool is_column_ref_expr() const
  {
    return EXPR_COLUMN_REF == expr_class_;
  }
  inline bool is_op_expr() const
  {
    return EXPR_OPERATOR == expr_class_;
  }
  inline bool is_set_op_expr() const
  {
    return EXPR_SET_OP == expr_class_;
  }
  inline bool is_case_op_expr() const
  {
    return EXPR_CASE_OPERATOR == expr_class_;
  }
  inline bool is_aggr_expr() const
  {
    return EXPR_AGGR == expr_class_;
  }
  inline bool is_sys_func_expr() const
  {
    return EXPR_SYS_FUNC == expr_class_;
  }
  inline bool is_udf_expr() const
  {
    return EXPR_UDF == expr_class_;
  }
  inline bool is_domain_index_func() const
  {
    return EXPR_DOMAIN_INDEX == expr_class_;
  }
  inline bool is_win_func_expr() const
  {
    return EXPR_WINDOW == expr_class_;
  }
  inline bool is_pseudo_column_expr() const
  {
    return EXPR_PSEUDO_COLUMN == expr_class_;
  }
  inline bool is_alias_ref_expr() const
  {
    return EXPR_ALIAS_REF == expr_class_;
  }
  inline bool is_terminal_expr() const
  {
    return is_var_expr() || is_const_expr() || is_query_ref_expr() || is_column_ref_expr() || is_alias_ref_expr();
  }
  inline void set_expr_type(ObItemType v)
  {
    type_ = v;
  }
  inline ObItemType get_expr_type() const
  {
    return type_;
  }

  void set_data_type(const common::ObObjType& data_type);
  common::ObObjType get_data_type() const;

  common::ObObjTypeClass get_type_class() const;
  inline const common::ObObjMeta& get_result_meta() const;
  const sql::ObExprResType& get_result_type() const;
  common::ObCollationLevel get_collation_level() const;
  common::ObCollationType get_collation_type() const;
  const common::ObAccuracy& get_accuracy() const;
  uint64_t get_udt_id() const;
  uint32_t get_result_flag() const;
  int get_length_for_meta_in_bytes(common::ObLength& length) const;
  void set_meta_type(const common::ObObjMeta& meta_type);
  void set_result_type(const sql::ObExprResType& result_type);
  void set_collation_level(common::ObCollationLevel cs_level);
  void set_collation_type(common::ObCollationType cs_type);
  void set_accuracy(const common::ObAccuracy& accuracy);
  void set_result_flag(const uint32_t flag);
  void set_scale(const int16_t scale);
  void set_precision(const int16_t precision);
  void set_length_semantics(const int16_t length_semantics);
  void set_udt_id(uint64_t id);
  void set_param(const common::ObObj& param);

  virtual int64_t get_children_count() const;
  virtual int get_children(ExprArray& jit_exprs) const;

  TO_STRING_KV(K_(type), K_(expr_class));

protected:
  ObItemType type_;
  ExprClass expr_class_;
  sql::ObExprResType result_type_;
};

inline void ObExpr::set_data_type(const common::ObObjType& data_type)
{
  result_type_.set_type(data_type);
}

inline common::ObObjType ObExpr::get_data_type() const
{
  return result_type_.get_type();
}

inline common::ObObjTypeClass ObExpr::get_type_class() const
{
  return result_type_.get_type_class();
}

inline const sql::ObExprResType& ObExpr::get_result_type() const
{
  return result_type_;
}
inline common::ObCollationLevel ObExpr::get_collation_level() const
{
  return result_type_.get_collation_level();
}
inline common::ObCollationType ObExpr::get_collation_type() const
{
  return result_type_.get_collation_type();
}
inline const common::ObAccuracy& ObExpr::get_accuracy() const
{
  return result_type_.get_accuracy();
}
inline uint64_t ObExpr::get_udt_id() const
{
  return result_type_.get_udt_id();
}
inline uint32_t ObExpr::get_result_flag() const
{
  return (result_type_.get_collation_type() == common::CS_TYPE_UTF8MB4_BIN ||
             result_type_.get_collation_type() == common::CS_TYPE_BINARY)
             ? result_type_.get_result_flag() | OB_MYSQL_BINARY_FLAG
             : result_type_.get_result_flag();
}
inline int ObExpr::get_length_for_meta_in_bytes(common::ObLength& length) const
{
  return result_type_.get_length_for_meta_in_bytes(length);
}
inline void ObExpr::set_meta_type(const common::ObObjMeta& meta_type)
{
  result_type_.set_meta(meta_type);
}
inline const common::ObObjMeta& ObExpr::get_result_meta() const
{
  return result_type_;
}
inline void ObExpr::set_result_type(const sql::ObExprResType& result_type)
{
  result_type_ = result_type;
}
inline void ObExpr::set_collation_level(common::ObCollationLevel cs_level)
{
  result_type_.set_collation_level(cs_level);
}
inline void ObExpr::set_collation_type(common::ObCollationType cs_type)
{
  result_type_.set_collation_type(cs_type);
}
inline void ObExpr::set_accuracy(const common::ObAccuracy& accuracy)
{
  result_type_.set_accuracy(accuracy);
}
inline void ObExpr::set_result_flag(const uint32_t flag)
{
  result_type_.set_result_flag(flag);
}
inline void ObExpr::set_scale(const int16_t scale)
{
  result_type_.set_scale(scale);
}
inline void ObExpr::set_precision(const int16_t precision)
{
  result_type_.set_precision(precision);
}
inline void ObExpr::set_length_semantics(const int16_t length_semantics)
{
  result_type_.set_length_semantics(length_semantics);
}
inline void ObExpr::set_udt_id(uint64_t id)
{
  result_type_.set_udt_id(id);
}
inline void ObExpr::set_param(const common::ObObj& param)
{
  result_type_.set_param(param);
}

inline int64_t ObExpr::get_children_count() const
{
  return 0;
}

inline int ObExpr::get_children(ExprArray& jit_exprs) const
{
  UNUSED(jit_exprs);
  return common::OB_NOT_SUPPORTED;
}

}  // namespace expr
}  // namespace jit
}  // namespace oceanbase

#endif /* OB_EXPR_H */
