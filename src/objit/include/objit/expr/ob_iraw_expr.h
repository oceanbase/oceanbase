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
#include "objit/common/ob_item_type.h"
#include "lib/container/ob_se_array.h"
#include "src/sql/engine/expr/ob_expr_res_type.h"

namespace oceanbase {
namespace jit {
namespace expr {

class ObExprVisitor;
class ObIRawExpr;

typedef ::oceanbase::common::ObSEArray<ObIRawExpr *, 2> ExprArray;
// The interface of OceanBase raw expression structure.
class ObIRawExpr
{
public:
  enum ExprClass
  {
    EXPR_INVALID_CLASS = 0,
    EXPR_CONST,
    EXPR_QUERY_REF,
    EXPR_COLUMN_REF,
    EXPR_OPERATOR,
    EXPR_CASE_OPERATOR,
    EXPR_AGGR,
    EXPR_SYS_FUNC,
    EXPR_WINDOW,
    EXPR_PSEUDO_COLUMN,
    EXPR_ALIAS_REF,
    EXPR_UDF,
    EXPR_VAR,
    EXPR_OP_PSEUDO_COLUMN,
    EXPR_SET_OP,
    EXPR_EXEC_PARAM,
    EXPR_PL_QUERY_REF,
    EXPR_MATCH_AGAINST,
  };

  typedef ObExprVisitor Visitor;

  ObIRawExpr(ObItemType expr_type = T_INVALID)
    : type_(expr_type),
      expr_class_(EXPR_INVALID_CLASS),
      result_type_()
  {
    set_data_type(common::ObMaxType);
  }

  ObIRawExpr(common::ObIAllocator &alloc, ObItemType expr_type = T_INVALID)
    : type_(expr_type),
      expr_class_(EXPR_INVALID_CLASS),
      result_type_(alloc)
  {
    set_data_type(common::ObMaxType);
  }

  virtual ~ObIRawExpr() {}

  virtual int preorder_accept(Visitor &v) const;
  virtual int postorder_accept(Visitor &v) const;

  // The ACCPET function will visit current node only, WITHOUT any
  // child. If you want to traverse whole expression tree, use
  // specific ACCEPT function instead such as PREORDER_ACCEPT or
  // POSTORDER_ACCEPT.
  //
  // There's one condition use this interface is more useful than
  // others that if traverse is implemented as part of Visitor but not
  // in expression tree.
  virtual int accept(Visitor &v) const;

  /* set/get expr class type */
  inline void set_expr_class(ExprClass expr_class) { expr_class_ = expr_class; }
  inline ExprClass get_expr_class() const { return expr_class_; }
  inline bool is_const_raw_expr() const { return EXPR_CONST == expr_class_; }
  inline bool is_exec_param_expr() const { return EXPR_EXEC_PARAM == expr_class_; }
  inline bool is_const_or_param_expr() const { return EXPR_CONST == expr_class_ ||
                                              EXPR_EXEC_PARAM == expr_class_; }
  inline bool is_var_expr() const { return EXPR_VAR == expr_class_; }
  inline bool is_op_pseudo_column_expr() const { return EXPR_OP_PSEUDO_COLUMN == expr_class_; }
  inline bool is_query_ref_expr() const { return EXPR_QUERY_REF == expr_class_; }
  inline bool is_column_ref_expr() const { return EXPR_COLUMN_REF == expr_class_; }
  inline bool is_op_expr() const { return EXPR_OPERATOR == expr_class_; }
  inline bool is_set_op_expr() const { return EXPR_SET_OP == expr_class_; }
  inline bool is_case_op_expr() const { return EXPR_CASE_OPERATOR  == expr_class_; }
  inline bool is_aggr_expr() const { return EXPR_AGGR == expr_class_; }
  inline bool is_sys_func_expr() const { return EXPR_SYS_FUNC == expr_class_; }
  inline bool is_udf_expr() const { return EXPR_UDF == expr_class_; }
  inline bool is_win_func_expr() const { return EXPR_WINDOW == expr_class_; }
  inline bool is_pseudo_column_expr() const { return EXPR_PSEUDO_COLUMN == expr_class_; }
  inline bool is_alias_ref_expr() const { return EXPR_ALIAS_REF == expr_class_; }
  inline bool is_match_against_expr() const { return EXPR_MATCH_AGAINST == expr_class_; }
  inline bool is_terminal_expr() const { return is_var_expr()
                                                || is_op_pseudo_column_expr()
                                                || is_const_or_param_expr()
                                                || is_query_ref_expr()
                                                || is_column_ref_expr()
                                                || is_alias_ref_expr(); }
  inline bool is_oracle_to_time_expr() const { return T_FUN_SYS_TO_DATE == type_
                                                      || T_FUN_SYS_TO_TIMESTAMP == type_
                                                      || T_FUN_SYS_TO_TIMESTAMP_TZ == type_; }
  inline bool is_calc_part_expr() const { return T_FUN_SYS_CALC_PARTITION_ID == type_
                                                      || T_FUN_SYS_CALC_TABLET_ID == type_
                                                      || T_FUN_SYS_CALC_PARTITION_TABLET_ID == type_; }
  inline bool is_basic_const_expr_mysql() const { return EXPR_CONST == expr_class_
                                                    || T_FUN_SYS_VERSION == type_
                                                    || T_FUN_SYS_OB_VERSION == type_
                                                    || T_FUN_SYS_ICU_VERSION == type_; }

  inline bool is_pl_expr() const { return EXPR_UDF == expr_class_
                                          || T_FUN_PL_COLLECTION_CONSTRUCT == type_
                                          || T_FUN_PL_OBJECT_CONSTRUCT == type_
                                          || T_FUN_SYS_PDB_GET_RUNTIME_INFO == type_; }
  inline void set_expr_type(ObItemType v) { type_ = v; }
  inline ObItemType get_expr_type() const { return type_; }

  void set_data_type(const common::ObObjType &data_type);
  common::ObObjType get_data_type() const;

  inline common::ObScale get_scale() const;
  common::ObObjTypeClass get_type_class() const;
  inline const common::ObObjMeta &get_result_meta() const;
  const sql::ObExprResType &get_result_type() const;
  common::ObCollationLevel get_collation_level() const;
  common::ObCollationType get_collation_type() const;
  const common::ObAccuracy &get_accuracy() const;
  uint64_t get_udt_id() const;
  uint16_t get_subschema_id() const;
  uint32_t get_result_flag() const;
  inline ExprClass get_expr_class() {return expr_class_;}
  int get_length_for_meta_in_bytes(common::ObLength &length) const;
  void set_meta_type(const common::ObObjMeta &meta_type);
  void set_result_type(const sql::ObExprResType &result_type);
  void set_collation_level(common::ObCollationLevel cs_level);
  void set_collation_type(common::ObCollationType cs_type);
  void set_accuracy(const common::ObAccuracy &accuracy);
  void set_result_flag(const uint32_t flag);
  void set_scale(const int16_t scale);
  void set_precision(const int16_t precision);
  void set_length_semantics(const int16_t length_semantics);
  void set_udt_id(uint64_t id);
  void set_subschema_id(const uint16_t subschema_id);
  void set_param(const common::ObObj &param);
  void set_length(common::ObLength length);

  virtual int64_t get_children_count() const;
  virtual int get_children(ExprArray &jit_exprs) const;

  TO_STRING_KV(K_(type), K_(expr_class));

protected:
  ObItemType  type_;
  ExprClass   expr_class_;
  sql::ObExprResType result_type_;
};

inline void ObIRawExpr::set_data_type(const common::ObObjType &data_type)
{
  result_type_.set_type(data_type);
}

inline common::ObObjType ObIRawExpr::get_data_type() const
{
  return result_type_.get_type();
}

inline common::ObScale ObIRawExpr::get_scale() const
{
  return result_type_.get_scale();
}

inline common::ObObjTypeClass ObIRawExpr::get_type_class() const
{
  return result_type_.get_type_class();
}

inline const sql::ObExprResType &ObIRawExpr::get_result_type() const
{
  return result_type_;
}
inline common::ObCollationLevel ObIRawExpr::get_collation_level() const
{
  return result_type_.get_collation_level();
}
inline common::ObCollationType ObIRawExpr::get_collation_type() const
{
  return result_type_.get_collation_type();
}
inline const common::ObAccuracy &ObIRawExpr::get_accuracy() const
{
  return result_type_.get_accuracy();
}
inline uint64_t ObIRawExpr::get_udt_id() const
{
  return result_type_.get_udt_id();
}

inline uint16_t ObIRawExpr::get_subschema_id() const
{
  return result_type_.get_subschema_id();
}

inline uint32_t ObIRawExpr::get_result_flag() const
{
  uint32_t flag = result_type_.get_result_flag();
  bool is_oracle_lob = false;
  ObObjType obj_type = result_type_.get_type();
  if (ObLongTextType == obj_type && lib::is_oracle_mode()) { // was ObLobType
    is_oracle_lob = true;
  }
  if (ObCharset::is_valid_collation(static_cast<ObCollationType>(result_type_.get_collation_type()))
    && ObCharset::is_bin_sort(result_type_.get_collation_type())) {
    if (!is_column_ref_expr() ||
       (!ob_is_numeric_type(result_type_.get_type()) &&
        !ob_is_year_tc(result_type_.get_type()) &&
        !is_oracle_lob)) {
      flag |= BINARY_FLAG;
    }
  }
  if (is_oracle_lob) {
    flag &= (~BLOB_FLAG); // was ObLobType
  }
  if (ob_is_bit_tc(obj_type) && get_accuracy().get_precision() > 1) {
    //
    // bit(1) flags -> UNSIGNED
    // bit(2) flags -> BINARY_FLAG | UNSIGNED
    flag |= BINARY_FLAG;
  }
  return flag;
}

inline int ObIRawExpr::get_length_for_meta_in_bytes(common::ObLength &length) const
{
  return result_type_.get_length_for_meta_in_bytes(length);
}
inline void ObIRawExpr::set_meta_type(const common::ObObjMeta &meta_type)
{
  result_type_.set_meta(meta_type);
}
inline const common::ObObjMeta &ObIRawExpr::get_result_meta() const
{
  return result_type_;
}
inline void ObIRawExpr::set_result_type(const sql::ObExprResType &result_type)
{
  result_type_ = result_type;
}
inline void ObIRawExpr::set_collation_level(common::ObCollationLevel cs_level)
{
  result_type_.set_collation_level(cs_level);
}
inline void ObIRawExpr::set_collation_type(common::ObCollationType cs_type)
{
  result_type_.set_collation_type(cs_type);
}
inline void ObIRawExpr::set_accuracy(const common::ObAccuracy &accuracy)
{
  result_type_.set_accuracy(accuracy);
}
inline void ObIRawExpr::set_result_flag(const uint32_t flag)
{
  result_type_.set_result_flag(flag);
}
inline void ObIRawExpr::set_scale(const int16_t scale)
{
  result_type_.set_scale(scale);
}
inline void ObIRawExpr::set_precision(const int16_t precision)
{
  result_type_.set_precision(precision);
}
inline void ObIRawExpr::set_length_semantics(const int16_t length_semantics)
{
  result_type_.set_length_semantics(length_semantics);
}
inline void ObIRawExpr::set_udt_id(uint64_t id)
{
  result_type_.set_udt_id(id);
}

inline void ObIRawExpr::set_subschema_id(const uint16_t subschema_id)
{
  result_type_.set_subschema_id(subschema_id);
}

inline void ObIRawExpr::set_param(const common::ObObj &param)
{
  result_type_.set_param(param);
}

inline void ObIRawExpr::set_length(common::ObLength length)
{
  result_type_.set_length(length);
}

inline int64_t ObIRawExpr::get_children_count() const
{
  return 0;
}

inline int ObIRawExpr::get_children(ExprArray &jit_exprs) const
{
  UNUSED(jit_exprs);
  return common::OB_NOT_SUPPORTED;
}

inline int ObIRawExpr::accept(Visitor __attribute__ ((unused)) &v) const
{
  return common::OB_NOT_SUPPORTED;
}

}  // expr
}  // jit
}  // oceanbase

#endif /* OB_EXPR_H */
