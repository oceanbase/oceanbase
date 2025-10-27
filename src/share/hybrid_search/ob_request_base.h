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

#ifndef OCEANBASE_SHARE_OB_REQUEST_BASE_H_
#define OCEANBASE_SHARE_OB_REQUEST_BASE_H_

#include "common/object/ob_object.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "objit/common/ob_item_type.h"
#include "sql/printer/ob_raw_expr_printer.h"

namespace oceanbase
{
namespace share
{

enum OpPrecedence {
  PREC_OR = 1,         // OR
  PREC_XOR,            // XOR
  PREC_AND,            // AND
  PREC_BIT,            // |, &, <<, >>
  PREC_COMP,           // =, <>, <, <=, >, >=, LIKE, IN, IS, IS NOT
  PREC_ADD,            // +, -
  PREC_MUL,            // *, /, DIV, MOD
  PREC_POW,            // ^
  PREC_UNARY           // NOT, ~, -, +
};

int get_op_precedence(ObItemType op_type);
bool is_atomic_expression(ObItemType op_type);
bool is_left_associative(ObItemType op_type);

enum ObReqScope
{
  FIELD_LIST_SCOPE = 0,
  ON_SCOPE,
  WHERE_SCOPE,
  LIMIT_SCOPE,
  ORDER_SCOPE,
};

enum ObReqExprType
{
  NORMAL_EXPR = 0,
  MATCH_EXPR ,
  COLUMN_EXPR,
  CONST_EXPR,
  OP_EXPR,
  CASE_EXPR,
};

class ObReqFromJson;
struct OrderInfo;
class ObReqExpr
{
public:
  ObReqExpr() = delete;
  static int construct_expr(ObReqExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, const ObString &alias_name = ObString());
  static int construct_expr(ObReqExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, ObReqExpr *param, const ObString &alias_name = ObString());
  static int construct_expr(ObReqExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, ObReqExpr *param1, ObReqExpr *param2, const ObString &alias_name = ObString());
  static int construct_expr(ObReqExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, const common::ObIArray<ObReqExpr *> &params, const ObString &alias_name = ObString());
  virtual ~ObReqExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::NORMAL_EXPR; }
  int translate_alias(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_);
  inline void set_alias(const ObString &alias) { alias_name = alias; }
  TO_STRING_KV("expr name", expr_name, N_PARAM, params);
  common::ObString expr_name;
  common::ObString alias_name;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> params;
protected:
  ObReqExpr(const ObString &expr_name, const ObString &alias_name = ObString())
    : expr_name(expr_name), alias_name(alias_name), params() {}
};

enum ObEsScoreType : int8_t
{
  SCORE_TYPE_BEST_FIELDS = 0,
  SCORE_TYPE_CROSS_FIELDS,
  SCORE_TYPE_MOST_FIELDS,
  SCORE_TYPE_PHRASE,
  SCORE_TYPE_COUNT
};

class ObReqMatchExpr : public ObReqExpr
{
public:
  ObReqMatchExpr() = delete;
  static int construct_match_expr(ObReqMatchExpr *&expr, ObIAllocator &alloc, ObReqExpr *field_expr, ObReqExpr *query_expr, ObEsScoreType score_type = SCORE_TYPE_BEST_FIELDS);
  virtual ~ObReqMatchExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::MATCH_EXPR; }
  ObEsScoreType score_type_;
private:
  ObReqMatchExpr(ObEsScoreType score_type) : ObReqExpr(ObString("match")), score_type_(score_type) {}
};

class ObReqCaseWhenExpr : public ObReqExpr
{
public:
  ObReqCaseWhenExpr() = delete;
  static int construct_case_when_expr(ObReqCaseWhenExpr *&expr, ObIAllocator &alloc,
                                      ObReqExpr *when_expr,
                                      ObReqExpr *then_expr,
                                      ObReqExpr *default_expr = nullptr,
                                      ObReqExpr *arg_expr = nullptr);
  static int construct_case_when_expr(ObReqCaseWhenExpr *&expr, ObIAllocator &alloc,
                                      const common::ObIArray<ObReqExpr *> &when_exprs,
                                      const common::ObIArray<ObReqExpr *> &then_exprs,
                                      ObReqExpr *default_expr = nullptr,
                                      ObReqExpr *arg_expr = nullptr);
  virtual ~ObReqCaseWhenExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::CASE_EXPR; }
  ObReqExpr *arg_expr_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> when_exprs_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> then_exprs_;
  ObReqExpr *default_expr_;
private:
  ObReqCaseWhenExpr(ObReqExpr *default_expr, ObReqExpr *when_expr, ObReqExpr *then_expr, ObReqExpr *arg_expr = nullptr) :
  ObReqExpr(ObString()), arg_expr_(arg_expr), when_exprs_(), then_exprs_(), default_expr_(default_expr) {
    when_exprs_.push_back(when_expr);
    then_exprs_.push_back(then_expr);
  }
  ObReqCaseWhenExpr(ObReqExpr *default_expr,
                    const common::ObIArray<ObReqExpr *> &when_exprs,
                    const common::ObIArray<ObReqExpr *> &then_exprs,
                    ObReqExpr *arg_expr = nullptr) :
  ObReqExpr(ObString()), arg_expr_(arg_expr), when_exprs_(), then_exprs_(), default_expr_(default_expr) {
    for (int64_t i = 0; i < when_exprs.count(); i++) {
      when_exprs_.push_back(when_exprs.at(i));
    }
    for (int64_t i = 0; i < then_exprs.count(); i++) {
      then_exprs_.push_back(then_exprs.at(i));
    }
  }

};

class ObReqColumnExpr : public ObReqExpr
{
public:
  ObReqColumnExpr() = delete;
  static int construct_column_expr(ObReqColumnExpr *&expr, ObIAllocator &alloc, const ObString &expr_name = ObString(), double weight = -1.0);
  static int construct_column_expr(ObReqColumnExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, const ObString &table_name, double weight = -1.0);
  virtual ~ObReqColumnExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::COLUMN_EXPR; }
  common::ObString table_name;
  double weight_;
private:
  ObReqColumnExpr(const ObString &expr_name, const ObString &table_name = ObString(), double weight = -1.0)
    : ObReqExpr(expr_name), table_name(table_name), weight_(weight) {}
};

class ObReqConstExpr : public ObReqExpr
{
public:
  ObReqConstExpr() = delete;
  static int construct_const_expr(ObReqConstExpr *&expr, ObIAllocator &alloc, const ObString &expr_name, ObObjType var_type);
  static int construct_const_numeric_expr(ObReqConstExpr *&expr, ObIAllocator &alloc, double num_value, ObObjType var_type);
  virtual ~ObReqConstExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::CONST_EXPR; }
  inline bool is_numeric() const { return is_numeric_; }
  inline double get_numeric_value() const { return numeric_value_; }
  int set_numeric(ObIAllocator &alloc, double numeric_value, ObObjType var_type);
  inline void set_numeric() { is_numeric_ = ob_is_numeric_type(var_type_); }
  ObObjType var_type_;
  double weight_;
private:
  ObReqConstExpr(ObObjType val_type, const ObString &expr_name) : ObReqExpr(expr_name), var_type_(val_type), weight_(-1.0), is_numeric_(false), numeric_value_(0.0) {}
  bool is_numeric_;
  double numeric_value_;
};

class ObReqOpExpr : public ObReqExpr
{
public:
  ObReqOpExpr() = delete;
  static int construct_binary_op_expr(ObReqOpExpr *&expr, ObIAllocator &alloc, ObItemType type, ObReqExpr *l_param, ObReqExpr *r_param, const ObString &alias_name = ObString());
  static int construct_unary_op_expr(ObReqOpExpr *&expr, ObIAllocator &alloc, ObItemType type,  ObReqExpr *param);
  static int construct_op_expr(ObReqOpExpr *&expr, ObIAllocator &alloc, ObItemType type, const common::ObIArray<ObReqExpr *> &params);
  static int construct_in_expr(ObIAllocator &alloc, ObReqColumnExpr *col_expr, common::ObIArray<ObReqConstExpr *> &value_exprs, ObReqOpExpr *&in_expr);
  virtual ~ObReqOpExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  int translate_in_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  inline void set_op_type(ObItemType op_type) { op_type_ = op_type; }
  virtual int get_expr_type() { return ObReqExprType::OP_EXPR; }
  bool need_parentheses_;
  inline ObItemType get_op_type() const { return op_type_; }
  bool has_multi_params_recursive() const;
  void simplify_recursive();
private:
  ObReqOpExpr(ObItemType op_type) : ObReqExpr(ObString()), need_parentheses_(false), op_type_(op_type) {}
  int init(ObReqExpr *l_para, ObReqExpr *r_para, ObItemType type);
  int get_op_string(ObString &op_str);
  int set_op_name();
  int need_parentheses_by_associativity(ObItemType parent_type, ObItemType child_type, int child_index, bool &need_parentheses) const;
  int need_parentheses_for_child(const ObReqOpExpr &child_expr, int child_index, bool &need_parentheses) const;
  int need_parentheses_for_sub_expr(const ObReqOpExpr &expr, int expr_index, ObItemType root_type, int root_param_count, bool &need_parentheses) const;
  bool need_right_operand_parentheses(ObItemType parent_type, int child_index) const;
  ObItemType op_type_;
};

class ObReqWindowFunExpr : public ObReqExpr
{
public:
  ObReqWindowFunExpr() = delete;
  static int construct_window_fun_expr(ObIAllocator &alloc, OrderInfo *order_info, const ObString &expr_name, const ObString &alias, ObReqWindowFunExpr *&expr);
  virtual ~ObReqWindowFunExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::NORMAL_EXPR; }
  common::ObArray<OrderInfo *, common::ModulePageAllocator, true> order_items_;
private :
  ObReqWindowFunExpr(const ObString &expr_name, const ObString &alias_name = ObString())
    : ObReqExpr(expr_name, alias_name), order_items_() {}
};

enum ReqTableType
{
  BASE_TABLE,
  SUB_QUERY,
  JOINED_TABLE,
  MULTI_SET,
  UNKNOWN_TABLE,
};

struct OrderInfo
{
  ObReqExpr *order_item;
  bool ascent;
  bool null_first;
  int translate(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE);
  TO_STRING_KV("N_ORDER_BY", *order_item, K(ascent), K(null_first));
};

class ObReqTable
{
public :
  ObReqTable() : table_type_(ReqTableType::BASE_TABLE), table_name_(), alias_name_(), database_name_(), ref_query_(nullptr) {}
  ObReqTable(ReqTableType table_type, const common::ObString &table_name,
             const common::ObString &database_name, ObReqFromJson *ref_query = NULL)
    : table_type_(table_type), table_name_(table_name), alias_name_(), database_name_(database_name), ref_query_(ref_query) {}
  virtual ~ObReqTable() {}
  ReqTableType table_type_;
  common::ObString table_name_;
  common::ObString alias_name_;
  common::ObString database_name_;
  ObReqFromJson *ref_query_;
  TO_STRING_KV(K_(table_type), K_(table_name), K_(database_name), K_(alias_name), K_(ref_query));
};

enum ObReqJoinType
{
  LEFT_OUTER_JOIN = 0,
  RIGHT_OUTER_JOIN,
  FULL_OUTER_JOIN,
  UNION_ALL,
  MAX_JOIN_TYPE
};

class ObReqJoinedTable : public ObReqTable
{
public :
  ObReqJoinedTable()
    : ObReqTable(), joined_type_(ObReqJoinType::LEFT_OUTER_JOIN),
      left_table_(nullptr), right_table_(nullptr), condition_(nullptr) {}
  ObReqJoinedTable(ReqTableType table_type, ObReqTable *left_table, ObReqTable *right_table,
    ObReqExpr *condition, ObReqJoinType joined_type)
    : ObReqTable() {
      init(left_table, right_table, condition, joined_type);
    }
  virtual ~ObReqJoinedTable() {}
  ObReqJoinType joined_type_;
  ObReqTable *left_table_;
  ObReqTable *right_table_;
  ObReqExpr *condition_;
  void init(ObReqTable *left_table, ObReqTable *right_table, ObReqExpr *condition, ObReqJoinType joined_type);
  TO_STRING_KV(K_(joined_type), K_(left_table), K_(right_table));
};

class ObMultiSetTable : public ObReqTable
{
public :
  ObMultiSetTable()
    : ObReqTable(), joined_type_(ObReqJoinType::UNION_ALL), sub_queries_() {}
  virtual ~ObMultiSetTable() {}
  ObReqJoinType joined_type_;
  common::ObSEArray<ObReqTable *, 4, common::ModulePageAllocator, true> sub_queries_;
  TO_STRING_KV(K_(joined_type));
};

class ObReqFromJson
{
public:
  ObReqFromJson()
    : from_items_(), req_hints_(), order_items_(),
      condition_items_(), cte_definitions_(), offset_item_(nullptr),
      limit_item_(nullptr), has_vec_approx_(false) {}
  virtual ~ObReqFromJson() {}
  int translate(char *buf, int64_t buf_len, int64_t &res_len);
  inline void set_vec_approx() { has_vec_approx_ = true; }
  inline void set_limit(ObReqExpr *limit_item) { limit_item_ = limit_item; }
  inline void set_offset(ObReqExpr *offset_item) { offset_item_ = offset_item; }
  inline const ObReqExpr* get_limit() { return limit_item_; }
  inline const ObReqExpr* get_offset() { return offset_item_; }
  inline bool has_vec_approx() const { return has_vec_approx_; }
  inline int add_req_hint(ObString hint) { return req_hints_.push_back(hint); }
  TO_STRING_KV(N_TABLE, from_items_,
               N_WHERE, condition_items_,
               N_ORDER_BY, order_items_,
               N_LIMIT, limit_item_,
               N_OFFSET, offset_item_,
               N_STMT_HINT, req_hints_,
               K_(has_vec_approx));
  common::ObSEArray<ObReqTable *, 4, common::ModulePageAllocator, true> from_items_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> req_hints_;
  common::ObSEArray<OrderInfo *, 4, common::ModulePageAllocator, true> order_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> condition_items_;
  common::ObSEArray<ObReqTable *, 4, common::ModulePageAllocator, true> cte_definitions_;
  ObReqExpr *offset_item_;
  ObReqExpr *limit_item_;
  bool has_vec_approx_;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_REQUEST_BASE_H_
