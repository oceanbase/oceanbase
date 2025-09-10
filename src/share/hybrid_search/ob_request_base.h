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

#include "share/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "common/object/ob_object.h"
#include "objit/common/ob_item_type.h"

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
class ObReqExpr
{
public:
  ObReqExpr() : expr_name(), alias_name(), params() {}
  virtual ~ObReqExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::NORMAL_EXPR; }
  int translate_alias(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_);
  inline void set_alias(const ObString &alias) { alias_name = alias; }
  TO_STRING_KV("expr name", expr_name, N_PARAM, params);
  common::ObString expr_name;
  common::ObString alias_name;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> params;
};

enum ObReqScoreType
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
  ObReqMatchExpr() : ObReqExpr(), score_type_(SCORE_TYPE_BEST_FIELDS) {}
  virtual ~ObReqMatchExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::MATCH_EXPR; }
  ObReqScoreType score_type_;
};

class ObReqCaseWhenExpr : public ObReqExpr
{
public:
  ObReqCaseWhenExpr() : ObReqExpr(), arg_expr_(nullptr),
    when_exprs_(), then_exprs_(), default_expr_(nullptr) {}
  virtual ~ObReqCaseWhenExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::CASE_EXPR; }
  ObReqExpr *arg_expr_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> when_exprs_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> then_exprs_;
  ObReqExpr *default_expr_;
};

class ObReqColumnExpr : public ObReqExpr
{
public:
  ObReqColumnExpr() : ObReqExpr(), table_name(), weight_(-1.0) {}
  virtual ~ObReqColumnExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::COLUMN_EXPR; }
  common::ObString table_name;
  double weight_;
};

class ObReqConstExpr : public ObReqExpr
{
public:
  ObReqConstExpr(ObObjType val_type) : ObReqExpr(), var_type_(val_type), weight_(-1.0) {}
  virtual ~ObReqConstExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  virtual int get_expr_type() { return ObReqExprType::CONST_EXPR; }
  ObObjType var_type_;
  double weight_;
};

class ObReqOpExpr : public ObReqExpr
{
public:
  ObReqOpExpr(ObItemType op_type, bool need_parentheses = true) : ObReqExpr(), need_parentheses_(need_parentheses), op_type_(op_type) {}
  virtual ~ObReqOpExpr() {}
  virtual int translate_expr(ObObjPrintParams &print_params_, char *buf_, int64_t buf_len_, int64_t *pos_, ObReqScope scope = FIELD_LIST_SCOPE, bool need_alias = true);
  int init(ObReqExpr *l_para, ObReqExpr *r_para, ObItemType type);
  inline void set_op_type(ObItemType op_type) { op_type_ = op_type; }
  virtual int get_expr_type() { return ObReqExprType::OP_EXPR; }
  bool need_parentheses_;
  inline ObItemType get_op_type() { return op_type_; }
private :
  int get_op_string(ObString &op_str, bool &need_parentheses);
  ObItemType op_type_;
};

enum ReqTableType
{
  BASE_TABLE,
  SUB_QUERY,
  JOINED_TABLE,
  MULTI_SET,
};

struct OrderInfo
{
  ObReqExpr *order_item;
  bool ascent;
  bool null_first;
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
