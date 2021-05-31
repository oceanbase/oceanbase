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

#define USING_LOG_PREFIX SQL_PC
#include "ob_sql_parameterization.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/json/ob_json_print_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/parser/parse_malloc.h"
#include "sql/ob_sql_utils.h"
#include "common/ob_smart_call.h"
#include <algorithm>

using namespace oceanbase;
using namespace sql;
using namespace common;
using namespace share::schema;

SqlInfo::SqlInfo() : total_(0), last_active_time_(0), hit_count_(0)
{}

namespace oceanbase {
namespace sql {
struct TransformTreeCtx {
  ObIAllocator* allocator_;
  ParseNode* top_node_;
  ParseNode* tree_;
  ObStmtScope expr_scope_;
  int64_t value_father_level_;
  ObCollationType collation_type_;
  ObCollationType national_collation_type_;
  const ObTimeZoneInfo* tz_info_;
  int64_t question_num_;
  ParamStore* params_;
  ObMaxConcurrentParam::FixParamStore* fixed_param_store_;
  bool not_param_;
  bool is_fast_parse_const_;   // current node is a constant that can be recognized by fp
  bool enable_contain_param_;  // this node and its child nodes are identifiable parameters of fp.
  SqlInfo* sql_info_;
  int64_t paramlized_questionmask_count_;  // The number of parameters that can be parameterized in this query
  bool is_transform_outline_;
  ObLengthSemantics default_length_semantics_;
  ObSEArray<void*, 16> project_list_;
  int64_t sql_offset_;
  const ObIArray<ObPCParam*>* raw_params_;
  TransformTreeCtx();
};

struct SelectItemTraverseCtx {
  const common::ObIArray<ObPCParam*>& raw_params_;
  const ParseNode* tree_;
  const ObString& org_expr_name_;
  const int64_t expr_start_pos_;
  const int64_t buf_len_;
  int64_t& expr_pos_;
  SelectItemParamInfo& param_info_;

  SelectItemTraverseCtx(const common::ObIArray<ObPCParam*>& raw_params, const ParseNode* tree,
      const ObString& org_expr_name, const int64_t expr_start_pos, const int64_t buf_len, int64_t& expr_pos,
      SelectItemParamInfo& param_info)
      : raw_params_(raw_params),
        tree_(tree),
        org_expr_name_(org_expr_name),
        expr_start_pos_(expr_start_pos),
        buf_len_(buf_len),
        expr_pos_(expr_pos),
        param_info_(param_info)
  {}
};

struct TraverseStackFrame {
  const ParseNode* cur_node_;
  int64_t next_child_idx_;

  TO_STRING_KV(K_(next_child_idx), K(cur_node_->type_));
};

}  // namespace sql
}  // namespace oceanbase

TransformTreeCtx::TransformTreeCtx()
    : allocator_(NULL),
      top_node_(NULL),
      tree_(NULL),
      expr_scope_(T_NONE_SCOPE),
      value_father_level_(ObSqlParameterization::NO_VALUES),
      collation_type_(),
      national_collation_type_(CS_TYPE_INVALID),
      tz_info_(NULL),
      question_num_(0),
      params_(NULL),
      fixed_param_store_(NULL),
      not_param_(false),
      is_fast_parse_const_(false),
      enable_contain_param_(true),
      sql_info_(NULL),
      paramlized_questionmask_count_(0),
      is_transform_outline_(false),
      default_length_semantics_(LS_BYTE),
      sql_offset_(OB_INVALID_INDEX),
      raw_params_(NULL)
{}

// replace const expr with ? in syntax tree
// seprate params from syntax tree
int ObSqlParameterization::transform_syntax_tree(ObIAllocator& allocator, const ObSQLSessionInfo& session,
    const ObIArray<ObPCParam*>* raw_params, const int64_t sql_offset, ParseNode* tree, SqlInfo& sql_info,
    ParamStore& params, SelectItemParamInfoArray* select_item_param_infos,
    ObMaxConcurrentParam::FixParamStore& fixed_param_store, bool is_transform_outline)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ParseNode* children_node = tree->children_[0];
  if (OB_ISNULL(tree)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(tree), K(ret));
  } else if (OB_ISNULL(children_node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(tree), K(ret));
  } else if (OB_FAIL(session.get_collation_connection(collation_connection))) {
    SQL_PC_LOG(WARN, "fail to get collation_connection", K(ret));
  } else {
    sql_info.sql_traits_.stmt_type_ = children_node->type_;
    TransformTreeCtx ctx;
    ctx.collation_type_ = collation_connection;
    ctx.national_collation_type_ = session.get_nls_collation_nation();
    ctx.tz_info_ = session.get_timezone_info();
    ctx.default_length_semantics_ = session.get_actual_nls_length_semantics();
    ctx.allocator_ = &allocator;
    ctx.tree_ = tree;
    ctx.top_node_ = tree;
    ctx.expr_scope_ = T_NONE_SCOPE;
    ctx.value_father_level_ = NO_VALUES;
    ctx.question_num_ = 0;
    ctx.params_ = &params;
    ctx.fixed_param_store_ = &fixed_param_store;
    ctx.not_param_ = false;
    ctx.is_fast_parse_const_ = false;
    ctx.enable_contain_param_ = true;
    ctx.sql_info_ = &sql_info;
    ctx.paramlized_questionmask_count_ = 0;            // used for outline sql current limiting
    ctx.is_transform_outline_ = is_transform_outline;  // used for outline sql current limiting
    ctx.sql_offset_ = sql_offset;
    ctx.raw_params_ = raw_params;
    if (OB_FAIL(transform_tree(ctx, session))) {
      if (OB_NOT_SUPPORTED != ret) {
        SQL_PC_LOG(WARN, "fail to transform syntax tree", K(ret));
      }
    } else if (is_transform_outline && (INT64_MAX != children_node->value_) &&
               (children_node->value_ != ctx.paramlized_questionmask_count_)) {
      ret = OB_INVALID_OUTLINE;
      LOG_USER_ERROR(OB_INVALID_OUTLINE, "? appears at invalid position in sql_text");
      SQL_PC_LOG(WARN,
          "? appear at invalid position",
          "total count of questionmasks in query",
          children_node->value_,
          "paramlized_questionmask_count",
          ctx.paramlized_questionmask_count_,
          K(ret));
    } else if (OB_NOT_NULL(raw_params) && OB_NOT_NULL(select_item_param_infos)) {
      select_item_param_infos->set_capacity(ctx.project_list_.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx.project_list_.count(); i++) {
        ParseNode* tmp_root = static_cast<ParseNode*>(ctx.project_list_.at(i));
        if (OB_ISNULL(tmp_root)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null child", K(ret), K(i), K(ctx.project_list_.at(i)));
        } else if (0 == tmp_root->is_val_paramed_item_idx_ &&
                   OB_FAIL(get_select_item_param_info(*raw_params, tmp_root, select_item_param_infos))) {
          SQL_PC_LOG(WARN, "failed to get select item param info", K(ret));
        } else {
          // do nothing
        }
      }  // for end
    }
    SQL_PC_LOG(DEBUG, "after transform_tree", "result_tree_", SJ(ObParserResultPrintWrapper(*tree)));
  }

  return ret;
}

int ObSqlParameterization::is_fast_parse_const(TransformTreeCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (NULL == ctx.tree_) {  // parse tree exist NULL
    ctx.is_fast_parse_const_ = false;
  } else {
    if (T_HINT_OPTION_LIST == ctx.tree_->type_) {
      ctx.enable_contain_param_ = false;
    }
    if (ctx.enable_contain_param_) {
      if ((T_NULL == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_) ||
          (T_VARCHAR == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_) ||
          (T_INT == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)) {
        ctx.is_fast_parse_const_ = false;
      } else {
        ctx.is_fast_parse_const_ =
            (IS_DATATYPE_OP(ctx.tree_->type_) || T_QUESTIONMARK == ctx.tree_->type_ ||
                T_COLLATION == ctx.tree_->type_ || T_CAST_ARGUMENT == ctx.tree_->type_ ||
                (T_SFU_INT == ctx.tree_->type_ && -1 != ctx.tree_->value_) || T_SFU_DECIMAL == ctx.tree_->type_ ||
                T_LIMIT_INT == ctx.tree_->type_ || T_LIMIT_UINT == ctx.tree_->type_);
      }
    } else {
      ctx.is_fast_parse_const_ = false;
    }
  }
  return ret;
}

// Determine whether the node is a node that cannot be parameterized
bool ObSqlParameterization::is_node_not_param(TransformTreeCtx& ctx)
{
  bool not_param = false;
  if (NULL == ctx.tree_) {
    not_param = true;
  } else if (!ctx.is_fast_parse_const_) {
    not_param = true;
  } else if (ctx.not_param_) {
    not_param = true;
  } else {
    not_param = false;
  }
  return not_param;
}

// judging that the node and its child nodes are constant.
bool ObSqlParameterization::is_tree_not_param(const ParseNode* tree)
{
  bool ret_bool = false;
  if (NULL == tree) {
    ret_bool = true;
  } else if (true == tree->is_tree_not_param_) {
    ret_bool = true;
  } else if (T_LIMIT_INT == tree->type_) {
    ret_bool = true;
  } else if (T_LIMIT_UINT == tree->type_) {
    ret_bool = true;
  } else if (share::is_mysql_mode() && T_GROUPBY_CLAUSE == tree->type_) {
    ret_bool = true;
  } else if (T_SORT_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_HINT_OPTION_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_COLLATION == tree->type_) {
    ret_bool = true;
  } else if (T_CAST_ARGUMENT == tree->type_) {
    ret_bool = true;
  } else if (T_FUN_SYS_UTC_TIMESTAMP == tree->type_) {
    ret_bool = true;
  } else if (T_FUN_SYS_SYSDATE == tree->type_) {
    ret_bool = true;
  } else if (T_FUN_SYS_SYSTIMESTAMP == tree->type_) {
    ret_bool = true;
  } else if (T_FUN_SYS_CUR_TIMESTAMP == tree->type_) {
    ret_bool = true;
  } else if (T_FUN_SYS_LOCALTIMESTAMP == tree->type_) {
    ret_bool = true;
  } else if (T_FUN_SYS_CUR_TIME == tree->type_) {
    ret_bool = true;
  } else if (T_FUN_SYS_CUR_DATE == tree->type_) {
    ret_bool = true;
  } else if (T_SFU_INT == tree->type_ || T_SFU_DECIMAL == tree->type_) {
    ret_bool = true;
  } else if (T_CYCLE_NODE == tree->type_) {
    ret_bool = true;
  } else if (T_INTO_FIELD_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_INTO_LINE_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_PIVOT_IN_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_WIN_NAMED_WINDOWS == tree->type_) {
    ret_bool = true;
  } else {
    // do nothing
  }
  return ret_bool;
}

// helper method of transform_syntax_tree
int ObSqlParameterization::transform_tree(TransformTreeCtx& ctx, const ObSQLSessionInfo& session_info)
{
  int ret = OB_SUCCESS;
  int64_t value_level = NO_VALUES;
  if (OB_ISNULL(ctx.top_node_) || OB_ISNULL(ctx.allocator_) || OB_ISNULL(ctx.sql_info_) ||
      OB_ISNULL(ctx.fixed_param_store_) || OB_ISNULL(ctx.params_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN,
        "top node is NULL",
        K(ctx.top_node_),
        K(ctx.allocator_),
        K(ctx.sql_info_),
        K(ctx.fixed_param_store_),
        K(ctx.params_),
        K(ret));
  } else if (NULL == ctx.tree_) {
    // do nothing
  } else {
    ParseNode* func_name_node = NULL;
    if (T_WHERE_SCOPE == ctx.expr_scope_ && T_FUN_SYS == ctx.tree_->type_) {
      if (OB_ISNULL(ctx.tree_->children_)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_PC_LOG(WARN, "invalid argument", K(ctx.tree_->children_), K(ret));
      } else if (NULL == (func_name_node = ctx.tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_PC_LOG(ERROR, "function name node is NULL", K(ret));
      } else {
        ObString func_name(func_name_node->str_len_, func_name_node->str_value_);
        ObString func_name_is_serving_tenant(N_IS_SERVING_TENANT);
        if (func_name == func_name_is_serving_tenant) {
          ret = OB_NOT_SUPPORTED;
        }
      }
    }

    if (OB_SUCC(ret) && T_PROJECT_STRING == ctx.tree_->type_ && OB_FAIL(ctx.project_list_.push_back(ctx.tree_))) {
      LOG_WARN("failed to push back element", K(ret));
    } else if (T_PROJECT_STRING == ctx.tree_->type_) {
      ctx.tree_->raw_sql_offset_ -= ctx.sql_offset_;
    } else {
      // do nothing
    }

    if (OB_SUCC(ret)) {
      ObObjParam value;
      ObAccuracy tmp_accuracy;
      bool is_fixed = true;
      if (ctx.is_fast_parse_const_) {
        if (!is_node_not_param(ctx)) {
          // for sql current limit
          if (T_QUESTIONMARK == ctx.tree_->type_) {
            ctx.paramlized_questionmask_count_++;
            is_fixed = false;
          }

          ObString literal_prefix;
          int64_t server_collation = CS_TYPE_INVALID;
          if (share::is_oracle_mode() &&
              OB_FAIL(session_info.get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
            LOG_WARN("get sys variable failed", K(ret));
          } else if (OB_FAIL(add_param_flag(ctx.tree_, *ctx.sql_info_))) {
            SQL_PC_LOG(WARN, "fail to get neg flag", K(ret));
          } else if (OB_FAIL(ObResolverUtils::resolve_const(ctx.tree_,
                         static_cast<stmt::StmtType>(ctx.sql_info_->sql_traits_.stmt_type_),
                         *(ctx.allocator_),
                         ctx.collation_type_,
                         ctx.national_collation_type_,
                         ctx.tz_info_,
                         value,
                         ctx.is_transform_outline_,
                         literal_prefix,
                         ctx.default_length_semantics_,
                         static_cast<ObCollationType>(server_collation)))) {
            SQL_PC_LOG(WARN, "fail to resolve const", K(ret));
          } else {
            if (VALUE_VECTOR_LEVEL == ctx.value_father_level_ &&
                (0 == ctx.tree_->num_child_ || T_VARCHAR == ctx.tree_->type_ ||
                    (lib::is_oracle_mode() && T_CHAR == ctx.tree_->type_))) {
              value.set_need_to_check_type(false);
            } else {
              value.set_need_to_check_type(true);
            }
            // Record which T_VARCHAR nodes need to be converted to T_CHAR in oracle mode
            if (lib::is_oracle_mode() && T_CHAR == ctx.tree_->type_ && true == ctx.tree_->is_change_to_char_ &&
                OB_FAIL(ctx.sql_info_->change_char_index_.add_member(ctx.sql_info_->total_))) {
              SQL_PC_LOG(WARN, "failed to add member", K(ret));
            }
            if (OB_SUCC(ret) && is_fixed) {
              ObFixedParam fix_param;
              fix_param.offset_ = ctx.params_->count();
              fix_param.value_ = value;
              if (OB_FAIL(ctx.fixed_param_store_->push_back(fix_param))) {
                SQL_PC_LOG(WARN, "fail to push back fix params", K(fix_param), K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(add_varchar_charset(ctx.tree_, *ctx.sql_info_))) {
                SQL_PC_LOG(WARN, "fail to add varchar charset", K(ret));
              }
            }
            ctx.tree_->type_ = T_QUESTIONMARK;
            ctx.tree_->value_ = ctx.question_num_++;
            ctx.tree_->raw_param_idx_ = ctx.sql_info_->total_++;
            if (1 == ctx.tree_->is_num_must_be_pos_ &&
                OB_FAIL(ctx.sql_info_->must_be_positive_index_.add_member(ctx.tree_->raw_param_idx_))) {
              LOG_WARN("failed to add bitset member", K(ret));
            }
            if (OB_NOT_NULL(ctx.tree_) && OB_NOT_NULL(ctx.raw_params_) && ctx.tree_->raw_param_idx_ >= 0 &&
                ctx.tree_->raw_param_idx_ < ctx.raw_params_->count() &&
                OB_NOT_NULL(ctx.raw_params_->at(ctx.tree_->raw_param_idx_))) {
              const ParseNode* node = ctx.raw_params_->at(ctx.tree_->raw_param_idx_)->node_;
              value.set_raw_text_info(
                  static_cast<int32_t>(node->raw_sql_offset_), static_cast<int32_t>(node->text_len_));
            }
            if (OB_FAIL(ret)) {
              // do nothing
            } else if (OB_FAIL(ctx.params_->push_back(value))) {
              SQL_PC_LOG(WARN, "fail to push into params", K(ret));
            }
          }
        } else if (add_not_param_flag(ctx.tree_, *ctx.sql_info_)) {  // not param
          SQL_PC_LOG(WARN, "fail to add not param flag", K(ret));
        }
      }  // if is_fast_parse_const end
    }

    if (OB_SUCC(ret)) {
      if (T_VALUE_LIST == ctx.tree_->type_) {
        value_level = VALUE_LIST_LEVEL;
      } else if (T_VALUE_VECTOR == ctx.tree_->type_ && VALUE_LIST_LEVEL == ctx.value_father_level_) {
        value_level = VALUE_VECTOR_LEVEL;
      } else if (ctx.value_father_level_ >= VALUE_VECTOR_LEVEL) {
        value_level = ctx.value_father_level_ + 1;
      }
    }

    // transform `operand - const_num_val` to `operand + (-const_num_val)`
    if (OB_SUCC(ret) && OB_FAIL(transform_minus_op(*(ctx.allocator_), ctx.tree_))) {
      LOG_WARN("failed to transform minums operation", K(ret));
    } else if (lib::is_oracle_mode()) {
      // in oracle mode, select +-1 from dual is prohibited, but with following orders, it can be executed successfully:
      // 1. select +1 from dual; (genereted plan with key: select +? from dual)
      // 2. select +-1 from dual; (hit plan before, executed successfully)
      // Thus, add a constraint in plan cache: the numeric value following `-` or `+` operators must be posbitive number
      if (T_OP_POS == ctx.tree_->type_ || T_OP_NEG == ctx.tree_->type_) {
        if (OB_ISNULL(ctx.tree_->children_) || ctx.tree_->num_child_ != 1 || OB_ISNULL(ctx.tree_->children_[0])) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid syntax tree", K(ret));
        } else if (ob_is_numeric_type(ITEM_TO_OBJ_TYPE(ctx.tree_->children_[0]->type_))) {
          ctx.tree_->children_[0]->is_num_must_be_pos_ = 1;
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }
    }
    if (T_LIMIT_CLAUSE == ctx.tree_->type_) {
      // limit a offset b, b must be postive
      // 0 is counted as positive, -0 is counted as negative
      if (OB_ISNULL(ctx.tree_->children_) || 2 != ctx.tree_->num_child_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid syntax tree", K(ret), K(ctx.tree_->children_), K(ctx.tree_->num_child_));
      } else if (OB_NOT_NULL(ctx.tree_->children_[1]) &&
                 ob_is_numeric_type(ITEM_TO_OBJ_TYPE(ctx.tree_->children_[1]->type_))) {
        ctx.tree_->children_[1]->is_num_must_be_pos_ = 1;
      } else {
        // do nothing
      }
    } else if (T_COMMA_LIMIT_CLAUSE == ctx.tree_->type_) {
      // limit a, b, a must be postive
      if (OB_ISNULL(ctx.tree_->children_) || 2 != ctx.tree_->num_child_ || OB_ISNULL(ctx.tree_->children_[0]) ||
          OB_ISNULL(ctx.tree_->children_[1])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid syntax tree", K(ret));
      } else if (ob_is_numeric_type(ITEM_TO_OBJ_TYPE(ctx.tree_->children_[0]->type_))) {
        ctx.tree_->children_[0]->is_num_must_be_pos_ = 1;
      }
    }

    bool enable_contain_param = ctx.enable_contain_param_;
    ParseNode* root = ctx.tree_;
    bool not_param = ctx.not_param_;
    for (int32_t i = 0; OB_SUCC(ret) && i < root->num_child_ && root->type_ != T_QUESTIONMARK &&
                        root->type_ != T_VARCHAR && root->type_ != T_CHAR;
         ++i) {
      // If not_param is true, there is no need to judge;
      // because a node is judged to be true, the subtree of that node is all true;
      if (OB_ISNULL(root->children_)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_PC_LOG(WARN, "invalid argument", K(ctx.tree_->children_), K(ret));
      } else {
        if (!ctx.not_param_) {
          if (OB_ISNULL(root->children_[i]) || root->children_[i]->is_tree_not_param_) {
            // do nothing
          } else if (T_OP_NEG == root->children_[i]->type_ && 1 == root->children_[i]->num_child_) {
            if (OB_ISNULL(root->children_[i]->children_) || OB_ISNULL(root->children_[i]->children_[0])) {
              // do nothing
            } else if (T_INT != root->children_[i]->children_[0]->type_ &&
                       T_NUMBER != root->children_[i]->children_[0]->type_ &&
                       T_DOUBLE != root->children_[i]->children_[0]->type_ &&
                       T_FLOAT != root->children_[i]->children_[0]->type_ &&
                       T_VARCHAR != root->children_[i]->children_[0]->type_) {
              // do nothing
            } else if (T_VARCHAR == root->children_[i]->children_[0]->type_) {
              root->children_[i]->children_[0]->is_neg_ = 1;
            } else if ((INT64_MIN == root->children_[i]->children_[0]->value_ &&
                           T_INT == root->children_[i]->children_[0]->type_) ||
                       root->children_[i]->children_[0]->is_assigned_from_child_) {
              // select --9223372036854775808 from dual;
              // 9223372036854775809 The previous T_OP_NEG was removed in the syntax stage,
              // there is no need to insert a minus sign when converting the syntax tree
              // do nothing
            } else if (OB_FAIL(insert_neg_sign(*(ctx.allocator_), root->children_[i]->children_[0]))) {
              SQL_PC_LOG(WARN, "fail to insert neg sign", K(ret));
            } else {
              root->children_[i] = root->children_[i]->children_[0];
              root->children_[i]->is_neg_ = 1;
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (T_STMT_LIST == root->type_) {
            ctx.top_node_ = root->children_[i];
          }
          if (T_WHERE_CLAUSE == root->type_) {
            ctx.expr_scope_ = T_WHERE_SCOPE;
          }
          ctx.tree_ = root->children_[i];
          ctx.value_father_level_ = value_level;

          if (T_PROJECT_STRING == root->type_) {
            if (OB_ISNULL(ctx.tree_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid child for T_PROJECT_STRING", K(ret), K(ctx.tree_));
            } else if (T_VARCHAR == ctx.tree_->type_ || (share::is_oracle_mode() && T_CHAR == ctx.tree_->type_)) {
              // Mark this projection column constant string for subsequent escaping of the string
              ctx.tree_->is_column_varchar_ = 1;
            } else {
              // do nothing
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(mark_tree(ctx.tree_))) {
              SQL_PC_LOG(WARN, "fail to mark function tree", K(ctx.tree_), K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            if (!ctx.not_param_) {
              ctx.not_param_ = is_tree_not_param(ctx.tree_);
            }
          }

          if (OB_SUCC(ret)) {
            // Determine whether the node is a constant node considered by fast_parse
            ctx.enable_contain_param_ = enable_contain_param;
            if (OB_FAIL(is_fast_parse_const(ctx))) {
              SQL_PC_LOG(WARN, "judge is fast parse const failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(SMART_CALL(transform_tree(ctx, session_info)))) {
            if (OB_NOT_SUPPORTED != ret) {
              SQL_PC_LOG(WARN, "fail to transform tree", K(ret));
            }
          } else {
            ctx.not_param_ = not_param;
            if (T_ALIAS == root->type_ && 0 == i) {
              // alias node has only one or 0 parameters
              for (int64_t param_cnt = 0; OB_SUCC(ret) && param_cnt < root->param_num_; param_cnt++) {
                if (OB_FAIL(ctx.sql_info_->not_param_index_.add_member(ctx.sql_info_->total_++))) {
                  SQL_PC_LOG(WARN, "failed to add member", K(ctx.sql_info_->total_), K(ret));
                } else if (OB_FAIL(add_varchar_charset(root, *ctx.sql_info_))) {
                  SQL_PC_LOG(WARN, "fail to add varchar charset", K(ret));
                } else {
                  // do nothing
                }
              }  // for end
            } else {
              // do nothing
            }
          }
        }
      }
    }  // for end
  }
  return ret;
}

int ObSqlParameterization::check_and_generate_param_info(
    const ObIArray<ObPCParam*>& raw_params, const SqlInfo& sql_info, ObIArray<ObPCParam*>& special_params)
{
  int ret = OB_SUCCESS;
  if (sql_info.total_ != raw_params.count()) {
    ret = OB_NOT_SUPPORTED;
#if !defined(NDEBUG)
    SQL_PC_LOG(ERROR,
        "const number of fast parse and normal parse is different",
        "fast_parse_const_num",
        raw_params.count(),
        "normal_parse_const_num",
        sql_info.total_);
#endif
  }
  ObPCParam* pc_param = NULL;
  for (int32_t i = 0; OB_SUCC(ret) && i < raw_params.count(); i++) {
    pc_param = raw_params.at(i);
    if (OB_ISNULL(pc_param)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (OB_ISNULL(pc_param->node_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(pc_param->node_), K(ret));
    } else if (sql_info.not_param_index_.has_member(i)) {  // not param
      pc_param->flag_ = NOT_PARAM;
      if (OB_FAIL(special_params.push_back(pc_param))) {
        SQL_PC_LOG(WARN, "fail to push item to array", K(ret));
      }
    } else if (sql_info.neg_param_index_.has_member(i)) {  // neg param
      if (T_VARCHAR == pc_param->node_->type_) {
        // do nothing
      } else {
        pc_param->flag_ = NEG_PARAM;
        if (OB_FAIL(special_params.push_back(pc_param))) {
          SQL_PC_LOG(WARN, "fail to push item to array", K(ret));
        }
      }
    } else if (sql_info.trans_from_minus_index_.has_member(i)) {
      pc_param->flag_ = TRANS_NEG_PARAM;
      if (OB_FAIL(special_params.push_back(pc_param))) {
        SQL_PC_LOG(WARN, "failed to push back item to array", K(ret));
      }
    } else {
      pc_param->flag_ = NORMAL_PARAM;
    }
  }  // for end

  return ret;
}

// Long path sql parameterization
int ObSqlParameterization::parameterize_syntax_tree(common::ObIAllocator& allocator, bool is_transform_outline,
    ObPlanCacheCtx& pc_ctx, ParseNode* tree, ParamStore& params)
{
  int ret = OB_SUCCESS;
  SqlInfo sql_info;
  ObMaxConcurrentParam::FixParamStore fix_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(&allocator));
  ObSQLSessionInfo* session = NULL;
  ObSEArray<ObPCParam*, OB_PC_SPECIAL_PARAM_COUNT> special_params;
  ObSEArray<ObString, 4> user_var_names;
  int64_t reserved_cnt = pc_ctx.fp_result_.raw_params_.count();
  if (OB_ISNULL(session = pc_ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(ERROR, "got session is NULL", K(ret));
  } else if (pc_ctx.outlined_sql_len_ != pc_ctx.raw_sql_.length()) {
    // not equal means stmt is transformed from outline
    // if so, faster parser is needed
    // otherwise, fast parser has been done before
    pc_ctx.fp_result_.reset();
    if (OB_FAIL(fast_parser(allocator,
            session->get_sql_mode(),
            session->get_local_collation_connection(),
            pc_ctx.raw_sql_,
            pc_ctx.sql_ctx_.handle_batched_multi_stmt(),
            pc_ctx.fp_result_))) {
      SQL_PC_LOG(WARN, "fail to fast parser", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(params.reserve(reserved_cnt))  // Reserve array to avoid extending
             || OB_FAIL(sql_info.param_charset_type_.reserve(reserved_cnt))) {
    LOG_WARN("failed to reserve array", K(ret));
  } else if (OB_FAIL(transform_syntax_tree(allocator,
                 *session,
                 &pc_ctx.fp_result_.raw_params_,
                 pc_ctx.outlined_sql_len_ - pc_ctx.raw_sql_.length(),
                 tree,
                 sql_info,
                 params,
                 &pc_ctx.select_item_param_infos_,
                 fix_param_store,
                 is_transform_outline))) {
    if (OB_NOT_SUPPORTED != ret) {
      SQL_PC_LOG(WARN, "fail to normal parameterized parser tree", K(ret));
    }
  } else if (OB_FAIL(check_and_generate_param_info(pc_ctx.fp_result_.raw_params_, sql_info, special_params))) {
    if (OB_NOT_SUPPORTED != ret) {
      SQL_PC_LOG(WARN, "fail to check and generate param info", K(ret));
    } else {
      // do nothing
    }
  } else if (OB_FAIL(gen_special_param_info(sql_info, pc_ctx))) {
    SQL_PC_LOG(WARN, "fail to gen special param info", K(ret));
  } else if (OB_FAIL(get_related_user_vars(tree, user_var_names))) {
    LOG_WARN("failed to get related session vars", K(ret));
  } else if (OB_FAIL(pc_ctx.sql_ctx_.set_related_user_var_names(user_var_names, allocator))) {
    LOG_WARN("failed to set related user var names for sql ctx", K(ret));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    char* buf = NULL;
    int32_t pos = 0;
    buf = (char*)allocator.alloc(pc_ctx.raw_sql_.length());
    if (NULL == buf) {
      SQL_PC_LOG(WARN, "fail to alloc buf", K(pc_ctx.raw_sql_.length()));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(construct_sql(
                   pc_ctx.fp_result_.pc_key_.name_, special_params, buf, pc_ctx.raw_sql_.length(), pos))) {
      SQL_PC_LOG(WARN, "fail to construct_sql", K(ret));
    } else {
      pc_ctx.bl_key_.constructed_sql_.assign_ptr(buf, pos);
      pc_ctx.normal_parse_const_cnt_ = sql_info.total_;
    }
  }
  return ret;
}

// Generate not param info information and index information
int ObSqlParameterization::gen_special_param_info(SqlInfo& sql_info, ObPlanCacheCtx& pc_ctx)
{
  int ret = OB_SUCCESS;
  ParseNode* raw_param = NULL;
  NotParamInfo np_info;
  pc_ctx.not_param_info_.set_capacity(sql_info.not_param_index_.num_members());
  for (int32_t i = 0; OB_SUCC(ret) && i < pc_ctx.fp_result_.raw_params_.count(); i++) {
    if (OB_ISNULL(pc_ctx.fp_result_.raw_params_.at(i))) {
      ret = OB_INVALID_ARGUMENT;
    } else if (NULL == (raw_param = pc_ctx.fp_result_.raw_params_.at(i)->node_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (sql_info.not_param_index_.has_member(i)) {  // not param
      np_info.reset();
      np_info.idx_ = i;
      np_info.raw_text_ = ObString(raw_param->text_len_, raw_param->raw_text_);
      if (OB_FAIL(pc_ctx.not_param_info_.push_back(np_info))) {
        SQL_PC_LOG(WARN, "fail to push item to array", K(ret));
      }
    }
  }  // for end

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pc_ctx.not_param_index_.add_members2(sql_info.not_param_index_))) {
      LOG_WARN("fail to add not param index members", K(ret));
    } else if (OB_FAIL(pc_ctx.neg_param_index_.add_members2(sql_info.neg_param_index_))) {
      LOG_WARN("fail to add neg param index members", K(ret));
    } else if (OB_FAIL(pc_ctx.neg_param_index_.add_members2(sql_info.trans_from_minus_index_))) {
      LOG_WARN("failed to add trans from minus index members", K(ret));
    } else if (OB_FAIL(pc_ctx.param_charset_type_.assign(sql_info.param_charset_type_))) {
      LOG_WARN("fail to assign param charset type", K(ret));
    } else if (OB_FAIL(pc_ctx.must_be_positive_index_.add_members2(sql_info.must_be_positive_index_))) {
      LOG_WARN("failed to add bitset members", K(ret));
    } else {
      // change char index used in plan_set, without any not param consts
      int64_t tmp_idx = 0;
      ObBitSet<> real_change_char_index;
      for (int64_t i = 0; i < sql_info.total_; i++) {
        if (sql_info.not_param_index_.has_member(i)) {
          // do nothing
        } else if (sql_info.change_char_index_.has_member(i)) {
          if (OB_FAIL(real_change_char_index.add_member(tmp_idx))) {
            LOG_WARN("failed to add bitset member", K(ret));
          } else {
            tmp_idx++;
          }
        } else {
          tmp_idx++;
        }
      }  // for end
      pc_ctx.change_char_index_ = real_change_char_index;
    }
  }

  return ret;
}

int ObSqlParameterization::construct_sql(
    const ObString& no_param_sql, ObIArray<ObPCParam*>& pc_params, char* buf, int32_t buf_len, int32_t& pos)
{
  int ret = OB_SUCCESS;
  int32_t idx = 0;
  ObPCParam* pc_param = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < pc_params.count(); i++) {
    pc_param = pc_params.at(i);
    int32_t len = 0;
    if (OB_ISNULL(pc_param)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (NOT_PARAM == pc_param->flag_) {
      int32_t len = (int32_t)pc_param->node_->pos_ - idx;
      if (len > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
      } else if (len > 0) {
        // copy text
        MEMCPY(buf + pos, no_param_sql.ptr() + idx, len);
        idx = (int32_t)pc_param->node_->pos_ + 1;
        pos += len;
        // copy raw param
        MEMCPY(buf + pos, pc_param->node_->raw_text_, pc_param->node_->text_len_);
        pos += (int32_t)pc_param->node_->text_len_;
      }
    } else if (NEG_PARAM == pc_param->flag_) {
      len = (int32_t)pc_param->node_->pos_ - idx;
      if (len > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
      } else if (len > 0) {
        MEMCPY(buf + pos, no_param_sql.ptr() + idx, len);
        pos += len;
      }
      if (OB_SUCC(ret)) {
        idx = (int32_t)pc_param->node_->pos_ + 1;
        buf[pos++] = '?';
      }
    } else if (TRANS_NEG_PARAM == pc_param->flag_) {
      len = (int32_t)pc_param->node_->pos_ - idx;
      if (len > buf_len - pos) {
        ret = OB_BUF_NOT_ENOUGH;
      } else if (len > 0) {
        MEMCPY(buf + pos, no_param_sql.ptr() + idx, len);
        pos += len;
      }
      if (OB_SUCC(ret)) {
        // for 'select * from t where a -   1 = 2', the statement is 'select * from t where a -   ? = ?'
        // so we need fill spaces between '-' and '?', so here it is
        buf[pos++] = '-';
        int64_t tmp_pos = 0;
        for (; tmp_pos < pc_param->node_->str_len_ && isspace(pc_param->node_->str_value_[tmp_pos]); tmp_pos++)
          ;
        if (OB_UNLIKELY(tmp_pos >= pc_param->node_->str_len_)) {
          int ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(tmp_pos), K(pc_param->node_->str_len_), K(ret));
        } else {
          if ('-' != pc_param->node_->str_value_[tmp_pos]) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expected neg sign here",
                K(tmp_pos),
                K(ObString(pc_param->node_->str_len_, pc_param->node_->str_value_)));
          } else {
            tmp_pos += 1;
            for (; tmp_pos < pc_param->node_->str_len_ && isspace(pc_param->node_->str_value_[tmp_pos]); tmp_pos++) {
              buf[pos++] = ' ';
            }
          }
          if (OB_SUCC(ret)) {
            buf[pos++] = '?';
            idx = pc_param->node_->pos_ + 1;
          }
        }
      }
    } else {
      // do nothing
    }
  }  // for end

  if (OB_SUCCESS == ret) {
    int32_t len = no_param_sql.length() - idx;
    if (len > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (len > 0) {
      MEMCPY(buf + pos, no_param_sql.ptr() + idx, len);
      idx += len;
      pos += len;
    }
  }
  return ret;
}

int ObSqlParameterization::fast_parser(ObIAllocator& allocator, ObSQLMode sql_mode,
    ObCollationType connection_collation, const ObString& sql, const bool enable_batched_multi_stmt,
    ObFastParserResult& fp_result)
{
  int ret = OB_SUCCESS;
  ObParser parser(allocator, sql_mode, connection_collation);
  SMART_VAR(ParseResult, parse_result)
  {
    if (OB_FAIL(parser.parse(sql, parse_result, FP_MODE, enable_batched_multi_stmt))) {
      SQL_PC_LOG(WARN, "fail to fast parser", K(sql), K(ret));
    } else {
      (void)fp_result.pc_key_.name_.assign_ptr(parse_result.no_param_sql_, parse_result.no_param_sql_len_);
      int64_t param_num = parse_result.param_node_num_;
      // copy raw params
      if (param_num > 0) {
        ObPCParam* pc_param = NULL;
        ParamList* p_list = parse_result.param_nodes_;
        char* ptr = (char*)allocator.alloc(param_num * sizeof(ObPCParam));
        fp_result.raw_params_.set_allocator(&allocator);
        fp_result.raw_params_.set_capacity(param_num);
        if (OB_ISNULL(ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_PC_LOG(ERROR, "fail to alloc memory for pc param", K(ret), K(ptr));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < param_num && NULL != p_list; i++) {
          pc_param = new (ptr) ObPCParam();
          ptr += sizeof(ObPCParam);
          pc_param->node_ = p_list->node_;
          if (OB_FAIL(fp_result.raw_params_.push_back(pc_param))) {
            SQL_PC_LOG(WARN, "fail to push into params", K(ret));
          } else {
            p_list = p_list->next_;
          }
        }      // for end
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

// used for outline
int ObSqlParameterization::raw_fast_parameterize_sql(ObIAllocator& allocator, const ObSQLSessionInfo& session,
    const ObString& sql, ObString& no_param_sql, ObIArray<ObPCParam*>& raw_params, ParseMode parse_mode)
{
  int ret = OB_SUCCESS;
  ObParser parser(allocator, session.get_sql_mode(), session.get_local_collation_connection());
  ParseResult parse_result;

  NG_TRACE(pc_fast_parse_start);
  if (OB_FAIL(parser.parse(sql, parse_result, parse_mode, false))) {
    SQL_PC_LOG(WARN, "fail to parse query", K(ret));
  }
  NG_TRACE(pc_fast_parse_end);
  if (OB_SUCC(ret)) {
    no_param_sql.assign(parse_result.no_param_sql_, parse_result.no_param_sql_len_);
  }
  if (OB_SUCC(ret)) {
    ParamList* param = parse_result.param_nodes_;
    for (int32_t i = 0; OB_SUCC(ret) && i < parse_result.param_node_num_ && NULL != param; i++) {
      void* ptr = allocator.alloc(sizeof(ObPCParam));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_PC_LOG(ERROR, "fail to alloc memory for pc param", K(ret), K(ptr));
      } else {
        ObPCParam* pc_param = new (ptr) ObPCParam();
        pc_param->node_ = param->node_;
        if (OB_FAIL(raw_params.push_back(pc_param))) {
          SQL_PC_LOG(WARN, "fail to push into params", K(ret));
        } else {
          param = param->next_;
        }
      }
    }  // for end
  }

  SQL_PC_LOG(DEBUG, "after raw fp", K(parse_result.param_node_num_));
  return ret;
}

int ObSqlParameterization::insert_neg_sign(ObIAllocator& alloc_buf, ParseNode* node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(node), K(ret));
  } else if (T_INT != node->type_ && T_DOUBLE != node->type_ && T_FLOAT != node->type_ && T_NUMBER != node->type_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(WARN, "invalid neg digit type", K(node->type_), K(ret));
  } else {
    if (T_INT == node->type_) {
      node->value_ = -node->value_;
    }
    char* new_str = static_cast<char*>(parse_malloc(node->str_len_ + 2, &alloc_buf));
    char* new_raw_text = static_cast<char*>(parse_malloc(node->text_len_ + 2, &alloc_buf));
    if (OB_ISNULL(new_str) || OB_ISNULL(new_raw_text)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_PC_LOG(ERROR, "parse_strdup failed", K(ret), K(new_raw_text), K(new_str));
    } else {
      new_str[0] = '-';
      new_raw_text[0] = '-';
      MEMMOVE(new_str + 1, node->str_value_, node->str_len_);
      MEMMOVE(new_raw_text + 1, node->raw_text_, node->text_len_);
      new_str[node->str_len_ + 1] = '\0';
      new_raw_text[node->text_len_ + 1] = '\0';

      node->str_value_ = new_str;
      node->raw_text_ = new_raw_text;
      node->str_len_++;
      node->text_len_++;
    }
  }
  return ret;
}

int ObSqlParameterization::add_param_flag(const ParseNode* node, SqlInfo& sql_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (1 == node->is_neg_) {
    if (OB_FAIL(sql_info.neg_param_index_.add_member(sql_info.total_))) {
      SQL_PC_LOG(WARN, "failed to add neg param index", K(sql_info.total_), K(ret));
    }
  } else if (node->is_trans_from_minus_) {
    if (OB_FAIL(sql_info.trans_from_minus_index_.add_member(sql_info.total_))) {
      SQL_PC_LOG(WARN, "failed to add trans_from_minus index", K(sql_info.total_), K(ret));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObSqlParameterization::add_not_param_flag(const ParseNode* node, SqlInfo& sql_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (T_CAST_ARGUMENT == node->type_ || T_COLLATION == node->type_) {
    for (int i = 0; OB_SUCC(ret) && i < node->param_num_; ++i) {
      if (OB_FAIL(sql_info.not_param_index_.add_member(sql_info.total_++))) {
        SQL_PC_LOG(WARN, "failed to add member", K(sql_info.total_));
      } else if (OB_FAIL(add_varchar_charset(node, sql_info))) {
        SQL_PC_LOG(WARN, "fail to add varchar charset", K(ret));
      }
    }
  } else {
    if (OB_FAIL(sql_info.not_param_index_.add_member(sql_info.total_++))) {
      SQL_PC_LOG(WARN, "failed to add member", K(sql_info.total_));
    } else if (OB_FAIL(add_varchar_charset(node, sql_info))) {
      SQL_PC_LOG(WARN, "fail to add varchar charset", K(ret));
    }
  }

  return ret;
}

// T_FUN_SYS function
int ObSqlParameterization::mark_args(ParseNode* arg_tree, const bool* mark_arr, int64_t arg_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg_tree) || OB_ISNULL(mark_arr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret), K(arg_tree));
  } else if (OB_ISNULL(arg_tree->children_) || arg_num != arg_tree->num_child_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret), K(arg_tree->type_), K(arg_tree->children_), K(arg_tree->num_child_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_num; i++) {
      if (true == mark_arr[i]) {
        if (OB_ISNULL(arg_tree->children_[i])) {
          ret = OB_INVALID_ARGUMENT;
          SQL_PC_LOG(WARN, "invalid argument", K(ret), K(arg_tree->children_[i]));
        } else {
          arg_tree->children_[i]->is_tree_not_param_ = true;
        }
      }
    }
  }
  return ret;
}

// Mark the nodes that cannot be parameterized and require special marking.
// Generally, nodes that cannot be directly identified by
// the node type can be marked in this function.

// Mark those special nodes that cannot be parameterized.
// After mark this node, it has following mechanism:
//       If a node is marked as cannot be parameterized,
//       CUREENT NODE AND ALL NODES OF IT'S SUBTREE cannot be parameterized.
int ObSqlParameterization::mark_tree(ParseNode* tree)
{
  int ret = OB_SUCCESS;
  if (NULL == tree) {
    // do nothing
  } else if (T_FUN_SYS == tree->type_) {
    ParseNode** node = tree->children_;  // node[0] : func name, node[1]: arg list
    if (2 != tree->num_child_) {
      // do nothing
    } else if (OB_ISNULL(node) || OB_ISNULL(node[0]) || OB_ISNULL(node[1])) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret), K(node));
    } else {
      ObString func_name(node[0]->str_len_, node[0]->str_value_);
      if ((0 == func_name.case_compare("USERENV") || 0 == func_name.case_compare("UNIX_TIMESTAMP")) &&
          (1 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_ONE = 1;
        bool mark_arr[ARGS_NUMBER_ONE] = {1};  // 0 for parameterized, 1 Means not parameterized
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_ONE))) {
          SQL_PC_LOG(WARN, "fail to mark arg", K(ret));
        }
      } else if (0 == func_name.case_compare("substr") && (3 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_THREE = 3;
        bool mark_arr[ARGS_NUMBER_THREE] = {0, 1, 1};
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_THREE))) {
          SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
        }
      } else if ((0 == func_name.case_compare("str_to_date")        // STR_TO_DATE(str,format)
                     || 0 == func_name.case_compare("date_format")  // DATE_FORMAT(date,format)
                     ||
                     0 == func_name.case_compare(
                              "from_unixtime")  // FROM_UNIXTIME(unix_timestamp), FROM_UNIXTIME(unix_timestamp,format)
                     || 0 == func_name.case_compare("round")  // ROUND(X), ROUND(X,D)
                     || 0 == func_name.case_compare("substr") ||
                     0 == func_name.case_compare("dbms_lob_convert_clob_charset") ||
                     0 == func_name.case_compare("truncate")) &&
                 (2 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_TWO = 2;
        bool mark_arr[ARGS_NUMBER_TWO] = {0, 1};
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_TWO))) {
          SQL_PC_LOG(WARN, "fail to mark arg", K(ret));
        }
      }
    }
  } else if (T_OP_LIKE == tree->type_) {
    if (2 == tree->num_child_) {  // child[0] like child[1]
      const int64_t ARGS_NUMBER_TWO = 2;
      bool mark_arr[ARGS_NUMBER_TWO] = {0, 1};  // 0 for parameterized, 1 Means not parameterized
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_TWO))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    } else if (3 == tree->num_child_) {  // child[0] like child[1] escape child[2]
      const int64_t ARGS_NUMBER_THREE = 3;
      bool mark_arr[ARGS_NUMBER_THREE] = {0, 1, 1};  // 0 for parameterized, 1 Means not parameterized
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_THREE))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    }
  } else if (T_OP_IS == tree->type_ || T_OP_IS_NOT == tree->type_) {
    if (tree->num_child_ == 2) {
      const int64_t ARGS_NUMBER_TWO = 2;
      bool mark_arr[ARGS_NUMBER_TWO] = {0, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_TWO))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    } else { /*do nothing*/
    }
  } else { /*do nothing*/
  }
  return ret;
}

int ObSqlParameterization::add_varchar_charset(const ParseNode* node, SqlInfo& sql_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(node));
  } else if (T_VARCHAR == node->type_ && NULL != node->children_ && NULL != node->children_[0] &&
             T_CHARSET == node->children_[0]->type_) {
    ObString charset(node->children_[0]->str_len_, node->children_[0]->str_value_);
    ObCharsetType charset_type = CHARSET_INVALID;
    if (CHARSET_INVALID == (charset_type = ObCharset::charset_type(charset.trim()))) {
      ret = OB_ERR_UNKNOWN_CHARSET;
      LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
    } else if (OB_FAIL(sql_info.param_charset_type_.push_back(charset_type))) {
      SQL_PC_LOG(WARN, "fail to add charset type", K(ret));
    }
  } else if (OB_FAIL(sql_info.param_charset_type_.push_back(CHARSET_INVALID))) {
    SQL_PC_LOG(WARN, "fail to add charset type", K(ret));
  }

  return ret;
}

int ObSqlParameterization::get_related_user_vars(const ParseNode* tree, common::ObIArray<common::ObString>& user_vars)
{
  int ret = OB_SUCCESS;
  ObString var_str;
  if (tree == NULL) {
    // do nothing
  } else {
    if (T_USER_VARIABLE_IDENTIFIER == tree->type_) {
      if (OB_ISNULL(tree->str_value_) || tree->str_len_ <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(tree->str_value_), K(tree->str_len_));
      } else {
        var_str.assign_ptr(tree->str_value_, static_cast<int32_t>(tree->str_len_));
        if (OB_FAIL(user_vars.push_back(var_str))) {
          LOG_WARN("failed to push back user variable", K(ret));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tree->num_child_; i++) {
        if (OB_ISNULL(tree->children_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(tree->children_), K(ret));
        } else if (OB_FAIL(SMART_CALL(get_related_user_vars(tree->children_[i], user_vars)))) {
          LOG_WARN("failed to get related user vars", K(ret), K(tree->children_[i]), K(i));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    user_vars.reset();
  }

  return ret;
}

int ObSqlParameterization::get_select_item_param_info(
    const common::ObIArray<ObPCParam*>& raw_params, ParseNode* tree, SelectItemParamInfoArray* select_item_param_infos)
{
  int ret = OB_SUCCESS;
  SelectItemParamInfo param_info;
  ObString org_field_name;
  int64_t expr_pos = tree->raw_sql_offset_;
  int64_t buf_len = SelectItemParamInfo::PARAMED_FIELD_BUF_LEN;
  ObSEArray<TraverseStackFrame, 64> stack_frames;

  if (T_PROJECT_STRING != tree->type_ || OB_ISNULL(tree->children_) || tree->num_child_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tree->type_), K(tree->children_), K(tree->num_child_));
  } else if (T_ALIAS == tree->children_[0]->type_ ||
             T_STAR == tree->children_[0]->type_) {  // have alias name, or is a '*', do not need parameterized
    // do nothing
  } else if (OB_FAIL(stack_frames.push_back(TraverseStackFrame{tree, 0}))) {
    LOG_WARN("failed to push back element", K(ret));
  } else {
    // start to construct paramed field name template...
    if (share::is_oracle_mode()) {
      org_field_name.assign_ptr(tree->raw_text_, (int32_t)tree->text_len_);
    } else {
      org_field_name.assign_ptr(tree->str_value_, (int32_t)tree->str_len_);
    }

    SelectItemTraverseCtx ctx(raw_params, tree, org_field_name, tree->raw_sql_offset_, buf_len, expr_pos, param_info);
    for (; OB_SUCC(ret) && stack_frames.count() > 0;) {
      int64_t frame_idx = stack_frames.count() - 1;
      ctx.tree_ = stack_frames.at(frame_idx).cur_node_;
      if (NULL == ctx.tree_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null node", K(ret), K(ctx.tree_));
      } else if (1 == ctx.tree_->is_val_paramed_item_idx_ || T_QUESTIONMARK == ctx.tree_->type_ ||
                 stack_frames.at(frame_idx).next_child_idx_ >= ctx.tree_->num_child_) {
        if (T_QUESTIONMARK == ctx.tree_->type_) {
          if (ctx.param_info_.name_len_ >= ctx.buf_len_) {
            // The length of the column is full,
            // no need to continue to construct the template, just jump out
            break;
          } else if (OB_FAIL(resolve_paramed_const(ctx))) {
            LOG_WARN("failed to resolve paramed const", K(ret));
          } else {
            // do nothing
          }
        }
        // pop stack
        stack_frames.pop_back();
        --frame_idx;
        LOG_DEBUG("after popping frame", K(stack_frames), K(frame_idx));
      } else {
        // do nothing
      }

      if (OB_FAIL(ret) || frame_idx < 0) {
        // do nothing
      } else if (stack_frames.at(frame_idx).cur_node_->num_child_ > 0) {
        if (OB_ISNULL(stack_frames.at(frame_idx).cur_node_->children_)) {
          ret = OB_INVALID_ARGUMENT;
          SQL_PC_LOG(
              WARN, "invalid null children", K(ret), K(stack_frames.at(frame_idx).cur_node_->children_), K(frame_idx));
        } else {
          TraverseStackFrame& frame = stack_frames.at(frame_idx);
          for (int64_t i = frame.next_child_idx_; OB_SUCC(ret) && i < frame.cur_node_->num_child_; i++) {
            if (OB_ISNULL(frame.cur_node_->children_[i])) {
              frame.next_child_idx_ = i + 1;
            } else if (OB_FAIL(stack_frames.push_back(TraverseStackFrame{frame.cur_node_->children_[i], 0}))) {
              LOG_WARN("failed to push back eleemnt", K(ret));
            } else {
              frame.next_child_idx_ = i + 1;
              LOG_DEBUG("after pushing frame", K(stack_frames));
              break;
            }
          }  // for end
        }
      }
    }  // for end

    if (OB_SUCC(ret)) {
      int64_t res_start_pos = expr_pos - tree->raw_sql_offset_;
      int64_t tmp_len = std::min(org_field_name.length() - res_start_pos, buf_len - param_info.name_len_);
      if (tmp_len > 0) {
        int32_t len = static_cast<int32_t>(tmp_len);
        MEMCPY(param_info.paramed_field_name_ + param_info.name_len_, org_field_name.ptr() + res_start_pos, len);
        param_info.name_len_ += len;
      }

      if (T_QUESTIONMARK == tree->children_[0]->type_ && 1 == tree->children_[0]->is_column_varchar_) {
        param_info.esc_str_flag_ = true;
      }
    }
  }

  if (OB_FAIL(ret) || 0 == param_info.params_idx_.count()) {
    // do nothing
  } else if (OB_FAIL(select_item_param_infos->push_back(param_info))) {
    SQL_PC_LOG(WARN, "failed to push back element", K(ret));
  } else {
    tree->value_ = select_item_param_infos->count() - 1;
    tree->is_val_paramed_item_idx_ = 1;

    LOG_DEBUG("add a paramed info", K(param_info));
  }

  return ret;
}

int ObSqlParameterization::resolve_paramed_const(SelectItemTraverseCtx& ctx)
{
  int ret = OB_SUCCESS;
  int64_t idx = ctx.tree_->raw_param_idx_;
  if (idx >= ctx.raw_params_.count()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret), K(idx), K(ctx.raw_params_.count()));
  } else if (OB_ISNULL(ctx.raw_params_.at(idx)) || OB_ISNULL(ctx.raw_params_.at(idx)->node_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid arguemnt", K(ret));
  } else {
    const ParseNode* param_node = ctx.raw_params_.at(idx)->node_;
    int64_t tmp_len = std::min(ctx.buf_len_ - ctx.param_info_.name_len_, param_node->raw_sql_offset_ - ctx.expr_pos_);
    if (tmp_len > 0) {
      int32_t len = static_cast<int64_t>(tmp_len);
      MEMCPY(ctx.param_info_.paramed_field_name_ + ctx.param_info_.name_len_,
          ctx.org_expr_name_.ptr() + ctx.expr_pos_ - ctx.expr_start_pos_,
          len);
      ctx.param_info_.name_len_ += len;
    }
    ctx.expr_pos_ = param_node->raw_sql_offset_ + param_node->text_len_;
    if (OB_FAIL(ctx.param_info_.questions_pos_.push_back(ctx.param_info_.name_len_))) {
      SQL_PC_LOG(WARN, "failed to push back element", K(ret));
    } else if (OB_FAIL(ctx.param_info_.params_idx_.push_back(idx))) {
      SQL_PC_LOG(WARN, "failed to push back element", K(ret));
    } else {
      if (ctx.param_info_.name_len_ < ctx.buf_len_) {
        ctx.param_info_.paramed_field_name_[ctx.param_info_.name_len_++] = '?';
      }
      if (ctx.tree_->is_neg_ && OB_FAIL(ctx.param_info_.neg_params_idx_.add_member(idx))) {
        SQL_PC_LOG(WARN, "failed to add member", K(ret), K(idx));
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("resolve a paramed const",
            K(ctx.expr_pos_),
            K(ctx.expr_start_pos_),
            K(ctx.org_expr_name_),
            K(param_node->raw_sql_offset_));
      }
    }
  }
  return ret;
}

int ObSqlParameterization::transform_minus_op(ObIAllocator& alloc, ParseNode* tree)
{
  int ret = OB_SUCCESS;
  if (T_OP_MINUS != tree->type_) {
    // do nothing
  } else if (2 != tree->num_child_ || OB_ISNULL(tree->children_) || OB_ISNULL(tree->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid minus tree", K(ret));
  } else if (1 == tree->children_[1]->is_assigned_from_child_) {
    // select 1 - (2) from dual;
    // select 1 - (2/3/4) from dual;
    // do nothing
  } else if (ob_is_numeric_type(ITEM_TO_OBJ_TYPE(tree->children_[1]->type_)) && tree->children_[1]->value_ >= 0) {
    ParseNode* child = tree->children_[1];
    tree->type_ = T_OP_ADD;

    if (T_INT == child->type_) {
      child->value_ = -child->value_;
    }

    char* new_str = static_cast<char*>(parse_malloc(child->str_len_ + 2, &alloc));
    char* new_raw_text = static_cast<char*>(parse_malloc(child->text_len_ + 2, &alloc));
    if (OB_ISNULL(new_str) || OB_ISNULL(new_raw_text)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(new_raw_text), K(new_str));
    } else {
      new_str[0] = '-';
      new_raw_text[0] = '-';
      MEMMOVE(new_str + 1, child->str_value_, child->str_len_);
      MEMMOVE(new_raw_text + 1, child->raw_text_, child->text_len_);
      new_str[child->str_len_ + 1] = '\0';
      new_raw_text[child->text_len_ + 1] = '\0';

      child->str_len_++;
      child->text_len_++;
      child->str_value_ = new_str;
      child->raw_text_ = new_raw_text;
      child->is_trans_from_minus_ = 1;
    }
  } else if (T_OP_MUL == tree->children_[1]->type_ || T_OP_DIV == tree->children_[1]->type_ ||
             (share::is_mysql_mode() && T_OP_MOD == tree->children_[1]->type_)) {
    // '0 - 2 * 3' should be transformed to '0 + (-2) * 3'
    // '0 - 2 / 3' should be transformed to '0 + (-2) / 3'
    // '0 - 4 mod 3' should be transformed to '0 + (-4 mod 3)'
    // '0 - 2/3/4' => '0 + (-2/3/4)'
    // notify that, syntax tree of '0 - 2/3/4' is
    //           -
    //         /   \
    //        0    div
    //            /   \
    //          div    4
    //         /   \
    //        2     3
    // so, we need to find the leftest leave node and change its value and str
    // same for '%','*', mod
    //
    // in oracle mode, select 1 - mod(mod(3, 4), 2) from dual;
    // The syntax tree is:
    //      -
    //    /  \
    //   1   mod
    //      /   \
    //    mod    2
    //   /  \
    //  3    4
    //  This syntax tree is the same as select 1-3%4%2 from dual in mysql mode,
    //  but-and 3 cannot be combined in oracle mode
    //
    ParseNode* const_node = NULL;
    ParseNode* op_node = tree->children_[1];
    if (OB_FAIL(find_leftest_const_node(*op_node, const_node))) {
      LOG_WARN("failed to find leftest const node", K(ret));
    } else if (OB_ISNULL(const_node)) {
      // do nothing
    } else {
      tree->type_ = T_OP_ADD;
      if (T_INT == const_node->type_) {
        const_node->value_ = -const_node->value_;
      }
      char* new_str = static_cast<char*>(parse_malloc(const_node->str_len_ + 2, &alloc));
      char* new_raw_text = static_cast<char*>(parse_malloc(const_node->text_len_ + 2, &alloc));

      if (OB_ISNULL(new_str) || OB_ISNULL(new_raw_text)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret), K(new_str), K(new_raw_text));
      } else {
        new_str[0] = '-';
        new_raw_text[0] = '-';
        MEMMOVE(new_str + 1, const_node->str_value_, const_node->str_len_);
        MEMMOVE(new_raw_text + 1, const_node->raw_text_, const_node->text_len_);
        new_str[const_node->str_len_ + 1] = '\0';
        new_raw_text[const_node->text_len_] = '\0';

        const_node->str_len_++;
        const_node->text_len_++;
        const_node->str_value_ = new_str;
        const_node->raw_text_ = new_raw_text;
        const_node->is_trans_from_minus_ = 1;
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObSqlParameterization::find_leftest_const_node(ParseNode& cur_node, ParseNode*& const_node)
{
  int ret = OB_SUCCESS;
  if (ob_is_numeric_type(ITEM_TO_OBJ_TYPE(cur_node.type_)) && 0 == cur_node.is_assigned_from_child_) {
    const_node = &cur_node;
  } else if (1 == cur_node.is_assigned_from_child_) {
    // do nothing
  } else if (T_OP_MUL == cur_node.type_ || T_OP_DIV == cur_node.type_ || T_OP_MOD == cur_node.type_) {
    //  for 1 - (2-3)/4
    //     -
    //    / \
    //   1  div
    //     /  \
    //    -    4
    //   / \
    //  2   3
    // the syntax tree tree cannot be converted
    // So only T_OP_MUL, T_OP_DIV and T_OP_MOD go on this path
    if (OB_ISNULL(cur_node.children_) || 2 != cur_node.num_child_ || OB_ISNULL(cur_node.children_[0]) ||
        OB_ISNULL(cur_node.children_[1])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument");
    } else if (OB_FAIL(find_leftest_const_node(*cur_node.children_[0], const_node))) {
      LOG_WARN("failed to find leftest const node", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}
