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
#include "sql/parser/ob_fast_parser.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/parser/parse_malloc.h"
#include "sql/ob_sql_utils.h"
#include "common/ob_smart_call.h"
#include <algorithm>

using namespace oceanbase;
using namespace sql;
using namespace common;
using namespace share::schema;

SqlInfo::SqlInfo() :
  total_(0),
  last_active_time_(0),
  hit_count_(0),
  ps_need_parameterized_(true),
  need_check_fp_(false)
{
}

namespace oceanbase {
namespace sql {
struct TransformTreeCtx
{
  ObIAllocator *allocator_;
  ParseNode *top_node_;
  ParseNode *tree_;
  ObStmtScope expr_scope_;
  int64_t value_father_level_;
  ObCollationType collation_type_;
  ObCollationType national_collation_type_;
  const ObTimeZoneInfo *tz_info_;
  int64_t question_num_;
  ParamStore *params_;
  ObMaxConcurrentParam::FixParamStore *fixed_param_store_;
  bool not_param_; //表示该节点及其子节点是常量时也不能参数化
  bool is_fast_parse_const_; //表示当前node是否为fp可识别的常量
  bool enable_contain_param_;//表示该节点及其子节点中常量是否为fp可识别的参数。
  SqlInfo *sql_info_;
  int64_t paramlized_questionmask_count_;//表示该查询中可以被参数化的?个数，用于sql限流
  bool is_transform_outline_;//是否在resolve outline, 用于sql限流
  ObLengthSemantics default_length_semantics_;
  ObSEArray<void*, 16> project_list_; // 记录下所有T_PROJECT_STRING节点
  const ObIArray<ObPCParam *> *raw_params_;
  SQL_EXECUTION_MODE mode_;
  bool is_project_list_scope_;
  int64_t assign_father_level_;
  const ObIArray<FixedParamValue> *udr_fixed_params_;
  bool ignore_scale_check_;
  bool is_from_pl_;
  TransformTreeCtx();
};

struct SelectItemTraverseCtx
{
  const common::ObIArray<ObPCParam *> &raw_params_;
  const ParseNode *tree_;
  const ObString &org_expr_name_;
  const int64_t expr_start_pos_;
  const int64_t buf_len_;
  int64_t &expr_pos_;
  SelectItemParamInfo &param_info_;

  SelectItemTraverseCtx(const common::ObIArray<ObPCParam *> &raw_params,
                        const ParseNode *tree,
                        const ObString &org_expr_name,
                        const int64_t expr_start_pos,
                        const int64_t buf_len,
                        int64_t &expr_pos,
                        SelectItemParamInfo &param_info)
    : raw_params_(raw_params), tree_(tree), org_expr_name_(org_expr_name),
      expr_start_pos_(expr_start_pos), buf_len_(buf_len), expr_pos_(expr_pos), param_info_(param_info) {}
};

struct TraverseStackFrame
{
  const ParseNode *cur_node_;
  int64_t next_child_idx_;

  TO_STRING_KV(K_(next_child_idx),
               K(cur_node_->type_));
};

}
}

TransformTreeCtx::TransformTreeCtx() :
 allocator_(NULL),
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
 raw_params_(NULL),
 mode_(INVALID_MODE),
 is_project_list_scope_(false),
 assign_father_level_(ObSqlParameterization::NO_VALUES),
 udr_fixed_params_(NULL),
 ignore_scale_check_(false),
 is_from_pl_(false)
{
}

// replace const expr with ? in syntax tree
// separate params from syntax tree
int ObSqlParameterization::transform_syntax_tree(ObIAllocator &allocator,
                                                 const ObSQLSessionInfo &session,
                                                 const ObIArray<ObPCParam *> *raw_params,
                                                 ParseNode *tree,
                                                 SqlInfo &sql_info,
                                                 ParamStore &params,
                                                 SelectItemParamInfoArray *select_item_param_infos,
                                                 ObMaxConcurrentParam::FixParamStore &fixed_param_store,
                                                 bool is_transform_outline,
                                                 SQL_EXECUTION_MODE execution_mode,
                                                 const ObIArray<FixedParamValue> *udr_fixed_params,
                                                 bool is_from_pl)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ParseNode *children_node = tree->children_[0];
  if (OB_ISNULL(tree)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(tree), K(ret));
  } else if (OB_ISNULL(children_node)){
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
    ctx.paramlized_questionmask_count_ = 0;//used for outline sql限流，
    ctx.is_transform_outline_ = is_transform_outline;//used for outline sql限流
    ctx.raw_params_ = raw_params;
    ctx.udr_fixed_params_ = udr_fixed_params;
    ctx.is_project_list_scope_ = false;
    ctx.mode_ = execution_mode;
    ctx.assign_father_level_ = NO_VALUES;
    ctx.is_from_pl_ = is_from_pl;

    if (OB_FAIL(transform_tree(ctx, session))) {
      if (OB_NOT_SUPPORTED != ret) {
        SQL_PC_LOG(WARN, "fail to transform syntax tree", K(ret));
      }
    } else if (is_transform_outline && (INT64_MAX != children_node->value_)
               && (children_node->value_ != ctx.paramlized_questionmask_count_)) {
      ret = OB_INVALID_OUTLINE;
      LOG_USER_ERROR(OB_INVALID_OUTLINE, "? appears at invalid position in sql_text");
      SQL_PC_LOG(WARN, "? appear at invalid position", "total count of questionmasks in query", children_node->value_,
                "paramlized_questionmask_count", ctx.paramlized_questionmask_count_, K(ret));
    } else if (OB_NOT_NULL(raw_params)
               && OB_NOT_NULL(select_item_param_infos)) {
      if (sql_info.total_ != raw_params->count()) {
        ret = OB_NOT_SUPPORTED;
        SQL_PC_LOG(TRACE, "const number of fast parse and normal parse is different",
                "fast_parse_const_num", raw_params->count(),
                "normal_parse_const_num", sql_info.total_,
                K(session.get_current_query_string()),
                "result_tree_", SJ(ObParserResultPrintWrapper(*ctx.top_node_)));
      } else {
        select_item_param_infos->set_capacity(ctx.project_list_.count());
        for (int64_t i = 0; OB_SUCC(ret) && i < ctx.project_list_.count(); i++) {
          ParseNode *tmp_root = static_cast<ParseNode *>(ctx.project_list_.at(i));
          if (OB_ISNULL(tmp_root)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid null child", K(ret), K(i), K(ctx.project_list_.at(i)));
          } else if (0 == tmp_root->is_val_paramed_item_idx_
                     && OB_FAIL(get_select_item_param_info(*raw_params,
                                                           tmp_root,
                                                           select_item_param_infos,
                                                           session))) {
            SQL_PC_LOG(WARN, "failed to get select item param info", K(ret));
          } else {
            // do nothing
          }
        } // for end
      }
    }
    SQL_PC_LOG(DEBUG, "after transform_tree",
               "result_tree_", SJ(ObParserResultPrintWrapper(*tree)));
  }
  return ret;
}

//判断是否是fast parse能识别的常量(只考虑本node)
//
//T_CAST_ARGUMENT,T_FUN_SYS_CUR_TIMESTAMP, T_SFU_INT
//该类型node在快速参数化时是常数(eg:cast ( 10 AS CHAR(20)中20, now(10))；
//但正常parse时node结点类型为不在常数范围,
//该node不需要参数化，但为了保证正常parse和fast parse常量个数匹配，所以加入特殊判断;
//该参数在正常parse时处理方式和order by中参数相同;
//
//T_SFU_INT 和T_FUN_SYS_UTC_TIME时判段为0 或-1，
//是因为now()和FOR UPDATE没有参数，但语法会给默认值,加判断是为了让正常parse不认为该值为常量。
int ObSqlParameterization::is_fast_parse_const(TransformTreeCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (NULL == ctx.tree_) { //parse tree中存在node为NULL
    ctx.is_fast_parse_const_ = false;
  } else {
    //如果其节点为T_HINT_OPTION_LIST则该节点及其子节点中常量都不是fp可识别的参数参数。
    if (T_HINT_OPTION_LIST == ctx.tree_->type_) {
      ctx.enable_contain_param_ = false;
    }
    if (ctx.enable_contain_param_) {
      //当该node为T_NULL/T_VARCHAR类型且is_hidden_const_为true时表示该node在fast parse中不能识别为常量
      if ((T_NULL == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || (T_VARCHAR == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || (T_INT == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || (T_CAST_ARGUMENT == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || (T_DOUBLE == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || (T_IEEE754_INFINITE == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || (T_IEEE754_NAN == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || (T_SFU_INT == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || (T_FLOAT == ctx.tree_->type_ && true == ctx.tree_->is_hidden_const_)
          || true == ctx.tree_->is_forbid_parameter_) {
        ctx.is_fast_parse_const_ = false;
      } else {
        ctx.is_fast_parse_const_ = (IS_DATATYPE_OP(ctx.tree_->type_)
                                    || T_QUESTIONMARK == ctx.tree_->type_
                                    || T_COLLATION == ctx.tree_->type_
                                    || T_CAST_ARGUMENT == ctx.tree_->type_
                                    || T_NULLX_CLAUSE == ctx.tree_->type_
                                    || (T_SFU_INT == ctx.tree_->type_ && -1 != ctx.tree_->value_)
                                    || T_SFU_DECIMAL == ctx.tree_->type_
                                    || T_WEIGHT_STRING_LEVEL_PARAM == ctx.tree_->type_);
      }
    } else {
      ctx.is_fast_parse_const_ = false;
    }
  }
  return ret;
}

bool ObSqlParameterization::is_udr_not_param(TransformTreeCtx &ctx)
{
  bool b_ret = false;
  if (OB_ISNULL(ctx.tree_) || (NULL == ctx.udr_fixed_params_)) {
    b_ret = false;
  } else {
    for (int64_t i = 0; !b_ret && i < ctx.udr_fixed_params_->count(); ++i) {
      const FixedParamValue &fixed_param = ctx.udr_fixed_params_->at(i);
      if (fixed_param.idx_ == ctx.tree_->raw_param_idx_) {
        b_ret = true;
        break;
      }
    }
  }
  return b_ret;
}

//判断该node是否为不能参数化的node
bool ObSqlParameterization::is_node_not_param(TransformTreeCtx &ctx)
{
  bool not_param = false;
  if (NULL == ctx.tree_) {
    not_param = true;
  } else if (!ctx.is_fast_parse_const_) {
    not_param = true;
  } else if (ctx.not_param_) { //如果其本身或其祖先已经判定了所有子节点为not param, 则为not param
    not_param = true;
  } else {
    not_param = false;
  }
  return not_param;
}

//判断该节点及其子节点是常量时也不能参数化。
bool ObSqlParameterization::is_tree_not_param(const ParseNode *tree)
{
  bool ret_bool = false;
  if (NULL == tree) {
    ret_bool = true;
  } else if (true == tree->is_tree_not_param_) {
    ret_bool = true;
  } else if (lib::is_mysql_mode() && T_GROUPBY_CLAUSE == tree->type_) {
    // oracle模式下，select a from t group by 1这种语法被禁止，所以可以开放group by的参数化
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
  } else if (T_FUN_SYS_UTC_TIME == tree->type_) {
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
  } else if (T_SFU_INT == tree->type_ ||
             T_SFU_DECIMAL == tree->type_ ||
             T_SFU_DOUBLE == tree->type_) {
    ret_bool = true;
  } else if (T_CYCLE_NODE == tree->type_) {
    ret_bool = true;
  } else if (T_INTO_FIELD_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_INTO_LINE_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_INTO_FILE_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_PIVOT_IN_LIST == tree->type_) {
    ret_bool = true;
  } else if (T_CHAR_CHARSET == tree->type_) {
    ret_bool = true;
  } else if (T_WIN_NAMED_WINDOWS == tree->type_) {//name window无法参数化，因为无法保证其参数化顺序
    ret_bool = true;
  } else {
    // do nothing
  }
  return ret_bool;
}

SQL_EXECUTION_MODE ObSqlParameterization::get_sql_execution_mode(ObPlanCacheCtx &pc_ctx)
{
  SQL_EXECUTION_MODE mode = INVALID_MODE;
  if (PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_) {
    if (pc_ctx.is_parameterized_execute_) {
      mode = (PC_PL_MODE == pc_ctx.mode_) ? PL_EXECUTE_MODE : PS_EXECUTE_MODE;
    } else {
      mode = (PC_PL_MODE == pc_ctx.mode_) ? PL_PREPARE_MODE : PS_PREPARE_MODE;
    }
  } else {
    mode = TEXT_MODE;
  }
  return mode;
}

bool ObSqlParameterization::is_prepare_mode(SQL_EXECUTION_MODE mode)
{
  return (PS_PREPARE_MODE == mode || PL_PREPARE_MODE == mode);
}

bool ObSqlParameterization::is_execute_mode(SQL_EXECUTION_MODE mode)
{
  return (PS_EXECUTE_MODE == mode || PL_EXECUTE_MODE == mode);
}

/* fix:
* decide if a number param can ignore scale check when choosing plancache.
* if current node type is expr list or number,
* and parent node is point, st_point, json array, or ctx.ignore_scale_check_ is true, return true,
* otherwise return false.
* example1 scale check could ignore:
*   'select point(1.1, 1.1)' and 'select point(1.1111, 1.1111)' could share a same plancache,
*   scale check about number literal in them are ignored
* example2 scale check cannot ignore:
*   'select point(cast(1.11 as double), 1.1)' and 'select point(cast(1.1111 as double), 1.1111)'
*   because 1.11 and 1.1111 in cast expr have different scales,
*   scale check will prevent them to share a same plancache
*/
bool ObSqlParameterization::is_ignore_scale_check(TransformTreeCtx &ctx, const ParseNode *parent)
{
  bool ret_bool = false;
  if (OB_ISNULL(parent) || OB_ISNULL(ctx.tree_)) {
    // do nothing
  } else if (T_EXPR_LIST == ctx.tree_->type_ || T_NUMBER == ctx.tree_->type_) {
    if (T_NUMBER == ctx.tree_->type_ && ctx.ignore_scale_check_) {
      ret_bool = true;
    } else if (T_FUN_SYS_POINT == parent->type_) {
      ret_bool = true;
    } else if (T_FUN_SYS == parent->type_) {
      ParseNode **node = parent->children_;
      if (OB_ISNULL(node) || OB_ISNULL(node[0])) {
        // do nothing
      } else {
        ObString func_name(node[0]->str_len_, node[0]->str_value_);
        if ((0 == func_name.case_compare("st_point"))) {
          ret_bool = true;
        }
      }
    } else { /* do nothing*/ }
  } else { /* do nothing*/ }
  return ret_bool;
}

// helper method of transform_syntax_tree
//正常parse和fast parse识别常量不一致的问题
/*
* 1.replace/insert的列带括号为空时导致正常parse和fast parse识别常量不一致
*   eg:replace into t2() values(40, 1, 'good')
*   原因：主要是因为opt_insert_columns，含括号，但为空时，会默认生成T_NULL节点，导致正常parse和fast parse识别常量不一致。
*   解法：将T_NULL类型改成T_EMPTY类型即可，在resolver阶段并没有对该类型识别，而是使用这种判断进行处理if （T_COLUMN_LIST ==）else {//处理为空的情况}
*2.default()导致的正常parse和fast parse常量不一致
*   eg :select * from type_t where int_c = default(int_c)
*   原因：default()在语法阶段生成4个T_NULL类型的null_node，在resolve阶段会填充具体的内容，导致不一致。
*   解法：将生成的null_node中is_hidden_const_字段置1， 在transform tree阶段，判断node为T_NULL类型且is_hidden_const_为true时表示该node在fast parse中不能识别为常量，从而不参数化，保证正常parse和fast parse识别常量一致。
*3.using charset_name导致的正常parse和fast parse识别常量不一致
*  eg:select convert('1234' using 'utf8’);
*  eg:select convert('1234' using utf8);
*  原因：using ‘utf8’ 和using utf8均会生成都会生成一个T_VARCHAR类型的节点，对于using utf8, fast parse不能识别出常量，因此导致正常parse和fast parse识别不一致。
*  解法：将using utf8生成的T_VARCHAR节点中is_hidden_const_字段置1，在transform tree阶段，判断node为T_VARCHAR类型且is_hidden_const_为true时表示该node在fast parse中不能识别为常量，从而不参数化，保证正常parse和fast parse识别常量一致。
*4.month等date_unit导致的正常parse和fast parse识别常量个数不一致
*  eg:SELECT month(NULL);
*  select * from type_t where date_c = date_add('2015-01-01', interval 5 month)
*  问题：在语法阶段，mouth，day等date_unit是解析为T_INT类型的节点，导致正常parse与fast parse识别不一致。之前的方案是将mouth等date_unit在词法阶段识别出来，并生成对应的 T_INT类型节点解决该问题。但这导致mouth()作为函数或当sql中存在以month等date_unit命名的table或unit时会导致正常parse与fast parse识别常量不一致的问题。
*  解法：将month,day等date_unit生成的T_INT节点中is_hidden_const_字段置1，在transform tree阶段，判断node为T_INT类型且is_hidden_const_为true时表示该node在fast parse中不能识别为常量，从而不参数化，保证正常parse和fast parse识别常量一致。
*5.as STRING_VALUE导致的正常parse和fast parse识别常量不一致
*  eg: select 'abc' as a;
*      select 'abc' as ‘a’;
*  问题：as a 和 as ‘a’ 在语法阶段会生成T_ALIAS节点，导致对于as ‘a’在fast parse能识别一个常量，而正常parse不能识别出常量。
*  解法：在T_ALIAS节点中记录对应的fast parse能够识别的常量个数（0个或1个），在transform时，如果是T_ALIAS节点，则将其判断为不能参数化的常量，并将对应的常量个数记下，从而保证下次fast parse时能够识别的常量的个数一致。
*
*6. 问题: 当函数中存在格式字符串时, 格式字符串不需要参数化, eg:STR_TO_DATE, FROM_UNIXTIME, DATE_FORMAT
*  解法: 在参数化过程中，遍历parse tree，识别到T_FUN_SYS节点后，识别第一个children（函数名）,然后遍历所有child，并对含格式串的函数中格式化参数节点及其子节点(该格式串可能由函数生成)均做不需要参数化标记。
*
* 7. 问题: weight_string函数在包含level参数时，level可以是变长的、结构化的数据，此时normal_parser会将level的常数进行扁平化处理（只有一个T_INT类型的ParseNode），但是fast_parser会识别到所有的数字，导致两者常量数量不一致
*    eg:
*       select weight_string("AAA" as char(1) level 1-2 );
*       select weight_string("AAA" as char(1) level 1,2,3,4,5,6,7,8,9,10,11 );
*       select weight_string("AAA" as char(1) level 1 desc,2 asc ,3 desc ,4 reverse,5,6,7,8,9 reverse,10,11 );
*    解法：创建一个新的item_type(T_WEIGHT_STRING_LEVEL_PARAM) ，根据不同的语法来设置不同的param_num_,这样normal_parser就可以和faster_parser相等了，等到具体处理T_WEIGHT_STRING_LEVEL_PARAM的时候,把它转换为T_INT进行后续的处理。
*/
int ObSqlParameterization::transform_tree(TransformTreeCtx &ctx,
                                          const ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  int64_t value_level = NO_VALUES;
  int64_t assign_level = NO_VALUES;
  ObCompatType compat_type = COMPAT_MYSQL57;
  if (OB_ISNULL(ctx.top_node_)
      || OB_ISNULL(ctx.allocator_)
      || OB_ISNULL(ctx.sql_info_)
      || OB_ISNULL(ctx.fixed_param_store_)
      || OB_ISNULL(ctx.params_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "top node is NULL",
               K(ctx.top_node_),
               K(ctx.allocator_),
               K(ctx.sql_info_),
               K(ctx.fixed_param_store_),
               K(ctx.params_),
               K(ret));
  } else if (OB_FAIL(session_info.get_compatibility_control(compat_type))) {
    LOG_WARN("failed to get compat type", K(ret));
  } else if (NULL == ctx.tree_) {
    // do nothing
  } else {
    ParseNode *func_name_node = NULL;
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
          LOG_WARN("is_serving_tenant is not supported", K(ret));
        }
      }
    }
    bool enable_decimal_int = false;
    if (OB_FAIL(ret)) {
    } else if (T_PROJECT_STRING == ctx.tree_->type_
        && OB_FAIL(ctx.project_list_.push_back(ctx.tree_))) {
      LOG_WARN("failed to push back element", K(ret));
    } else if (OB_FAIL(ObSQLUtils::check_enable_decimalint(&session_info, enable_decimal_int))) {
      LOG_WARN("fail to check enable decimal int", K(ret));
    } else {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      ObObjParam value;
      ObAccuracy tmp_accuracy;
      bool is_fixed = true;
      if (ctx.is_fast_parse_const_) { //这里面需要获取fast parse识别为常量的所有信息
        if (!is_node_not_param(ctx)) { //判定是否为可以参数化的常量
          //for sql 限流
          ParseNode* node = NULL;
          if (OB_NOT_NULL(ctx.raw_params_) &&
              ctx.tree_->value_ < ctx.raw_params_->count() &&
              T_QUESTIONMARK == ctx.tree_->type_ &&
              !is_prepare_mode(ctx.mode_)) {
            node = ctx.raw_params_->at(ctx.tree_->value_)->node_;
          } else {
            node = ctx.tree_;
          }
          if (T_QUESTIONMARK == ctx.tree_->type_) {
            ctx.paramlized_questionmask_count_++;
            is_fixed = false;
          }

          ObString literal_prefix;
          int64_t server_collation = CS_TYPE_INVALID;
          if (lib::is_oracle_mode() &&OB_FAIL(
            session_info.get_sys_variable(share::SYS_VAR_COLLATION_SERVER, server_collation))) {
            LOG_WARN("get sys variable failed", K(ret));
          } else if (OB_FAIL(add_param_flag(ctx.tree_, *ctx.sql_info_))) {
            SQL_PC_LOG(WARN, "fail to get neg flag", K(ret));
          } else if (OB_FAIL(ObResolverUtils::resolve_const(node,
                              static_cast<stmt::StmtType>(ctx.sql_info_->sql_traits_.stmt_type_),
                              *(ctx.allocator_),
                              ctx.collation_type_,
                              ctx.national_collation_type_,
                              ctx.tz_info_,
                              value,
                              ctx.is_transform_outline_,
                              literal_prefix,
                              ctx.default_length_semantics_,
                              static_cast<ObCollationType>(server_collation),
                              NULL, session_info.get_sql_mode(),
                              enable_decimal_int,
                              compat_type,
                              ctx.is_from_pl_))) {
            SQL_PC_LOG(WARN, "fail to resolve const", K(ret));
          } else {
            //对于字符串值，其T_VARCHAR型的parse node有一个T_VARCHAR类型的子node，该子node描述字符串的charset等信息。
            //因此对于可参数化的参数，当该节点的父节点为T_VALUE_VECTOR, 且类型为T_VARCHAR或没有子node时，认为是单值(不属于复杂表达式中参数);
            //此时将value中单值参数标记为不需要在plan cache中强匹配类型的参数
            if ((VALUE_VECTOR_LEVEL == ctx.value_father_level_
                || ASSIGN_ITEM_LEVEL == ctx.assign_father_level_)
                && (0 == ctx.tree_->num_child_
                    || T_VARCHAR == ctx.tree_->type_
                    || (lib::is_oracle_mode() && T_CHAR == ctx.tree_->type_)
                    || T_QUESTIONMARK == ctx.tree_->type_)) {
              if (T_QUESTIONMARK == ctx.tree_->type_) {
                if (OB_FAIL(ctx.sql_info_->no_check_type_offsets_.push_back(ctx.tree_->value_))) {
                  SQL_PC_LOG(WARN, "failed to add no check type offsets", K(ret));
                }
              } else {
                value.set_need_to_check_type(false);
              }
            } else {
              if (T_QUESTIONMARK == ctx.tree_->type_) {
                if (OB_FAIL(ctx.sql_info_->need_check_type_param_offsets_.add_member(ctx.tree_->value_))) {
                  SQL_PC_LOG(WARN, "failed to add member", K(ctx.tree_->value_));
                }
              } else {
                value.set_need_to_check_type(true); 
              }
              // if value type is decimal int, it should not ignore scale check
              if (ctx.ignore_scale_check_ && !value.is_decimal_int()) {
                value.set_ignore_scale_check(true);
              }
            }
            //用于sql限流，记录哪些参数需要严格比对
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
            if (OB_SUCC(ret) && ctx.tree_->type_ != T_QUESTIONMARK) {
              if (OB_FAIL(ctx.sql_info_->fixed_param_idx_.push_back(ctx.question_num_))) {
                SQL_PC_LOG(WARN, "failed to add question mark idx", K(ret));
              }
            }
            ctx.tree_->is_literal_bool_ = (T_BOOL == ctx.tree_->type_);
            value.set_is_boolean(value.is_boolean() || ctx.tree_->is_literal_bool_);
            if (T_QUESTIONMARK != ctx.tree_->type_ && ctx.is_project_list_scope_) {
              ctx.sql_info_->ps_need_parameterized_ = false;
            }
            // If it is internal sql, such as pl internal sql, the formatted text string is used.
            // because it is necessary to replace the sql variable in pl with the standard ps text
            // for hard parsing. In this case, the param store has been constructed in advance
            // so the constant cannot be resolved as a question mark, and there is no need to
            // add it to the param store.
            ObItemType node_type;
            if (!is_execute_mode(ctx.mode_)) {
              node_type = ctx.tree_->type_;
              ctx.tree_->type_ = T_QUESTIONMARK;
              ctx.tree_->raw_param_idx_ = ctx.sql_info_->total_;
              ctx.tree_->value_ = ctx.question_num_;
            }
            ctx.question_num_++;
            ctx.sql_info_->total_++;
            if (1 == ctx.tree_->is_num_must_be_pos_ && OB_SUCC(ret)
                && OB_FAIL(ctx.sql_info_->must_be_positive_index_.add_member(ctx.tree_->raw_param_idx_))) {
              LOG_WARN("failed to add bitset member", K(ret));
            }
            if (OB_NOT_NULL(ctx.tree_) &&
                OB_NOT_NULL(ctx.raw_params_) &&
                ctx.tree_->raw_param_idx_ >= 0 &&
                ctx.tree_->raw_param_idx_ < ctx.raw_params_->count() &&
                OB_NOT_NULL(ctx.raw_params_->at(ctx.tree_->raw_param_idx_))) {
              const ParseNode *node = ctx.raw_params_->at(ctx.tree_->raw_param_idx_)->node_;
              value.set_raw_text_info(static_cast<int32_t>(node->raw_sql_offset_),
                                      static_cast<int32_t>(node->text_len_));
              if (ctx.sql_info_->need_check_fp_) {
                ObPCParseInfo p_info;
                p_info.param_idx_ = ctx.sql_info_->total_ - 1;
                p_info.flag_ = NORMAL_PARAM;
                p_info.raw_text_pos_ = ctx.tree_->sql_str_off_;
                if (ctx.tree_->sql_str_off_ == -1) {
                  ret = OB_NOT_SUPPORTED;
                  LOG_WARN("invalid str off", K(lbt()), K(ctx.tree_),
                      K(ctx.tree_->raw_param_idx_), K(get_type_name(node_type)),
                      K(session_info.get_current_query_string()),
                      "result_tree_", SJ(ObParserResultPrintWrapper(*ctx.top_node_)));
                }
                if (OB_FAIL(ret)) {
                  // do nothing
                } else if (OB_FAIL(ctx.sql_info_->parse_infos_.push_back(p_info))) {
                  SQL_PC_LOG(WARN, "fail to push parser info", K(ret));
                }
              }
            }
            if (OB_FAIL(ret)) {
              //do nothing
            } else if (!is_execute_mode(ctx.mode_) && OB_FAIL(ctx.params_->push_back(value))) {
              SQL_PC_LOG(WARN, "fail to push into params", K(ret));
            } else if (is_udr_not_param(ctx) && OB_FAIL(add_not_param_flag(ctx.tree_, *ctx.sql_info_))) {
              SQL_PC_LOG(WARN, "fail to add not param flag", K(ret));
            }
          }
        } else if (OB_FAIL(add_not_param_flag(ctx.tree_, *ctx.sql_info_))) { //not param
          SQL_PC_LOG(WARN, "fail to add not param flag", K(ret));
        }
        if (ctx.sql_info_->need_check_fp_ && ret == OB_NOT_SUPPORTED) {
          LOG_WARN("print tree", K(session_info.get_current_query_string()),
              "result_tree_", SJ(ObParserResultPrintWrapper(*ctx.top_node_)));
        }
      } //if is_fast_parse_const end
    }

    // sql with charset need not ps parameterize
    if (OB_SUCC(ret)) {
      if (T_QUESTIONMARK == ctx.tree_->type_ && OB_NOT_NULL(ctx.tree_->children_)
          && OB_NOT_NULL(ctx.tree_->children_[0]) && ctx.tree_->children_[0]->type_ == T_CHARSET) {
        ctx.sql_info_->ps_need_parameterized_ = false;
      } else if (T_INTO_OUTFILE == ctx.tree_->type_) {
        ctx.sql_info_->ps_need_parameterized_ = false;
      }
    }

    //判断insert中values()在tree中的哪一层，当某结点value_father_level_处于VALUE_VECTOR_LEVEL时,
    //便可通过判断该节点的num_child_数是否为0,来判断values中的项是否为复杂表达式；
    if (OB_SUCC(ret)) {
      if (T_VALUE_LIST == ctx.tree_->type_) {
        value_level = VALUE_LIST_LEVEL;
      } else if (T_VALUE_VECTOR == ctx.tree_->type_
                 && VALUE_LIST_LEVEL == ctx.value_father_level_) {
        value_level = VALUE_VECTOR_LEVEL;
      } else if (ctx.value_father_level_ >= VALUE_VECTOR_LEVEL) {
        value_level = ctx.value_father_level_ + 1;
      }
    }
    if (OB_SUCC(ret)) {
      if (T_ASSIGN_LIST == ctx.tree_->type_) {
        assign_level = ASSIGN_LIST_LEVEL;
      } else if (T_ASSIGN_ITEM == ctx.tree_->type_
                 && ASSIGN_LIST_LEVEL == ctx.assign_father_level_) {
        assign_level = ASSIGN_ITEM_LEVEL;
      } else if (ctx.assign_father_level_ >= ASSIGN_ITEM_LEVEL) {
        assign_level = ctx.assign_father_level_ + 1;
      }
    }

    // transform `operand - const_num_val` to `operand + (-const_num_val)`
    if (OB_SUCC(ret) && OB_FAIL(transform_minus_op(*(ctx.allocator_), ctx.tree_, ctx.is_from_pl_))) {
      LOG_WARN("failed to transform minus operation", K(ret));
    } else if (lib::is_oracle_mode()) {
      // in oracle mode, select +-1 from dual is prohibited, but with following orders, it can be executed successfully:
      // 1. select +1 from dual; (generated plan with key: select +? from dual)
      // 2. select +-1 from dual; (hit plan before, executed successfully)
      // Thus, add a constraint in plan cache: the numeric value following `-` or `+` operators must be positive number
      if (T_OP_POS == ctx.tree_->type_ || T_OP_NEG == ctx.tree_->type_ ) {
        if (OB_ISNULL(ctx.tree_->children_)
            || ctx.tree_->num_child_ != 1
            || OB_ISNULL(ctx.tree_->children_[0])) {
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
      // limit a offset b, a and b must be positive
      // 0 is counted as positive, -0 is counted as negative
      if (OB_ISNULL(ctx.tree_->children_) || 2 != ctx.tree_->num_child_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid syntax tree", K(ret),
                 K(ctx.tree_->children_), K(ctx.tree_->num_child_));
      } else if (OB_NOT_NULL(ctx.tree_->children_[0])
                 && ob_is_numeric_type(ITEM_TO_OBJ_TYPE(ctx.tree_->children_[0]->type_))
                 && FALSE_IT(ctx.tree_->children_[0]->is_num_must_be_pos_ = 1)) {
      } else if (OB_NOT_NULL(ctx.tree_->children_[1])
                 && ob_is_numeric_type(ITEM_TO_OBJ_TYPE(ctx.tree_->children_[1]->type_))
                 && FALSE_IT(ctx.tree_->children_[1]->is_num_must_be_pos_ = 1)) {
      }
    } else if (T_COMMA_LIMIT_CLAUSE == ctx.tree_->type_) {
      // limit a, b, a and b must be positive
      if (OB_ISNULL(ctx.tree_->children_)
          || 2 != ctx.tree_->num_child_
          || OB_ISNULL(ctx.tree_->children_[0])
          || OB_ISNULL(ctx.tree_->children_[1])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid syntax tree", K(ret));
      } else if (ob_is_numeric_type(ITEM_TO_OBJ_TYPE(ctx.tree_->children_[0]->type_))
                 && FALSE_IT(ctx.tree_->children_[0]->is_num_must_be_pos_ = 1)) {
      } else if (ob_is_numeric_type(ITEM_TO_OBJ_TYPE(ctx.tree_->children_[1]->type_))
                 && FALSE_IT(ctx.tree_->children_[1]->is_num_must_be_pos_ = 1)) {
      }
    }

    bool enable_contain_param = ctx.enable_contain_param_;
    ParseNode *root = ctx.tree_;
    //当type为T_QUESTIONMARK时不需要再考虑子节点的参数化,
    //对于select '1'的T_VARCHAR和T_CHAR(oracle模式下) node有数据一样的T_VARCHAR子节点，
    //由于select中投影列不需要参数化，所以会导致正常parse识别有两个常量，
    //而fast parse只识别一个常量,所以在此加T_VARCHAR的判断, 使得两种parse均只能识别一个常量。
    bool not_param = ctx.not_param_;
    if (not_param) {
      ctx.sql_info_->ps_need_parameterized_ = false;
    }
    bool is_project_list_scope = T_PROJECT_LIST == root->type_;
    if (is_project_list_scope) {
      ctx.is_project_list_scope_ = true;
    }
    bool ignore_scale_check = ctx.ignore_scale_check_;
    for (int32_t i = 0;
         OB_SUCC(ret) && i < root->num_child_ && root->type_ != T_QUESTIONMARK
         && root->type_ != T_VARCHAR && root->type_ != T_CHAR && root->type_ != T_NCHAR;
         ++i) {
      //如果not_param本来就是true则不需要再判断；因为某结点判断为true，则该结点子树均为true；
      if (OB_ISNULL(root->children_)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_PC_LOG(WARN, "invalid argument", K(ctx.tree_->children_), K(ret));
      } else {
        if (!ctx.not_param_) { //如果该节点及其子节点的常量均不为参数, 则不需要进行替换
          //对T_OP_NEG节点进行改写,解决正负数不匹配问题
          //如果是T_OP_NEG->T_INT节点，则改成T_INT节点且值乘-1;
          //如果是T_OP_NEG->T_DOUBLE或T_NUMBER, 则改成T_DOUBLE或T_NUMBER节点的str前加－
          if (OB_ISNULL(root->children_[i])
              || root->children_[i]->is_tree_not_param_) { //如果该children在mark tree时已标记为not_param, 则不需要处理neg
            //do nothing
          } else if (T_OP_NEG == root->children_[i]->type_ && 1 == root->children_[i]->num_child_) {
            if (OB_ISNULL(root->children_[i]->children_) || OB_ISNULL(root->children_[i]->children_[0])) {
              //do nothing
            } else if (T_INT != root->children_[i]->children_[0]->type_
                       && T_NUMBER != root->children_[i]->children_[0]->type_
                       && T_DOUBLE != root->children_[i]->children_[0]->type_
                       && T_FLOAT != root->children_[i]->children_[0]->type_
                       && T_VARCHAR != root->children_[i]->children_[0]->type_) {
              //do nothing
            } else if (T_VARCHAR == root->children_[i]->children_[0]->type_) { //T_VARCHAR不需要insert neg
              root->children_[i]->children_[0]->is_neg_ = 1;
            } else if ((INT64_MIN == root->children_[i]->children_[0]->value_
                       && T_INT == root->children_[i]->children_[0]->type_)
                         || root->children_[i]->children_[0]->is_assigned_from_child_) {
              // select --9223372036854775808 from dual;
              // 9223372036854775809 之前的T_OP_NEG在语法阶段就被移除，这里在转换语法树的时候不需要再插入负号
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
          if (T_WHERE_CLAUSE == root->type_) { //目前只处理where clause
            ctx.expr_scope_ = T_WHERE_SCOPE;
          }
          ctx.tree_ = root->children_[i];
          ctx.value_father_level_ = value_level;
          ctx.assign_father_level_ = assign_level;

          if (T_PROJECT_STRING == root->type_) {
            if (OB_ISNULL(ctx.tree_)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid child for T_PROJECT_STRING", K(ret), K(ctx.tree_));
            } else if (T_VARCHAR == ctx.tree_->type_
                       || (lib::is_oracle_mode() && T_CHAR == ctx.tree_->type_)) {
              // 标记这一个投影列是一个常量字符串，以便后续转义字符串
              ctx.tree_->is_column_varchar_ = 1;
            } else {
              // do nothing
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(mark_tree(ctx.tree_ , *ctx.sql_info_))) {
              SQL_PC_LOG(WARN, "fail to mark function tree", K(ctx.tree_), K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            //如果not_param本来就是true则不需要再判断；因为某结点判断为true，则该结点子树均为true;
            if (!ctx.not_param_) {
              ctx.not_param_ = is_tree_not_param(ctx.tree_);
            }
          }

          if (OB_SUCC(ret)) {
            //判断该节点是否为fast_parse 认为的常量节点
            ctx.enable_contain_param_ = enable_contain_param;
            if (OB_FAIL(is_fast_parse_const(ctx))) {
              SQL_PC_LOG(WARN, "judge is fast parse const failed", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            ctx.ignore_scale_check_ = is_ignore_scale_check(ctx, root);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(SMART_CALL(transform_tree(ctx, session_info)))) {
            if (OB_NOT_SUPPORTED != ret) {
              SQL_PC_LOG(WARN, "fail to transform tree", K(ret));
            }
          } else {
            ctx.not_param_ = not_param;
            ctx.ignore_scale_check_ = ignore_scale_check;
            // select a + 1 as 'a' from t where b = ?; 'a' in SQL will be recognized as a constant by fast parser
            // In the ps parameterization scenario, 'a' will be added to the param store as a fixed parameter in
            // the execute phase. The param store has two parameters, causing correctness problems.
            // Therefore, the scene ps parameterization ability of specifying aliases needs to be disabled.
            if (T_ALIAS == root->type_ && NULL != root->str_value_) {
              ctx.sql_info_->ps_need_parameterized_ = false;
            }
            if (T_ALIAS == root->type_ && 0 == i) {
              // alias node的param_num_处理必须等到其第一个子节点转换完之后
              // select a + 1 as 'a'，'a'不能被参数化，但是它在raw_params数组内的下标必须是计算了1的下标之后才能得到
              // alias node只有一个或者0个参数
              for (int64_t param_cnt = 0; OB_SUCC(ret) && param_cnt < root->param_num_; param_cnt++) {
                if (OB_FAIL(ctx.sql_info_->not_param_index_.add_member(ctx.sql_info_->total_++))) {
                  SQL_PC_LOG(WARN, "failed to add member", K(ctx.sql_info_->total_), K(ret));
                } else if (OB_FAIL(add_varchar_charset(root, *ctx.sql_info_))) {
                  SQL_PC_LOG(WARN, "fail to add varchar charset", K(ret));
                } else {
                  if (ctx.sql_info_->need_check_fp_) {
                    ObPCParseInfo p_info;
                    p_info.param_idx_ = ctx.sql_info_->total_ - 1;
                    p_info.flag_ = NOT_PARAM;
                    p_info.raw_text_pos_ = root->sql_str_off_;
                    if (root->sql_str_off_ == -1) {
                      ret = OB_NOT_SUPPORTED;
                      LOG_WARN("invalid str off", K(lbt()), K(ctx.tree_),
                          K(root->raw_param_idx_), K(get_type_name(root->type_)),
                          K(session_info.get_current_query_string()),
                          "result_tree_", SJ(ObParserResultPrintWrapper(*ctx.top_node_)));
                    }
                    if (OB_FAIL(ret)) {
                      // do nothing
                    } else if (OB_FAIL(ctx.sql_info_->parse_infos_.push_back(p_info))) {
                      SQL_PC_LOG(WARN, "fail to push parser info", K(ret));
                    }
                  }
                }
              } // for end
            } else {
              // do nothing
            }
          }
        }
      }
    } // for end
    if (is_project_list_scope) {
      ctx.is_project_list_scope_ = false;
    }
  }
  return ret;
}

int ObSqlParameterization::check_and_generate_param_info(const ObIArray<ObPCParam *> &raw_params,
                                                         const SqlInfo &sql_info,
                                                         ObIArray<ObPCParam *> &special_params)
{
  int ret = OB_SUCCESS;
  if (sql_info.total_ != raw_params.count()) {
    ret = OB_NOT_SUPPORTED;
    if (sql_info.need_check_fp_) {
      SQL_PC_LOG(ERROR, "const number of fast parse and normal parse is different",
                "fast_parse_const_num", raw_params.count(),
                "normal_parse_const_num", sql_info.total_);
    }
  }

  if (OB_FAIL(ret)) {

  } else {
    ObPCParam *pc_param = NULL;
    for (int32_t i = 0; OB_SUCC(ret) && i < raw_params.count(); i ++) {
      //not param 和neg param不可能是同一个param, 在transform_tree时已保证
      pc_param = raw_params.at(i);
      if (OB_ISNULL(pc_param)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_PC_LOG(WARN, "invalid argument", K(ret));
      } else if (OB_ISNULL(pc_param->node_)) {
        ret = OB_INVALID_ARGUMENT;
        SQL_PC_LOG(WARN, "invalid argument", K(pc_param->node_), K(ret));
      } else if (sql_info.not_param_index_.has_member(i)) { //not param
        pc_param->flag_ = NOT_PARAM;
        if (OB_FAIL(special_params.push_back(pc_param))) {
          SQL_PC_LOG(WARN, "fail to push item to array", K(ret));
        }
      } else if (sql_info.neg_param_index_.has_member(i)) {//neg param
        //如果是T_VARCHAR则不需要记录为负数, ?sql也不需要合并-?
        if (T_VARCHAR == pc_param->node_->type_) {
          //do nothing
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
    } // for end

    if (sql_info.need_check_fp_) {
      // do nothing
      int last_pos = -1;
      int last_param_idx = -1;

      for (int i = 0; OB_SUCC(ret) && i < sql_info.parse_infos_.count(); i++) {
        if (last_pos > sql_info.parse_infos_.at(i).raw_text_pos_ ||
            last_param_idx > sql_info.parse_infos_.at(i).param_idx_) {
          ret = OB_NOT_SUPPORTED;
          SQL_PC_LOG(ERROR, "invalid parse order", K(last_param_idx), K(last_pos),
                                                   K(sql_info.parse_infos_.at(i).raw_text_pos_),
                                                   K(sql_info.parse_infos_.at(i).param_idx_),
                                                   K(sql_info.parse_infos_), K(ret));
        } else {
          last_pos = sql_info.parse_infos_.at(i).raw_text_pos_;
          last_param_idx = sql_info.parse_infos_.at(i).param_idx_;
        }
      }
    }
  }

  return ret;
}

//长路径sql参数化
int ObSqlParameterization::parameterize_syntax_tree(common::ObIAllocator &allocator,
                                                    bool is_transform_outline,
                                                    ObPlanCacheCtx &pc_ctx,
                                                    ParseNode *tree,
                                                    ParamStore &params,
                                                    ObCharsets4Parser charsets4parser)
{
  int ret = OB_SUCCESS;
  SqlInfo sql_info;
  bool need_parameterized = false;
  SQL_EXECUTION_MODE mode = get_sql_execution_mode(pc_ctx);
  ObMaxConcurrentParam::FixParamStore fix_param_store(OB_MALLOC_NORMAL_BLOCK_SIZE,
                                                      ObWrapperAllocator(&allocator));
  ObSQLSessionInfo *session = NULL;
  ObSEArray<ObPCParam *, OB_PC_SPECIAL_PARAM_COUNT> special_params;
  ObSEArray<ObString, 4> user_var_names;

  int tmp_ret = OB_SUCCESS;
  tmp_ret = OB_E(EventTable::EN_SQL_PARAM_FP_NP_NOT_SAME_ERROR) OB_SUCCESS;
  if (OB_SUCCESS != tmp_ret) {
    sql_info.need_check_fp_ = true;
  }
  int64_t reserved_cnt = 0;
  if (OB_ISNULL(session = pc_ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_PC_LOG(ERROR, "got session is NULL", K(ret));
  } else if (is_prepare_mode(mode)
            || is_transform_outline
#ifdef OB_BUILD_SPM
            || pc_ctx.sql_ctx_.spm_ctx_.is_retry_for_spm_
#endif
            ) {
    // if so, faster parser is needed
    // otherwise, fast parser has been done before
    pc_ctx.fp_result_.reset();
    FPContext fp_ctx(charsets4parser);
    fp_ctx.enable_batched_multi_stmt_ = pc_ctx.sql_ctx_.handle_batched_multi_stmt();
    fp_ctx.sql_mode_ = session->get_sql_mode();
    fp_ctx.is_udr_mode_ = pc_ctx.is_rewrite_sql_;
    fp_ctx.def_name_ctx_ = pc_ctx.def_name_ctx_;
    ObString raw_sql = pc_ctx.raw_sql_;
    if (pc_ctx.sql_ctx_.is_do_insert_batch_opt()) {
      raw_sql = pc_ctx.insert_batch_opt_info_.new_reconstruct_sql_;
    } else if (pc_ctx.exec_ctx_.has_dynamic_values_table()) {
      raw_sql = pc_ctx.new_raw_sql_;
    }
    if (OB_FAIL(fast_parser(allocator,
                            fp_ctx,
                            raw_sql,
                            pc_ctx.fp_result_))) {
      SQL_PC_LOG(WARN, "fail to fast parser", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (FALSE_IT(reserved_cnt = pc_ctx.fp_result_.raw_params_.count())) {
  } else if (!is_execute_mode(mode)
             && (OB_FAIL(params.reserve(reserved_cnt)) // Reserve array to avoid extending
             || OB_FAIL(sql_info.param_charset_type_.reserve(reserved_cnt))
             || OB_FAIL(sql_info.fixed_param_idx_.reserve(reserved_cnt)))) {
    LOG_WARN("failed to reserve array", K(ret));
  } else if (OB_FAIL(transform_syntax_tree(allocator,
                                           *session,
                                           is_execute_mode(mode) ? NULL : &pc_ctx.fp_result_.raw_params_,
                                           tree,
                                           sql_info,
                                           params,
                                           is_prepare_mode(mode) ? NULL : &pc_ctx.select_item_param_infos_,
                                           fix_param_store,
                                           is_transform_outline,
                                           mode,
                                           &pc_ctx.fixed_param_info_list_))) {
    if (OB_NOT_SUPPORTED != ret) {
      SQL_PC_LOG(WARN, "fail to normal parameterized parser tree", K(ret));
    }
  } else {
    need_parameterized = (!(PC_PS_MODE == pc_ctx.mode_ || PC_PL_MODE == pc_ctx.mode_)
                          || (is_prepare_mode(mode) && sql_info.ps_need_parameterized_));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_related_user_vars(tree, user_var_names))) {
    LOG_WARN("failed to get related session vars", K(ret));
  } else if (OB_FAIL(pc_ctx.sql_ctx_.set_related_user_var_names(user_var_names, allocator))) {
    LOG_WARN("failed to set related user var names for sql ctx", K(ret));
  } else if (is_execute_mode(mode)) {
    if (OB_FAIL(gen_ps_not_param_var(sql_info.ps_not_param_offsets_, params, pc_ctx))) {
      SQL_PC_LOG(WARN, "fail to gen ps not param var", K(ret));
    } else if (OB_FAIL(construct_no_check_type_params(sql_info.no_check_type_offsets_,
                                                      sql_info.need_check_type_param_offsets_,
                                                      params))) {
      SQL_PC_LOG(WARN, "fail to construct no check type params", K(ret));
    }
  } else if (need_parameterized) {
    if (OB_FAIL(check_and_generate_param_info(pc_ctx.fp_result_.raw_params_,
                                              sql_info,
                                              special_params))) {
      if (OB_NOT_SUPPORTED != ret) {
        SQL_PC_LOG(WARN, "fail to check and generate param info", K(ret));
      } else if (sql_info.need_check_fp_) {
        SQL_PC_LOG(INFO, "print tree", K(session->get_current_query_string()), "result_tree_", SJ(ObParserResultPrintWrapper(*tree)));
      } else {
        // do nothing
      }
    } else if (OB_FAIL(gen_special_param_info(sql_info, pc_ctx))) {
      SQL_PC_LOG(WARN, "fail to gen special param info", K(ret));
    } else {
      // do nothing
    }
    if (OB_SUCC(ret)) {
      char *buf = NULL;
      int32_t pos = 0;
      buf = (char *)allocator.alloc(pc_ctx.raw_sql_.length());
      if (NULL == buf) {
        SQL_PC_LOG(WARN, "fail to alloc buf", K(pc_ctx.raw_sql_.length()));
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(construct_sql(pc_ctx.fp_result_.pc_key_.name_, special_params, buf, pc_ctx.raw_sql_.length(), pos))) {
        SQL_PC_LOG(WARN, "fail to construct_sql", K(ret));
      } else if (is_prepare_mode(mode) && OB_FAIL(transform_neg_param(pc_ctx.fp_result_.raw_params_))) {
        SQL_PC_LOG(WARN, "fail to transform_neg_param", K(ret));
      } else {
        pc_ctx.sql_ctx_.spm_ctx_.bl_key_.constructed_sql_.assign_ptr(buf, pos);
        pc_ctx.ps_need_parameterized_ = sql_info.ps_need_parameterized_;
        pc_ctx.normal_parse_const_cnt_ = sql_info.total_;
      }
    }
  }
  return ret;
}

 //生成not param info信息及index信息
int ObSqlParameterization::gen_special_param_info(SqlInfo &sql_info, ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ParseNode *raw_param = NULL;
  NotParamInfo np_info;
  pc_ctx.not_param_info_.set_capacity(sql_info.not_param_index_.num_members());
  for (int32_t i = 0; OB_SUCC(ret) && i < pc_ctx.fp_result_.raw_params_.count(); i ++) {
    if (OB_ISNULL(pc_ctx.fp_result_.raw_params_.at(i))) {
      ret = OB_INVALID_ARGUMENT;
    } else if (NULL == (raw_param = pc_ctx.fp_result_.raw_params_.at(i)->node_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (sql_info.not_param_index_.has_member(i)) { //not param
      np_info.reset();
      np_info.idx_ = i;
      np_info.raw_text_ = ObString(raw_param->text_len_, raw_param->raw_text_);
      if (OB_FAIL(pc_ctx.not_param_info_.push_back(np_info))) {
        SQL_PC_LOG(WARN, "fail to push item to array", K(ret));
      }
    }
  } // for end

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pc_ctx.not_param_index_.add_members2(sql_info.not_param_index_))) {
      LOG_WARN("fail to add not param index members", K(ret));
    } else if (OB_FAIL(pc_ctx.neg_param_index_.add_members2(sql_info.neg_param_index_))) {
      LOG_WARN("fail to add neg param index members", K(ret));
    } else if (OB_FAIL(pc_ctx.neg_param_index_.add_members2(sql_info.trans_from_minus_index_))) {
      LOG_WARN("failed to add trans from minus index members", K(ret));
    } else if (OB_FAIL(pc_ctx.param_charset_type_.assign(sql_info.param_charset_type_))) {
      LOG_WARN("fail to assign param charset type", K(ret));
    } else if (OB_FAIL(pc_ctx.fixed_param_idx_.assign(sql_info.fixed_param_idx_))) {
      LOG_WARN("fail to assign fixed param idx", K(ret));
    } else if (OB_FAIL(pc_ctx.must_be_positive_index_.add_members2(sql_info.must_be_positive_index_))) {
      LOG_WARN("failed to add bitset members", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObSqlParameterization::gen_ps_not_param_var(const ObIArray<int64_t> &offsets,
                                                ParamStore &params,
                                                ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  pc_ctx.not_param_var_.set_capacity(offsets.count());
  for (int i = 0; OB_SUCC(ret) && i < offsets.count(); ++i) {
    const int64_t offset = offsets.at(i);
    PsNotParamInfo ps_not_param_var;
    ps_not_param_var.idx_ = offset;
    if (offset >= params.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("offset should not oversize param size", K(ret), K(offset), K(params.count()));
    } else {
      ps_not_param_var.ps_param_ = params.at(offset);
      if (OB_FAIL(pc_ctx.not_param_var_.push_back(ps_not_param_var))) {
        LOG_WARN("fail to push item to array", K(ret));
      } else if (OB_FAIL(pc_ctx.not_param_index_.add_member(offset))) {
        LOG_WARN("add member failed", K(ret), K(offset));
      }
    }
  }
  return ret;
}

int ObSqlParameterization::construct_no_check_type_params(const ObIArray<int64_t> &no_check_type_offsets,
                                                          const ObBitSet<> &need_check_type_offsets,
                                                          ParamStore &params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < no_check_type_offsets.count(); i++) {
    const int64_t offset = no_check_type_offsets.at(i);
    if (offset < 0 || offset >= params.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid offset", K(ret), K(offset), K(params.count()));
    } else if (need_check_type_offsets.has_member(offset)) {
      // do nothing
    } else if (!params.at(offset).is_ext()) { // extend type need to be checked
      params.at(offset).set_need_to_check_type(false);
    } else {
      // real type do not need to be checked
      params.at(offset).set_need_to_check_extend_type(false);
    }
  } // for end

  LOG_DEBUG("ps obj param infos", K(params), K(no_check_type_offsets), K(need_check_type_offsets));
  return ret;
}

int ObSqlParameterization::transform_neg_param(ObIArray<ObPCParam *> &pc_params)
{
  int ret = OB_SUCCESS;
  ObPCParam *pc_param = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < pc_params.count(); i ++) {
    pc_param = pc_params.at(i);
    if (OB_ISNULL(pc_param)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (TRANS_NEG_PARAM == pc_param->flag_) {
      int64_t tmp_pos = 0;
      for (; tmp_pos < pc_param->node_->str_len_ && isspace(pc_param->node_->str_value_[tmp_pos]); tmp_pos++);
      if (OB_UNLIKELY(tmp_pos >= pc_param->node_->str_len_)) {
        int ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(tmp_pos), K(pc_param->node_->str_len_), K(ret));
      } else {
        if ('-' != pc_param->node_->str_value_[tmp_pos]) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected neg sign here", K(tmp_pos), K(ObString(pc_param->node_->str_len_,
                                                                    pc_param->node_->str_value_)));
        } else {
          tmp_pos += 1;
          for (; tmp_pos < pc_param->node_->str_len_ && isspace(pc_param->node_->str_value_[tmp_pos]); tmp_pos++);
        }
        if (OB_SUCC(ret)) {
          pc_param->node_->str_value_ += tmp_pos;
          pc_param->node_->str_len_ -= tmp_pos;
          if (T_NUMBER != pc_param->node_->type_ && pc_param->node_->value_ < 0) {
            pc_param->node_->value_ = 0 - pc_param->node_->value_;
          }
        }
      }
    }
  }
  return ret;
}

int ObSqlParameterization::construct_not_param(const ObString &no_param_sql,
                                                ObPCParam *pc_param,
                                                char *buf,
                                                int32_t buf_len,
                                                int32_t &pos,
                                                int32_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pc_param)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else {
    int32_t len = (int32_t)pc_param->node_->pos_ - idx;
    if (len > buf_len - pos) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (len > 0) {
      //copy text
      MEMCPY(buf + pos, no_param_sql.ptr() + idx, len);
      idx = (int32_t)pc_param->node_->pos_ + 1;
      pos += len;
      //copy raw param
      MEMCPY(buf + pos, pc_param->node_->raw_text_, pc_param->node_->text_len_);
      pos += (int32_t)pc_param->node_->text_len_;
    }
  }
  return ret;
}

int ObSqlParameterization::construct_neg_param(const ObString &no_param_sql,
                                                ObPCParam *pc_param,
                                                char *buf,
                                                int32_t buf_len,
                                                int32_t &pos,
                                                int32_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pc_param)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else {
    int32_t len = (int32_t)pc_param->node_->pos_ - idx;
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
  }
  return ret;
}

int ObSqlParameterization::construct_trans_neg_param(const ObString &no_param_sql,
                                                      ObPCParam *pc_param,
                                                      char *buf,
                                                      int32_t buf_len,
                                                      int32_t &pos,
                                                      int32_t &idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pc_param)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else {
    int32_t len = (int32_t)pc_param->node_->pos_ - idx;
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
      for (; tmp_pos < pc_param->node_->str_len_ && isspace(pc_param->node_->str_value_[tmp_pos]); tmp_pos++);
      if (OB_UNLIKELY(tmp_pos >= pc_param->node_->str_len_)) {
        int ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(tmp_pos), K(pc_param->node_->str_len_), K(ret));
      } else {
        if ('-' != pc_param->node_->str_value_[tmp_pos]) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected neg sign here", K(tmp_pos), K(ObString(pc_param->node_->str_len_,
                                                                    pc_param->node_->str_value_)));
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
  }
  return ret;
}

int ObSqlParameterization::construct_sql(const ObString &no_param_sql,
                                         ObIArray<ObPCParam *> &pc_params,
                                         char *buf,
                                         int32_t buf_len,
                                         int32_t &pos) //已存的长度
{
  int ret = OB_SUCCESS;
  int32_t idx = 0; //原始带?sql的偏移位置
  ObPCParam *pc_param = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < pc_params.count(); i ++) {
    pc_param = pc_params.at(i);
    int32_t len = 0; //需要copy的text的长度
    if (OB_ISNULL(pc_param)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (NOT_PARAM == pc_param->flag_) {
      OZ (construct_not_param(no_param_sql, pc_param, buf, buf_len, pos, idx));
    } else if (NEG_PARAM == pc_param->flag_) {
      OZ (construct_neg_param(no_param_sql, pc_param, buf, buf_len, pos, idx));
    } else if (TRANS_NEG_PARAM == pc_param->flag_) {
      OZ (construct_trans_neg_param(no_param_sql, pc_param, buf, buf_len, pos, idx));
    } else {
      //do nothing
    }
  } //for end

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

int ObSqlParameterization::construct_sql_for_pl(const ObString &no_param_sql,
                                                ObIArray<ObPCParam *> &pc_params,
                                                char *buf,
                                                int32_t buf_len,
                                                int32_t &pos) //已存的长度
{
  int ret = OB_SUCCESS;
  int32_t idx = 0; //原始带?sql的偏移位置
  ObPCParam *pc_param = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < pc_params.count(); i ++) {
    pc_param = pc_params.at(i);
    int32_t len = 0; //需要copy的text的长度
    if (OB_ISNULL(pc_param)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else if (NOT_PARAM == pc_param->flag_) {
      int32_t len = (int32_t)pc_param->node_->pos_ - idx;
      if (0 == len) {
        MEMCPY(buf + pos, pc_param->node_->raw_text_, pc_param->node_->text_len_);
        pos += (int32_t)pc_param->node_->text_len_;
        idx = (int32_t)pc_param->node_->pos_ + 1;
      } else {
        OZ (construct_not_param(no_param_sql, pc_param, buf, buf_len, pos, idx));
      }
    } else if (NEG_PARAM == pc_param->flag_) {
      OZ (construct_neg_param(no_param_sql, pc_param, buf, buf_len, pos, idx));
    } else if (TRANS_NEG_PARAM == pc_param->flag_) {
      OZ (construct_trans_neg_param(no_param_sql, pc_param, buf, buf_len, pos, idx));
    } else {
      //do nothing
    }
  } //for end

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

bool ObSqlParameterization::need_fast_parser(const ObString &sql)
{
  bool b_ret = true;
  const char *stmt = sql.ptr();
  int64_t len = sql.length();
  int64_t leading_space_len = 0;
  while (leading_space_len < len && isspace(stmt[leading_space_len])) {
    ++leading_space_len;
  }
  len -= leading_space_len;
  stmt += leading_space_len;
  if (len > 4 && ('s' == stmt[0] || 'S' == stmt[0]) && ('h' == stmt[1] || 'H' == stmt[1])) {
    if (0 == STRNCASECMP(stmt, "show", 4)) {
      b_ret = false;
    }
  }
  return b_ret;
}

int ObSqlParameterization::fast_parser(ObIAllocator &allocator,
                                       const FPContext &fp_ctx,
                                       const ObString &sql,
                                       ObFastParserResult &fp_result)
{
  //UNUSED(sql_mode);
  int ret = OB_SUCCESS;
  int64_t param_num = 0;
  char *no_param_sql_ptr = NULL;
  int64_t no_param_sql_len = 0;
  ParamList *p_list = NULL;
  bool is_call_procedure = false;
  bool is_contain_select = (sql.length() > 6 && 0 == STRNCASECMP(sql.ptr(), "select", 6));
  if (!is_contain_select && (!need_fast_parser(sql)
    || (ObParser::is_pl_stmt(sql, nullptr, &is_call_procedure) && !is_call_procedure))) {
    (void)fp_result.pc_key_.name_.assign_ptr(sql.ptr(), sql.length());
  } else if (GCONF._ob_enable_fast_parser) {
    if (OB_FAIL(ObFastParser::parse(sql, fp_ctx, allocator, no_param_sql_ptr, no_param_sql_len,
                                    p_list, param_num, fp_result, fp_result.values_token_pos_))) {
      LOG_WARN("fast parse error", K(param_num),
              K(ObString(no_param_sql_len, no_param_sql_ptr)), K(sql));
    }

    if (OB_SUCC(ret)) {
      (void)fp_result.pc_key_.name_.assign_ptr(no_param_sql_ptr, no_param_sql_len);
      if (param_num > 0) {
        ObPCParam *pc_param = NULL;
        char *ptr = (char *)allocator.alloc(param_num * sizeof(ObPCParam));
        fp_result.raw_params_.reset();
        if (OB_ISNULL(ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_PC_LOG(WARN, "fail to alloc memory for pc param", K(ret), K(ptr));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < param_num && NULL != p_list; i++) {
          pc_param = new(ptr)ObPCParam();
          ptr += sizeof(ObPCParam);
          pc_param->node_ = p_list->node_;
          if (OB_FAIL(fp_result.raw_params_.push_back(pc_param))) {
            SQL_PC_LOG(WARN, "fail to push into params", K(ret));
          } else {
            p_list = p_list->next_;
          }
        } // for end
      } else { /*do nothing*/}
    }
  } else {
    ObParser parser(allocator, fp_ctx.sql_mode_, fp_ctx.charsets4parser_);
    SMART_VAR(ParseResult, parse_result) {
      if (OB_FAIL(parser.parse(sql, parse_result, FP_MODE, fp_ctx.enable_batched_multi_stmt_))) {
        SQL_PC_LOG(WARN, "fail to fast parser", K(sql), K(ret));
      } else {
        (void)fp_result.pc_key_.name_.assign_ptr(parse_result.no_param_sql_, parse_result.no_param_sql_len_);
        int64_t param_num = parse_result.param_node_num_;
        //copy raw params
        if (param_num > 0) {
          ObPCParam *pc_param = NULL;
          ParamList *p_list = parse_result.param_nodes_;
          char *ptr = (char *)allocator.alloc(param_num * sizeof(ObPCParam));
          if (OB_ISNULL(ptr)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SQL_PC_LOG(WARN, "fail to alloc memory for pc param", K(ret), K(ptr));
          }
          for (int64_t i = 0;
              OB_SUCC(ret) && i < param_num && NULL != p_list;//当p_list = NULL,表示链表结束
              i++) {
            pc_param = new(ptr)ObPCParam();
            ptr += sizeof(ObPCParam);
            pc_param->node_ = p_list->node_;
            if (OB_FAIL(fp_result.raw_params_.push_back(pc_param))) {
              SQL_PC_LOG(WARN, "fail to push into params", K(ret));
            } else {
              p_list = p_list->next_;
            }
          } // for end
        } else { /*do nothing*/}
      }
    }
  }
  return ret;
}

//used for outline
int ObSqlParameterization::raw_fast_parameterize_sql(ObIAllocator &allocator,
                                                     const ObSQLSessionInfo &session,
                                                     const ObString &sql,
                                                     ObString &no_param_sql,
                                                     ObIArray<ObPCParam *> &raw_params,
                                                     ParseMode parse_mode)
{
  int ret = OB_SUCCESS;
  ObParser parser(allocator, session.get_sql_mode(), session.get_charsets4parser());
  ParseResult parse_result;

  NG_TRACE(pc_fast_parse_start);
  if (OB_FAIL(parser.parse(sql,
                           parse_result,
                           parse_mode,
                           false))) {
    SQL_PC_LOG(WARN, "fail to parse query", K(ret));
  }
  NG_TRACE(pc_fast_parse_end);
  if (OB_SUCC(ret)) {
    no_param_sql.assign(parse_result.no_param_sql_, parse_result.no_param_sql_len_);
  }
  if (OB_SUCC(ret)) {
    ParamList *param = parse_result.param_nodes_;
    for (int32_t i = 0;
         OB_SUCC(ret) && i < parse_result.param_node_num_ && NULL != param;//当param = NULL,表示链表结束
         i ++) {
      void *ptr = allocator.alloc(sizeof(ObPCParam));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_PC_LOG(ERROR, "fail to alloc memory for pc param", K(ret), K(ptr));
      } else {
        ObPCParam *pc_param = new(ptr)ObPCParam();
        pc_param->node_ = param->node_;
        if (OB_FAIL(raw_params.push_back(pc_param))) {
          SQL_PC_LOG(WARN, "fail to push into params", K(ret));
        } else {
          param = param->next_;
        }
      }
    } // for end
  }

  SQL_PC_LOG(DEBUG, "after raw fp", K(parse_result.param_node_num_));
  return ret;
}

int ObSqlParameterization::insert_neg_sign(ObIAllocator &alloc_buf, ParseNode *node)
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
    char *new_str = static_cast<char *>(parse_malloc(node->str_len_ + 2, &alloc_buf));
    char *new_raw_text = static_cast<char *>(parse_malloc(node->text_len_ + 2, &alloc_buf));
    if (OB_ISNULL(new_str) || OB_ISNULL(new_raw_text)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_PC_LOG(ERROR, "parse_strdup failed", K(ret), K(new_raw_text), K(new_str));
    } else {
      new_str[0] = '-';
      new_raw_text[0] = '-';
      MEMMOVE(new_str+1, node->str_value_, node->str_len_);
      MEMMOVE(new_raw_text+1, node->raw_text_, node->text_len_);
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

int ObSqlParameterization::add_param_flag(const ParseNode *node, SqlInfo &sql_info)
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

int ObSqlParameterization::add_not_param_flag(const ParseNode *node, SqlInfo &sql_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (T_QUESTIONMARK == node->type_) {
    if (OB_FAIL(sql_info.ps_not_param_offsets_.push_back(node->value_))) {
      LOG_WARN("pushback offset failed", K(node->value_));
    } else if (OB_FAIL(sql_info.not_param_index_.add_member(node->value_))) {
      SQL_PC_LOG(WARN, "failed to add member", K(node->value_));
    }
  } else if (T_CAST_ARGUMENT == node->type_        //如果是cast类型，则需要添加N个cast节点对应的常数, 因为正常parse不识别为常量, 但fast parse时会识别为常量
             || T_COLLATION == node->type_
             || T_NULLX_CLAUSE == node->type_ // deal null clause on json expr
             || T_WEIGHT_STRING_LEVEL_PARAM == node->type_) {
    for (int i = 0; OB_SUCC(ret) && i < node->param_num_; ++i) {
      if (OB_FAIL(sql_info.not_param_index_.add_member(sql_info.total_++))) {
        SQL_PC_LOG(WARN, "failed to add member", K(sql_info.total_));
      } else if (OB_FAIL(add_varchar_charset(node, sql_info))) {
        SQL_PC_LOG(WARN, "fail to add varchar charset", K(ret));
      }
      if (sql_info.need_check_fp_) {
        ObPCParseInfo p_info;
        p_info.param_idx_ = sql_info.total_ - 1;
        p_info.flag_ = NOT_PARAM;
        p_info.raw_text_pos_ = node->sql_str_off_;
        if (node->sql_str_off_ == -1) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid str off", K(lbt()), K(node),
              K(node->raw_param_idx_), K(get_type_name(node->type_)));
        }
        if (OB_FAIL(ret)) {

        } else if (OB_FAIL(sql_info.parse_infos_.push_back(p_info))) {
          SQL_PC_LOG(WARN, "fail to push parser info", K(ret));
        }
      }
    }
  } else {
    if (OB_FAIL(sql_info.not_param_index_.add_member(sql_info.total_++))) {
      SQL_PC_LOG(WARN, "failed to add member", K(sql_info.total_));
    } else if (OB_FAIL(add_varchar_charset(node, sql_info))) {
      SQL_PC_LOG(WARN, "fail to add varchar charset", K(ret));
    }
    if (sql_info.need_check_fp_) {
      ObPCParseInfo p_info;
      p_info.param_idx_ = sql_info.total_ - 1;
      p_info.flag_ = NOT_PARAM;
      p_info.raw_text_pos_ = node->sql_str_off_;
      if (node->sql_str_off_ == -1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid str off", K(lbt()), K(node),
            K(node->raw_param_idx_), K(get_type_name(node->type_)));
      }
      if (OB_FAIL(ret)) {

      } else if (OB_FAIL(sql_info.parse_infos_.push_back(p_info))) {
        SQL_PC_LOG(WARN, "fail to push parser info", K(ret));
      }
    }
  }

  return ret;
}

//T_FUN_SYS类型函数
//根据mark_arr将func中参数节点标记为该节点及其子节点不能参数化
int ObSqlParameterization::mark_args(ParseNode *arg_tree,
                                     const bool *mark_arr,
                                     int64_t arg_num,
                                     SqlInfo &sql_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg_tree) || OB_ISNULL(mark_arr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret), K(arg_tree));
  } else if (OB_ISNULL(arg_tree->children_)
             || arg_num != arg_tree->num_child_) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret),
               K(arg_tree->type_), K(arg_tree->children_), K(arg_tree->num_child_));
  } else {
    sql_info.ps_need_parameterized_ = false;
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

// 对于不能参数化需要特殊标记的节点进行标记, 一般是不能直接通过节点类型判别的节点可在该函数中进行标记

// Mark those special nodes that cannot be parameterized.
// After mark this node, it has following mechanism:
//       If a node is marked as cannot be parameterized,
//       CUREENT NODE AND ALL NODES OF IT'S SUBTREE cannot be parameterized.
int ObSqlParameterization::mark_tree(ParseNode *tree ,SqlInfo &sql_info)
{
  int ret = OB_SUCCESS;
  if (NULL == tree) {
    //do nothing
  } else if (T_FUN_SYS == tree->type_) {
    ParseNode **node = tree->children_;//node[0] : func name, node[1]: arg list
    if (2 != tree->num_child_) {
      //do nothing  如果不是fun_name和arg_list则不在需要标记的考虑内
    } else if (OB_ISNULL(node) || OB_ISNULL(node[0]) || OB_ISNULL(node[1])) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret), K(node));
    } else {
      ObString func_name(node[0]->str_len_, node[0]->str_value_);
      if ((0 == func_name.case_compare("USERENV")
           || 0 == func_name.case_compare("UNIX_TIMESTAMP"))
          && (1 == node[1]->num_child_)) {
        // USERENV函数的返回类型是由参数的具体值来决定，如果参数化后，无法拿到具体值，所以暂时不进行参数化
        // UNIX_TIMESTAMP(param_str)，结果的精度和param_str有关，不能参数化
        const int64_t ARGS_NUMBER_ONE = 1;
        bool mark_arr[ARGS_NUMBER_ONE] = {1}; //0表示参数化, 1 表示不参数化
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_ONE, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark arg", K(ret));
        }
      } else if ((0 == func_name.case_compare("substr")
                  || 0 == func_name.case_compare("extract_xml"))
          && (3 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_THREE = 3;
        bool mark_arr[ARGS_NUMBER_THREE] = {0, 1, 1}; //0表示参数化, 1 表示不参数化
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_THREE, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark arg", K(ret));
        }
      } else if (0 == func_name.case_compare("xmlserialize")
            && (10 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_TEN = 10;
        bool mark_arr[ARGS_NUMBER_TEN] = {1, 0, 1, 1, 1, 1, 1, 1, 1, 1}; //0表示参数化, 1 表示不参数化
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_TEN, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark weight_string arg", K(ret));
        }
      }else if (0 == func_name.case_compare("weight_string")
          && (5 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_FIVE = 5;
        bool mark_arr[ARGS_NUMBER_FIVE] = {0, 1, 1, 1, 1}; //0表示参数化, 1 表示不参数化
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_FIVE, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark weight_string arg", K(ret));
        }
      } else if ((0==func_name.case_compare("convert")
                  || (0==func_name.case_compare("char")))
                  && (2 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_TWO = 2;
        bool mark_arr[ARGS_NUMBER_TWO] = {0, 1};
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_TWO, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
        }
      } else if ((0 == func_name.case_compare("str_to_date") // STR_TO_DATE(str,format)
                  || 0 == func_name.case_compare("date_format") //DATE_FORMAT(date,format)
                  || 0 == func_name.case_compare("from_unixtime")//FROM_UNIXTIME(unix_timestamp), FROM_UNIXTIME(unix_timestamp,format)
                  || 0 == func_name.case_compare("round")        //ROUND(X), ROUND(X,D)
                  || 0 == func_name.case_compare("left") // the length of result should be set with the value of the second param
                  || 0 == func_name.case_compare("substr")
                  || 0 == func_name.case_compare("dbms_lob_convert_clob_charset")
                  || 0 == func_name.case_compare("truncate")) // truncate结果的精度需要根据第二个参数进行推导，所以不能参数化
                 && (2 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_TWO = 2;
        bool mark_arr[ARGS_NUMBER_TWO] = {0, 1};
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_TWO, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark arg", K(ret));
        }
      } else if ((0 == func_name.case_compare("name_const"))
                  && (2 == node[1]->num_child_)) {
        const int64_t ARGS_NUMBER_TWO = 2;
        bool mark_arr[ARGS_NUMBER_TWO] = {1, 0};
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_TWO, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark arg", K(ret));
        }
      } else if ((0 == func_name.case_compare("concat")) && 1 == node[0]->reserved_) {
        sql_info.ps_need_parameterized_ = false;
      } else if ((0 == func_name.case_compare("json_equal"))) {
        sql_info.ps_need_parameterized_ = false;
      } else if ((0 == func_name.case_compare("json_extract"))) {
        sql_info.ps_need_parameterized_ = false;
        for (int64_t i = 1; OB_SUCC(ret) && i < tree->num_child_; i++) {
          if (OB_ISNULL(tree->children_[i])) {
            ret = OB_INVALID_ARGUMENT;
            SQL_PC_LOG(WARN, "invalid argument", K(ret), K(tree->children_[i]));
          } else {
            tree->children_[i]->is_tree_not_param_ = true;
          }
        }
      } else if ((0 == func_name.case_compare("json_member_of"))) {
        sql_info.ps_need_parameterized_ = false;
        if (2 == tree->num_child_) {
          const int64_t ARGS_NUMBER_TWO = 2;
          bool mark_arr[ARGS_NUMBER_TWO] = {0, 1}; //0表示参数化, 1 表示不参数化
          if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_TWO, sql_info))) {
            SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
          }
        }
      } else if ((0 == func_name.case_compare("json_contains"))) {
        sql_info.ps_need_parameterized_ = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < tree->num_child_; i++) {
          if (OB_ISNULL(tree->children_[i])) {
            ret = OB_INVALID_ARGUMENT;
            SQL_PC_LOG(WARN, "invalid argument", K(ret), K(tree->children_[i]));
          } else if (1 != 1) {
            tree->children_[i]->is_tree_not_param_ = true;
          }
        }
      } else if ((0 == func_name.case_compare("json_overlaps"))) {
        const int64_t ARGS_NUMBER_TWO = 2;
        bool mark_arr[ARGS_NUMBER_TWO] = {1, 1};
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_TWO, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark arg", K(ret));
        }
      } else if ((0 == func_name.case_compare("json_schema_valid"))
                || (0 == func_name.case_compare("json_schema_validation_report"))) {
        const int64_t ARGS_NUMBER_TWO = 2;
        bool mark_arr[ARGS_NUMBER_TWO] = {1, 0};
        if (OB_FAIL(mark_args(node[1], mark_arr, ARGS_NUMBER_TWO, sql_info))) {
          SQL_PC_LOG(WARN, "fail to mark arg", K(ret));
        }
      }
    }
  } else if (T_OP_LIKE == tree->type_) {
    if (3 == tree->num_child_) {   // child[0] like child[1] escape child[2]
      const int64_t ARGS_NUMBER_THREE = 3;
      bool mark_arr[ARGS_NUMBER_THREE] = {0, 1, 1}; //0表示参数化, 1 表示不参数化
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_THREE, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    }
  } else if (T_OP_IS == tree->type_ || T_OP_IS_NOT == tree->type_) {
    if (tree->num_child_ == 2) {
      const int64_t ARGS_NUMBER_TWO = 2;
      bool mark_arr[ARGS_NUMBER_TWO] = {0,1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_TWO, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    } else { /*do nothing*/ }
  } else if(T_FUN_SYS_JSON_VALUE == tree->type_) {
    if (10 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid json value expr argument", K(ret), K(tree->num_child_)); 
    } else {
      const int64_t ARGS_NUMBER_TEN = 10;
      bool mark_arr[ARGS_NUMBER_TEN] = {0, 1, 1, 1, 1, 1, 1, 1, 1, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_TEN, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    }
  } else if(T_FUN_SYS_JSON_OBJECT == tree->type_) {
    if (5 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid json object expr argument", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_FIVE = 5;
      bool mark_arr[ARGS_NUMBER_FIVE] = {1, 1, 1, 1, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_FIVE, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    }
  } else if(T_FUN_SYS_IS_JSON == tree->type_) {
    if (5 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument num for IS json", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_FIVE = 5;
      bool mark_arr[ARGS_NUMBER_FIVE] = {0, 1, 1, 1, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_FIVE, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    }
  } else if(T_FUN_SYS_JSON_QUERY == tree->type_) {
    if (13 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid json query expr argument", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_THIRTEEN = 13;
      bool mark_arr[ARGS_NUMBER_THIRTEEN] = {0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};   // json doc type will affect returning type,
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_THIRTEEN, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    }
  } else if(T_FUN_SYS_JSON_EXISTS == tree->type_) {
    if (5 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument num for json_exists", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_FIVE = 5;
      bool mark_arr[ARGS_NUMBER_FIVE] = {0, 1, 1, 1, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_FIVE, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark json_exists arg", K(ret));
      }
    }
  } else if(T_FUN_SYS_JSON_EQUAL == tree->type_) {
    if (3 < tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid json query expr argument", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_THREE = 3;
      bool mark_arr[ARGS_NUMBER_THREE] = {0, 0, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_THREE, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark substr arg", K(ret));
      }
    }
  } else if(T_FUN_SYS_JSON_ARRAY == tree->type_) {
    if (4 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid json array expr argument", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_FOUR = 4;
      bool mark_arr[ARGS_NUMBER_FOUR] = {0, 1, 1, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_FOUR, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark json array arg", K(ret));
      }
    }
  } else if(T_FUN_SYS_JSON_MERGE_PATCH == tree->type_) {
    if (7 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid json mergepatch expr argument", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_SEVEN = 7;
      bool mark_arr[ARGS_NUMBER_SEVEN] = {0, 0, 1, 1, 1, 1, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_SEVEN, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark json mergepatch arg", K(ret));
      }
    }
  } else if (T_JSON_TABLE_EXPRESSION == tree->type_) {
    if (5 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid json table expr argument", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_FIVE = 5;
      bool mark_arr[ARGS_NUMBER_FIVE] = {0, 1, 1, 1, 1};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_FIVE, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark json table arg", K(ret));
      }
    }
  } else if (T_XML_TABLE_EXPRESSION == tree->type_) {
    if (6 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid xml table expr argument", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_SIX = 6;
      bool mark_arr[ARGS_NUMBER_SIX] = {1, 1, 0, 1, 1, 1}; // because of namespace deal in resolve, so can not parameter
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_SIX, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark xml table arg", K(ret));
      }
    }
  } else if (T_FUN_SYS_TREAT == tree->type_) {
    if (2 != tree->num_child_) {
      ret = OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid treat expr argument", K(ret), K(tree->num_child_));
    } else {
      const int64_t ARGS_NUMBER_TWO = 2;
      bool mark_arr[ARGS_NUMBER_TWO] = {1, 0};
      if (OB_FAIL(mark_args(tree, mark_arr, ARGS_NUMBER_TWO, sql_info))) {
        SQL_PC_LOG(WARN, "fail to mark treat arg", K(ret));
      }
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObSqlParameterization::add_varchar_charset(const ParseNode *node, SqlInfo &sql_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(node));
  } else if (T_VARCHAR == node->type_
             && NULL != node->children_
             && NULL != node->children_[0]
             && T_CHARSET == node->children_[0]->type_) {
    ObString charset(node->children_[0]->str_len_,
                     node->children_[0]->str_value_);
    ObCharsetType charset_type = CHARSET_INVALID;
    if (CHARSET_INVALID == (charset_type =
          ObCharset::charset_type(charset.trim()))) {
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

int ObSqlParameterization::get_related_user_vars(const ParseNode *tree, common::ObIArray<common::ObString> &user_vars)
{
  int ret = OB_SUCCESS;
  ObString var_str;
  if (tree == NULL) {
    // do nothing
  } else {
    if (T_USER_VARIABLE_IDENTIFIER == tree -> type_) {
      if (OB_ISNULL(tree -> str_value_) || tree -> str_len_ < 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(tree -> str_value_), K(tree -> str_len_));
      } else {
        var_str.assign_ptr(tree -> str_value_, static_cast<int32_t>(tree -> str_len_));
        if (OB_FAIL(user_vars.push_back(var_str))) {
          LOG_WARN("failed to push back user variable", K(ret));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tree -> num_child_; i++) {
        if (OB_ISNULL(tree -> children_)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(tree -> children_), K(ret));
        } else if (OB_FAIL(SMART_CALL(get_related_user_vars(tree -> children_[i], user_vars)))) {
          LOG_WARN("failed to get related user vars", K(ret), K(tree -> children_[i]), K(i));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    user_vars.reset();
  }

  return ret;
}

int ObSqlParameterization::get_select_item_param_info(const common::ObIArray<ObPCParam *> &raw_params,
                                                      ParseNode *tree,
                                                      SelectItemParamInfoArray *select_item_param_infos,
                                                      const ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  SelectItemParamInfo param_info;
  ObString org_field_name;
  int64_t expr_pos = tree->raw_sql_offset_;
  int64_t buf_len = SelectItemParamInfo::PARAMED_FIELD_BUF_LEN;
  ObSEArray<TraverseStackFrame, 64> stack_frames;
  bool enable_modify_null_name = false;

  if (T_PROJECT_STRING != tree->type_ || OB_ISNULL(tree->children_) || tree->num_child_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tree->type_), K(tree->children_), K(tree->num_child_));
  } else if (T_ALIAS == tree->children_[0]->type_
             || T_STAR == tree->children_[0]->type_) { // have alias name, or is a '*', do not need parameterized
    // do nothing
  } else if (OB_FAIL(stack_frames.push_back(TraverseStackFrame{tree, 0}))) {
    LOG_WARN("failed to push back element", K(ret));
  } else {
    // start to construct paramed field name template...
    if (lib::is_oracle_mode()) { // oracle要使用原始的字符串，以正确处理偏移
      org_field_name.assign_ptr(tree->raw_text_, (int32_t)tree->text_len_);
    } else {
      org_field_name.assign_ptr(tree->str_value_, (int32_t)tree->str_len_);
    }

    SelectItemTraverseCtx ctx(raw_params,
                              tree,
                              org_field_name,
                              tree->raw_sql_offset_,
                              buf_len,
                              expr_pos,
                              param_info);
    // 模拟函数递归操作，遍历子树
    // 栈上每一个元素为 {cur_node, next_child_idx}
    // 出栈操作：
    // 如cur_node是的is_val_paramed_item_idx_为true，说明这是一个T_PROJECT_STRING，并且已经被遍历
    // 或者T_QUESTION_MARK，或子节点都已经遍历完，出栈
    //
    // 入栈操作：
    // 选取cur_node的第一个不为空的子节点push到堆栈中，并更新当前堆栈的next_child_idx_
    for (; OB_SUCC(ret) && stack_frames.count() > 0; ) {
      int64_t frame_idx = stack_frames.count() - 1;
      ctx.tree_ = stack_frames.at(frame_idx).cur_node_;
      if (NULL == ctx.tree_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null node", K(ret), K(ctx.tree_));
      } else if (1 == ctx.tree_->is_val_paramed_item_idx_
                 || T_QUESTIONMARK == ctx.tree_->type_
                 || stack_frames.at(frame_idx).next_child_idx_ >= ctx.tree_->num_child_) {
        if (T_QUESTIONMARK == ctx.tree_->type_) {
          if (ctx.param_info_.name_len_ >= ctx.buf_len_) {
            // column长度已经满了，不需要再继续构造模板，直接跳出
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
          SQL_PC_LOG(WARN, "invalid null children", K(ret), K(stack_frames.at(frame_idx).cur_node_->children_), K(frame_idx));
        } else {
          TraverseStackFrame frame = stack_frames.at(frame_idx);
          for (int64_t i = frame.next_child_idx_; OB_SUCC(ret) && i < frame.cur_node_->num_child_; i++) {
            if (OB_ISNULL(frame.cur_node_->children_[i])) {
              stack_frames.at(frame_idx).next_child_idx_ = i + 1;
            } else if (OB_FAIL(stack_frames.push_back(TraverseStackFrame{frame.cur_node_->children_[i], 0}))) {
              LOG_WARN("failed to push back element", K(ret));
            } else {
              stack_frames.at(frame_idx).next_child_idx_ = i + 1;
              LOG_DEBUG("after pushing frame", K(stack_frames));
              break;
            }
          } // for end
        }
      }
    } // for end

    if (OB_SUCC(ret)) {
      // 如果常量之后还有字符串
      int64_t res_start_pos = expr_pos - tree->raw_sql_offset_;
      int64_t tmp_len = std::min(org_field_name.length() - res_start_pos, buf_len - param_info.name_len_);
      if (tmp_len > 0) {
        int32_t len = static_cast<int32_t>(tmp_len);
        MEMCPY(param_info.paramed_field_name_ + param_info.name_len_, org_field_name.ptr() + res_start_pos, len);
        param_info.name_len_ += len;
      }

      if (T_QUESTIONMARK == tree->children_[0]->type_
          && 1 == tree->children_[0]->is_column_varchar_) {
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

  // MySQL sets the alias of standalone null value("\N","null"...) to "NULL" during projection.
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(session.check_feature_enable(ObCompatFeatureType::PROJECT_NULL,
                                                  enable_modify_null_name))) {
    LOG_WARN("failed to check feature enable", K(ret));
  } else if (is_mysql_mode() &&
             1 == param_info.params_idx_.count() &&
             0 == ObString(param_info.name_len_, param_info.paramed_field_name_).compare("?") &&
             enable_modify_null_name) {
    int64_t idx = param_info.params_idx_.at(0);
    if (idx >= raw_params.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid index", K(idx), K(raw_params.count()));
    } else if (OB_ISNULL(raw_params.at(idx)) || OB_ISNULL(raw_params.at(idx)->node_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(raw_params.at(idx)), K(raw_params.at(idx)->node_));
    } else if (T_NULL == raw_params.at(idx)->node_->type_) {
      tree->str_value_ = "NULL";
      tree->str_len_ = strlen("NULL");
    }
  }

  return ret;
}

int ObSqlParameterization::resolve_paramed_const(SelectItemTraverseCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t idx = ctx.tree_->raw_param_idx_;
  if (idx >= ctx.raw_params_.count()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret), K(idx), K(ctx.raw_params_.count()));
  } else if (OB_ISNULL(ctx.raw_params_.at(idx)) || OB_ISNULL(ctx.raw_params_.at(idx)->node_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else {
    const ParseNode *param_node = ctx.raw_params_.at(idx)->node_;
    int64_t tmp_len = std::min(ctx.buf_len_ - ctx.param_info_.name_len_, param_node->raw_sql_offset_ - ctx.expr_pos_);
    // In the case of select _binary 'abc';, special processing is required, because the value of
    // org_expr_name_ is the same as that of raw param in this scenario.
    // So it is judged here that if org_expr_name_ is the same as param_node->str_value_ value
    // there is no need to copy it. paramed_field_name_ should be replaced with '?'
    if (0 == ctx.org_expr_name_.case_compare(ObString(param_node->str_len_, param_node->str_value_))) {
      // do nothing
    } else if (tmp_len > 0 && ctx.org_expr_name_.length() > 0) {
      int32_t len = static_cast<int64_t>(tmp_len);
      MEMCPY(ctx.param_info_.paramed_field_name_ + ctx.param_info_.name_len_, ctx.org_expr_name_.ptr() + ctx.expr_pos_ - ctx.expr_start_pos_, len);
      ctx.param_info_.name_len_ += len;
    }
    ctx.expr_pos_ = param_node->raw_sql_offset_ + param_node->text_len_;
    if (OB_FAIL(ctx.param_info_.questions_pos_.push_back(ctx.param_info_.name_len_))) {
      SQL_PC_LOG(WARN, "failed to push back element", K(ret));
    } else if (OB_FAIL(ctx.param_info_.params_idx_.push_back(idx))) {
      SQL_PC_LOG(WARN, "failed to push back element", K(ret));
    } else {
      if (ctx.param_info_.name_len_ < ctx.buf_len_) {
        ctx.param_info_.paramed_field_name_[ctx.param_info_.name_len_++] = '?'; // 替换常量为'?'
      }
      if (ctx.tree_->is_neg_ && OB_FAIL(ctx.param_info_.neg_params_idx_.add_member(idx))) {
        SQL_PC_LOG(WARN, "failed to add member", K(ret), K(idx));
      }
      if (OB_SUCC(ret)) {
        LOG_DEBUG("resolve a paramed const",
                  K(ctx.expr_pos_), K(ctx.expr_start_pos_), K(ctx.org_expr_name_),
                  K(param_node->raw_sql_offset_));
      }
    }
  }
  return ret;
}

int ObSqlParameterization::transform_minus_op(ObIAllocator &alloc, ParseNode *tree, bool is_from_pl)
{
  int ret = OB_SUCCESS;
  if (T_OP_MINUS != tree->type_) {
    // do nothing
  } else if (2 != tree->num_child_
             || OB_ISNULL(tree->children_)
             || OB_ISNULL(tree->children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid minus tree", K(ret));
  } else if (1 == tree->children_[1]->is_assigned_from_child_) {
    // select 1 - (2) from dual;
    // select 1 - (2/3/4) from dual;
    // 对于常量节点2，都不能转换成-2
    // do nothing
  } else if (ob_is_number_or_decimal_int_tc(ITEM_TO_OBJ_TYPE(tree->children_[1]->type_))
             || ((ob_is_integer_type(ITEM_TO_OBJ_TYPE(tree->children_[1]->type_))
                  || ob_is_real_type(ITEM_TO_OBJ_TYPE(tree->children_[1]->type_)))
                 && tree->children_[1]->value_ >= 0)) {
    ParseNode *child = tree->children_[1];
    tree->type_ = T_OP_ADD;
    if (!is_from_pl) {
      if (T_INT == child->type_) {
        child->value_ = -child->value_;
      }

      char *new_str = static_cast<char *>(parse_malloc(child->str_len_ + 2, &alloc));
      char *new_raw_text = static_cast<char *>(parse_malloc(child->text_len_ + 2, &alloc));
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
        child->text_len_ ++;
        child->str_value_ = new_str;
        child->raw_text_ = new_raw_text;
        child->is_trans_from_minus_ = 1;
      }
    } else {
      child->is_trans_from_minus_ = 1;
    }
  } else if (T_OP_MUL == tree->children_[1]->type_ || T_OP_DIV == tree->children_[1]->type_
             || T_OP_INT_DIV == tree->children_[1]->type_
             || (lib::is_mysql_mode() && T_OP_MOD == tree->children_[1]->type_)) {
    /*  '0 - 2 * 3' should be transformed to '0 + (-2) * 3' */
    /*  '0 - 2 / 3' should be transformed to '0 + (-2) / 3' */
    /*  '0 - 4 mod 3' should be transformed to '0 + (-4 mod 3)' */
    /*  '0 - 2/3/4' => '0 + (-2/3/4)' */
    /*  notify that, syntax tree of '0 - 2/3/4' is */
    /*            - */
    /*          /   \ */
    /*         0    div */
    /*             /   \ */
    /*           div    4 */
    /*          /   \ */
    /*         2     3 */
    /*  so, we need to find the leftest leave node and change its value and str */
    /*  same for '%','*', mod */
    /*  */
    /*  在oracle模式下只有mod函数，比如select 1 - mod(mod(3, 4), 2) from dual; */
    /*  语法树为： */
    /*       - */
    /*     /  \ */
    /*    1   mod */
    /*       /   \ */
    /*     mod    2 */
    /*    /  \ */
    /*   3    4 */
    /*   这个语法树和mysql模式下的select 1 - 3%4%2 from dual是一样的，但是-和3在oracle模式下不能结合在一起 */
    /*   否则快速参数化和硬解析得到的常量不一样（3和-3)，所以oracle模式下T_OP_MOD不能转换减号 */
    ParseNode *const_node = NULL;
    ParseNode *op_node = tree->children_[1];
    if (OB_FAIL(find_leftest_const_node(*op_node, const_node))) {
      LOG_WARN("failed to find leftest const node", K(ret));
    } else if (OB_ISNULL(const_node)) {
      // 1 - (2)/3, -和2也是不能结合的
      // do nothing
    } else {
      tree->type_ = T_OP_ADD;
      if (!is_from_pl) {
        if (T_INT == const_node->type_) {
          const_node->value_ = -const_node->value_;
        }
        char *new_str = static_cast<char *>(parse_malloc(const_node->str_len_ + 2, &alloc));
        char *new_raw_text = static_cast<char *>(parse_malloc(const_node->text_len_ + 2, &alloc));

        if (OB_ISNULL(new_str) || OB_ISNULL(new_raw_text)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret), K(new_str), K(new_raw_text));
        } else {
          new_str[0] = '-';
          new_raw_text[0] = '-';
          MEMMOVE(new_str + 1, const_node->str_value_, const_node->str_len_);
          MEMMOVE(new_raw_text + 1, const_node->raw_text_, const_node->text_len_);
          new_str[const_node->str_len_ + 1] = '\0';
          new_raw_text[const_node->text_len_]= '\0';

          const_node->str_len_++;
          const_node->text_len_++;
          const_node->str_value_ = new_str;
          const_node->raw_text_ = new_raw_text;
          const_node->is_trans_from_minus_ = 1;
        }
      } else {
        const_node->is_trans_from_minus_ = 1;
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObSqlParameterization::find_leftest_const_node(ParseNode &cur_node, ParseNode *&const_node)
{
  int ret = OB_SUCCESS;
  if (ob_is_numeric_type(ITEM_TO_OBJ_TYPE(cur_node.type_))
      && 0 == cur_node.is_assigned_from_child_) {
    const_node = &cur_node;
  } else if (1 == cur_node.is_assigned_from_child_) {
    // do nothing
  } else if (T_OP_MUL == cur_node.type_ || T_OP_DIV == cur_node.type_
    || T_OP_INT_DIV == cur_node.type_ || T_OP_MOD == cur_node.type_) {
    /*   对于1 - (2-3)/4，语法树为 */
    /*      - */
    /*     / \ */
    /*    1  div */
    /*      /  \ */
    /*     -    4 */
    /*    / \ */
    /*   2   3 */
    /*  这时候-是不能和2结合的，也就是不能转换语树 */
    /*  对于一元操作符（一元操作符优先级大于减操作),比如负号 */
    /*  1 - (-2)/4 */
    /*  语法树为： */
    /*     - */
    /*    / \ */
    /*   1  div */
    /*     /  \ */
    /*    neg  4 */
    /*     | */
    /*     2 */
    /*  这种情况下，-也不能2结合 */
    /*  所以只有T_OP_MUL、T_OP_DIV和T_OP_MOD走到这条路径上 */
    if (OB_ISNULL(cur_node.children_) || 2 != cur_node.num_child_
        || OB_ISNULL(cur_node.children_[0]) || OB_ISNULL(cur_node.children_[1])) {
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
