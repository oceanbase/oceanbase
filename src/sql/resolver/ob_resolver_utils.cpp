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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ob_resolver_utils.h"
#include "lib/charset/ob_charset.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/schema/ob_column_schema.h"
#include "share/object/ob_obj_cast.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "common/object/ob_obj_type.h"
#include "sql/parser/parse_malloc.h"
#include "sql/parser/parse_node.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_column_ref.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_part_func_checker.h"
#include "sql/resolver/expr/ob_raw_expr_part_expr_checker.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "sql/ob_sql_utils.h"
#include "observer/ob_server_struct.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/engine/expr/ob_expr_column_conv.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace obrpc;
namespace sql {
ObItemType ObResolverUtils::item_type_ = T_INVALID;

const ObString ObResolverUtils::stmt_type_string[] = {
#define OB_STMT_TYPE_DEF(stmt_type, priv_check_func, id) ObString::make_string(#stmt_type),
#include "sql/resolver/ob_stmt_type.h"
#undef OB_STMT_TYPE_DEF
};

int ObResolverUtils::get_all_function_table_column_names(
    const TableItem& table_item, ObResolverParams& params, ObIArray<ObString>& column_names)
{
  UNUSED(table_item);
  UNUSED(params);
  UNUSED(column_names);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObResolverUtils::check_function_table_column_exist(
    const TableItem& table_item, ObResolverParams& params, const ObString& column_name)
{
  UNUSED(table_item);
  UNUSED(params);
  UNUSED(column_name);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObResolverUtils::resolve_extended_type_info(const ParseNode& str_list_node, ObIArray<ObString>& type_info_array)
{
  int ret = OB_SUCCESS;
  ObString cur_type_info;
  CK(str_list_node.num_child_ > 0);
  for (int64_t i = 0; OB_SUCC(ret) && i < str_list_node.num_child_; ++i) {
    cur_type_info.reset();
    const ParseNode* str_node = str_list_node.children_[i];
    if (OB_ISNULL(str_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid str_node", K(ret), K(str_node), K(i));
    } else if (FALSE_IT(cur_type_info.assign_ptr(
                   str_node->str_value_, static_cast<ObString::obstr_size_t>(str_node->str_len_)))) {
    } else if (OB_FAIL(type_info_array.push_back(cur_type_info))) {
      LOG_WARN("fail to push back type info", K(ret), K(i), K(cur_type_info));
    }
  }
  return ret;
}

int ObResolverUtils::check_extended_type_info(common::ObIAllocator& alloc, ObIArray<ObString>& type_infos,
    ObCollationType ori_cs_type, const ObString& col_name, ObObjType col_type, ObCollationType cs_type,
    ObSQLMode sql_mode)
{
  int ret = OB_SUCCESS;
  ObString cur_type_info;
  const ObString& sep = ObCharsetUtils::get_const_str(cs_type, ',');
  int32_t dup_cnt;
  // convert type infos from %ori_cs_type to %cs_type first
  FOREACH_CNT_X(str, type_infos, OB_SUCC(ret))
  {
    OZ(ObCharset::charset_convert(alloc, *str, ori_cs_type, cs_type, *str));
  }
  if (OB_SUCC(ret)) {}

  for (int64_t i = 0; OB_SUCC(ret) && i < type_infos.count(); ++i) {
    ObString& cur_val = type_infos.at(i);
    int32_t no_sp_len = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(cs_type, cur_val.ptr(), cur_val.length()));
    cur_val.assign_ptr(cur_val.ptr(), static_cast<ObString::obstr_size_t>(no_sp_len));  // remove tail space
    int32_t char_len = static_cast<int32_t>(ObCharset::strlen_char(cs_type, cur_val.ptr(), cur_val.length()));
    if (OB_UNLIKELY(char_len > OB_MAX_INTERVAL_VALUE_LENGTH)) {  // max value length
      ret = OB_ER_TOO_LONG_SET_ENUM_VALUE;
      LOG_USER_ERROR(OB_ER_TOO_LONG_SET_ENUM_VALUE, col_name.length(), col_name.ptr());
    } else if (ObSetType == col_type  // set can't contain commas
               && 0 != ObCharset::instr(cs_type, cur_val.ptr(), cur_val.length(), sep.ptr(), sep.length())) {
      ret = OB_ERR_ILLEGAL_VALUE_FOR_TYPE;
      LOG_USER_ERROR(OB_ERR_ILLEGAL_VALUE_FOR_TYPE, "set", cur_val.length(), cur_val.ptr());
    } else { /*do nothing*/
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_duplicates_in_type_infos(  // check duplicate value
                 type_infos,
                 col_name,
                 col_type,
                 cs_type,
                 sql_mode,
                 dup_cnt))) {
    LOG_WARN("fail to check duplicate", K(ret), K(dup_cnt), K(type_infos), K(col_name), K(col_type));
  } else if (OB_FAIL(check_max_val_count(  // check value count
                 col_type,
                 col_name,
                 type_infos.count(),
                 dup_cnt))) {
    LOG_WARN("fail to check max val count", K(ret), K(col_type), K(col_name), K(type_infos), K(dup_cnt));
  } else { /*do nothing*/
  }
  return ret;
}

int ObResolverUtils::check_duplicates_in_type_infos(const ObIArray<common::ObString>& type_infos,
    const ObString& col_name, ObObjType col_type, ObCollationType cs_type, ObSQLMode sql_mode, int32_t& dup_cnt)
{
  int ret = OB_SUCCESS;
  dup_cnt = 0;
  for (uint32_t i = 0; OB_SUCC(ret) && i < type_infos.count() - 1; ++i) {
    int32_t pos = i + 1;
    const ObString& cur_val = type_infos.at(i);
    if (OB_FAIL(find_type(type_infos, cs_type, cur_val, pos))) {
      LOG_WARN("fail to find type", K(type_infos), K(cur_val), K(pos));
    } else if (OB_UNLIKELY(pos >= 0)) {
      ++dup_cnt;
      if (is_strict_mode(sql_mode)) {
        ret = OB_ER_DUPLICATED_VALUE_IN_TYPE;
        LOG_USER_ERROR(OB_ER_DUPLICATED_VALUE_IN_TYPE,
            col_name.length(),
            col_name.ptr(),
            cur_val.length(),
            cur_val.ptr(),
            ob_sql_type_str(col_type));
      } else {
        LOG_USER_WARN(OB_ER_DUPLICATED_VALUE_IN_TYPE,
            col_name.length(),
            col_name.ptr(),
            cur_val.length(),
            cur_val.ptr(),
            ob_sql_type_str(col_type));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_max_val_count(ObObjType type, const ObString& col_name, int64_t val_cnt, int32_t dup_cnt)
{
  int ret = OB_SUCCESS;
  if (ObEnumType == type) {
    if (val_cnt > OB_MAX_ENUM_ELEMENT_NUM) {
      ret = OB_ER_TOO_BIG_ENUM;
      LOG_USER_ERROR(OB_ER_TOO_BIG_ENUM, col_name.length(), col_name.ptr());
    }
  } else if (ObSetType == type) {
    if (val_cnt - dup_cnt > OB_MAX_SET_ELEMENT_NUM) {
      ret = OB_ERR_TOO_BIG_SET;
      LOG_USER_ERROR(OB_ERR_TOO_BIG_SET, col_name.length(), col_name.ptr());
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), K(val_cnt), K(dup_cnt));
  }
  return ret;
}

int ObResolverUtils::resolve_column_ref(
    const ParseNode* node, const ObNameCaseMode case_mode, ObQualifiedName& column_ref)
{
  int ret = OB_SUCCESS;
  ParseNode* db_node = NULL;
  ParseNode* relation_node = NULL;
  ParseNode* column_node = NULL;
  ObString column_name;
  ObString table_name;
  ObString database_name;
  if (OB_ISNULL(node) || OB_UNLIKELY(node->type_ != T_COLUMN_REF)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse node is invalid", K(node));
  } else {
    db_node = node->children_[0];
    relation_node = node->children_[1];
    column_node = node->children_[2];
    if (db_node != NULL) {
      column_ref.database_name_.assign_ptr(
          const_cast<char*>(db_node->str_value_), static_cast<int32_t>(db_node->str_len_));
    }
  }
  if (OB_SUCC(ret)) {
    if (relation_node != NULL) {
      column_ref.tbl_name_.assign_ptr(
          const_cast<char*>(relation_node->str_value_), static_cast<int32_t>(relation_node->str_len_));
    }
    if (OB_ISNULL(column_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column node is null");
    } else if (column_node->type_ == T_STAR) {
      column_ref.is_star_ = true;
    } else {
      column_ref.is_star_ = false;
      column_ref.col_name_.assign_ptr(
          const_cast<char*>(column_node->str_value_), static_cast<int32_t>(column_node->str_len_));
    }
  }

  if (OB_SUCC(ret) && share::is_mysql_mode() && OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, column_ref.database_name_);
    ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, column_ref.tbl_name_);
  }
  return ret;
}

int ObResolverUtils::resolve_obj_access_ref_node(
    ObRawExprFactory& expr_factory, const ParseNode* node, ObQualifiedName& q_name)
{
  int ret = OB_SUCCESS;
  // generate raw expr
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (T_IDENT == node->type_ /*Mysql mode*/
             || T_COLUMN_REF == node->type_ /*Mysql mode*/) {
    ObTimeZoneInfo tz_info;
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    ObExprResolveContext ctx(expr_factory, &tz_info, case_mode);
    ctx.session_info_ = NULL;
    // ctx.is_oracle_compatible_ = (T_OBJ_ACCESS_REF == node->type_);

    ObRawExprResolverImpl expr_resolver(ctx);
    ObRawExpr* expr = NULL;
    ObSEArray<ObQualifiedName, 1> columns;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObArray<ObOpRawExpr*> op_exprs;
    ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
    if (OB_FAIL(expr_resolver.resolve(
            node, expr, columns, sys_vars, sub_query_info, aggr_exprs, win_exprs, op_exprs, user_var_exprs))) {
      LOG_WARN("failed to resolve expr tree", K(ret));
    } else if (OB_UNLIKELY(1 != columns.count()) || OB_UNLIKELY(!sys_vars.empty()) ||
               OB_UNLIKELY(!sub_query_info.empty()) || OB_UNLIKELY(!aggr_exprs.empty()) ||
               OB_UNLIKELY(!win_exprs.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is invalid",
          K(op_exprs.empty()),
          K(columns.count()),
          K(sys_vars.count()),
          K(sub_query_info.count()),
          K(aggr_exprs.count()),
          K(win_exprs.count()),
          K(ret));
    } else if (OB_FAIL(q_name.assign(columns.at(0)))) {
      LOG_WARN("assign qualified name failed", K(ret), K(columns));
    } else { /*do nothing*/
    }
  } else if (T_SYSTEM_VARIABLE == node->type_ || T_USER_VARIABLE_IDENTIFIER == node->type_) {
    ObString access_name(node->str_len_, node->str_value_);
    ObObjAccessIdent ident(access_name);
    ret = q_name.access_idents_.push_back(ident);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is invalid", K_(node->type));
  }
  return ret;
}

stmt::StmtType ObResolverUtils::get_stmt_type_by_item_type(const ObItemType item_type)
{
  stmt::StmtType type;
  switch (item_type) {
#define SET_STMT_TYPE(item_type) \
  case item_type: {              \
    type = stmt::item_type;      \
  } break;
    // dml
    SET_STMT_TYPE(T_SELECT);
    SET_STMT_TYPE(T_INSERT);
    SET_STMT_TYPE(T_DELETE);
    SET_STMT_TYPE(T_UPDATE);
    SET_STMT_TYPE(T_REPLACE);
    // ps
    SET_STMT_TYPE(T_PREPARE);
    SET_STMT_TYPE(T_EXECUTE);
    SET_STMT_TYPE(T_DEALLOCATE);
    // ddl
    // tenant resource
    SET_STMT_TYPE(T_CREATE_RESOURCE_POOL);
    SET_STMT_TYPE(T_DROP_RESOURCE_POOL);
    SET_STMT_TYPE(T_ALTER_RESOURCE_POOL);
    SET_STMT_TYPE(T_SPLIT_RESOURCE_POOL);
    SET_STMT_TYPE(T_MERGE_RESOURCE_POOL);
    SET_STMT_TYPE(T_CREATE_RESOURCE_UNIT);
    SET_STMT_TYPE(T_ALTER_RESOURCE_UNIT);
    SET_STMT_TYPE(T_DROP_RESOURCE_UNIT);
    SET_STMT_TYPE(T_CREATE_TENANT);
    SET_STMT_TYPE(T_DROP_TENANT);
    SET_STMT_TYPE(T_MODIFY_TENANT);
    SET_STMT_TYPE(T_LOCK_TENANT);
    // database
    SET_STMT_TYPE(T_CREATE_DATABASE);
    SET_STMT_TYPE(T_ALTER_DATABASE);
    SET_STMT_TYPE(T_DROP_DATABASE);
    // tablegroup
    SET_STMT_TYPE(T_CREATE_TABLEGROUP);
    SET_STMT_TYPE(T_ALTER_TABLEGROUP);
    SET_STMT_TYPE(T_DROP_TABLEGROUP);
    // table
    SET_STMT_TYPE(T_CREATE_TABLE);
    SET_STMT_TYPE(T_DROP_TABLE);
    SET_STMT_TYPE(T_RENAME_TABLE);
    SET_STMT_TYPE(T_TRUNCATE_TABLE);
    SET_STMT_TYPE(T_CREATE_TABLE_LIKE);
    SET_STMT_TYPE(T_ALTER_TABLE);
    SET_STMT_TYPE(T_OPTIMIZE_TABLE);
    SET_STMT_TYPE(T_OPTIMIZE_TENANT);
    SET_STMT_TYPE(T_OPTIMIZE_ALL);
    // view
    SET_STMT_TYPE(T_CREATE_VIEW);
    SET_STMT_TYPE(T_ALTER_VIEW);
    SET_STMT_TYPE(T_DROP_VIEW);
    // index
    SET_STMT_TYPE(T_CREATE_INDEX);
    SET_STMT_TYPE(T_DROP_INDEX);
    // flashback
    SET_STMT_TYPE(T_FLASHBACK_TENANT);		
    SET_STMT_TYPE(T_FLASHBACK_DATABASE);
    SET_STMT_TYPE(T_FLASHBACK_TABLE_FROM_RECYCLEBIN);
    SET_STMT_TYPE(T_FLASHBACK_INDEX);	
    // purge
    SET_STMT_TYPE(T_PURGE_RECYCLEBIN);
    SET_STMT_TYPE(T_PURGE_TENANT);
    SET_STMT_TYPE(T_PURGE_DATABASE);
    SET_STMT_TYPE(T_PURGE_TABLE);
    SET_STMT_TYPE(T_PURGE_INDEX);
    // outline
    SET_STMT_TYPE(T_CREATE_OUTLINE);
    SET_STMT_TYPE(T_ALTER_OUTLINE);
    SET_STMT_TYPE(T_DROP_OUTLINE);
    // synonym
    SET_STMT_TYPE(T_CREATE_SYNONYM);
    SET_STMT_TYPE(T_DROP_SYNONYM);
    // variable set
    SET_STMT_TYPE(T_VARIABLE_SET);
    // plan baseline
    SET_STMT_TYPE(T_ALTER_BASELINE);
    // read only
    SET_STMT_TYPE(T_EXPLAIN);
    SET_STMT_TYPE(T_SHOW_COLUMNS);
    SET_STMT_TYPE(T_SHOW_TABLES);
    SET_STMT_TYPE(T_SHOW_DATABASES);
    SET_STMT_TYPE(T_SHOW_TABLE_STATUS);
    SET_STMT_TYPE(T_SHOW_SERVER_STATUS);
    SET_STMT_TYPE(T_SHOW_VARIABLES);
    SET_STMT_TYPE(T_SHOW_SCHEMA);
    SET_STMT_TYPE(T_SHOW_CREATE_DATABASE);
    SET_STMT_TYPE(T_SHOW_CREATE_TABLE);
    SET_STMT_TYPE(T_SHOW_CREATE_VIEW);
    SET_STMT_TYPE(T_SHOW_WARNINGS);
    SET_STMT_TYPE(T_SHOW_ERRORS);
    SET_STMT_TYPE(T_SHOW_GRANTS);
    SET_STMT_TYPE(T_SHOW_CHARSET);
    SET_STMT_TYPE(T_SHOW_COLLATION);
    SET_STMT_TYPE(T_SHOW_PARAMETERS);
    SET_STMT_TYPE(T_SHOW_INDEXES);
    SET_STMT_TYPE(T_SHOW_PROCESSLIST);
    SET_STMT_TYPE(T_SHOW_TABLEGROUPS);
    SET_STMT_TYPE(T_SHOW_RESTORE_PREVIEW);
    SET_STMT_TYPE(T_SHOW_RECYCLEBIN);
    SET_STMT_TYPE(T_SHOW_TENANT);
    SET_STMT_TYPE(T_CREATE_SAVEPOINT);
    SET_STMT_TYPE(T_RELEASE_SAVEPOINT);
    SET_STMT_TYPE(T_ROLLBACK_SAVEPOINT);
    SET_STMT_TYPE(T_SHOW_QUERY_RESPONSE_TIME);
    SET_STMT_TYPE(T_HELP);
#undef SET_STMT_TYPE
    case T_ROLLBACK:
    case T_COMMIT: {
      type = stmt::T_END_TRANS;
    } break;
    case T_SP_CREATE_TYPE: {
      type = stmt::T_CREATE_TYPE;
    } break;
    case T_SP_DROP_TYPE: {
      type = stmt::T_DROP_TYPE;
    } break;
    // stored procedure
    case T_SP_CREATE:
    case T_SF_CREATE: {
      type = stmt::T_CREATE_ROUTINE;
    } break;
    case T_SP_ALTER:
    case T_SF_ALTER: {
      type = stmt::T_ALTER_ROUTINE;
    } break;
    case T_SP_DROP:
    case T_SF_DROP: {
      type = stmt::T_DROP_ROUTINE;
    } break;
    // package
    case T_PACKAGE_CREATE: {
      type = stmt::T_CREATE_PACKAGE;
    } break;
    case T_PACKAGE_CREATE_BODY: {
      type = stmt::T_CREATE_PACKAGE_BODY;
    } break;
    case T_PACKAGE_ALTER: {
      type = stmt::T_ALTER_PACKAGE;
    } break;
    case T_PACKAGE_DROP: {
      type = stmt::T_DROP_PACKAGE;
    } break;
    default: {
      type = stmt::T_NONE;
    } break;
  }

  return type;
}

int ObResolverUtils::resolve_stmt_type(const ParseResult& result, stmt::StmtType& type)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(result.result_tree_));
  CK((T_STMT_LIST == result.result_tree_->type_));
  CK((1 == result.result_tree_->num_child_));
  CK(OB_NOT_NULL(result.result_tree_->children_[0]));
  if (OB_SUCC(ret)) {
    type = get_stmt_type_by_item_type(result.result_tree_->children_[0]->type_);
  }
  return ret;
}

int ObResolverUtils::resolve_const(const ParseNode* node, const stmt::StmtType stmt_type, ObIAllocator& allocator,
    const ObCollationType connection_collation, const ObCollationType nchar_collation, const ObTimeZoneInfo* tz_info,
    ObObjParam& val, const bool is_paramlize, common::ObString& literal_prefix,
    const ObLengthSemantics default_length_semantics, const ObCollationType server_collation,
    ObExprInfo* parents_expr_info)
{
  UNUSED(stmt_type);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_UNLIKELY(node->type_ < T_INVALID) || OB_UNLIKELY(node->type_ >= T_MAX_CONST)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse node is invalid", K(node));
  } else {
    LOG_DEBUG("resolve item", "item_type", get_type_name(static_cast<int>(node->type_)));
    int16_t precision = PRECISION_UNKNOWN_YET;
    int16_t scale = SCALE_UNKNOWN_YET;
    // const value contains NOT_NULL flag
    val.set_result_flag(OB_MYSQL_NOT_NULL_FLAG);
    switch (node->type_) {
      case T_HEX_STRING: {
        ObString str_val;
        str_val.assign_ptr(const_cast<char*>(node->str_value_), static_cast<int32_t>(node->str_len_));
        val.set_hex_string(str_val);
        val.set_collation_level(CS_LEVEL_COERCIBLE);
        val.set_scale(0);
        val.set_length(static_cast<int32_t>(node->str_len_));
        val.set_param_meta(val.get_meta());
        break;
      }
      case T_VARCHAR:
      case T_CHAR:
      case T_NVARCHAR2:
      case T_NCHAR: {
        if (lib::is_oracle_mode() && node->str_len_ == 0) {
          val.set_null();
          val.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
          val.set_length(0);
          val.set_length_semantics(default_length_semantics);
          ObObjMeta null_meta = ObObjMeta();
          null_meta.set_collation_type(connection_collation);
          null_meta.set_type(ObCharType);
          null_meta.set_collation_level(CS_LEVEL_COERCIBLE);
          val.set_param_meta(null_meta);
        } else {
          ObString str_val;
          ObObj result_val;
          str_val.assign_ptr(const_cast<char*>(node->str_value_), static_cast<int32_t>(node->str_len_));
          val.set_string(static_cast<ObObjType>(node->type_), str_val);
          // decide collation
          /*
           MySQL determines a literal's character set and collation in the following manner:

           If both _X and COLLATE Y are specified, character set X and collation Y are used.

           If _X is specified but COLLATE is not specified, character set X and its default collation
           are used. To see the default collation for each character set, use the SHOW COLLATION statement.

           Otherwise, the character set and collation given by the character_set_connection and
           collation_connection system variables are used.
           */
          val.set_collation_level(CS_LEVEL_COERCIBLE);
          // TODO::  raw
          //        if (share::is_oracle_mode()) {
          //          val.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          //          LOG_DEBUG("oracle use default cs_type", K(val), K(connection_collation));
          //        } else if (0 == node->num_child_) {
          if (0 == node->num_child_) {
            // for STRING without collation, e.g. show tables like STRING;
            val.set_collation_type(connection_collation);
          } else {
            // STRING in SQL expression
            ParseNode* charset_node = NULL;
            if (NULL != node->children_[0] && T_CHARSET == node->children_[0]->type_) {
              charset_node = node->children_[0];
            }

            if (NULL == charset_node) {
              val.set_collation_type(connection_collation);
            } else {
              ObCharsetType charset_type = CHARSET_INVALID;
              ObCollationType collation_type = CS_TYPE_INVALID;
              if (charset_node != NULL) {
                ObString charset(charset_node->str_len_, charset_node->str_value_);
                if (CHARSET_INVALID == (charset_type = ObCharset::charset_type(charset.trim()))) {
                  ret = OB_ERR_UNKNOWN_CHARSET;
                  LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
                } else {
                  // use the default collation of the specified charset
                  collation_type = ObCharset::get_default_collation(charset_type);
                  val.set_collation_type(collation_type);
                  LOG_DEBUG("use default collation", K(charset_type), K(collation_type));
                  ObLength length = static_cast<ObLength>(
                      ObCharset::strlen_char(val.get_collation_type(), val.get_string_ptr(), val.get_string_len()));
                  val.set_length(length);

                  if (OB_SUCC(ret) && OB_FAIL(ObSQLUtils::check_well_formed_str(val, result_val, true, true))) {
                    LOG_WARN("invalid str", K(ret), K(val));
                  }
                }
              }
            }
          }
          ObLengthSemantics length_semantics = LS_DEFAULT;
          if (OB_SUCC(ret)) {
            if (T_NVARCHAR2 == node->type_ || T_NCHAR == node->type_) {
              length_semantics = LS_CHAR;
            } else {
              length_semantics = default_length_semantics;
            }
            if (is_oracle_byte_length(share::is_oracle_mode(), length_semantics) && T_NVARCHAR2 != node->type_ &&
                T_NCHAR != node->type_) {
              val.set_length(val.get_string_len());
              val.set_length_semantics(LS_BYTE);
            } else {
              ObLength length = static_cast<ObLength>(
                  ObCharset::strlen_char(val.get_collation_type(), val.get_string_ptr(), val.get_string_len()));
              val.set_length(length);
              val.set_length_semantics(LS_CHAR);
            }
          }

          if (OB_SUCC(ret) && share::is_oracle_mode()) {
            ObCollationType target_collation = CS_TYPE_INVALID;
            if (T_NVARCHAR2 == node->type_ || T_NCHAR == node->type_) {
              target_collation = nchar_collation;
            } else {
              target_collation = server_collation;
            }

            if (ObCharset::charset_type_by_coll(connection_collation) ==
                ObCharset::charset_type_by_coll(target_collation)) {
              val.set_collation_type(target_collation);
            } else {
              ObString str;
              val.get_string(str);
              char* buf = nullptr;
              const int CharConvertFactorNum = 4;
              int32_t buf_len = str.length() * CharConvertFactorNum;
              uint32_t result_len = 0;
              if (0 == buf_len) {
                // do nothing
              } else if (CS_TYPE_INVALID == target_collation) {
                ret = OB_ERR_UNEXPECTED;
              } else if (OB_UNLIKELY(nullptr == (buf = static_cast<char*>(allocator.alloc(buf_len))))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_ERROR("alloc memory failed", K(ret), K(buf_len));
              } else {
                ret = ObCharset::charset_convert(
                    connection_collation, str.ptr(), str.length(), target_collation, buf, buf_len, result_len);
                if (OB_SUCCESS != ret) {
                  int32_t str_offset = 0;
                  int64_t buf_offset = 0;
                  ObString question_mark = ObCharsetUtils::get_const_str(target_collation, '?');
                  while (str_offset < str.length() && buf_offset + question_mark.length() <= buf_len) {
                    int64_t offset =
                        ObCharset::charpos(connection_collation, str.ptr() + str_offset, str.length() - str_offset, 1);
                    ret = ObCharset::charset_convert(connection_collation,
                        str.ptr() + str_offset,
                        offset,
                        target_collation,
                        buf + buf_offset,
                        buf_len - buf_offset,
                        result_len);
                    str_offset += offset;
                    if (OB_SUCCESS == ret) {
                      buf_offset += result_len;
                    } else {
                      MEMCPY(buf + buf_offset, question_mark.ptr(), question_mark.length());
                      buf_offset += question_mark.length();
                    }
                  }
                  if (str_offset < str.length()) {
                    ret = OB_SIZE_OVERFLOW;
                    LOG_WARN("size overflow", K(ret), K(str), KPHEX(str.ptr(), str.length()));
                  } else {
                    LOG_DEBUG("charset convert failed", K(ret), K(connection_collation), K(target_collation));
                    result_len = buf_offset;
                    ret = OB_SUCCESS;
                  }
                }

                if (OB_SUCC(ret)) {
                  val.set_string(val.get_type(), buf, static_cast<int32_t>(result_len));
                  val.set_collation_type(target_collation);
                  val.set_collation_level(CS_LEVEL_COERCIBLE);
                }
              }
            }
          }

          if (OB_SUCC(ret) && share::is_mysql_mode() &&
              OB_FAIL(ObSQLUtils::check_well_formed_str(val, result_val, true, true))) {
            LOG_WARN("invalid str", K(ret), K(val));
          }
          val.set_param_meta(val.get_meta());
        }
        LOG_DEBUG("resolve const char", K(val));
        break;
      }
      case T_FLOAT:
      case T_DOUBLE: {
        int err = 0;
        double value = 0;
        char* endptr = NULL;
        value = ObCharset::strntod(node->str_value_, static_cast<int32_t>(node->str_len_), &endptr, &err);
        if (EOVERFLOW == err) {
          ret = OB_NUMERIC_OVERFLOW;
          literal_prefix.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
          LOG_WARN("float/double out of range", K(literal_prefix), K(ret));
        } else {
          if (T_DOUBLE == node->type_) {
            val.set_double(value);
          } else {
            val.set_float(value);
          }
          val.set_length(static_cast<int16_t>(node->str_len_));
          ObString tmp_input(static_cast<int32_t>(node->str_len_), node->str_value_);
          if (lib::is_oracle_mode()) {
            literal_prefix.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
            val.set_precision(PRECISION_UNKNOWN_YET);
            val.set_scale(NUMBER_SCALE_UNKNOWN_YET);
            if (val.is_double() ? (0 != isinf(val.get_double())) : (0 != isinff(val.get_float()))) {
              ret = OB_NUMERIC_OVERFLOW;
              LOG_WARN("float/double out of range", K(literal_prefix), K(val), K(ret));
            }
          }
          _LOG_DEBUG("finish parse double from str, tmp_input=%.*s, value=%.30lf, ret=%d, type=%d, literal_prefix=%.*s",
              tmp_input.length(),
              tmp_input.ptr(),
              value,
              ret,
              node->type_,
              literal_prefix.length(),
              literal_prefix.ptr());
        }
        val.set_param_meta(val.get_meta());
        break;
      }
      case T_UINT64:
      case T_INT: {
        if (NULL != parents_expr_info) {
          LOG_DEBUG("T_INT as pl acc idx", K(parents_expr_info->has_member(IS_PL_ACCESS_IDX)));
        }
        if (lib::is_oracle_mode() && NULL != node->str_value_ &&
            (NULL == parents_expr_info || !parents_expr_info->has_member(IS_PL_ACCESS_IDX))) {
          // go through as T_NUMBER
          LOG_DEBUG("from int to number", K(node->str_value_), K(lbt()));
        } else {
          if (T_INT == node->type_) {
            val.set_int(node->value_);
          } else {
            val.set_uint64(static_cast<uint64_t>(node->value_));
          }
          val.set_scale(0);
          val.set_precision(static_cast<int16_t>(node->str_len_));
          val.set_length(static_cast<int16_t>(node->str_len_));
          val.set_param_meta(val.get_meta());
          if (true == node->is_date_unit_) {
            literal_prefix.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
          }

          break;
        }
      }
      case T_NUMBER: {
        number::ObNumber nmb;
        int16_t len = 0;
        ObString tmp_string(static_cast<int32_t>(node->str_len_), node->str_value_);
        if (NULL != tmp_string.find('e') || NULL != tmp_string.find('E')) {
          ret = nmb.from_sci(node->str_value_, static_cast<int32_t>(node->str_len_), allocator, &precision, &scale);
          len = static_cast<int16_t>(node->str_len_);
          if (lib::is_oracle_mode()) {
            literal_prefix.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
          }
        } else {
          ret = nmb.from(node->str_value_, static_cast<int32_t>(node->str_len_), allocator, &precision, &scale);
          len = static_cast<int16_t>(node->str_len_);
        }
        if (OB_FAIL(ret)) {
          if (OB_INTEGER_PRECISION_OVERFLOW == ret) {
            LOG_WARN("integer presision overflow", K(ret));
          } else if (OB_NUMERIC_OVERFLOW == ret) {
            LOG_WARN("numeric overflow");
          } else {
            LOG_WARN("unexpected error", K(ret));
          }
        } else {
          if (lib::is_oracle_mode()) {
            precision = PRECISION_UNKNOWN_YET;
            scale = NUMBER_SCALE_UNKNOWN_YET;
          }
          val.set_number(nmb);
          val.set_precision(precision);
          val.set_scale(scale);
          val.set_length(len);
          LOG_DEBUG("finish parse number from str", K(literal_prefix), K(nmb));
        }
        val.set_param_meta(val.get_meta());
        break;
      }
      case T_NUMBER_FLOAT: {
        number::ObNumber nmb;
        if (OB_FAIL(nmb.from(node->str_value_, static_cast<int32_t>(node->str_len_), allocator, &precision, &scale))) {
          if (OB_INTEGER_PRECISION_OVERFLOW == ret) {
            LOG_WARN("integer presision overflow", K(ret));
          } else if (OB_NUMERIC_OVERFLOW == ret) {
            LOG_WARN("numeric overflow");
          } else {
            LOG_WARN("unexpected error", K(ret));
          }
        } else {
          val.set_number(nmb);
          val.set_precision(PRECISION_UNKNOWN_YET);
          val.set_length(static_cast<int16_t>(node->str_len_));
        }
        val.set_param_meta(val.get_meta());
        break;
      }
      case T_QUESTIONMARK: {
        if (!is_paramlize) {
          val.set_unknown(node->value_);
        } else {
          // used for sql concurrency control
          val.set_int(0);
          val.set_scale(0);
          val.set_precision(1);
          val.set_length(1);
        }
        val.set_param_meta(val.get_meta());
        break;
      }
      case T_BOOL: {
        val.set_bool(node->value_ == 1 ? true : false);
        val.set_scale(0);
        val.set_precision(1);
        val.set_length(1);
        val.set_param_meta(val.get_meta());
        break;
      }
      case T_YEAR: {
        ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
        uint8_t time_val = 0;
        if (OB_FAIL(ObTimeConverter::str_to_year(time_str, time_val))) {
          LOG_WARN("fail to convert str to year", K(time_str), K(ret));
        } else {
          val.set_year(time_val);
          val.set_scale(0);
          val.set_param_meta(val.get_meta());
        }
        break;
      }
      case T_DATE: {
        ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
        int32_t time_val = 0;
        if (OB_FAIL(ObTimeConverter::str_to_date(time_str, time_val))) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "DATE", to_cstring(time_str));
        } else {
          val.set_date(time_val);
          val.set_scale(0);
          val.set_param_meta(val.get_meta());
          literal_prefix = ObString::make_string(LITERAL_PREFIX_DATE);
        }
        break;
      }
      case T_TIME: {
        ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
        int64_t time_val = 0;
        if (OB_FAIL(ObTimeConverter::str_to_time(time_str, time_val, &scale))) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "TIME", to_cstring(time_str));
        } else {
          val.set_time(time_val);
          val.set_scale(scale);
          val.set_param_meta(val.get_meta());
          literal_prefix = ObString::make_string(MYSQL_LITERAL_PREFIX_TIME);
        }
        break;
      }
      case T_DATETIME: {
        ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
        int64_t time_val = 0;
        ObTimeConvertCtx cvrt_ctx(tz_info, false);
        if (OB_FAIL(ObTimeConverter::literal_date_validate_oracle(time_str, cvrt_ctx, time_val))) {
          LOG_WARN("fail to convert str to oracle date", K(time_str), K(ret));
        } else {
          val.set_datetime(time_val);
          val.set_scale(OB_MAX_DATE_PRECISION);
          val.set_param_meta(val.get_meta());
          literal_prefix = ObString::make_string(LITERAL_PREFIX_DATE);
        }
        break;
      }
      case T_TIMESTAMP: {
        ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
        int64_t time_val = 0;
        ObTimeConvertCtx cvrt_ctx(tz_info, false);
        if (OB_FAIL(ObTimeConverter::str_to_datetime(time_str, cvrt_ctx, time_val, &scale))) {
          ret = OB_ERR_WRONG_VALUE;
          LOG_USER_ERROR(OB_ERR_WRONG_VALUE, "DATETIME", to_cstring(time_str));
        } else {
          val.set_datetime(time_val);
          val.set_scale(scale);
          val.set_param_meta(val.get_meta());
          literal_prefix = ObString::make_string(LITERAL_PREFIX_TIMESTAMP);
        }
        break;
      }
      case T_TIMESTAMP_TZ: {
        ObObjType value_type = ObNullType;
        ObOTimestampData tz_value;
        ObTimeConvertCtx cvrt_ctx(tz_info, true);
        ObString time_str(static_cast<int32_t>(node->str_len_), node->str_value_);
        // if (OB_FAIL(ObTimeConverter::str_to_otimestamp(time_str, cvrt_ctx, tmp_type, ot_data))) {
        if (OB_FAIL(ObTimeConverter::literal_timestamp_validate_oracle(time_str, cvrt_ctx, value_type, tz_value))) {
          LOG_WARN("fail to str_to_otimestamp", K(time_str), K(ret));
        } else {
          /* use max scale bug:#18093350 */
          val.set_otimestamp_value(value_type, tz_value);
          val.set_scale(OB_MAX_TIMESTAMP_TZ_PRECISION);
          literal_prefix = ObString::make_string(LITERAL_PREFIX_TIMESTAMP);
          val.set_param_meta(val.get_meta());
        }
        break;
      };
      case T_INTERVAL_YM: {
        char* tmp_ptr = NULL;
        ObString tmp_str;
        ObIntervalYMValue interval_value;
        ObScale defined_scale;
        ObString interval_str;
        int16_t leading_precision = -1;
        ObDateUnitType part_begin = DATE_UNIT_MAX;
        ObDateUnitType part_end = DATE_UNIT_MAX;
        ObValueChecker<int16_t> precision_checker(
            0, MAX_SCALE_FOR_ORACLE_TEMPORAL, OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE);

        literal_prefix.assign_ptr(node->raw_text_, static_cast<int32_t>(node->text_len_));

        OZ(ob_dup_cstring(allocator, literal_prefix, tmp_ptr));
        OX(tmp_str = ObString(tmp_ptr));

        if (OB_SUCC(ret)) {
          char* to_pos = strcasestr(tmp_ptr, "to");
          if (OB_NOT_NULL(to_pos)) {
            *to_pos = '\0';
            OZ(parse_interval_ym_type(tmp_ptr, part_begin));
            OZ(parse_interval_ym_type(to_pos + 1, part_end));
          } else {
            OZ(parse_interval_ym_type(tmp_ptr, part_begin));
            part_end = part_begin;
          }
        }

        OZ(parse_interval_precision(tmp_ptr, leading_precision, 2));
        OZ(precision_checker.validate(leading_precision), leading_precision);

        if (OB_SUCC(ret)) {
          defined_scale = ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(static_cast<int8_t>(leading_precision));
          interval_str.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
          ObScale scale = defined_scale;
          if (OB_FAIL(ObTimeConverter::literal_interval_ym_validate_oracle(
                  interval_str, interval_value, scale, part_begin, part_end))) {
            LOG_WARN(
                "fail to validate interval literal", K(ret), K(interval_str), K(part_begin), K(part_end), K(scale));
          } else {
            val.set_interval_ym(interval_value);
            val.set_scale(defined_scale);
            val.set_param_meta(val.get_meta());
          }
        }

        LOG_DEBUG("interval year to month literal resolve", K(interval_str), K(literal_prefix));
        break;
      }
      case T_INTERVAL_DS: {
        char* tmp_ptr = NULL;
        ObString tmp_str;
        ObIntervalDSValue interval_value;
        ObScale defined_scale;
        ObString interval_str;
        int16_t leading_precision = -1;
        int16_t second_precision = -1;
        ObDateUnitType part_begin = DATE_UNIT_MAX;
        ObDateUnitType part_end = DATE_UNIT_MAX;
        ObValueChecker<int16_t> precision_checker(
            0, MAX_SCALE_FOR_ORACLE_TEMPORAL, OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE);

        literal_prefix.assign_ptr(node->raw_text_, static_cast<int32_t>(node->text_len_));
        OZ(ob_dup_cstring(allocator, literal_prefix, tmp_ptr));
        OX(tmp_str = ObString(tmp_ptr));

        if (OB_SUCC(ret)) {
          char* to_pos = strcasestr(tmp_ptr, "to");
          if (OB_NOT_NULL(to_pos)) {  // interval 'xxx' day (x) to second (x)
            *to_pos = '\0';
            OZ(parse_interval_ds_type(tmp_ptr, part_begin));
            OZ(parse_interval_precision(tmp_ptr, leading_precision, 2));
            OZ(parse_interval_ds_type(to_pos + 1, part_end));
            OZ(parse_interval_precision(to_pos + 1, second_precision, DATE_UNIT_SECOND == part_end ? 6 : 0));
          } else {  // interval 'xxx' day (x)
            char* comma_pos = strchr(tmp_ptr, ',');
            OZ(parse_interval_ds_type(tmp_ptr, part_begin));
            part_end = part_begin;
            if (OB_NOT_NULL(comma_pos)) {  // interval 'xxx' second(x, x)
              *comma_pos = ')';
              OZ(parse_interval_precision(tmp_ptr, leading_precision, 2));
              *comma_pos = '(';
              OZ(parse_interval_precision(comma_pos, second_precision, 6));
            } else {
              OZ(parse_interval_precision(tmp_ptr, leading_precision, 2));
              second_precision = DATE_UNIT_SECOND == part_end ? 6 : 0;
            }
          }
        }

        OZ(precision_checker.validate(leading_precision), leading_precision);
        OZ(precision_checker.validate(second_precision), second_precision);

        if (OB_SUCC(ret)) {
          defined_scale = ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(
              static_cast<int8_t>(leading_precision), static_cast<int8_t>(second_precision));
          interval_str.assign_ptr(node->str_value_, static_cast<int32_t>(node->str_len_));
          ObScale scale = defined_scale;
          if (OB_FAIL(ObTimeConverter::literal_interval_ds_validate_oracle(
                  interval_str, interval_value, scale, part_begin, part_end))) {
            LOG_WARN(
                "fail to validate interval literal", K(ret), K(interval_str), K(part_begin), K(part_end), K(scale));
          } else {
            val.set_interval_ds(interval_value);
            val.set_scale(defined_scale);
            val.set_param_meta(val.get_meta());
          }
        }

        LOG_DEBUG("interval day to second literal resolve", K(interval_str), K(literal_prefix));
        break;
      }
      case T_NULL: {
        val.set_null();
        val.unset_result_flag(OB_MYSQL_NOT_NULL_FLAG);
        val.set_length(0);
        val.set_param_meta(val.get_meta());
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unknown item type here", K(ret), K(node->type_));
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_timestamp_node(const bool is_set_null, const bool is_set_default,
    const bool is_first_timestamp_column, ObSQLSessionInfo* session_info, ObColumnSchemaV2& column)
{
  int ret = OB_SUCCESS;
  // compatable for mysql
  bool explicit_value = false;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session info is NULL", K(ret));
  } else if (OB_FAIL(session_info->get_explicit_defaults_for_timestamp(explicit_value))) {
    LOG_WARN("fail to get explicit_defaults_for_timestamp", K(ret));
  } else if (!explicit_value) {
    if (is_first_timestamp_column && !is_set_null && !is_set_default && !column.is_on_update_current_timestamp()) {
      column.set_nullable(false);
      const_cast<ObObj*>(&column.get_cur_default_value())->set_ext(ObActionFlag::OP_DEFAULT_NOW_FLAG);
      column.set_on_update_current_timestamp(true);
    } else if (!is_set_null) {
      column.set_nullable(false);
      if (!is_set_default) {
        if (is_no_zero_date(session_info->get_sql_mode())) {
          ret = OB_INVALID_DEFAULT;
          LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
        } else {
          const_cast<ObObj*>(&column.get_cur_default_value())->set_timestamp(ObTimeConverter::ZERO_DATETIME);
        }
      } else if (column.get_cur_default_value().is_null()) {
        ret = OB_INVALID_DEFAULT;
        LOG_USER_ERROR(OB_INVALID_DEFAULT, column.get_column_name_str().length(), column.get_column_name_str().ptr());
      }
    } else {
      column.set_nullable(true);
      if (!is_set_default) {
        const_cast<ObObj*>(&column.get_cur_default_value())->set_null();
      }
    }
  }
  return ret;
}

int ObResolverUtils::add_column_ids_without_duplicate(const uint64_t column_id, common::ObIArray<uint64_t>* array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(array) || OB_UNLIKELY(OB_INVALID_ID == column_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(array), K(column_id));
  } else {
    bool exist = false;
    for (int64_t i = 0; !exist && i < array->count(); i++) {
      if (array->at(i) == column_id) {
        exist = true;
      }
    }
    if (!exist) {
      if (OB_FAIL(array->push_back(column_id))) {
        LOG_WARN("fail to push back column id", K(ret));
      }
    }
  }
  return ret;
}

// for recursively process columns item in resolve_const_expr
// just wrap columns process logic in resolve_const_expr
int ObResolverUtils::resolve_columns_for_const_expr(
    ObRawExpr*& expr, ObArray<ObQualifiedName>& columns, ObResolverParams& resolve_params)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(resolve_params.allocator_));
  CK(OB_NOT_NULL(resolve_params.expr_factory_));
  CK(OB_NOT_NULL(resolve_params.session_info_));
  CK(OB_NOT_NULL(resolve_params.schema_checker_));
  CK(OB_NOT_NULL(resolve_params.schema_checker_->get_schema_guard()));
  // CK(OB_NOT_NULL(resolve_params.sql_proxy_));
  ObArray<ObRawExpr*> real_exprs;
  ObRawExpr* real_ref_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ObQualifiedName& q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      ObSysFunRawExpr* sys_func_expr = q_name.access_idents_.at(0).sys_func_expr_;
      if (OB_ISNULL(sys_func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys expr is null", K(ret));
      } else if (OB_FAIL(sys_func_expr->check_param_num())) {
        LOG_WARN("sys func check param failed", K(ret));
      } else {
        real_ref_expr = sys_func_expr;
      }
    } else {
      // const expr can't use column
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString column_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      ObString scope_name = ObString::make_string("field_list");
      LOG_USER_ERROR(
          OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(), scope_name.length(), scope_name.ptr());
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(real_exprs.push_back(real_ref_expr))) {
        LOG_WARN("push back error", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < real_exprs.count(); ++i) {
      OZ(ObRawExprUtils::replace_ref_column(real_ref_expr, columns.at(i).ref_expr_, real_exprs.at(i)));
    }
    OZ(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr));
  }
  return ret;
}

int ObResolverUtils::resolve_const_expr(
    ObResolverParams& params, const ParseNode& node, ObRawExpr*& const_expr, ObIArray<ObVarInfo>* var_infos)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObOpRawExpr*> op_exprs;
  ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (OB_ISNULL(params.expr_factory_) || OB_ISNULL(params.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K_(params.session_info));
  } else if (OB_FAIL(params.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObExprResolveContext ctx(*params.expr_factory_, params.session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params.param_list_;
    ctx.is_extract_param_type_ = !params.is_prepare_protocol_;  // when prepare do not extract
    ctx.schema_checker_ = params.schema_checker_;
    ctx.session_info_ = params.session_info_;
    ctx.query_ctx_ = params.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node,
                   const_expr,
                   columns,
                   sys_vars,
                   sub_query_info,
                   aggr_exprs,
                   win_exprs,
                   op_exprs,
                   user_var_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (OB_FAIL(resolve_columns_for_const_expr(const_expr, columns, params))) {
      LOG_WARN("resolve columnts for const expr failed", K(ret));
    } else if (sub_query_info.count() > 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "subqueries or stored function calls here");
    }

    // process oracle compatible implicit conversion
    if (OB_SUCC(ret) && op_exprs.count() > 0) {
      LOG_WARN("impicit cast for oracle", K(ret));
      if (OB_FAIL(ObRawExprUtils::resolve_op_exprs_for_oracle_implicit_cast(
              ctx.expr_factory_, ctx.session_info_, op_exprs))) {
        LOG_WARN("impicit cast failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (T_SP_CPARAM == const_expr->get_expr_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "call procedure");
      } else {
        OZ(const_expr->formalize(params.session_info_));
      }
      if (OB_SUCC(ret)) {
        if (var_infos != NULL) {
          if (ObRawExprUtils::merge_variables(sys_vars, *var_infos)) {
            LOG_WARN("merge variables failed", K(ret));
          }
        } else {
          params.prepare_param_count_ += ctx.prepare_param_count_;  // prepare param count
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::get_collation_type_of_names(
    const ObSQLSessionInfo* session_info, const ObNameTypeClass type_class, ObCollationType& cs_type)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  cs_type = CS_TYPE_INVALID;
  if (share::is_mysql_mode()) {
    if (OB_TABLE_NAME_CLASS == type_class) {
      if (OB_ISNULL(session_info)) {
        ret = OB_NOT_INIT;
        LOG_WARN("session info is null");
      } else if (OB_FAIL(session_info->get_name_case_mode(case_mode))) {
        LOG_WARN("fail to get name case mode", K(ret));
      } else if (OB_ORIGIN_AND_SENSITIVE == case_mode) {
        cs_type = CS_TYPE_UTF8MB4_BIN;
      } else if (OB_ORIGIN_AND_INSENSITIVE == case_mode || OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
        cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
      }
    } else if (OB_COLUMN_NAME_CLASS == type_class) {
      cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
    } else if (OB_USER_NAME_CLASS == type_class) {
      cs_type = CS_TYPE_UTF8MB4_BIN;
    }
  } else {
    cs_type = CS_TYPE_UTF8MB4_BIN;
  }
  return ret;
}

int ObResolverUtils::name_case_cmp(const ObSQLSessionInfo* session_info, const ObString& name,
    const ObString& name_other, const ObNameTypeClass type_class, bool& is_equal)
{

  ObCollationType cs_type = CS_TYPE_INVALID;
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_collation_type_of_names(session_info, type_class, cs_type))) {
    LOG_WARN("fail to get collation type of name", K(name), K(name_other), K(type_class), K(ret));
  } else if (0 == ObCharset::strcmp(cs_type, name, name_other)) {
    is_equal = true;
  } else {
    is_equal = false;
  }
  return ret;
}

bool ObResolverUtils::is_valid_oracle_partition_data_type(const ObObjType data_type, const bool check_value)
{
  bool bret = false;
  switch (data_type) {
    case ObIntType:
    case ObFloatType:
    case ObDoubleType:
    case ObNumberType:
    case ObDateTimeType:
    case ObVarcharType:
    case ObCharType:
    case ObTimestampNanoType: {
      bret = true;
      break;
    }
    case ObTimestampType:
    case ObTimestampLTZType: {
      bret = !check_value;
      break;
    }
    case ObTimestampTZType: {
      bret = check_value;
      break;
    }
    case ObRawType:
    case ObIntervalYMType:
    case ObIntervalDSType:
    case ObNumberFloatType:
    case ObNVarchar2Type:
    case ObNCharType: {
      bret = true;
      break;
    }
    default: {
      bret = false;
    }
  }
  return bret;
}

bool ObResolverUtils::is_valid_partition_column_type(
    const ObObjType type, const ObPartitionFuncType part_type, const bool is_check_value)
{
  int bret = false;
  if (PARTITION_FUNC_TYPE_HASH == part_type || PARTITION_FUNC_TYPE_HASH_V2 == part_type ||
      PARTITION_FUNC_TYPE_RANGE == part_type || PARTITION_FUNC_TYPE_LIST == part_type) {
    LOG_DEBUG("check partition column type", K(share::is_oracle_mode()), K(part_type), K(type), K(lbt()));
    if (ob_is_integer_type(type) || ObYearType == type) {
      bret = true;
    } else if (share::is_oracle_mode() && is_valid_oracle_partition_data_type(type, is_check_value)) {
      bret = true;
    }
  } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
    if (share::is_oracle_mode()) {
      if (is_valid_oracle_partition_data_type(type, is_check_value)) {
        bret = true;
      }
    } else {
      /**
       * All integer types: TINYINT, SMALLINT, MEDIUMINT, INT (INTEGER), and BIGINT. (This is the same as with
       * partitioning by RANGE and LIST.) Other numeric data types (such as DECIMAL or FLOAT) are not supported as
       * partitioning columns. DATE and DATETIME. Columns using other data types relating to dates or times are not
       * supported as partitioning columns. The following string types: CHAR, VARCHAR, BINARY, and VARBINARY. TEXT and
       * BLOB columns are not supported as partitioning columns.
       */
      // https://dev.mysql.com/doc/refman/5.7/en/partitioning-columns.html
      ObObjTypeClass type_class = ob_obj_type_class(type);
      if (ObIntTC == type_class || ObUIntTC == type_class || ObDateTimeTC == type_class || ObDateTC == type_class ||
          ObStringTC == type_class || ObYearTC == type_class) {
        bret = true;
      }
    }
  }
  return bret;
}

/* src_expr.type only can be int or number
   restype only can be (unsigned tiny/smallint/int/bigint)
*/
int ObResolverUtils::try_convert_to_unsiged(const ObExprResType restype, ObRawExpr& src_expr, bool& is_out_of_range)
{
  int ret = OB_SUCCESS;
  ObCastCtx cast_ctx;
  const ObObj& src = static_cast<sql::ObConstRawExpr*>(&src_expr)->get_value();
  ObObj& result = static_cast<sql::ObConstRawExpr*>(&src_expr)->get_value();

  is_out_of_range = true;

  if (OB_FAIL(ObObjCaster::to_type(restype.get_type(), cast_ctx, src, result))) {
    if (ret == OB_DATA_OUT_OF_RANGE) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("to type fail", K(restype), K(src_expr), K(ret));
    }
  } else {
    is_out_of_range = false;
  }

  return ret;
}

int ObResolverUtils::check_partition_range_value_result_type(
    const ObPartitionFuncType part_type, const ObColumnRefRawExpr& part_column_expr, ObRawExpr& part_value_expr)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("check_partition_range_value_result_type ", K(part_type), K(part_column_expr), K(part_value_expr));

  ObObjTypeClass expect_value_tc = ObMaxTC;
  bool need_cs_check = false;
  const bool is_oracle_mode = share::is_oracle_mode();
  ObObjType part_column_expr_type = part_column_expr.get_data_type();
  switch (part_column_expr_type) {
    case ObTinyIntType:
    case ObSmallIntType:
    case ObMediumIntType:
    case ObInt32Type:
    case ObIntType: {
      expect_value_tc = ObIntTC;
      need_cs_check = false;
      break;
    }
    case ObUTinyIntType:
    case ObUSmallIntType:
    case ObUMediumIntType:
    case ObUInt32Type:
    case ObUInt64Type: {
      expect_value_tc = ObUIntTC;
      need_cs_check = false;
      break;
    }
    case ObDateTimeType:
    case ObDateType:
    case ObTimeType:
    case ObRawType:
    case ObTimestampLTZType:
    case ObTimestampNanoType: {
      expect_value_tc = ObStringTC;
      need_cs_check = true;
      break;
    }
    case ObVarcharType:
    case ObCharType:
    case ObNVarchar2Type:
    case ObNCharType: {
      expect_value_tc = ObStringTC;
      need_cs_check = true;
      if (is_oracle_mode && CS_TYPE_UTF8MB4_GENERAL_CI == part_column_expr.get_collation_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "partition key with CS_TYPE_UTF8MB4_GENERAL_CI collation");
        LOG_WARN("Partition key is string with CS_TYPE_UTF8MB4_GENERAL_CI", K(ret));
      }
      break;
    }
    case ObIntervalYMType:
    case ObIntervalDSType: {
      expect_value_tc = ob_obj_type_class(part_column_expr_type);
      break;
    }
    case ObFloatType:
    case ObDoubleType: {
      if (is_oracle_mode) {
        expect_value_tc = ObStringTC;
        break;
      }
    }
    case ObNumberType:
    case ObNumberFloatType: {
      if (is_oracle_mode) {
        expect_value_tc = ObNumberTC;
        break;
      }
    }
    default: {
      ret = OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
      LOG_USER_ERROR(OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
          part_column_expr.get_column_name().length(),
          part_column_expr.get_column_name().ptr());
      LOG_WARN("Field is not a allowed type for this type of partitioning", K(ret), K(part_column_expr_type));
    }
  }
  UNUSED(need_cs_check);
  if (OB_SUCC(ret)) {
    ObObjType part_value_expr_type = part_value_expr.get_data_type();
    ObObjTypeClass part_value_expr_tc = part_value_expr.get_type_class();
    if (part_value_expr_tc != expect_value_tc || ObTimestampLTZType == part_column_expr_type) {
      bool is_allow = false;
      if (ObNullTC == part_value_expr_tc && PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
        is_allow = true;
      } else if (is_oracle_mode) {
        switch (part_column_expr_type) {
          case ObFloatType:
          case ObDoubleType:
          case ObNumberFloatType:
          case ObNumberType: {
            is_allow = (ObStringTC == part_value_expr_tc ||
                        (ObIntTC <= part_value_expr_tc && part_value_expr_tc <= ObNumberTC));
            break;
          }
          case ObDateTimeType:
          case ObTimestampNanoType: {
            is_allow = (ObDateTimeType == part_value_expr_type || ObTimestampNanoType == part_value_expr_type ||
                        ObTimestampTZType == part_value_expr_type);
            break;
          }
          case ObTimestampLTZType: {
            is_allow = (ObTimestampTZType == part_value_expr_type);
            break;
          }
          case ObVarcharType:
          case ObCharType:
          case ObNVarchar2Type:
          case ObNCharType: {
            is_allow = true;
            break;
          }
          default: {
            break;
          }
        }
      } else {
        if (OB_SUCC(ret) && !is_allow) {
          if (part_value_expr_type == part_column_expr_type) {
            is_allow = true;
          } else if (ObDateTimeType == part_column_expr_type &&
                     (ObDateType == part_value_expr_type || ObTimeType == part_value_expr_type)) {
            is_allow = true;
          }
        }
        bool is_out_of_range = true;
        /* for mysql mode only (siged -> unsigned ) */
        if (part_value_expr.is_const_expr() && (part_value_expr_tc == ObIntTC || part_value_expr_tc == ObNumberTC) &&
            expect_value_tc == ObUIntTC) {

          if (OB_FAIL(try_convert_to_unsiged(part_column_expr.get_result_type(), part_value_expr, is_out_of_range))) {
            LOG_WARN("check const range fail", K(ret));
          } else {
            is_allow = (is_out_of_range == false);
          }
        }
      }
      if (OB_SUCC(ret) && !is_allow) {
        ret = (ObTimestampLTZType == part_column_expr_type ? OB_ERR_WRONG_TIMESTAMP_LTZ_COLUMN_VALUE_ERROR
                                                           : OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);

        LOG_WARN("object type is invalid ",
            K(ret),
            K(expect_value_tc),
            K(part_value_expr_tc),
            K(part_column_expr_type),
            K(part_value_expr_type));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_valid_column_for_hash_or_range(
    const ObRawExpr& part_expr, const ObPartitionFuncType part_func_type)
{
  int ret = OB_SUCCESS;
  if (part_expr.is_column_ref_expr()) {
    const ObColumnRefRawExpr& column_ref = static_cast<const ObColumnRefRawExpr&>(part_expr);
    if (!is_valid_partition_column_type(column_ref.get_data_type(), part_func_type, false)) {
      ret = OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
      LOG_USER_ERROR(OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD,
          column_ref.get_column_name().length(),
          column_ref.get_column_name().ptr());
      LOG_WARN("Field is not a allowed type for this type of partitioning", K(ret), "type", column_ref.get_data_type());
    }
  } else {
    if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_func_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_func_type) {
      ret = OB_ERR_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD;
      LOG_WARN("partition by range columns should be column_ref expr", K(ret));
    }
  }
  return ret;
}

int ObResolverUtils::check_partition_value_expr_for_range(const ObString& part_name, const ObRawExpr& part_func_expr,
    ObRawExpr& part_value_expr, const ObPartitionFuncType part_type, const bool& in_tablegroup)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    bool gen_col_check = false;
    bool accept_charset_function = in_tablegroup;
    ObRawExprPartFuncChecker part_func_checker(gen_col_check, accept_charset_function);
    if (OB_FAIL(part_value_expr.preorder_accept(part_func_checker))) {
      if (lib::is_oracle_mode() && OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED == ret) {
        ret = (share::schema::is_list_part(part_type) ? OB_ERR_WRONG_TYPE_COLUMN_VALUE_V2_ERROR
                                                      : OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
      }
      LOG_WARN("check partition function failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (PARTITION_FUNC_TYPE_RANGE == part_type || PARTITION_FUNC_TYPE_LIST == part_type) {
      ObObjType value_type = part_value_expr.get_data_type();
      if (share::is_oracle_mode() && is_valid_oracle_partition_data_type(value_type, true)) {
        // oracle mode support int/numeric/datetime/timestamp/string/raw
      } else if (share::is_mysql_mode() && ob_is_integer_type(value_type)) {
        // iin partition by range(xx) partition p0 values less than (expr), expr would always be integer
      } else if (ObNullTC == part_value_expr.get_type_class() && PARTITION_FUNC_TYPE_LIST == part_type) {
        // do nothing
      } else {
        ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
        LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR, part_name.length(), part_name.ptr());
        LOG_WARN("part_value_expr type is not correct",
            K(ret),
            "data_type",
            part_value_expr.get_data_type(),
            K(share::is_oracle_mode()),
            K(share::is_mysql_mode()));
      }
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
      if (!part_func_expr.is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition by range columns() should be column expr", K(ret), "type", part_func_expr.get_expr_type());
      } else {
        const ObColumnRefRawExpr& column_ref = static_cast<const ObColumnRefRawExpr&>(part_func_expr);
        if (OB_FAIL(check_partition_range_value_result_type(part_type, column_ref, part_value_expr))) {
          LOG_WARN("get partition range value result type failed", K(ret), K(part_name));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition type", K(ret), K(part_type));
    }
  }
  return ret;
}

int ObResolverUtils::check_partition_value_expr_for_range(const ObString& part_name, ObRawExpr& part_value_expr,
    const ObPartitionFuncType part_type, const bool& in_tablegroup)
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(ret)) {
    bool gen_col_check = false;
    bool accept_charset_function = in_tablegroup;
    ObRawExprPartFuncChecker part_func_checker(gen_col_check, accept_charset_function);
    if (OB_FAIL(part_value_expr.preorder_accept(part_func_checker))) {
      LOG_WARN("check partition function failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (PARTITION_FUNC_TYPE_RANGE == part_type || PARTITION_FUNC_TYPE_LIST == part_type) {
      // partition by range(xx) partition p0 values less than (expr): expr is always integer
      if (!ob_is_integer_type(part_value_expr.get_data_type())) {
        ret = OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR;
        LOG_USER_ERROR(OB_ERR_VALUES_IS_NOT_INT_TYPE_ERROR, part_name.length(), part_name.ptr());
        LOG_WARN("part_value_expr type is not correct", K(ret), "date_type", part_value_expr.get_data_type());
      }
    } else if (PARTITION_FUNC_TYPE_RANGE_COLUMNS == part_type || PARTITION_FUNC_TYPE_LIST_COLUMNS == part_type) {
      const ObObjType value_type = part_value_expr.get_data_type();
      if (!is_valid_partition_column_type(value_type, part_type, true)) {
        ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
        LOG_USER_ERROR(OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
        LOG_WARN("object type is invalid ", K(ret), K(value_type));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected partition type", K(ret), K(part_type));
    }
  }
  return ret;
}

int ObResolverUtils::check_partition_expr_for_oracle_hash(
    ObRawExpr& expr, const ObPartitionFuncType part_type, const bool& in_tablegroup)
{
  int ret = OB_SUCCESS;
  UNUSED(in_tablegroup);
  if (T_FUN_SYS_PART_HASH_V1 != expr.get_expr_type() && T_FUN_SYS_PART_HASH_V2 != expr.get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition func should be T_FUN_SYS_PART_HASH_V1 or T_FUN_SYS_PART_HASH_V2 ", K(ret), K(expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    ObRawExpr* param_expr = expr.get_param_expr(i);
    if (OB_ISNULL(param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr should not be null", K(ret));
    } else if (OB_FAIL(check_valid_column_for_hash_or_range(*param_expr, part_type))) {
      LOG_WARN("chck_valid_column_for_hash_func failed", K(ret));
    } else if (ObVarcharType == param_expr->get_data_type() || ObCharType == param_expr->get_data_type() ||
               ob_is_nstring_type(param_expr->get_data_type())) {
      if (CS_TYPE_UTF8MB4_GENERAL_CI == param_expr->get_collation_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Partition key with CS_TYPE_UTF8MB4_GENERAL_CI");
        LOG_WARN("Partition key is string with CS_TYPE_UTF8MB4_GENERAL_CI", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_partition_expr_for_hash_or_range(
    ObRawExpr& expr, const ObPartitionFuncType part_type, const bool& in_tablegroup)
{
  int ret = OB_SUCCESS;
  ObRawExpr* part_expr = NULL;
  LOG_DEBUG("check_partition_expr_for_hash_or_range", K(ret), K(expr), K(part_type), K(in_tablegroup));

  if (is_hash_part(part_type)) {
    if (expr.get_param_count() != 1 ||
        (T_FUN_SYS_PART_HASH_V1 != expr.get_expr_type() && T_FUN_SYS_PART_HASH_V2 != expr.get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param count of hash func should be 1",
          K(ret),
          "param count",
          expr.get_param_count(),
          "expr type",
          expr.get_expr_type());
    } else {
      part_expr = expr.get_param_expr(0);
    }
  } else if (is_range_part(part_type) || is_list_part(part_type)) {
    part_expr = &expr;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition type is invalid", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(part_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part expr should not be null", K(ret));
    } else if (OB_FAIL(check_valid_column_for_hash_or_range(*part_expr, part_type))) {
      LOG_WARN("chck_valid_column_for_hash_func failed", K(ret));
    } else {
      bool gen_col_check = false;
      bool accept_charset_function = in_tablegroup;
      ObRawExprPartFuncChecker part_func_checker(gen_col_check, accept_charset_function);
      if (OB_FAIL(part_expr->preorder_accept(part_func_checker))) {
        LOG_WARN("check partition function failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObRawExprPartExprChecker part_expr_checker;
      if (OB_FAIL(part_expr->preorder_accept(part_expr_checker))) {
        LOG_WARN("check_part_expr failed", K(ret));
      }
    }
  }
  return ret;
}

// "index + view" combination
// fill depend/base table to RS for sync version
int ObResolverUtils::update_materialized_view_schema(
    const ObMaterializedViewContext& ctx, ObSchemaChecker& schema_checker, ObTableSchema& view_schema)
{
  int ret = OB_SUCCESS;

  view_schema.set_table_type(MATERIALIZED_VIEW);
  view_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
  view_schema.set_data_table_id(ctx.base_table_id_);

  ret = view_schema.add_base_table_id(ctx.base_table_id_);
  if (OB_SUCC(ret)) {
    ret = view_schema.add_depend_table_id(ctx.depend_table_id_);
  }
  if (OB_SUCC(ret)) {
    const uint64_t base_table_id = ctx.base_table_id_;
    const ObTableSchema* base_table_schema = NULL;
    if (OB_FAIL(schema_checker.get_table_schema(base_table_id, base_table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(base_table_id));
    } else if (OB_ISNULL(base_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(base_table_schema), K(base_table_id));
    } else {
      // keep properties same as base table
      view_schema.set_block_size(base_table_schema->get_block_size());
      view_schema.set_read_only(base_table_schema->is_read_only());
      view_schema.set_progressive_merge_num(base_table_schema->get_progressive_merge_num());
      view_schema.set_tablet_size(base_table_schema->get_tablet_size());
      view_schema.set_collation_type(base_table_schema->get_collation_type());
      view_schema.set_pctfree(base_table_schema->get_pctfree());
      view_schema.set_row_store_type(base_table_schema->get_row_store_type());
      view_schema.set_store_format(base_table_schema->get_store_format());
      view_schema.set_storage_format_version(base_table_schema->get_storage_format_version());
      if (OB_FAIL(view_schema.set_compress_func_name(base_table_schema->get_compress_func_name()))) {
        LOG_WARN("set_compress_func_name failed", K(*base_table_schema));
      }
    }
  }

  return ret;
}

// check column_name duplicate with select_column_name
// if duplicated, generate a new colume name with format:
//   __mv_col_$1_$2_$3 % (table_id, column_id, old_column_name)
int ObResolverUtils::check_and_generate_column_name(const ObMaterializedViewContext& ctx, char* buf,
    const int64_t buf_len, const uint64_t table_id, const uint64_t column_id, const ObString& col_name,
    ObString& new_col_name)
{
  int ret = OB_SUCCESS;
  new_col_name.reset();

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is invalid", K(ret), K(buf), K(buf_len));
  } else {
    ObColumnRefRawExpr* dup_expr = NULL;
    for (int64_t i = 0; NULL == dup_expr && OB_SUCC(ret) && i < ctx.select_cols_.count(); ++i) {
      ObColumnRefRawExpr* col_expr = ctx.select_cols_.at(i);
      if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(col_expr));
      } else {
        if (col_expr->get_table_id() == table_id && col_expr->get_column_id() == column_id) {
          dup_expr = col_expr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (NULL != dup_expr) {
        new_col_name =
            dup_expr->get_alias_column_name().empty() ? dup_expr->get_column_name() : dup_expr->get_alias_column_name();
      } else {
        new_col_name = col_name;
        while (OB_SUCC(ret)) {
          bool same_as = false;
          for (int64_t i = 0; !same_as && OB_SUCC(ret) && i < ctx.select_cols_.count(); ++i) {
            const ObColumnRefRawExpr* col_expr = ctx.select_cols_.at(i);
            if (OB_ISNULL(col_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL ptr", K(ret), K(col_expr));
            } else {
              const ObString& cur_col_name = col_expr->get_alias_column_name().empty()
                                                 ? col_expr->get_column_name()
                                                 : col_expr->get_alias_column_name();
              same_as = ObCharset::case_insensitive_equal(cur_col_name, new_col_name);
            }
          }
          if (OB_SUCC(ret)) {
            if (!same_as) {
              break;
            } else {
              int64_t n = snprintf(buf,
                  buf_len,
                  "__mv_col_%lu_%lu_%.*s",
                  table_id,
                  column_id,
                  new_col_name.length(),
                  new_col_name.ptr());
              if (n <= 0 || n >= buf_len) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("snprintf failed", K(n), K(buf_len));
              } else {
                new_col_name.assign_ptr(buf, static_cast<int32_t>(n));
              }
            }
          }
        }
      }
    }
  }
  LOG_INFO("new column name", K(new_col_name));

  return ret;
}

// generate materialized view schema
// rowkey: order by cols + base table's rowkey cols
// normal cols:  depend table's rowkey cols + base table's join cols
int ObResolverUtils::make_columns_for_materialized_view(
    const ObMaterializedViewContext& ctx, ObSchemaChecker& schema_checker, ObTableSchema& view_schema)
{
  int ret = OB_SUCCESS;

  LOG_INFO("dump mv without columns", K(view_schema));

  const uint64_t base_table_id = ctx.base_table_id_;
  const uint64_t depend_table_id = ctx.depend_table_id_;
  const ObTableSchema* base_table_schema = NULL;
  const ObTableSchema* depend_table_schema = NULL;
  if (OB_FAIL(schema_checker.get_table_schema(base_table_id, base_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(base_table_id));
  } else if (OB_FAIL(schema_checker.get_table_schema(depend_table_id, depend_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(depend_table_id));
  } else if (OB_ISNULL(base_table_schema) || OB_ISNULL(depend_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(base_table_schema), K(depend_table_schema));
  } else {
    const int64_t buf_len = OB_MAX_COLUMN_NAME_BUF_LENGTH;
    char buf[buf_len];

    // rowkey: order by cols
    for (int64_t i = 0; i < ctx.order_cols_.count(); ++i) {
      ObColumnRefRawExpr* col_expr = ctx.order_cols_.at(i);
      if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(col_expr));
      } else {
        ObString col_name =
            col_expr->get_alias_column_name().empty() ? col_expr->get_column_name() : col_expr->get_alias_column_name();
        ObString new_col_name;
        if (NULL != view_schema.get_column_schema(col_expr->get_column_id())) {
          // do-nothing
        } else if (OB_FAIL(check_and_generate_column_name(ctx,
                       buf,
                       buf_len,
                       col_expr->get_table_id(),
                       col_expr->get_column_id(),
                       col_name,
                       new_col_name))) {
          LOG_WARN("check and gen column name failed",
              K(ret),
              K(col_expr->get_table_id()),
              K(col_expr->get_column_id()),
              K(col_name));
        } else if (OB_FAIL(add_column_for_schema(*base_table_schema,
                       col_expr->get_column_id(),
                       col_expr->get_column_id(),
                       new_col_name,
                       true,
                       view_schema))) {
          LOG_WARN("fail to add column", K(ret), K(col_expr->get_column_id()), K(new_col_name));
        }
      }
    }

    // rowkey: base table's rowkey cols
    // normal cols: depend table's rowkey cols
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
        const bool is_base_table = 0 == i;
        const ObTableSchema* table_schema = is_base_table ? base_table_schema : depend_table_schema;
        const uint64_t table_id = is_base_table ? base_table_id : depend_table_id;
        const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
        for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_info.get_size(); ++j) {
          uint64_t column_id = 0;
          const ObColumnSchemaV2* col_schema = NULL;
          if (OB_FAIL(rowkey_info.get_column_id(j, column_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get rowkey info", K(ret), K(j), K(rowkey_info));
          } else if (NULL == (col_schema = table_schema->get_column_schema(column_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret), K(column_id));
          } else {
            uint64_t new_column_id = column_id;
            ObString new_col_name;
            if (!is_base_table) {
              new_column_id = view_schema.gen_materialized_view_column_id(column_id);
            }
            if (NULL != view_schema.get_column_schema(new_column_id)) {
              // do-nothing
            } else if (OB_FAIL(check_and_generate_column_name(
                           ctx, buf, buf_len, table_id, column_id, col_schema->get_column_name_str(), new_col_name))) {
              LOG_WARN("check and gen column name failed",
                  K(ret),
                  K(table_id),
                  K(column_id),
                  K(col_schema->get_column_name_str()));
            } else if (OB_FAIL(add_column_for_schema(
                           *table_schema, column_id, new_column_id, new_col_name, is_base_table, view_schema))) {
              LOG_WARN("fail to add column", K(ret), K(column_id), K(new_column_id), K(new_col_name), K(is_base_table));
            }
          }
        }
      }
    }

    // normal cols: base table's cols in join condition:
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < ctx.base_join_cols_.count(); ++i) {
        ObColumnRefRawExpr* col_expr = ctx.base_join_cols_.at(i);
        if (OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(col_expr));
        } else {
          ObString col_name = col_expr->get_alias_column_name().empty() ? col_expr->get_column_name()
                                                                        : col_expr->get_alias_column_name();
          ObString new_col_name;
          if (NULL != view_schema.get_column_schema(col_expr->get_column_id())) {
            // do-nothing
          } else if (OB_FAIL(check_and_generate_column_name(ctx,
                         buf,
                         buf_len,
                         col_expr->get_table_id(),
                         col_expr->get_column_id(),
                         col_name,
                         new_col_name))) {
            LOG_WARN("check and gen column name failed",
                K(ret),
                K(col_expr->get_table_id()),
                K(col_expr->get_column_id()),
                K(col_name));
          } else if (OB_FAIL(add_column_for_schema(*base_table_schema,
                         col_expr->get_column_id(),
                         col_expr->get_column_id(),
                         new_col_name,
                         false,
                         view_schema))) {
            LOG_WARN("fail to add column", K(ret), K(col_expr->get_column_id()), K(new_col_name));
          }
        }
      }
    }

    // normal cols: select cols
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
        const bool is_base_table = 0 == i;
        const ObTableSchema* table_schema = is_base_table ? base_table_schema : depend_table_schema;
        const ObIArray<ObColumnRefRawExpr*>& col_exprs =
            is_base_table ? ctx.base_select_cols_ : ctx.depend_select_cols_;
        for (int64_t j = 0; j < col_exprs.count() && OB_SUCC(ret); ++j) {
          ObColumnRefRawExpr* col_expr = col_exprs.at(j);
          if (OB_ISNULL(col_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret), K(col_expr));
          } else {
            ObString col_name = col_expr->get_alias_column_name().empty() ? col_expr->get_column_name()
                                                                          : col_expr->get_alias_column_name();
            const uint64_t column_id = col_expr->get_column_id();
            uint64_t new_column_id = column_id;
            if (!is_base_table) {
              new_column_id = view_schema.gen_materialized_view_column_id(column_id);
            }
            if (NULL != view_schema.get_column_schema(new_column_id)) {
              // do-nothing
            } else if (NULL != view_schema.get_column_schema(col_name)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL ptr", K(ret), K(col_name));
            } else if (OB_FAIL(add_column_for_schema(
                           *table_schema, column_id, new_column_id, col_name, false, view_schema))) {
              LOG_WARN("fail to add column",
                  K(ret),
                  K(table_schema->get_table_id()),
                  K(column_id),
                  K(new_column_id),
                  K(col_name));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObResolverUtils::make_columns_for_cte_view(const ObMaterializedViewContext& ctx, ObSchemaChecker& schema_checker,
    ObTableSchema& view_schema, common::ObArray<ObString> cte_col_names)
{
  int ret = OB_SUCCESS;

  LOG_INFO("dump mv without columns", K(view_schema));

  const uint64_t base_table_id = ctx.base_table_id_;
  const uint64_t depend_table_id = ctx.depend_table_id_;
  const ObTableSchema* base_table_schema = NULL;
  const ObTableSchema* depend_table_schema = NULL;
  if (OB_FAIL(schema_checker.get_table_schema(base_table_id, base_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(base_table_id));
  } else if (OB_FAIL(schema_checker.get_table_schema(depend_table_id, depend_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(depend_table_id));
  } else if (OB_ISNULL(base_table_schema) || OB_ISNULL(depend_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(base_table_schema), K(depend_table_schema));
  } else {
    // const int64_t buf_len = OB_MAX_COLUMN_NAME_LENGTH;
    // char buf[buf_len];

    // rowkey: order by cols
    for (int64_t i = 0; i < ctx.order_cols_.count(); ++i) {}

    // rowkey: base table's rowkey cols
    // normal cols: depend table's rowkey cols
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {}
    }

    // normal cols: base table's cols in join condition:
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < ctx.base_join_cols_.count(); ++i) {}
    }

    // normal cols: select cols
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
        const bool is_base_table = 0 == i;
        const ObTableSchema* table_schema = is_base_table ? base_table_schema : depend_table_schema;
        const ObIArray<ObColumnRefRawExpr*>& col_exprs =
            is_base_table ? ctx.base_select_cols_ : ctx.depend_select_cols_;
        if (col_exprs.count() != cte_col_names.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cte define column num does not match the select item nums from left query");
        }
        for (int64_t j = 0; j < col_exprs.count() && OB_SUCC(ret); ++j) {
          ObColumnRefRawExpr* col_expr = col_exprs.at(j);
          if (OB_ISNULL(col_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(ret), K(col_expr));
          } else {
            ObString col_name = col_expr->get_alias_column_name().empty() ? col_expr->get_column_name()
                                                                          : col_expr->get_alias_column_name();
            const uint64_t column_id = col_expr->get_column_id();
            uint64_t new_column_id = column_id;
            if (!is_base_table) {
              new_column_id = view_schema.gen_materialized_view_column_id(column_id);
            }
            if (NULL != view_schema.get_column_schema(new_column_id)) {
              // do-nothing
            } else if (NULL != view_schema.get_column_schema(col_name)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL ptr", K(ret), K(col_name));
            } else if (OB_FAIL(add_column_for_schema(
                           *table_schema, column_id, new_column_id, col_name, false, view_schema))) {
              LOG_WARN("fail to add column",
                  K(ret),
                  K(table_schema->get_table_id()),
                  K(column_id),
                  K(new_column_id),
                  K(col_name));
            }
          }
        }
      }
    }
  }

  return ret;
}

#if 0
int ObResolverUtils::find_base_and_depend_table(ObSchemaChecker &schema_checker,
                                                ObSelectStmt &stmt,
                                                uint64_t &base_table_id,
                                                uint64_t &depend_table_id,
                                                ObIArray<ObColumnRefRawExpr *> &base_table_cols,
                                                ObIArray<ObColumnRefRawExpr *> &depend_table_cols)
{
  int ret = OB_SUCCESS;

  return ret;
}
#endif

int ObResolverUtils::find_base_and_depend_table(
    ObSchemaChecker& schema_checker, ObSelectStmt& stmt, ObMaterializedViewContext& ctx)
{
  int ret = OB_SUCCESS;
  ctx.base_table_id_ = OB_INVALID_ID;
  ctx.depend_table_id_ = OB_INVALID_ID;
  ctx.base_join_cols_.reset();
  ctx.depend_join_cols_.reset();

  TableItem* l_tbl = NULL;
  TableItem* r_tbl = NULL;
  ObArray<ObRawExpr*> join_exprs;
  int64_t from_item_size = stmt.get_from_item_size();
  if (from_item_size <= 0 || from_item_size > 2) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported table refrence", K(ret), K(from_item_size));
  } else if (1 == from_item_size) {
    if (stmt.get_joined_tables().count() != 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported table refrence", K(ret), K(stmt.get_joined_tables().count()));
    } else {
      JoinedTable* join_tbl = stmt.get_joined_tables().at(0);
      if (join_tbl->joined_type_ != INNER_JOIN) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsuported table refrence", K(join_tbl->joined_type_), K(ret));
      } else {
        l_tbl = join_tbl->left_table_;
        r_tbl = join_tbl->right_table_;
        for (int64_t i = 0; i < join_tbl->join_conditions_.count(); ++i) {
          ret = join_exprs.push_back(join_tbl->join_conditions_.at(i));
        }
      }
    }
  } else {
    if (stmt.get_from_item(0).is_joined_ || stmt.get_from_item(1).is_joined_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported table refrence",
          K(ret),
          K(stmt.get_from_item(0).is_joined_),
          K(stmt.get_from_item(1).is_joined_));
    } else if (stmt.get_table_size() != 2) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported table refrence", K(ret), K(stmt.get_table_size()));
    } else {
      l_tbl = stmt.get_table_item(0);
      r_tbl = stmt.get_table_item(1);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(l_tbl) || OB_ISNULL(r_tbl)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(l_tbl), K(r_tbl), K(ret));
    } else if (!l_tbl->is_basic_table() || !r_tbl->is_basic_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported table refrence", K(l_tbl->is_basic_table()), K(r_tbl->is_basic_table()), K(ret));
    } else if (TableItem::ALIAS_TABLE == l_tbl->type_ || TableItem::ALIAS_TABLE == r_tbl->type_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported table refrence", K(l_tbl->type_), K(r_tbl->type_), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_exprs().count(); ++i) {
      ret = join_exprs.push_back(stmt.get_condition_exprs().at(i));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("table items", K(*l_tbl), K(*r_tbl));
    LOG_INFO("join conditions", K(join_exprs));
  }

  if (OB_SUCC(ret)) {
    const uint64_t l_tbl_id = l_tbl->table_id_;
    const uint64_t r_tbl_id = r_tbl->table_id_;
    ObArray<ObColumnRefRawExpr*> l_tbl_cols;
    ObArray<ObColumnRefRawExpr*> r_tbl_cols;
    for (int64_t i = 0; OB_SUCC(ret) && i < join_exprs.count(); ++i) {
      ObRawExpr* expr = join_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(expr), K(ret));
      } else if (expr->get_expr_type() != T_OP_EQ) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported join condition", K(expr->get_expr_type()), K(ret));
      } else {
        ObColumnRefRawExpr* l_col = NULL;
        ObColumnRefRawExpr* r_col = NULL;
        for (int64_t j = 0; OB_SUCC(ret) && j < 2; ++j) {
          ObRawExpr* param_expr = expr->get_param_expr(j);
          if (OB_ISNULL(param_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(param_expr), K(ret));
          } else if (!param_expr->is_column_ref_expr()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("only basic column ref allowed in join condition", K(ret), "type", param_expr->get_expr_type());
          } else {
            ObColumnRefRawExpr*& e = 0 == j ? l_col : r_col;
            e = static_cast<ObColumnRefRawExpr*>(param_expr);
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(l_col) || OB_ISNULL(r_col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL ptr", K(l_col), K(r_col), K(ret));
          } else if (l_col->get_table_id() == r_col->get_table_id()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN(
                "join condition's table should differ", K(l_col->get_table_id()), K(r_col->get_table_id()), K(ret));
          } else if (!((l_col->get_table_id() == l_tbl_id && r_col->get_table_id() == r_tbl_id) ||
                         (l_col->get_table_id() == r_tbl_id && r_col->get_table_id() == l_tbl_id))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected join condition's column",
                K(l_tbl_id),
                K(r_tbl_id),
                K(l_col->get_table_id()),
                K(r_col->get_table_id()),
                K(ret));
          } else {
            if (l_col->get_table_id() == l_tbl_id) {
              ret = l_tbl_cols.push_back(l_col);
              if (OB_SUCC(ret)) {
                ret = r_tbl_cols.push_back(r_col);
              }
            } else {
              ret = r_tbl_cols.push_back(l_col);
              if (OB_SUCC(ret)) {
                ret = l_tbl_cols.push_back(r_col);
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (l_tbl_cols.count() != r_tbl_cols.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(l_tbl_cols.count()), K(r_tbl_cols.count()), K(ret));
      } else {
        bool l_candi = false;
        bool r_candi = false;
        const ObTableSchema* l_tbl_schema = NULL;
        const ObTableSchema* r_tbl_schema = NULL;
        if (OB_FAIL(schema_checker.get_table_schema(l_tbl_id, l_tbl_schema))) {
          LOG_WARN("unexpected error", K(ret), K(l_tbl_id));
        } else if (OB_FAIL(schema_checker.get_table_schema(r_tbl_id, r_tbl_schema))) {
          LOG_WARN("unexpected error", K(ret), K(r_tbl_id));
        } else if (OB_ISNULL(l_tbl_schema) || OB_ISNULL(r_tbl_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(l_tbl_schema), K(r_tbl_schema), K(ret));
        } else {
          // check pk col cover join condition
          bool l_cover = false;
          bool r_cover = false;
          for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
            const ObTableSchema* tbl = 0 == i ? l_tbl_schema : r_tbl_schema;
            const ObArray<ObColumnRefRawExpr*>& tbl_cols = 0 == i ? l_tbl_cols : r_tbl_cols;
            const ObRowkeyInfo& rowkey_info = tbl->get_rowkey_info();
            bool cover = true;
            for (int64_t j = 0; cover && OB_SUCC(ret) && j < rowkey_info.get_size(); ++j) {
              uint64_t rowkey_column_id = 0;
              if (OB_FAIL(rowkey_info.get_column_id(j, rowkey_column_id))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("fail to get rowkey info", K(ret), K(j), K(rowkey_info));
              } else {
                bool has = false;
                for (int64_t k = 0; !has && OB_SUCC(ret) && k < tbl_cols.count(); ++k) {
                  has = rowkey_column_id == tbl_cols.at(k)->get_column_id();
                }
                if (OB_SUCC(ret)) {
                  if (!has) {
                    cover = false;
                  }
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (cover) {
                if (0 == i) {
                  l_cover = true;
                } else {
                  r_cover = true;
                }
              }
            }
          }
          bool l_cover_r = false;
          bool r_cover_r = false;
          // check join condition cover pk col
          for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
            const ObTableSchema* tbl = 0 == i ? l_tbl_schema : r_tbl_schema;
            const ObArray<ObColumnRefRawExpr*>& tbl_cols = 0 == i ? l_tbl_cols : r_tbl_cols;
            bool cover = true;
            for (int64_t j = 0; cover && OB_SUCC(ret) && j < tbl_cols.count(); ++j) {
              const ObRowkeyInfo& rowkey_info = tbl->get_rowkey_info();
              bool is_rowkey_col = false;
              if (OB_FAIL(rowkey_info.is_rowkey_column(tbl_cols.at(j)->get_column_id(), is_rowkey_col))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN(
                    "check is rowkey info failed", K(ret), K(j), K(tbl_cols.at(j)->get_column_id()), K(rowkey_info));
              } else if (!is_rowkey_col) {
                cover = false;
              }
            }
            if (OB_SUCC(ret)) {
              if (cover) {
                if (0 == i) {
                  l_cover_r = true;
                } else {
                  r_cover_r = true;
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            bool l_is = false;
            bool r_is = false;
            if (OB_FAIL(schema_checker.check_is_all_server_readonly_replica(l_tbl_schema->get_table_id(), l_is))) {
              LOG_WARN("fail to check", K(ret));
            } else if (OB_FAIL(
                           schema_checker.check_is_all_server_readonly_replica(r_tbl_schema->get_table_id(), r_is))) {
              LOG_WARN("fail to check", K(ret));
            } else {
              l_candi = l_cover && l_cover_r && l_is;
              r_candi = r_cover && r_cover_r && r_is;
            }
            LOG_INFO("cover info", K(ret), K(l_cover), K(l_cover_r), K(l_is), K(r_cover), K(r_cover_r), K(r_is));
          }
        }
        if (OB_SUCC(ret)) {
          if (!l_candi && !r_candi) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("no depend table candidates", K(ret));
          } else if (!l_candi) {
            ctx.depend_table_id_ = r_tbl_id;
            ctx.base_table_id_ = l_tbl_id;
            ctx.depend_join_cols_ = r_tbl_cols;
            ctx.base_join_cols_ = l_tbl_cols;
          } else {
            ctx.depend_table_id_ = l_tbl_id;
            ctx.base_table_id_ = r_tbl_id;
            ctx.depend_join_cols_ = l_tbl_cols;
            ctx.base_join_cols_ = r_tbl_cols;
          }
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("table info", K(ctx));
        }
        if (OB_SUCC(ret)) {
          if (!l_candi && !r_candi) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("no depend table candidates", K(ret));
          } else if (!l_candi) {
            ctx.depend_table_id_ = r_tbl_id;
            ctx.base_table_id_ = l_tbl_id;
            ctx.depend_join_cols_ = r_tbl_cols;
            ctx.base_join_cols_ = l_tbl_cols;
          } else {
            ctx.depend_table_id_ = l_tbl_id;
            ctx.base_table_id_ = r_tbl_id;
            ctx.depend_join_cols_ = l_tbl_cols;
            ctx.base_join_cols_ = r_tbl_cols;
          }
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("table info", K(ctx));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t depend_tbl_id = ctx.depend_table_id_;
    const ObTableSchema* depend_tbl_schema = NULL;
    if (OB_FAIL(schema_checker.get_table_schema(depend_tbl_id, depend_tbl_schema))) {
      LOG_WARN("unexpected error", K(ret), K(depend_tbl_id));
    } else if (OB_ISNULL(depend_tbl_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(depend_tbl_schema), K(depend_tbl_id), K(ret));
    } else if (depend_tbl_schema->get_part_level() != PARTITION_LEVEL_ZERO) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("depend table must be non-partitioned table",
          K(depend_tbl_id),
          K(depend_tbl_schema->get_part_level()),
          K(ret));
    }
  }

  return ret;
}

// limitation:
int ObResolverUtils::check_materialized_view_limitation(ObSelectStmt& stmt, ObMaterializedViewContext& ctx)
{
  int ret = OB_SUCCESS;

  // 1. forbiden same projection name
  // 2. forbiden same ref column
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
    ObRawExpr* expr = stmt.get_select_item(i).expr_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(ret), K(expr));
    } else if (!expr->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected", K(ret), K(expr->get_expr_type()));
    } else {
      ObColumnRefRawExpr* select_expr = static_cast<ObColumnRefRawExpr*>(expr);
      const ObString& select_name = !select_expr->get_alias_column_name().empty() ? select_expr->get_alias_column_name()
                                                                                  : select_expr->get_column_name();
      for (int64_t j = i + 1; OB_SUCC(ret) && j < stmt.get_select_item_size(); j++) {
        ObRawExpr* oth_expr = stmt.get_select_item(j).expr_;
        if (OB_ISNULL(oth_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(oth_expr));
        } else if (!oth_expr->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected", K(ret), K(oth_expr->get_expr_type()));
        } else {
          ObColumnRefRawExpr* oth_select_expr = static_cast<ObColumnRefRawExpr*>(oth_expr);
          const ObString& oth_select_name = !oth_select_expr->get_alias_column_name().empty()
                                                ? oth_select_expr->get_alias_column_name()
                                                : oth_select_expr->get_column_name();
          if (ObCharset::case_insensitive_equal(select_name, oth_select_name)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupport same projection name", K(ret), K(select_name), K(oth_select_name));
          } else if (select_expr->get_table_id() == oth_select_expr->get_table_id() &&
                     select_expr->get_column_id() == oth_select_expr->get_column_id()) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupport same column ref",
                K(ret),
                K(select_expr->get_table_id()),
                K(oth_select_expr->get_table_id()),
                K(select_expr->get_column_id()),
                K(oth_select_expr->get_column_id()));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (select_expr->get_table_id() == ctx.base_table_id_) {
          ret = ctx.base_select_cols_.push_back(select_expr);
        } else if (select_expr->get_table_id() == ctx.depend_table_id_) {
          ret = ctx.depend_select_cols_.push_back(select_expr);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table id", K(ret), K(ctx.depend_table_id_), K(select_expr->get_table_id()));
        }
        if (OB_SUCC(ret)) {
          ret = ctx.select_cols_.push_back(select_expr);
        }
      }
    }
  }

  // L1: must one partition table: 'base table'
  if (OB_SUCC(ret)) {
    int64_t n_part_exprs = stmt.get_part_exprs().count();
    if (n_part_exprs > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("at most one partition table support", K(ret), "current is", n_part_exprs);
    }
  }

  if (OB_INVALID_ID == ctx.base_table_id_ || OB_INVALID_ID == ctx.depend_table_id_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("base/depend table must be exist", K(ret), K(ctx.base_table_id_), K(ctx.depend_table_id_));
  }

  // L2: order by must be:
  // - nake column
  // - from base table
  // - exist in select item
  // - order direction must be ASC
  if (OB_SUCC(ret)) {
    const ObIArray<OrderItem>& order_items = stmt.get_order_items();
    for (int64_t i = 0; i < order_items.count() && OB_SUCC(ret); i++) {
      const OrderItem& order_item = order_items.at(i);
      if (!order_item.expr_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret));
      } else if (!order_item.expr_->is_column_ref_expr()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("order by item must be nake column", K(ret), "expr", order_item.expr_);
      } else if (is_descending_direction(order_item.order_type_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("order direction must be asc", K(ret));
      } else {
        ObColumnRefRawExpr* col_ref = static_cast<ObColumnRefRawExpr*>(order_item.expr_);
        if (col_ref->get_table_id() != ctx.base_table_id_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("order by columns must be from base table",
              K(ret),
              "base table_id",
              ctx.base_table_id_,
              "conflict table_id",
              col_ref->get_table_id());
        } else {
          ret = ctx.order_cols_.push_back(col_ref);
        }
      }

    }  // for
  }

  // L3: join type must be inner join
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < stmt.get_joined_tables().count() && OB_SUCC(ret); i++) {
      const JoinedTable* join = stmt.get_joined_tables().at(i);
      if (!join) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join table is null", K(ret));
      } else if (join->joined_type_ != INNER_JOIN) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only inner join supported", K(ret));
      }
    }
  }

  // left and right column of join condition must have same collation
  if (OB_SUCC(ret)) {
    if (ctx.base_join_cols_.count() != ctx.depend_join_cols_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join cols must be equal", K(ret), K(ctx.base_join_cols_.count()), K(ctx.depend_join_cols_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx.base_join_cols_.count(); ++i) {
      ObColumnRefRawExpr* base_col = ctx.base_join_cols_.at(i);
      ObColumnRefRawExpr* depend_col = ctx.depend_join_cols_.at(i);
      if (OB_ISNULL(base_col) || OB_ISNULL(depend_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret), K(base_col), K(depend_col));
      } else if (base_col->get_collation_type() != depend_col->get_collation_type()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN(
            "left and right column of join condition must have same collation", K(ret), K(*base_col), K(*depend_col));
      }
    }
  }

  // L4: no limit
  if (OB_SUCC(ret)) {
    if (stmt.get_limit_expr()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support for limit", K(ret));
    }
  }

  // L5: no union/intersect/except
  if (OB_SUCC(ret)) {
    if (ObSelectStmt::NONE != stmt.get_set_op()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support for set", K(ret));
    }
  }

  // L6: no rownum / aggr

  LOG_INFO("mv info", K(ctx));

  return ret;
}

//
// update necessary info for column before add to schema
//
int ObResolverUtils::add_column_for_schema(const ObTableSchema& ori_table, const uint64_t ori_col_id,
    const uint64_t new_col_id, const ObString& new_col_name, const bool is_rowkey, ObTableSchema& view_schema)
{
  int ret = OB_SUCCESS;

  const ObColumnSchemaV2* col = ori_table.get_column_schema(ori_col_id);
  if (OB_ISNULL(col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(col));
  } else {
    ObColumnSchemaV2 column = *col;
    column.set_tenant_id(view_schema.get_tenant_id());
    column.set_table_id(view_schema.get_table_id());
    column.set_column_id(new_col_id);
    column.set_column_name(new_col_name);
    if (is_rowkey) {
      int64_t rowkey_pos = view_schema.get_rowkey_column_num();
      column.set_rowkey_position(++rowkey_pos);
      view_schema.set_rowkey_column_num(rowkey_pos);
    } else {
      column.set_rowkey_position(0);
    }
    column.set_schema_version(view_schema.get_schema_version());
    if (OB_FAIL(view_schema.add_column(column))) {
      LOG_WARN("fail to add column for view schema", K(ret), K(column));
    } else {
      LOG_INFO("dump mv column",
          "ori table_id",
          ori_table.get_table_id(),
          "ori column_id",
          ori_col_id,
          "ori column_name",
          col->get_column_name_str(),
          "new table_id",
          view_schema.get_table_id(),
          "new column_id",
          new_col_id,
          "new column_name",
          new_col_name,
          K(is_rowkey),
          K(column));
    }
  }

  return ret;
}

int ObResolverUtils::fill_table_ids_for_mv(ObSelectStmt* select_stmt, ObSchemaChecker* schema_checker)
{
  int ret = OB_SUCCESS;
  for (uint64_t i = 0; i < select_stmt->get_table_size() && OB_SUCC(ret); i++) {
    bool has_all_server_readonly_replica = false;
    uint64_t table_id = select_stmt->get_table_item(i)->ref_id_;
    if (OB_FAIL(schema_checker->check_has_all_server_readonly_replica(table_id, has_all_server_readonly_replica))) {
      LOG_WARN("fail to check has all server readonly replica", K(table_id), K(ret));
    } else {
      if (has_all_server_readonly_replica) {
        select_stmt->set_depend_table_id(table_id);
      } else {
        select_stmt->set_base_table_id(table_id);
      }
    }
  }
  return ret;
}

int ObResolverUtils::log_err_msg_for_partition_value(const ObQualifiedName& name)
{
  int ret = OB_SUCCESS;
  if (!name.tbl_name_.empty() && !name.col_name_.empty()) {
    ret = OB_ERR_UNKNOWN_TABLE;
    if (!name.database_name_.empty()) {
      LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE,
          name.tbl_name_.length(),
          name.tbl_name_.ptr(),
          name.database_name_.length(),
          name.database_name_.ptr());
    } else {
      ObString scope_name("partition function");
      LOG_USER_ERROR(
          OB_ERR_UNKNOWN_TABLE, name.tbl_name_.length(), name.tbl_name_.ptr(), scope_name.length(), scope_name.ptr());
    }
    LOG_WARN("unknown table in partition function", K(name));
  } else if (!name.col_name_.empty()) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    ObString scope_name("partition function");
    LOG_USER_ERROR(
        OB_ERR_BAD_FIELD_ERROR, name.col_name_.length(), name.col_name_.ptr(), scope_name.length(), scope_name.ptr());
  } else if (!name.access_idents_.empty()) {
    const ObString& func_name = name.access_idents_.at(name.access_idents_.count() - 1).access_name_;
    ret = OB_ERR_FUNCTION_UNKNOWN;
    LOG_WARN("Invalid function name in partition function", K(name.access_idents_.count()), K(ret), K(func_name));
    LOG_USER_ERROR(OB_ERR_FUNCTION_UNKNOWN, func_name.length(), func_name.ptr());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("name is invalid", K(name), K(ret));
  }
  return ret;
}

int ObResolverUtils::resolve_partition_list_value_expr(ObResolverParams& params, const ParseNode& node,
    const ObString& part_name, const ObPartitionFuncType part_type, const ObIArray<ObRawExpr*>& part_func_exprs,
    ObIArray<ObRawExpr*>& part_value_expr_array, const bool& in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (node.type_ == T_EXPR_LIST) {
    if (node.num_child_ != part_func_exprs.count()) {
      ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
      LOG_WARN("Inconsistency in usage of column lists for partitioning near",
          K(ret),
          "node_child_num",
          node.num_child_,
          "part_func_expr",
          part_func_exprs.count());
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; i++) {
      ObRawExpr* part_value_expr = NULL;
      ObRawExpr* part_func_expr = NULL;
      if (OB_FAIL(part_func_exprs.at(i, part_func_expr))) {
        LOG_WARN("get part expr failed", K(i), "size", part_func_exprs.count(), K(ret));
      } else if (OB_ISNULL(part_func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_func_expr is invalid", K(ret));
      } else if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(params,
                     *(node.children_[i]),
                     part_name,
                     part_type,
                     *part_func_expr,
                     part_value_expr,
                     in_tablegroup))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      } else if (OB_FAIL(part_value_expr_array.push_back(part_value_expr))) {
        LOG_WARN("array push back fail", K(ret));
      } else {
      }
    }
  } else {
    ObRawExpr* part_value_expr = NULL;
    ObRawExpr* part_func_expr = NULL;
    if (OB_FAIL(part_func_exprs.at(0, part_func_expr))) {
      LOG_WARN("get part expr failed", "size", part_func_exprs.count(), K(ret));
    } else if (OB_ISNULL(part_func_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_func_expr is invalid", K(ret));
    } else if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(
                   params, node, part_name, part_type, *part_func_expr, part_value_expr, in_tablegroup))) {
      LOG_WARN("resolve partition expr failed", K(ret));
    } else if (OB_FAIL(part_value_expr_array.push_back(part_value_expr))) {
      LOG_WARN("array push back fail", K(ret));
    } else {
    }
  }
  return ret;
}

int ObResolverUtils::resolve_partition_range_value_expr(ObResolverParams& params, const ParseNode& node,
    const ObString& part_name, const ObPartitionFuncType part_type, const ObRawExpr& part_func_expr,
    ObRawExpr*& part_value_expr, const bool& in_tablegroup)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (part_name.empty() && !lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part name is invalid", K(ret), K(part_name));
  } else if (OB_ISNULL(params.expr_factory_) || OB_ISNULL(params.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K_(params.session_info));
  } else if (OB_FAIL(params.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObArray<ObQualifiedName> columns;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObArray<ObColumnRefRawExpr*> part_column_refs;
    ObArray<ObOpRawExpr*> op_exprs;
    ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
    ObExprResolveContext ctx(*params.expr_factory_, params.session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = ObCharset::charset_type_by_coll(part_func_expr.get_collation_type());
    ctx.param_list_ = params.param_list_;
    ctx.session_info_ = params.session_info_;
    ctx.query_ctx_ = params.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node,
                   part_value_expr,
                   columns,
                   sys_vars,
                   sub_query_info,
                   aggr_exprs,
                   win_exprs,
                   op_exprs,
                   user_var_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (sub_query_info.count() > 0) {
      ret = OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED;

    } else if (OB_FAIL(resolve_columns_for_partition_range_value_expr(part_value_expr, columns))) {
      LOG_WARN("resolve columns failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(part_value_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part expr should not be null", K(ret));
      } else if (OB_FAIL(part_value_expr->formalize(params.session_info_))) {
        LOG_WARN("formailize expr failed", K(ret));
      } else if (OB_FAIL(check_partition_value_expr_for_range(
                     part_name, part_func_expr, *part_value_expr, part_type, in_tablegroup))) {
        LOG_WARN("check_valid_column_for_hash or range func failed", K(part_type), K(part_name), K(ret));
      } else {
        LOG_DEBUG("succ to check_partition_value_expr_for_range",
            K(part_type),
            K(part_name),
            K(part_func_expr),
            KPC(part_value_expr),
            K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_partition_list_value_expr(ObResolverParams& params, const ParseNode& node,
    const ObString& part_name, const ObPartitionFuncType part_type, int64_t& expr_num,
    ObIArray<ObRawExpr*>& part_value_expr_array, const bool& in_tablegroup)
{
  int ret = OB_SUCCESS;
  if (node.type_ == T_EXPR_LIST) {
    if (OB_INVALID_COUNT != expr_num && node.num_child_ != expr_num) {
      ret = OB_ERR_PARTITION_COLUMN_LIST_ERROR;
      LOG_WARN("Inconsistency in usage of column lists for partitioning near",
          K(ret),
          K(expr_num),
          "child_num",
          node.num_child_);
    } else {
      expr_num = node.num_child_;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; i++) {
      ObRawExpr* part_value_expr = NULL;
      if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(
              params, *(node.children_[i]), part_name, part_type, part_value_expr, in_tablegroup))) {
        LOG_WARN("resolve partition expr failed", K(ret));
      } else if (OB_FAIL(part_value_expr_array.push_back(part_value_expr))) {
        LOG_WARN("array push back fail", K(ret));
      }
    }
  } else {
    expr_num = 1;
    ObRawExpr* part_value_expr = NULL;
    if (OB_FAIL(ObResolverUtils::resolve_partition_range_value_expr(
            params, node, part_name, part_type, part_value_expr, in_tablegroup))) {
      LOG_WARN("resolve partition expr failed", K(ret));
    } else if (OB_FAIL(part_value_expr_array.push_back(part_value_expr))) {
      LOG_WARN("array push back fail", K(ret));
    } else {
    }
  }
  return ret;
}

// for recursively process columns item in resolve_partition_range_value_expr
// just wrap columns process logic in resolve_partition_range_value_expr
int ObResolverUtils::resolve_columns_for_partition_range_value_expr(ObRawExpr*& expr, ObArray<ObQualifiedName>& columns)
{
  int ret = OB_SUCCESS;
  ObArray<std::pair<ObRawExpr*, ObRawExpr*>> real_sys_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    ObQualifiedName& q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      ObSysFunRawExpr* sys_func_expr = q_name.access_idents_.at(0).sys_func_expr_;
      if (OB_ISNULL(sys_func_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys expr is null", K(ret));
      } else {
        ObRawExpr* real_ref_expr = static_cast<ObRawExpr*>(sys_func_expr);
        for (int64_t j = 0; OB_SUCC(ret) && j < real_sys_exprs.count(); ++j) {
          if (OB_FAIL(ObRawExprUtils::replace_ref_column(
                  real_ref_expr, real_sys_exprs.at(j).first, real_sys_exprs.at(j).second))) {
            LOG_WARN("failed to replace ref column", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(sys_func_expr->check_param_num())) {
          LOG_WARN("faield to check param num", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr))) {
          LOG_WARN("failed to replace ref column", K(ret));
        } else if (OB_FAIL(
                       real_sys_exprs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, real_ref_expr)))) {
          LOG_WARN("failed to push back pari exprs", K(ret));
        }
      }
    } else {
      if (OB_FAIL(log_err_msg_for_partition_value(q_name))) {
        LOG_WARN("log error msg for range value expr faield", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_partition_range_value_expr(ObResolverParams& params, const ParseNode& node,
    const ObString& part_name, const ObPartitionFuncType part_type, ObRawExpr*& part_value_expr,
    const bool& in_tablegroup)
{
  int ret = OB_SUCCESS;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  if (part_name.empty() && !lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part name is invalid", K(ret), K(part_name));
  } else if (OB_ISNULL(params.expr_factory_) || OB_ISNULL(params.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K_(params.session_info));
  } else if (OB_FAIL(params.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObArray<ObQualifiedName> columns;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObArray<ObColumnRefRawExpr*> part_column_refs;
    ObArray<ObOpRawExpr*> op_exprs;
    ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
    ObExprResolveContext ctx(*params.expr_factory_, params.session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params.param_list_;
    ctx.session_info_ = params.session_info_;
    ctx.query_ctx_ = params.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node,
                   part_value_expr,
                   columns,
                   sys_vars,
                   sub_query_info,
                   aggr_exprs,
                   win_exprs,
                   op_exprs,
                   user_var_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (sub_query_info.count() > 0) {
      ret = OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED;

    } else if (OB_FAIL(resolve_columns_for_partition_range_value_expr(part_value_expr, columns))) {
      LOG_WARN("resolve columns failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(part_value_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part expr should not be null", K(ret));
      } else if (OB_FAIL(part_value_expr->formalize(params.session_info_))) {
        LOG_WARN("formailize expr failed", K(ret));
      } else if (OB_FAIL(check_partition_value_expr_for_range(part_name, *part_value_expr, part_type, in_tablegroup))) {
        LOG_WARN("check_valid_column_for_hash or range func failed", K(part_type), K(part_name), K(ret));
      }
    }
  }
  return ret;
}

// for recursively process columns item in resolve_partition_expr
// just wrap columns process logic in resolve_partition_expr
int ObResolverUtils::resolve_columns_for_partition_expr(ObRawExpr*& expr, ObIArray<ObQualifiedName>& columns,
    const ObTableSchema& tbl_schema, ObPartitionFuncType part_func_type, int64_t partition_key_start,
    ObIArray<ObString>& partition_keys)
{
  int ret = OB_SUCCESS;
  ObArray<std::pair<ObRawExpr*, ObRawExpr*>> real_sys_exprs;
  ObArray<ObColumnRefRawExpr*> part_column_refs;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
    const ObQualifiedName& q_name = columns.at(i);
    ObRawExpr* real_ref_expr = NULL;
    if (q_name.is_sys_func()) {
      ObSysFunRawExpr* sys_func_expr = q_name.access_idents_.at(0).sys_func_expr_;
      CK(OB_NOT_NULL(sys_func_expr));
      if (OB_SUCC(ret)) {
        real_ref_expr = static_cast<ObRawExpr*>(sys_func_expr);
        for (int64_t i = 0; OB_SUCC(ret) && i < real_sys_exprs.count(); ++i) {
          OZ(ObRawExprUtils::replace_ref_column(
              real_ref_expr, real_sys_exprs.at(i).first, real_sys_exprs.at(i).second));
        }

        OZ(sys_func_expr->check_param_num());
        OZ(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr));
        OZ(real_sys_exprs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, real_ref_expr)));
      }
    } else if (q_name.database_name_.length() > 0 || q_name.tbl_name_.length() > 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString scope_name = "partition function";
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    } else if (OB_ISNULL(q_name.ref_expr_) || OB_UNLIKELY(!q_name.ref_expr_->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref expr is null", K_(q_name.ref_expr));
    } else {
      // check column name whether duplicated, if partition by key, duplicated column means error
      // if partition by hash, add partition keys without duplicated name
      const ObColumnSchemaV2* col_schema = NULL;
      bool is_duplicated = false;
      int64_t j = partition_key_start;
      ObColumnRefRawExpr* col_expr = q_name.ref_expr_;
      for (j = partition_key_start; OB_SUCC(ret) && j < partition_keys.count(); ++j) {
        common::ObString& temp_name = partition_keys.at(j);
        if (ObCharset::case_insensitive_equal(temp_name, q_name.col_name_)) {
          is_duplicated = true;
          break;
        }
      }
      if (!is_duplicated) {
        if (NULL == (col_schema = tbl_schema.get_column_schema(q_name.col_name_)) || col_schema->is_hidden()) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          ObString scope_name = "partition function";
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
              q_name.col_name_.length(),
              q_name.col_name_.ptr(),
              scope_name.length(),
              scope_name.ptr());
        } else if (OB_FAIL(partition_keys.push_back(q_name.col_name_))) {
          LOG_WARN("add column name failed", K(ret), K_(q_name.col_name));
        } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema, *col_expr))) {
          LOG_WARN("init column expr failed", K(ret));
        } else if (OB_FAIL(part_column_refs.push_back(col_expr))) {
          LOG_WARN("push back column expr failed", K(ret));
        } else { /*do nothing*/
        }
      } else {
        // for partition by key, duplicated column is forbidden
        // for partition by hash, duplicated column must use the same column ref expr
        ObColumnRefRawExpr* ref_expr = NULL;
        if (PARTITION_FUNC_TYPE_KEY == part_func_type || PARTITION_FUNC_TYPE_KEY_V2 == part_func_type ||
            PARTITION_FUNC_TYPE_KEY_V3 == part_func_type) {
          ret = OB_ERR_SAME_NAME_PARTITION_FIELD;
          LOG_USER_ERROR(OB_ERR_SAME_NAME_PARTITION_FIELD, q_name.col_name_.length(), q_name.col_name_.ptr());
        } else if (OB_FAIL(part_column_refs.at(j - partition_key_start, ref_expr))) {
          LOG_WARN(
              "Failed to get part expr", K(ret), "idx", j - partition_key_start, "count", part_column_refs.count());
        } else if (OB_ISNULL(ref_expr) || OB_ISNULL(col_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Part expr should not be NULL", K(ret), KPC(ref_expr), KPC(col_expr));
        } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, col_expr, ref_expr))) {
          LOG_WARN("replace column ref failed", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::resolve_partition_expr(ObResolverParams& params, const ParseNode& node,
    const ObTableSchema& tbl_schema, ObPartitionFuncType part_func_type, ObRawExpr*& part_expr,
    ObIArray<ObString>* part_keys)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObString> tmp_part_keys;
  ObArray<ObOpRawExpr*> op_exprs;
  ObSEArray<ObUserVarIdentRawExpr*, 1> user_var_exprs;
  ObCollationType collation_connection = CS_TYPE_INVALID;
  ObCharsetType character_set_connection = CHARSET_INVALID;
  // part_keys is not null, means that need output partition keys
  ObIArray<ObString>& partition_keys = (part_keys != NULL ? *part_keys : tmp_part_keys);
  int64_t partition_key_start = partition_keys.count();
  if (OB_ISNULL(params.expr_factory_) || OB_ISNULL(params.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K_(params.session_info));
  } else if (OB_FAIL(params.session_info_->get_collation_connection(collation_connection))) {
    LOG_WARN("fail to get collation_connection", K(ret));
  } else if (OB_FAIL(params.session_info_->get_character_set_connection(character_set_connection))) {
    LOG_WARN("fail to get character_set_connection", K(ret));
  } else {
    ObExprResolveContext ctx(*params.expr_factory_, params.session_info_->get_timezone_info(), OB_NAME_CASE_INVALID);
    ctx.dest_collation_ = collation_connection;
    ctx.connection_charset_ = character_set_connection;
    ctx.param_list_ = params.param_list_;
    ctx.session_info_ = params.session_info_;
    ctx.query_ctx_ = params.query_ctx_;
    ObRawExprResolverImpl expr_resolver(ctx);
    if (OB_FAIL(params.session_info_->get_name_case_mode(ctx.case_mode_))) {
      LOG_WARN("fail to get name case mode", K(ret));
    } else if (OB_FAIL(expr_resolver.resolve(&node,
                   part_expr,
                   columns,
                   sys_vars,
                   sub_query_info,
                   aggr_exprs,
                   win_exprs,
                   op_exprs,
                   user_var_exprs))) {
      LOG_WARN("resolve expr failed", K(ret));
    } else if (sub_query_info.count() > 0) {
      ret = OB_ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED;

    } else if (columns.size() <= 0) {
      // partition by hash(1+1+1) /partition by range (1+1+1)
      // handling cases like partition by hash(1)
      ret = OB_ERR_WRONG_EXPR_IN_PARTITION_FUNC_ERROR;
      LOG_WARN("const expr is invalid for thie type of partitioning", K(ret));
    } else if (OB_FAIL(resolve_columns_for_partition_expr(
                   part_expr, columns, tbl_schema, part_func_type, partition_key_start, partition_keys))) {
      LOG_WARN("resolve columns for partition expr failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(part_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part expr should not be null", K(ret));
      } else if (share::is_oracle_mode() && is_hash_part(part_func_type)) {
        if (OB_FAIL(check_partition_expr_for_oracle_hash(*part_expr, part_func_type))) {
          LOG_WARN("check_partition_expr_for_oracle_hash func failed", K(ret));
        }
      } else if (is_hash_part(part_func_type) || is_range_part(part_func_type) || is_list_part(part_func_type)) {
        if (OB_FAIL(check_partition_expr_for_hash_or_range(*part_expr, part_func_type))) {
          LOG_WARN("check_valid_column_for_hash or range func failed", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(part_expr->formalize(params.session_info_))) {
      LOG_WARN("formailize expr failed", K(ret));
    }
  }
  return ret;
}

int ObResolverUtils::resolve_generated_column_expr(ObResolverParams& params, const ObString& expr_str,
    ObTableSchema& tbl_schema, ObColumnSchemaV2& generated_column, ObRawExpr*& expr,
    const PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  const ParseNode* expr_node = NULL;
  if (OB_ISNULL(params.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null");
  } else if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(expr_str, *params.allocator_, expr_node))) {
    LOG_WARN("parse expr node from str failed", K(ret), K(expr_str));
  } else if (OB_FAIL(
                 resolve_generated_column_expr(params, expr_node, tbl_schema, generated_column, expr, check_status))) {
    LOG_WARN("resolve generated column expr failed", K(ret), K(expr_str));
  }
  return ret;
}

int ObResolverUtils::resolve_generated_column_expr(ObResolverParams& params, const ParseNode* node,
    ObTableSchema& tbl_schema, ObColumnSchemaV2& generated_column, ObRawExpr*& expr,
    const PureFunctionCheckStatus check_status)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2* col_schema = NULL;
  ObSEArray<ObQualifiedName, 2> columns;
  ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 2> ref_sys_exprs;
  ObSQLSessionInfo* session_info = params.session_info_;
  ObRawExprFactory* expr_factory = params.expr_factory_;
  if (OB_ISNULL(expr_factory) || OB_ISNULL(session_info) || OB_ISNULL(node)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K(session_info), K(node));
  } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(
                 *expr_factory, *session_info, *node, expr, columns, params.schema_checker_, check_status))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName& q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated column expr has invalid qualified name", K(q_name));
    } else if (q_name.database_name_.length() > 0 || q_name.tbl_name_.length() > 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString scope_name = "generated column function";
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    } else if (NULL == (col_schema = tbl_schema.get_column_schema(q_name.col_name_)) || col_schema->is_hidden()) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString scope_name = "generated column function";
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
          q_name.col_name_.length(),
          q_name.col_name_.ptr(),
          scope_name.length(),
          scope_name.ptr());
    } else if (col_schema->is_generated_column()) {
      ret = OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN;
      LOG_USER_ERROR(
          OB_ERR_UNSUPPORTED_ACTION_ON_GENERATED_COLUMN, "Defining a generated column on generated column(s)");
    } else if (share::is_oracle_mode() && col_schema->get_meta_type().is_blob()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Define a blob column in generated column def is not supported", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "blob column in generated column definition");
    } else if (share::is_mysql_mode() && col_schema->is_autoincrement()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("generated column cannot refer to auto-increment column", K(ret), K(*expr));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "generated column refer to auto-increment column");
    } else if (OB_FAIL(ObRawExprUtils::init_column_expr(*col_schema, *q_name.ref_expr_))) {
      LOG_WARN("init column expr failed", K(ret));
    } else if (OB_FAIL(generated_column.add_cascaded_column_id(col_schema->get_column_id()))) {
      LOG_WARN("add cascaded column id to generated column failed", K(ret));
    } else {
      col_schema->add_column_flag(GENERATED_DEPS_CASCADE_FLAG);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("formalize expr failed", K(ret));
    } else if (share::is_oracle_mode() && ob_is_string_type(generated_column.get_data_type())) {
      int64_t generated_column_length_in_byte = 0;
      int64_t expr_length_in_byte = 0;
      common::ObCollationType cs_type = generated_column.get_collation_type();
      int64_t mbmaxlen = 1;
      if (LS_CHAR == generated_column.get_length_semantics()) {
        if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
          LOG_WARN("fail to get mbmaxlen for generated_column", K(cs_type), K(ret));
        } else {
          generated_column_length_in_byte = generated_column.get_data_length() * mbmaxlen;
        }
      } else {
        generated_column_length_in_byte = generated_column.get_data_length();
      }
      if (OB_SUCC(ret)) {
        if (LS_CHAR == expr->get_accuracy().get_length_semantics()) {
          cs_type = expr->get_collation_type();
          if (OB_FAIL(common::ObCharset::get_mbmaxlen_by_coll(cs_type, mbmaxlen))) {
            LOG_WARN("fail to get mbmaxlen for expr", K(cs_type), K(ret));
          } else {
            expr_length_in_byte = expr->get_accuracy().get_length() * mbmaxlen;
          }
        } else {
          expr_length_in_byte = expr->get_accuracy().get_length();
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(generated_column_length_in_byte < expr_length_in_byte)) {
        ret = OB_ERR_DATA_TOO_LONG;
        LOG_WARN("the length of generated column expr is more than the length of specified type",
            K(generated_column_length_in_byte),
            K(expr_length_in_byte));
      }
    } else if (ob_is_raw(generated_column.get_data_type())) {
      if (ob_is_string_type(expr->get_data_type())) {
        if (OB_UNLIKELY(generated_column.get_data_length() * 2 < (expr->get_accuracy()).get_length())) {
          ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("the length of generated column expr is more than the length of specified type");
        }
      } else if (ob_is_raw(expr->get_data_type())) {
        if (OB_UNLIKELY(generated_column.get_data_length() < (expr->get_accuracy()).get_length())) {
          ret = OB_ERR_DATA_TOO_LONG;
          LOG_WARN("the length of generated column expr is more than the length of specified type");
        }
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent datatypes", K(expr->get_result_type().get_type()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLResolver::print_expr_to_default_value(
                   *expr, generated_column, session_info->get_timezone_info()))) {
      LOG_WARN("fail to print_expr_to_default_value", KPC(expr), K(generated_column), K(ret));
    }
  }
  return ret;
}

// This function is used to resolve the dependent columns of generated column when retrieve schema.
// We use this function instead of build_generated_column_expr because there is not a thread-safe
// mem_context and the expr is not necessary.
int ObResolverUtils::resolve_generated_column_info(
    const ObString& expr_str, ObIAllocator& allocator, ObItemType& root_expr_type, ObIArray<ObString>& column_names)
{
  int ret = OB_SUCCESS;
  const ParseNode* node = NULL;
  if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(expr_str, allocator, node))) {
    LOG_WARN("parse expr node from string failed", K(ret), K(expr_str));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else if (OB_FAIL(SMART_CALL(resolve_column_info_recursively(node, column_names)))) {
    LOG_WARN("failed to resolve column into");
  } else {
    ObItemType type = node->type_;
    if (T_FUN_SYS == type) {
      if (OB_UNLIKELY(1 > node->num_child_) || OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0])) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid node children for fun_sys node", K(ret));
      } else {
        ObString func_name(node->children_[0]->str_len_, node->children_[0]->str_value_);
        if (0 == func_name.case_compare("bin")) {
          type = ObExprOperatorFactory::get_type_by_name("conv");
        } else if (0 == func_name.case_compare("oct")) {
          type = ObExprOperatorFactory::get_type_by_name("conv");
        } else if (0 == func_name.case_compare("lcase")) {
          type = ObExprOperatorFactory::get_type_by_name("lower");
        } else if (0 == func_name.case_compare("ucase")) {
          type = ObExprOperatorFactory::get_type_by_name("upper");
          // don't alias "power" to "pow" in oracle mode
        } else if (!lib::is_oracle_mode() && 0 == func_name.case_compare("power")) {
          type = ObExprOperatorFactory::get_type_by_name("pow");
        } else if (0 == func_name.case_compare("ws")) {
          type = ObExprOperatorFactory::get_type_by_name("word_segment");
        } else if (0 == func_name.case_compare("inet_ntoa")) {
          type = ObExprOperatorFactory::get_type_by_name("int2ip");
        } else {
          type = ObExprOperatorFactory::get_type_by_name(func_name);
        }
        if (OB_UNLIKELY(T_INVALID == (type))) {
          ret = OB_ERR_FUNCTION_UNKNOWN;
          LOG_WARN("function not exist", K(func_name), K(ret));
        }
      }
    }
    OX(root_expr_type = type);
  }
  return ret;
}

int ObResolverUtils::resolve_column_info_recursively(const ParseNode* node, ObIArray<ObString>& column_names)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null");
  } else if (T_COLUMN_REF == node->type_) {
    if (OB_UNLIKELY(node->num_child_ != 3)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node type", K_(node->type), K(node->num_child_), K(ret));
    } else if (OB_UNLIKELY(node->children_[0] != NULL || node->children_[1] != NULL)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database node or table node is not null", K_(node->type), K(ret));
    } else if (OB_ISNULL(node->children_[2])) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column node is null", K_(node->type), K(ret));
    } else if (OB_UNLIKELY(T_STAR == node->children_[2]->type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column node can't be T_STAR", K_(node->type), K(ret));
    } else {
      ObString column_name(static_cast<int32_t>(node->children_[2]->str_len_), node->children_[2]->str_value_);
      if (OB_FAIL(column_names.push_back(column_name))) {
        LOG_WARN("Add column failed", K(ret));
      }
    }
  } else if (T_OBJ_ACCESS_REF == node->type_) {
    if (OB_UNLIKELY(node->num_child_ != 2)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid node type", K_(node->type), K(node->num_child_), K(ret));
    } else if (OB_ISNULL(node->children_[0])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is NULL", K(node->num_child_));
    } else if (T_IDENT == node->children_[0]->type_) {
      ObString column_name(static_cast<int32_t>(node->children_[0]->str_len_), node->children_[0]->str_value_);
      OZ(column_names.push_back(column_name));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node->num_child_; ++i) {
    const ParseNode* child_node = node->children_[i];
    if (NULL == child_node) {
      // do nothing
    } else if (OB_FAIL(SMART_CALL(resolve_column_info_recursively(child_node, column_names)))) {
      LOG_WARN("recursive resolve column node failed", K(ret));
    }
  }
  return ret;
}

int ObResolverUtils::resolve_default_expr_v2_column_expr(
    ObResolverParams& params, const ObString& expr_str, ObColumnSchemaV2& default_expr_v2_column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  const ParseNode* expr_node = NULL;
  if (OB_ISNULL(params.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null");
  } else if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(expr_str, *params.allocator_, expr_node))) {
    LOG_WARN("parse expr node from str failed", K(ret), K(expr_str));
  } else if (OB_FAIL(resolve_default_expr_v2_column_expr(params, expr_node, default_expr_v2_column, expr))) {
    LOG_WARN("resolve default_expr_v2_column expr failed", K(ret), K(expr_str));
  }
  return ret;
}

int ObResolverUtils::resolve_default_expr_v2_column_expr(
    ObResolverParams& params, const ParseNode* node, ObColumnSchemaV2& default_expr_v2_column, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObSQLSessionInfo* session_info = params.session_info_;
  ObRawExprFactory* expr_factory = params.expr_factory_;
  if (OB_ISNULL(expr_factory) || OB_ISNULL(session_info) || OB_ISNULL(node)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K(session_info), K(node));
  } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(*expr_factory, *session_info, *node, expr, columns))) {
    LOG_WARN("build expr_default column expr failed",
        "is_oracle_compatible",
        session_info->get_compatibility_mode(),
        K(ret));
  } else if (share::is_oracle_mode() && columns.count() > 0) {
    bool is_all_sys_func = true;
    ObRawExpr* real_ref_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_all_sys_func && i < columns.count(); i++) {
      ObQualifiedName& q_name = columns.at(i);
      if (!q_name.is_sys_func()) {
        is_all_sys_func = false;
      } else if (OB_ISNULL(q_name.access_idents_.at(0).sys_func_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(ret));
      } else if (OB_FAIL(q_name.access_idents_.at(0).sys_func_expr_->check_param_num())) {
        LOG_WARN("sys func param number not match", K(ret));
      } else {
        real_ref_expr = static_cast<ObRawExpr*>(q_name.access_idents_.at(0).sys_func_expr_);
        if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, real_ref_expr))) {
          LOG_WARN("replace column ref expr failed", K(ret));
        } else {
          real_ref_expr = NULL;
        }
      }
    }
    if (!is_all_sys_func) {
      // log user error
      ret = OB_ERR_BAD_FIELD_ERROR;
      const ObQualifiedName& q_name = columns.at(0);
      ObString scope_name = default_expr_v2_column.get_column_name_str();
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("formalize expr failed", K(ret));
    } else {
      LOG_DEBUG("succ to resolve_default_expr_v2_column_expr",
          "is_const_expr",
          expr->is_const_expr(),
          "is_calculable_expr",
          expr->has_flag(IS_CALCULABLE_EXPR),
          KPC(expr),
          K(ret));
    }
  } else if (OB_UNLIKELY(!columns.empty())) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    const ObQualifiedName& q_name = columns.at(0);
    ObString scope_name = default_expr_v2_column.get_column_name_str();
    ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
  } else if (OB_FAIL(expr->formalize(session_info))) {
    LOG_WARN("formalize expr failed", K(ret));
  } else {
    LOG_DEBUG("succ to resolve_default_expr_v2_column_expr",
        "is_const_expr",
        expr->is_const_expr(),
        "is_calculable_expr",
        expr->has_flag(IS_CALCULABLE_EXPR),
        KPC(expr),
        K(ret));
  }
  return ret;
}

int ObResolverUtils::resolve_check_constraint_expr(ObResolverParams& params, const ParseNode* node,
    const ObTableSchema& tbl_schema, ObConstraint& constraint, ObRawExpr*& expr,
    const share::schema::ObColumnSchemaV2* column_schema)
{
  int ret = OB_SUCCESS;
  ObArray<ObQualifiedName> columns;
  ObArray<std::pair<ObRawExpr*, ObRawExpr*>> ref_sys_exprs;
  ObSQLSessionInfo* session_info = params.session_info_;
  ObRawExprFactory* expr_factory = params.expr_factory_;
  bool is_col_level_cst = false;  // table level cst
  common::ObSEArray<uint64_t, common::SEARRAY_INIT_NUM> column_ids;

  if (NULL != column_schema) {
    // column_schema is NULL, table level constraint
    is_col_level_cst = true;
  }
  if (OB_ISNULL(expr_factory) || OB_ISNULL(session_info) || OB_ISNULL(node)) {
    ret = OB_NOT_INIT;
    LOG_WARN("resolve status is invalid", K_(params.expr_factory), K(session_info), K(node));
  } else if (OB_FAIL(ObRawExprUtils::build_check_constraint_expr(*expr_factory, *session_info, *node, expr, columns))) {
    LOG_WARN("build generated column expr failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName& q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      ObRawExpr* sys_func = q_name.access_idents_.at(0).sys_func_expr_;
      CK(OB_NOT_NULL(sys_func));
      if (OB_SUCC(ret) &&
          (T_FUN_SYS_SYS_CONTEXT == sys_func->get_expr_type() || T_FUN_SYS_USERENV == sys_func->get_expr_type())) {
        ret = OB_ERR_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST;
        LOG_WARN("date or system variable wrongly specified in CHECK constraint", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < ref_sys_exprs.count(); ++i) {
        OZ(ObRawExprUtils::replace_ref_column(sys_func, ref_sys_exprs.at(i).first, ref_sys_exprs.at(i).second));
      }
      OZ(q_name.access_idents_.at(0).sys_func_expr_->check_param_num());
      OZ(ObRawExprUtils::replace_ref_column(expr, q_name.ref_expr_, sys_func));
      OZ(ref_sys_exprs.push_back(std::pair<ObRawExpr*, ObRawExpr*>(q_name.ref_expr_, sys_func)));
    } else if (q_name.database_name_.length() > 0 || q_name.tbl_name_.length() > 0) {
      ret = OB_ERR_BAD_FIELD_ERROR;
      ObString scope_name = "constraint column function";
      ObString col_name = concat_qualified_name(q_name.database_name_, q_name.tbl_name_, q_name.col_name_);
      LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, col_name.length(), col_name.ptr(), scope_name.length(), scope_name.ptr());
    } else {
      if (!is_col_level_cst) {
        if (OB_ISNULL(column_schema = tbl_schema.get_column_schema(q_name.col_name_)) || column_schema->is_hidden()) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          ObString scope_name = "constraint column function";
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR,
              q_name.col_name_.length(),
              q_name.col_name_.ptr(),
              scope_name.length(),
              scope_name.ptr());
        }
      } else {  // is_col_level_cst
        if (0 != columns.at(i).col_name_.compare(column_schema->get_column_name_str())) {
          ret = OB_ERR_COL_CHECK_CST_REFER_ANOTHER_COL;
          LOG_WARN("column check constraint cannot reference other columns",
              K(ret),
              K(columns.at(i).col_name_),
              K(column_schema->get_column_name_str()));
        }
      }
      if (OB_SUCC(ret)) {
        if (column_ids.end() == std::find(column_ids.begin(), column_ids.end(), column_schema->get_column_id())) {
          if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
            LOG_WARN("push back to column_ids failed", K(ret), K(column_schema->get_column_id()));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObRawExprUtils::init_column_expr(*column_schema, *q_name.ref_expr_))) {
          LOG_WARN("init column expr failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(constraint.assign_column_ids(column_ids))) {
      LOG_WARN("fail to assign_column_ids", K(column_ids));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(expr->formalize(session_info))) {
    LOG_WARN("formalize expr failed", K(ret));
  }
  if (OB_SUCC(ret) && share::is_mysql_mode()) {
    if (T_OP_EQ != expr->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("constraint expr type should be T_OP_EQ", K(expr->get_expr_type()), K(ret));
    } else {
      ObOpRawExpr* eq_expr = static_cast<ObOpRawExpr*>(expr);
      ObRawExpr* exprs[2] = {eq_expr->get_param_expr(0), eq_expr->get_param_expr(1)};
      for (int i = 0; OB_SUCC(ret) && i < 2; i++) {
        if (OB_ISNULL(exprs[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr", K(ret), K(exprs[i]));
        } else if (T_FUN_SYS_SUBSTR == exprs[i]->get_expr_type()) {
          ObSysFunRawExpr* sys_expr = static_cast<ObSysFunRawExpr*>(exprs[i]);
          for (int64_t j = 0; OB_SUCC(ret) && j < sys_expr->get_param_count(); j++) {
            ObRawExpr* param_expr = sys_expr->get_param_expr(j);
            if (OB_ISNULL(param_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL ptr", K(ret), K(param_expr));
            } else if (0 == j && !param_expr->is_column_ref_expr()) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("column ref expr expected", K(ret), K(param_expr->get_expr_type()));
            } else if (0 != j && !param_expr->is_const_expr()) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("const expr expected", K(ret), K(param_expr->get_expr_type()));
            }
          }
        } else if (exprs[i]->is_column_ref_expr()) {
          // do-nothing
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("substring or column ref expr expected", K(ret), K(exprs[i]->get_expr_type()));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    HEAP_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], expr_str_buf)
    {
      MEMSET(expr_str_buf, 0, sizeof(expr_str_buf));
      int64_t pos = 0;
      ObString expr_def;
      ObRawExprPrinter expr_printer(expr_str_buf, OB_MAX_DEFAULT_VALUE_LENGTH, &pos, session_info->get_timezone_info());
      if (OB_FAIL(expr_printer.do_print(expr, T_NONE_SCOPE, true))) {
        LOG_WARN("print expr definition failed", K(ret));
      } else if (FALSE_IT(expr_def.assign_ptr(expr_str_buf, static_cast<int32_t>(pos)))) {
      } else if (OB_FAIL(constraint.set_check_expr(expr_def))) {
        LOG_WARN("set check expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::build_partition_key_expr(ObResolverParams& params,
    const share::schema::ObTableSchema& table_schema, ObRawExpr*& partition_key_expr,
    ObIArray<ObQualifiedName>* qualified_names, const bool is_key_implicit_v2)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr* func_expr = NULL;
  if (OB_ISNULL(params.expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("expr factory is null", K(params.expr_factory_));
  } else if (!is_key_implicit_v2 && OB_FAIL(params.expr_factory_->create_raw_expr(T_FUN_SYS_PART_KEY_V2, func_expr))) {
    LOG_WARN("create sysfunc expr failed", K(ret));
  } else if (is_key_implicit_v2 && OB_FAIL(params.expr_factory_->create_raw_expr(T_FUN_SYS_PART_KEY_V3, func_expr))) {
    LOG_WARN("create sysfunc expr failed", K(ret));
  } else {
    if (is_key_implicit_v2) {
      ObString partition_fun_name("partition_key_v3");
      func_expr->set_func_name(partition_fun_name);
    } else {
      ObString partition_fun_name("partition_key_v2");
      func_expr->set_func_name(partition_fun_name);
    }
    if (OB_FAIL(func_expr->add_flag(IS_FUNC))) {
      LOG_WARN("failed to add flag IS_FUNC");
    } else if (OB_FAIL(func_expr->add_flag(CNT_COLUMN))) {
      LOG_WARN("failed to add flag CNT_COLUMN");
    } else if (OB_FAIL(func_expr->add_flag(CNT_FUNC))) {
      LOG_WARN("failed to add flag CNT_FUNC");
    } else {
    }
  }
  // use primary key column to build a ObColumnRefRawExpr
  for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
       OB_SUCC(ret) && iter != table_schema.column_end();
       ++iter) {
    const ObColumnSchemaV2& column_schema = (**iter);
    if (!column_schema.is_original_rowkey_column() || column_schema.is_hidden()) {
      // parition by key() use primary key to create partition key not hidden auto_increment primary key
      continue;
    } else if (column_schema.is_autoincrement()) {
      ret = OB_ERR_AUTO_PARTITION_KEY;
      LOG_USER_ERROR(OB_ERR_AUTO_PARTITION_KEY,
          column_schema.get_column_name_str().length(),
          column_schema.get_column_name_str().ptr());
    } else {
      ObColumnRefRawExpr* column_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_column_expr(*params.expr_factory_, column_schema, column_expr))) {
        LOG_WARN("failed to build column schema!", K(column_expr), K(column_schema));
      } else if (OB_FAIL(func_expr->add_param_expr(column_expr))) {
        LOG_WARN("failed to add param expr!", K(ret));
      } else if (qualified_names != NULL) {
        ObQualifiedName name;
        name.col_name_.assign_ptr(column_schema.get_column_name(), column_schema.get_column_name_str().length());
        name.ref_expr_ = column_expr;
        name.is_star_ = false;
        if (OB_FAIL(qualified_names->push_back(name))) {
          LOG_WARN("failed to push back qualified name", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    partition_key_expr = func_expr;
    if (OB_FAIL(partition_key_expr->formalize(params.session_info_))) {
      LOG_WARN("deduce type failed", K(ret));
    }
  }
  return ret;
}

int ObResolverUtils::check_column_name(const ObSQLSessionInfo* session_info, const ObQualifiedName& q_name,
    const ObColumnRefRawExpr& col_ref, bool& is_hit)
{
  int ret = OB_SUCCESS;
  is_hit = true;
  if (!q_name.database_name_.empty()) {
    if (OB_FAIL(name_case_cmp(
            session_info, q_name.database_name_, col_ref.get_database_name(), OB_TABLE_NAME_CLASS, is_hit))) {
      LOG_WARN("cmp database name failed", K(ret), K(q_name), K(col_ref.get_database_name()));
    }
  }
  if (OB_SUCC(ret) && !q_name.tbl_name_.empty() && is_hit) {
    ObString table_name = col_ref.get_synonym_name().empty() ? col_ref.get_table_name() : col_ref.get_synonym_name();
    if (OB_FAIL(name_case_cmp(session_info, q_name.tbl_name_, table_name, OB_TABLE_NAME_CLASS, is_hit))) {
      LOG_WARN("compare table name failed", K(q_name), K(q_name), K(col_ref));
    }
  }
  if (OB_SUCC(ret) && is_hit) {
    is_hit = ObCharset::case_insensitive_equal(q_name.col_name_, col_ref.get_column_name());
  }
  return ret;
}

int ObResolverUtils::create_generate_table_column(
    ObRawExprFactory& expr_factory, const TableItem& table_item, uint64_t column_id, ColumnItem& col_item)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* col_expr = NULL;
  if (OB_UNLIKELY(!table_item.is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not generated table", K_(table_item.type));
  } else {
    int64_t item_index = column_id - OB_APP_MIN_COLUMN_ID;
    ObSelectStmt* ref_stmt = table_item.ref_query_;
    if (OB_ISNULL(ref_stmt)) {
      ret = OB_NOT_INIT;
      LOG_WARN("generate table ref stmt is null");
    } else if (OB_UNLIKELY(item_index < 0 || item_index >= ref_stmt->get_select_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item index is invalid", K(item_index), K(ref_stmt->get_select_item_size()));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_REF_COLUMN, col_expr))) {
      LOG_WARN("create column ref expr failed", K(ret));
    } else if (OB_ISNULL(col_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col expr is null");
    } else {
      col_expr->set_ref_id(table_item.table_id_, column_id);
      col_expr->set_result_type(ref_stmt->get_select_item(item_index).expr_->get_result_type());
      col_expr->set_column_attr(table_item.alias_name_, ref_stmt->get_select_item(item_index).alias_name_);
      col_item.set_default_value(ref_stmt->get_select_item(item_index).default_value_);
    }
  }
  // init column item
  if (OB_SUCC(ret)) {
    col_item.expr_ = col_expr;
    col_item.table_id_ = col_expr->get_table_id();
    col_item.column_id_ = col_expr->get_column_id();
    col_item.column_name_ = col_expr->get_column_name();
  }
  return ret;
}

int ObResolverUtils::check_unique_index_cover_partition_column(
    const ObTableSchema& table_schema, const ObCreateIndexArg& arg)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_partitioned_table() ||
      (INDEX_TYPE_PRIMARY != arg.index_type_ && INDEX_TYPE_UNIQUE_LOCAL != arg.index_type_ &&
          INDEX_TYPE_UNIQUE_GLOBAL != arg.index_type_)) {
    // nothing to do
  } else {
    const common::ObPartitionKeyInfo& partition_info = table_schema.get_partition_key_info();
    const common::ObPartitionKeyInfo& subpartition_info = table_schema.get_subpartition_key_info();
    ObSEArray<uint64_t, 5> idx_col_ids;
    if (OB_FAIL(get_index_column_ids(table_schema, arg.index_columns_, idx_col_ids))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get index column ids", K(ret), K(table_schema), K(arg.index_columns_));
    } else if (OB_FAIL(unique_idx_covered_partition_columns(table_schema, idx_col_ids, partition_info))) {
      LOG_WARN("Unique index covered partition columns failed", K(ret));
    } else if (OB_FAIL(unique_idx_covered_partition_columns(table_schema, idx_col_ids, subpartition_info))) {
      LOG_WARN("Unique index convered partition columns failed", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObResolverUtils::get_index_column_ids(
    const ObTableSchema& table_schema, const ObIArray<ObColumnSortItem>& columns, ObIArray<uint64_t>& column_ids)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* column_schema = NULL;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < columns.count(); ++idx) {
    if (OB_ISNULL(column_schema = table_schema.get_column_schema(columns.at(idx).column_name_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get column schema", K(ret), "column id", columns.at(idx));
    } else if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
      LOG_WARN("Failed to add column id", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObResolverUtils::unique_idx_covered_partition_columns(const ObTableSchema& table_schema,
    const ObIArray<uint64_t>& index_columns, const ObPartitionKeyInfo& partition_info)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* column_schema = NULL;
  const ObPartitionKeyColumn* column = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_info.get_size(); i++) {
    column = partition_info.get_column(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get parition info", K(ret), K(column));
    } else if (!has_exist_in_array(index_columns, column->column_id_)) {
      if (OB_ISNULL(column_schema = table_schema.get_column_schema(column->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Column schema is NULL", K(ret));
      } else if (column_schema->is_generated_column()) {
        ObSEArray<uint64_t, 5> cascaded_columns;
        if (OB_FAIL(column_schema->get_cascaded_column_ids(cascaded_columns))) {
          LOG_WARN("Failed to get cascaded column ids", K(ret));
        } else {
          for (int64_t idx = 0; OB_SUCC(ret) && idx < cascaded_columns.count(); ++idx) {
            if (!has_exist_in_array(index_columns, cascaded_columns.at(idx))) {
              ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
              LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "UNIQUE INDEX");
            }
          }
        }
      } else {
        ret = OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF;
        LOG_USER_ERROR(OB_EER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF, "UNIQUE INDEX");
      }
    } else {
    }  // do nothing
  }    // end for
  return ret;
}

int ObResolverUtils::resolve_data_type(const ParseNode& type_node, const ObString& ident_name, ObDataType& data_type,
    const int is_oracle_mode /*1:Oracle, 0:MySql */, const ObSessionNLSParams& nls_session_param)
{
  int ret = OB_SUCCESS;
  data_type.set_obj_type(static_cast<ObObjType>(type_node.type_));
  int32_t length = type_node.int32_values_[0];
  int16_t precision = type_node.int16_values_[0];
  int16_t scale = type_node.int16_values_[1];
  const int16_t number_type = type_node.int16_values_[2];
  int64_t max_precision = PRECISION_UNKNOWN_YET;
  int64_t max_scale = SCALE_UNKNOWN_YET;
  const ObAccuracy& default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[is_oracle_mode][data_type.get_obj_type()];
  const bool has_specify_scale = (1 == type_node.int16_values_[2]);
  LOG_DEBUG("resolve_data_type",
      K(ret),
      K(has_specify_scale),
      K(type_node.type_),
      K(type_node.param_num_),
      K(number_type),
      K(scale),
      K(precision),
      K(length));
  switch (data_type.get_type_class()) {
    case ObIntTC:
      // fallthrough
    case ObUIntTC:
      if (precision <= 0) {
        precision = default_accuracy.get_precision();
      }
      if (precision > OB_MAX_INTEGER_DISPLAY_WIDTH) {
        ret = OB_ERR_TOO_BIG_DISPLAYWIDTH;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_DISPLAYWIDTH, ident_name.ptr(), OB_MAX_INTEGER_DISPLAY_WIDTH);
      } else {
        data_type.set_precision(precision);
        data_type.set_scale(0);
      }
      data_type.set_zero_fill(static_cast<bool>(type_node.int16_values_[2]));
      break;
    case ObFloatTC:
      // fallthrough
    case ObDoubleTC: {
      if (is_oracle_mode) {
        data_type.set_precision(precision);
        data_type.set_scale(scale);
      } else {
        if (OB_UNLIKELY(scale > OB_MAX_DOUBLE_FLOAT_SCALE)) {
          ret = OB_ERR_TOO_BIG_SCALE;
          LOG_USER_ERROR(OB_ERR_TOO_BIG_SCALE, scale, ident_name.ptr(), OB_MAX_DOUBLE_FLOAT_SCALE);
          LOG_WARN("scale of double overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(OB_DECIMAL_NOT_SPECIFIED == scale &&
                               precision > OB_MAX_DOUBLE_FLOAT_PRECISION)) {
          ret = OB_ERR_COLUMN_SPEC;
          LOG_USER_ERROR(OB_ERR_COLUMN_SPEC, ident_name.length(), ident_name.ptr());
          LOG_WARN("precision of double overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(OB_DECIMAL_NOT_SPECIFIED != scale &&
                   precision > OB_MAX_DOUBLE_FLOAT_DISPLAY_WIDTH)) {
          ret = OB_ERR_TOO_BIG_DISPLAYWIDTH;
          LOG_USER_ERROR(OB_ERR_TOO_BIG_DISPLAYWIDTH,
                         ident_name.ptr(),
                         OB_MAX_INTEGER_DISPLAY_WIDTH);
        } else if (OB_UNLIKELY(precision < scale)) {
          ret = OB_ERR_M_BIGGER_THAN_D;
          LOG_USER_ERROR(OB_ERR_M_BIGGER_THAN_D, to_cstring(ident_name));
          LOG_WARN("precision less then scale", K(ret), K(scale), K(precision));
        } else {
          // mysql> create table t1(a decimal(0, 0));
          // mysql> desc t1;
          // +-------+---------------+------+-----+---------+-------+
          // | Field | Type          | Null | Key | Default | Extra |
          // +-------+---------------+------+-----+---------+-------+
          // | a     | decimal(10,0) | YES  |     | NULL    |       |
          // +-------+---------------+------+-----+---------+-------+
          // the same as float and double.
          if (precision <= 0 && scale <= 0) {
            precision = default_accuracy.get_precision();
            scale = default_accuracy.get_scale();
          }
          // A precision from 24 to 53 results in an 8-byte double-precision DOUBLE column.
          if (T_FLOAT == type_node.type_ && precision > OB_MAX_FLOAT_PRECISION && -1 == scale) {
            data_type.set_obj_type(static_cast<ObObjType>(T_DOUBLE));
          }
          data_type.set_precision(precision);
          data_type.set_scale(scale);
          data_type.set_zero_fill(static_cast<bool>(type_node.int16_values_[2]));
        }
      }
      break;
    }
    case ObNumberTC: {
      if (data_type.get_meta_type().is_number_float()) {
        if (precision != PRECISION_UNKNOWN_YET && (OB_UNLIKELY(precision < OB_MIN_NUMBER_FLOAT_PRECISION) ||
                                                      OB_UNLIKELY(precision > OB_MAX_NUMBER_FLOAT_PRECISION))) {
          ret = OB_FLOAT_PRECISION_OUT_RANGE;
          LOG_WARN("precision of float out of range", K(ret), K(precision));
        } else {
          data_type.set_precision(precision);
          data_type.set_scale(ORA_NUMBER_SCALE_UNKNOWN_YET);
        }
      } else if (is_oracle_mode) {
        if ((number_type == NPT_PERC_SCALE || number_type == NPT_PERC) &&
            (OB_UNLIKELY(precision < OB_MIN_NUMBER_PRECISION) || OB_UNLIKELY(precision > OB_MAX_NUMBER_PRECISION))) {
          ret = OB_NUMERIC_PRECISION_OUT_RANGE;
          LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
        }

        if (OB_SUCC(ret)) {
          if ((number_type == NPT_PERC_SCALE || number_type == NPT_STAR_SCALE) &&
              (OB_UNLIKELY(scale < OB_MIN_NUMBER_SCALE) || OB_UNLIKELY(scale > OB_MAX_NUMBER_SCALE))) {
            ret = OB_NUMERIC_SCALE_OUT_RANGE;
            LOG_WARN("scale of number out of range", K(ret), K(scale));
          }
        }

        if (OB_SUCC(ret)) {
          data_type.set_precision(precision);
          data_type.set_scale(scale);
        }
      } else {
        if (OB_UNLIKELY(precision > OB_MAX_DECIMAL_PRECISION)) {
          ret = OB_ERR_TOO_BIG_PRECISION;
          LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, precision, ident_name.ptr(), OB_MAX_DECIMAL_PRECISION);
          LOG_WARN("precision of number overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(scale > OB_MAX_DECIMAL_SCALE)) {
          ret = OB_ERR_TOO_BIG_SCALE;
          LOG_USER_ERROR(OB_ERR_TOO_BIG_SCALE, scale, ident_name.ptr(), OB_MAX_DECIMAL_SCALE);
          LOG_WARN("scale of number overflow", K(ret), K(scale), K(precision));
        } else if (OB_UNLIKELY(precision < scale)) {
          ret = OB_ERR_M_BIGGER_THAN_D;
          LOG_USER_ERROR(OB_ERR_M_BIGGER_THAN_D, to_cstring(ident_name));
          LOG_WARN("precision less then scale", K(ret), K(scale), K(precision));
        } else {
          // mysql> create table t1(a decimal(0, 0));
          // mysql> desc t1;
          // +-------+---------------+------+-----+---------+-------+
          // | Field | Type          | Null | Key | Default | Extra |
          // +-------+---------------+------+-----+---------+-------+
          // | a     | decimal(10,0) | YES  |     | NULL    |       |
          // +-------+---------------+------+-----+---------+-------+
          // the same as float and double.
          if (precision <= 0 && scale <= 0) {
            precision = default_accuracy.get_precision();
            scale = default_accuracy.get_scale();
          }
          data_type.set_precision(precision);
          data_type.set_scale(scale);
          data_type.set_zero_fill(static_cast<bool>(type_node.int16_values_[2]));
        }
      }
      break;
    }
    case ObOTimestampTC:
      if (!has_specify_scale) {
        scale = default_accuracy.get_scale();
      }
      if (OB_UNLIKELY(scale > OB_MAX_TIMESTAMP_TZ_PRECISION)) {
        ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

      } else {
        data_type.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + scale));
        data_type.set_scale(scale);
      }
      break;
    case ObDateTimeTC:
      if (scale > OB_MAX_DATETIME_PRECISION) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, ident_name.ptr(), OB_MAX_DATETIME_PRECISION);
      } else {
        data_type.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + scale));
        data_type.set_scale(scale);
      }
      break;
    case ObDateTC:
      // nothing to do.
      data_type.set_precision(default_accuracy.get_precision());
      data_type.set_scale(default_accuracy.get_scale());
      break;
    case ObTimeTC:
      if (scale > OB_MAX_DATETIME_PRECISION) {
        ret = OB_ERR_TOO_BIG_PRECISION;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_PRECISION, scale, ident_name.ptr(), OB_MAX_DATETIME_PRECISION);
      } else {
        if (scale < 0) {
          scale = default_accuracy.get_scale();
        }
        data_type.set_precision(static_cast<int16_t>(default_accuracy.get_precision() + scale));
        data_type.set_scale(scale);
      }
      break;
    case ObYearTC:
      data_type.set_precision(default_accuracy.get_precision());
      data_type.set_scale(default_accuracy.get_scale());
      // nothing to do.
      break;
    case ObStringTC:
      data_type.set_length(length);

      if (ObVarcharType != data_type.get_obj_type() && ObCharType != data_type.get_obj_type() &&
          ObNVarchar2Type != data_type.get_obj_type() && ObNCharType != data_type.get_obj_type()) {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(ERROR, "column type must be ObVarcharType or ObCharType", K(ret));
      } else if (type_node.int32_values_[1] /*is binary*/) {
        data_type.set_charset_type(CHARSET_BINARY);
        data_type.set_collation_type(CS_TYPE_BINARY);
      } else if (OB_FAIL(resolve_str_charset_info(type_node, data_type))) {
        SQL_RESV_LOG(WARN, "fail to resolve string charset and collation", K(ret), K(data_type));
      } else if (is_oracle_mode) {
        int64_t nchar_mbminlen = 0;
        ObCollationType cs_type = ob_is_nstring_type(data_type.get_obj_type()) ? nls_session_param.nls_nation_collation_
                                                                               : nls_session_param.nls_collation_;

        if (OB_UNLIKELY(0 == length)) {
          ret = OB_ERR_ZERO_LEN_COL;
          LOG_WARN("Oracle not allowed zero length", K(ret));
        } else if (OB_FAIL(ObCharset::get_mbminlen_by_coll(nls_session_param.nls_nation_collation_, nchar_mbminlen))) {
          LOG_WARN("fail to get mbminlen of nchar", K(ret), K(nls_session_param));
        } else if (((ObVarcharType == data_type.get_obj_type() || ObNVarchar2Type == data_type.get_obj_type()) &&
                       OB_MAX_ORACLE_VARCHAR_LENGTH < length) ||
                   (ObCharType == data_type.get_obj_type() && OB_MAX_ORACLE_CHAR_LENGTH_BYTE < length) ||
                   (ObNCharType == data_type.get_obj_type() &&
                       OB_MAX_ORACLE_CHAR_LENGTH_BYTE < length * nchar_mbminlen)) {
          ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
          LOG_WARN("column data length is invalid", K(ret), K(length), K(data_type), K(nchar_mbminlen));
          LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH,
              ident_name.ptr(),
              static_cast<int>(
                  (ObVarcharType == data_type.get_obj_type() || ObNVarchar2Type == data_type.get_obj_type())
                      ? OB_MAX_ORACLE_VARCHAR_LENGTH
                      : OB_MAX_ORACLE_CHAR_LENGTH_BYTE));
        } else if (type_node.length_semantics_ == LS_DEFAULT) {
          data_type.set_length_semantics(nls_session_param.nls_length_semantics_);
        } else if (OB_UNLIKELY(type_node.length_semantics_ != LS_BYTE && type_node.length_semantics_ != LS_CHAR)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("length_semantics_ is invalid", K(ret), K(type_node.length_semantics_));
        } else {
          data_type.set_length_semantics(type_node.length_semantics_);
        }
        data_type.set_charset_type(ObCharset::charset_type_by_coll(cs_type));
        data_type.set_collation_type(cs_type);
        LOG_DEBUG("check data type after resolve", K(ret), K(data_type));
      } else {
        // do nothing
      }
      break;
    case ObRawTC:
      data_type.set_length(length);
      data_type.set_charset_type(CHARSET_BINARY);
      data_type.set_collation_type(CS_TYPE_BINARY);
      break;
    case ObTextTC:
    case ObLobTC:
      data_type.set_length(length);
      data_type.set_scale(default_accuracy.get_scale());
      if (type_node.int32_values_[1] /*is binary*/) {
        data_type.set_charset_type(CHARSET_BINARY);
        data_type.set_collation_type(CS_TYPE_BINARY);
      } else if (OB_FAIL(resolve_str_charset_info(type_node, data_type))) {
        SQL_RESV_LOG(WARN, "fail to resolve string charset and collation", K(ret), K(data_type));
      } else {
        // do nothing
      }
      break;
    case ObJsonTC:
      if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_313) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create json column before cluster min version 3.1.3.");
      } else {
        data_type.set_length(length);
        data_type.set_scale(default_accuracy.get_scale());
        data_type.set_charset_type(CHARSET_UTF8MB4);
        data_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
      break;
    case ObBitTC:
      if (precision < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("precision of bit is negative", K(ret), K(precision));
      } else if (precision > OB_MAX_BIT_LENGTH) {
        ret = OB_ERR_TOO_BIG_DISPLAYWIDTH;
        LOG_USER_ERROR(OB_ERR_TOO_BIG_DISPLAYWIDTH, ident_name.ptr(), OB_MAX_BIT_LENGTH);
      } else if (0 == precision) {  // compatable with Mysql 5.6, 5.7 fails
        data_type.set_precision(default_accuracy.get_precision());
        data_type.set_scale(default_accuracy.get_scale());
      } else {
        data_type.set_precision(precision);
        data_type.set_scale(default_accuracy.get_scale());
      }
      break;
    case ObEnumSetTC:
      if (OB_FAIL(resolve_str_charset_info(type_node, data_type))) {
        LOG_WARN("fail to resolve column charset and collation", K(ident_name), K(ret));
      }
      break;
    case ObIntervalTC:
      if (data_type.get_meta_type().is_interval_ym()) {
        if (0 == type_node.int16_values_[1]) {
          data_type.set_scale(default_accuracy.get_scale());
        } else {
          if (!ObIntervalScaleUtil::scale_check(type_node.int16_values_[0])) {
            ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

          } else {
            ObScale scale =
                ObIntervalScaleUtil::interval_ym_scale_to_ob_scale(static_cast<int8_t>(type_node.int16_values_[0]));
            data_type.set_scale(scale);
          }
        }
      } else {  // interval ds
        int8_t day_scale =
            ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(static_cast<int8_t>(default_accuracy.get_scale()));
        int8_t fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(
            static_cast<int8_t>(default_accuracy.get_scale()));
        if (OB_SUCC(ret) && 0 != type_node.int16_values_[1]) {
          if (!ObIntervalScaleUtil::scale_check(type_node.int16_values_[0])) {
            ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

          } else {
            day_scale = static_cast<int8_t>(type_node.int16_values_[0]);
          }
        }
        if (OB_SUCC(ret) && 0 != type_node.int16_values_[3]) {
          if (!ObIntervalScaleUtil::scale_check(type_node.int16_values_[2])) {
            ret = OB_ERR_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE;

          } else {
            fs_scale = static_cast<int8_t>(type_node.int16_values_[2]);
          }
        }
        ObScale scale = ObIntervalScaleUtil::interval_ds_scale_to_ob_scale(day_scale, fs_scale);
        data_type.set_scale(scale);
      }
      break;
    case ObRowIDTC:
      if (ob_is_urowid(data_type.get_obj_type())) {
        if (length > OB_MAX_USER_ROW_KEY_LENGTH) {
          ret = OB_ERR_TOO_LONG_COLUMN_LENGTH;
          LOG_WARN("column data length is invalid", K(ret), K(length), K(data_type));
          LOG_USER_ERROR(OB_ERR_TOO_LONG_COLUMN_LENGTH, ident_name.ptr(), static_cast<int>(OB_MAX_USER_ROW_KEY_LENGTH));
        } else {
          data_type.set_length(length);
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        SQL_RESV_LOG(WARN, "only support urowid type for now", K(ret), K(data_type));
      }
      break;
    case ObExtendTC:
      // do nothing
      break;
    default:
      ret = OB_ERR_ILLEGAL_TYPE;
      SQL_RESV_LOG(WARN, "Unsupport data type of column definiton", K(ident_name), K(data_type), K(ret));
      break;
  }
  LOG_DEBUG("resolve data type", K(ret), K(data_type), K(lbt()));
  return ret;
}

int ObResolverUtils::resolve_str_charset_info(const ParseNode& type_node, ObDataType& data_type)
{
  int ret = OB_SUCCESS;
  bool is_binary = false;
  ObString charset;
  ObString collation;
  ObCharsetType charset_type = CHARSET_INVALID;
  ObCollationType collation_type = CS_TYPE_INVALID;
  const ParseNode* charset_node = NULL;
  const ParseNode* collation_node = NULL;
  const ParseNode* binary_node = NULL;

  if (OB_ISNULL(type_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type node children is null");
  } else {
    charset_node = type_node.children_[0];
    collation_node = type_node.children_[1];
    binary_node = type_node.children_[2];
  }
  if (OB_SUCC(ret) && NULL != binary_node) {
    is_binary = true;
  }
  if (OB_SUCC(ret) && NULL != charset_node) {
    if (share::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("set charset in oracle mode is not supported now", K(ret));
    } else {
      charset.assign_ptr(charset_node->str_value_, static_cast<int32_t>(charset_node->str_len_));
      if (CHARSET_INVALID == (charset_type = ObCharset::charset_type(charset))) {
        ret = OB_ERR_UNKNOWN_CHARSET;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, charset.length(), charset.ptr());
      }
    }
  }
  if (OB_SUCC(ret) && NULL != collation_node) {
    if (share::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("set collate in oracle mode is not supported now", K(ret));
    } else {
      collation.assign_ptr(collation_node->str_value_, static_cast<int32_t>(collation_node->str_len_));
      if (CS_TYPE_INVALID == (collation_type = ObCharset::collation_type(collation))) {
        ret = OB_ERR_UNKNOWN_COLLATION;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, collation.length(), collation.ptr());
      }
    }
  }

  if (OB_SUCC(ret)) {
    data_type.set_charset_type(charset_type);
    data_type.set_collation_type(collation_type);
    data_type.set_binary_collation(is_binary);
  }
  return ret;
}

int ObResolverUtils::check_sync_ddl_user(ObSQLSessionInfo* session_info, bool& is_sync_ddl_user)
{
  int ret = OB_SUCCESS;
  is_sync_ddl_user = false;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Empty pointer session_info", K(ret));
  } else {
    const ObString current_user(session_info->get_user_name());
    if (session_info->is_inner() || (ObCharset::case_insensitive_equal(current_user, OB_RESTORE_USER_NAME)) ||
        (ObCharset::case_insensitive_equal(current_user, OB_DRC_USER_NAME))) {
      is_sync_ddl_user = true;
    } else {
      is_sync_ddl_user = false;
    }
  }
  return ret;
}

bool ObResolverUtils::is_restore_user(ObSQLSessionInfo& session_info)
{
  int bret = false;
  const ObString current_user(session_info.get_user_name());
  if (ObCharset::case_insensitive_equal(current_user, OB_RESTORE_USER_NAME)) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

bool ObResolverUtils::is_drc_user(ObSQLSessionInfo& session_info)
{
  int bret = false;
  const ObString current_user(session_info.get_user_name());
  if (ObCharset::case_insensitive_equal(current_user, OB_DRC_USER_NAME)) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

int ObResolverUtils::set_sync_ddl_id_str(ObSQLSessionInfo* session_info, ObString& ddl_id_str)
{
  int ret = OB_SUCCESS;
  ddl_id_str.reset();

  bool is_sync_ddl_user = false;
  if (OB_FAIL(ObResolverUtils::check_sync_ddl_user(session_info, is_sync_ddl_user))) {
    LOG_WARN("Failed to check_sync_ddl_user", K(ret));
  } else if (session_info->is_inner()) {
    // do-nothing
  } else if (is_sync_ddl_user) {
    const ObString var_name(common::OB_DDL_ID_VAR_NAME);
    common::ObObj var_obj;
    if (OB_FAIL(session_info->get_user_variable_value(var_name, var_obj))) {
      if (OB_ERR_USER_VARIABLE_UNKNOWN == ret) {
        LOG_DEBUG("no __oceanbase_ddl_id user variable: ", K(ddl_id_str));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get value of __oceanbase_ddl_id user variable", K(ret), K(var_name));
      }
    } else {
      if (ob_is_string_type(var_obj.get_type())) {
        ddl_id_str = var_obj.get_string();
        LOG_DEBUG("__oceanbase_ddl_id user variable: ", K(ddl_id_str));
      } else {
        ret = OB_ERR_WRONG_TYPE_FOR_VAR;
        LOG_WARN("data type of __oceanbase_ddl_id user variable is not string", K(ret), K(var_obj));
      }
    }
  }
  return ret;
}

// for create table with fk in oracle mode
int ObResolverUtils::check_dup_foreign_keys_exist(const common::ObSArray<obrpc::ObCreateForeignKeyArg>& fk_args)
{
  int ret = OB_SUCCESS;

  for (int i = 0; OB_SUCC(ret) && (i < fk_args.count() - 1); ++i) {
    for (int j = i + 1; OB_SUCC(ret) && (j < fk_args.count()); ++j) {
      if (0 == fk_args.at(i).parent_database_.case_compare(fk_args.at(j).parent_database_) &&
          0 == fk_args.at(i).parent_table_.case_compare(fk_args.at(j).parent_table_)) {
        if (is_match_columns_with_order(fk_args.at(i).child_columns_,
                fk_args.at(i).parent_columns_,
                fk_args.at(j).child_columns_,
                fk_args.at(j).parent_columns_)) {
          ret = OB_ERR_DUP_FK_IN_TABLE;
          LOG_WARN("duplicate fks in table", K(ret), K(i), K(j), K(fk_args));
        }
      }
    }
  }

  return ret;
}

// for alter table add fk in oracle mode
int ObResolverUtils::check_dup_foreign_keys_exist(const common::ObIArray<share::schema::ObForeignKeyInfo>& fk_infos,
    const common::ObIArray<uint64_t>& child_column_ids, const common::ObIArray<uint64_t>& parent_column_ids,
    const uint64_t parent_table_id)
{
  int ret = OB_SUCCESS;

  for (int i = 0; OB_SUCC(ret) && (i < fk_infos.count()); ++i) {
    if (parent_table_id == fk_infos.at(i).parent_table_id_) {
      if (is_match_columns_with_order(fk_infos.at(i).child_column_ids_,
              fk_infos.at(i).parent_column_ids_,
              child_column_ids,
              parent_column_ids)) {
        ret = OB_ERR_DUP_FK_EXISTS;
        LOG_WARN("duplicate fk already exists in the table", K(ret), K(i), K(fk_infos.at(i)));
      }
    }
  }

  return ret;
}

int ObResolverUtils::foreign_key_column_match_uk_pk_column(const ObTableSchema& parent_table_schema,
    ObSchemaChecker& schema_checker, ObIArray<ObString>& parent_columns, ObSArray<ObCreateIndexArg>& index_arg_list,
    obrpc::ObCreateForeignKeyArg& arg, bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  const ObRowkeyInfo& rowkey_info = parent_table_schema.get_rowkey_info();
  common::ObSEArray<ObString, 8> pk_columns;

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
    uint64_t column_id = 0;
    const ObColumnSchemaV2* col_schema = NULL;
    if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get rowkey info", K(ret), K(i), K(rowkey_info));
    } else if (NULL == (col_schema = parent_table_schema.get_column_schema(column_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index column schema failed", K(ret));
    } else if (col_schema->is_hidden() || col_schema->is_shadow_column()) {
      // do nothing
    } else if (OB_FAIL(pk_columns.push_back(col_schema->get_column_name()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("push back index column failed", K(ret));
    } else {
    }  // do nothing
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_match_columns(parent_columns, pk_columns, is_match))) {
      LOG_WARN("Failed to check_match_columns", K(ret));
    } else if (is_match) {
      arg.ref_cst_type_ = CONSTRAINT_TYPE_PRIMARY_KEY;
      for (ObTableSchema::const_constraint_iterator iter = parent_table_schema.constraint_begin();
           iter != parent_table_schema.constraint_end();
           ++iter) {
        if (CONSTRAINT_TYPE_PRIMARY_KEY == (*iter)->get_constraint_type()) {
          arg.ref_cst_id_ = (*iter)->get_constraint_id();
          break;
        }
      }
    } else if (index_arg_list.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < index_arg_list.count(); ++i) {
        ObCreateIndexArg index_arg;
        if (OB_FAIL(index_arg.assign(index_arg_list.at(i)))) {
          LOG_WARN("fail to assign schema", K(ret));
        } else if (INDEX_TYPE_UNIQUE_LOCAL == index_arg.index_type_ ||
                   INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_) {
          ObSEArray<ObString, 8> uk_columns;
          for (int64_t j = 0; OB_SUCC(ret) && j < index_arg.index_columns_.count(); ++j) {
            const ObColumnSortItem& sort_item = index_arg.index_columns_.at(j);
            if (OB_FAIL(uk_columns.push_back(sort_item.column_name_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("push back index column failed", K(ret), K(sort_item.column_name_));
            }
          }
          if (OB_FAIL(check_match_columns(parent_columns, uk_columns, is_match))) {
            LOG_WARN("Failed to check_match_columns", K(ret));
          } else if (is_match) {
            arg.ref_cst_type_ = CONSTRAINT_TYPE_UNIQUE_KEY;
          }
        }
      }
    } else {
      ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
      if (OB_FAIL(parent_table_schema.get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
        LOG_WARN("get simple_index_infos without delay_deleted_tid failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < simple_index_infos.count(); ++i) {
        const ObTableSchema* index_table_schema = NULL;
        if (OB_FAIL(schema_checker.get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
          LOG_WARN("get_table_schema failed", K(ret), "table id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema should not be null", K(ret));
        } else if (index_table_schema->is_unique_index()) {
          const ObColumnSchemaV2* index_col = NULL;
          const ObIndexInfo& index_info = index_table_schema->get_index_info();
          ObSEArray<ObString, 8> uk_columns;
          for (int64_t i = 0; OB_SUCC(ret) && i < index_info.get_size(); ++i) {
            if (OB_ISNULL(index_col = index_table_schema->get_column_schema(index_info.get_column(i)->column_id_))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get index column schema failed", K(ret));
            } else if (index_col->is_hidden() || index_col->is_shadow_column()) {
              // do nothing
            } else if (OB_FAIL(uk_columns.push_back(index_col->get_column_name()))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("push back index column failed", K(ret));
            } else {
            }  // do nothing
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(check_match_columns(parent_columns, uk_columns, is_match))) {
              LOG_WARN("Failed to check_match_columns", K(ret));
            } else if (is_match) {
              arg.ref_cst_type_ = CONSTRAINT_TYPE_UNIQUE_KEY;
              arg.ref_cst_id_ = index_table_schema->get_table_id();
            }
          }
        }
      }
    }
  }
  return ret;
}

bool ObResolverUtils::is_match_columns_with_order(const common::ObIArray<ObString>& child_columns_1,
    const common::ObIArray<ObString>& parent_columns_1, const common::ObIArray<ObString>& child_columns_2,
    const common::ObIArray<ObString>& parent_columns_2)
{
  bool dup_foreign_keys_exist = true;

  if (child_columns_1.count() != child_columns_2.count()) {
    dup_foreign_keys_exist = false;
  } else {
    for (int i = 0; dup_foreign_keys_exist && (i < child_columns_1.count()); ++i) {
      int j = 0;
      bool find_same_child_column = false;
      for (j = 0; !find_same_child_column && (j < child_columns_2.count()); ++j) {
        if (0 == child_columns_1.at(i).case_compare(child_columns_2.at(j))) {
          find_same_child_column = true;
          if (0 != parent_columns_1.at(i).case_compare(parent_columns_2.at(j))) {
            dup_foreign_keys_exist = false;
          }
        }
      }
      if (!find_same_child_column) {
        dup_foreign_keys_exist = false;
      }
    }
  }

  return dup_foreign_keys_exist;
}

bool ObResolverUtils::is_match_columns_with_order(const common::ObIArray<uint64_t>& child_column_ids_1,
    const common::ObIArray<uint64_t>& parent_column_ids_1, const common::ObIArray<uint64_t>& child_column_ids_2,
    const common::ObIArray<uint64_t>& parent_column_ids_2)
{
  bool dup_foreign_keys_exist = true;

  if (child_column_ids_1.count() != child_column_ids_2.count()) {
    dup_foreign_keys_exist = false;
  } else {
    for (int i = 0; dup_foreign_keys_exist && (i < child_column_ids_1.count()); ++i) {
      int j = 0;
      bool find_same_child_column = false;
      for (j = 0; !find_same_child_column && (j < child_column_ids_2.count()); ++j) {
        if (child_column_ids_1.at(i) == child_column_ids_2.at(j)) {
          find_same_child_column = true;
          if (parent_column_ids_1.at(i) != parent_column_ids_2.at(j)) {
            dup_foreign_keys_exist = false;
          }
        }
      }
      if (!find_same_child_column) {
        dup_foreign_keys_exist = false;
      }
    }
  }

  return dup_foreign_keys_exist;
}

int ObResolverUtils::check_match_columns(
    const ObIArray<ObString>& parent_columns, const ObIArray<ObString>& key_columns, bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObSEArray<ObString, 8> tmp_parent_columns;
  ObSEArray<ObString, 8> tmp_key_columns;
  if (parent_columns.count() == key_columns.count() && parent_columns.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_columns.count(); ++i) {
      if (OB_FAIL(tmp_parent_columns.push_back(parent_columns.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      } else if (OB_FAIL(tmp_key_columns.push_back(key_columns.at(i)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (tmp_parent_columns.count() == tmp_key_columns.count() && tmp_parent_columns.count() > 0) {
        std::sort(tmp_parent_columns.begin(), tmp_parent_columns.end());
        std::sort(tmp_key_columns.begin(), tmp_key_columns.end());
        bool is_tmp_match = true;
        for (int64_t i = 0; is_tmp_match && i < tmp_parent_columns.count(); ++i) {
          if (0 != tmp_parent_columns.at(i).case_compare(tmp_key_columns.at(i))) {
            is_tmp_match = false;
          }
        }
        if (is_tmp_match) {
          is_match = true;
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_match_columns_strict(
    const ObIArray<ObString>& columns_array_1, const ObIArray<ObString>& columns_array_2, bool& is_match)
{
  int ret = OB_SUCCESS;
  bool is_tmp_match = true;
  is_match = false;
  if (columns_array_1.count() == columns_array_2.count() && columns_array_1.count() > 0) {
    for (int64_t i = 0; is_tmp_match && i < columns_array_1.count(); ++i) {
      if (0 != columns_array_1.at(i).case_compare(columns_array_2.at(i))) {
        is_tmp_match = false;
      }
    }
    if (is_tmp_match) {
      is_match = true;
    }
  }
  return ret;
}

int ObResolverUtils::check_match_columns_strict_with_order(
    const ObTableSchema* index_table_schema, const ObCreateIndexArg& create_index_arg, bool& is_match)
{
  int ret = OB_SUCCESS;
  bool is_tmp_match = true;
  ObString tmp_col_name_1;
  ObString tmp_col_name_2;
  ObOrderType tmp_order_type_1;
  ObOrderType tmp_order_type_2;
  const ObColumnSchemaV2* tmp_index_col = NULL;
  const ObIndexInfo& index_info = index_table_schema->get_index_info();
  is_match = false;

  if (index_info.get_size() == create_index_arg.index_columns_.count() && create_index_arg.index_columns_.count() > 0) {
    for (int64_t idx = 0; is_tmp_match && idx < index_info.get_size(); ++idx) {
      if (OB_ISNULL(tmp_index_col = index_table_schema->get_column_schema(index_info.get_column(idx)->column_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get index column schema failed", K(ret));
      } else {
        tmp_col_name_1 = tmp_index_col->get_column_name_str();
        tmp_order_type_1 = tmp_index_col->get_order_in_rowkey();
        tmp_col_name_2 = create_index_arg.index_columns_.at(idx).column_name_;
        tmp_order_type_2 = create_index_arg.index_columns_.at(idx).order_type_;
        if ((0 != tmp_col_name_1.case_compare(tmp_col_name_2)) || (tmp_order_type_1 != tmp_order_type_2)) {
          is_tmp_match = false;
        }
      }
    }
    if (is_tmp_match) {
      is_match = true;
    }
  }

  return ret;
}

int ObResolverUtils::check_pk_idx_duplicate(const ObTableSchema& table_schema, const ObCreateIndexArg& create_index_arg,
    const ObIArray<ObString>& input_uk_columns_name, bool& is_match)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo& rowkey = table_schema.get_rowkey_info();
  const ObColumnSchemaV2* column = NULL;
  uint64_t column_id = OB_INVALID_ID;
  ObSEArray<ObString, 8> pk_columns_name;
  is_match = false;

  const bool is_multi_pk_for_heap_table = table_schema.is_new_no_pk_table() && rowkey.get_size() > 1;
  // generate pk_columns_name_array
  for (int64_t rowkey_idx = 0; rowkey_idx < rowkey.get_size(); ++rowkey_idx) {
    if (OB_FAIL(rowkey.get_column_id(rowkey_idx, column_id))) {
      LOG_WARN("fail to get column id", K(ret));
    } else if (OB_ISNULL(column = table_schema.get_column_schema(column_id))) {
      LOG_WARN("fail to get column schema", K(ret), K(column_id), K(rowkey));
    } else if (column->is_hidden() && !is_multi_pk_for_heap_table) {
      // skip hidden pk col
    } else if (OB_FAIL(pk_columns_name.push_back(column->get_column_name_str()))) {
      LOG_WARN("fail to push back to pk_columns_name array",
          K(ret),
          K(rowkey_idx),
          K(column_id),
          K(column->get_column_name_str()));
    }
  }
  // check if pk uk duplicate
  if (OB_SUCC(ret) && rowkey.get_size() != 0) {
    if (OB_FAIL(ObResolverUtils::check_match_columns_strict(input_uk_columns_name, pk_columns_name, is_match))) {
      LOG_WARN("Failed to check_match_columns", K(ret));
    } else if (is_match) {
      for (int64_t i = 0; is_match && i < create_index_arg.index_columns_.count(); ++i) {
        if (ObOrderType::DESC == create_index_arg.index_columns_.at(i).order_type_) {
          is_match = false;
        }
      }
    }
  }

  return ret;
}

int ObResolverUtils::check_foreign_key_columns_type(const ObTableSchema& child_table_schema,
    const ObTableSchema& parent_table_schema, ObIArray<ObString>& child_columns, ObIArray<ObString>& parent_columns,
    const share::schema::ObColumnSchemaV2* column)
{
  int ret = OB_SUCCESS;
  if (child_columns.count() != parent_columns.count()) {
    ret = OB_ERR_WRONG_FK_DEF;
    LOG_WARN("the count of foreign key columns is not equal to the count of reference columns",
        K(ret),
        K(child_columns.count()),
        K(parent_columns.count()));
  } else if (child_columns.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
    LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    LOG_WARN("the count of foreign key columns should be between [1,64]",
        K(ret),
        K(child_columns.count()),
        K(parent_columns.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_columns.count(); ++i) {
      const ObColumnSchemaV2* child_col = NULL;
      const ObColumnSchemaV2* parent_col = parent_table_schema.get_column_schema(parent_columns.at(i));
      if (NULL == column) {  // table-level fk
        child_col = child_table_schema.get_column_schema(child_columns.at(i));
      } else {  // column level fk
        child_col = column;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(child_col)) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("child column is not exist", K(ret));
      } else if (OB_ISNULL(parent_col)) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("parent column is not exist", K(ret));
      } else if ((child_col->get_data_type() != parent_col->get_data_type()) &&
                 !is_synonymous_type(child_col->get_data_type(), parent_col->get_data_type())) {
        ret = OB_ERR_CANNOT_ADD_FOREIGN;
        LOG_WARN("Column data types between child table and parent table are different",
            K(ret),
            K(child_col->get_data_type()),
            K(parent_col->get_data_type()));
      } else if (child_col->is_virtual_generated_column() || parent_col->is_virtual_generated_column()) {
        ret = OB_ERR_CANNOT_ADD_FOREIGN;
        LOG_WARN("cannot create foreign key based on virtual generated columns", K(ret));
      } else if (share::is_mysql_mode() &&
                 (ob_is_float_tc(child_col->get_data_type()) || ob_is_double_tc(child_col->get_data_type()) ||
                     ob_is_number_tc(child_col->get_data_type()))) {
        if (child_col->get_data_precision() < parent_col->get_data_precision() ||
            child_col->get_data_scale() < parent_col->get_data_scale()) {
          ret = OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK;
          LOG_USER_ERROR(OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK,
              child_col->get_column_name_str().length(),
              child_col->get_column_name_str().ptr(),
              parent_col->get_column_name_str().length(),
              parent_col->get_column_name_str().ptr());
        }
      } else if (ob_is_string_type(child_col->get_data_type())) {
        if (child_col->get_collation_type() != parent_col->get_collation_type()) {
          ret = OB_ERR_CANNOT_ADD_FOREIGN;
          LOG_WARN("The collation types are different",
              K(ret),
              K(child_col->get_collation_type()),
              K(parent_col->get_collation_type()));
        } else if (share::is_mysql_mode() && (child_col->get_data_length() < parent_col->get_data_length())) {
          ret = OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK;
          LOG_USER_ERROR(OB_ERR_INVALID_CHILD_COLUMN_LENGTH_FK,
              child_col->get_column_name_str().length(),
              child_col->get_column_name_str().ptr(),
              parent_col->get_column_name_str().length(),
              parent_col->get_column_name_str().ptr());
        } else {
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::transform_func_sys_to_udf(ObIAllocator* allocator, const ParseNode* func_sys,
    const ObString& db_name, const ObString& pkg_name, ParseNode*& func_udf)
{
  int ret = OB_SUCCESS;
  ParseNode* db_name_node = NULL;
  ParseNode* pkg_name_node = NULL;
  func_udf = NULL;
  if (OB_ISNULL(allocator) || OB_ISNULL(func_sys)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator or sys func node is null", K(ret));
  } else if (OB_ISNULL(func_udf = new_non_terminal_node(allocator, T_FUN_UDF, 4, nullptr, nullptr, nullptr, nullptr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("make T_FUN_UDF node failed", K(ret));
  } else {
    // assign db name node
    if (!db_name.empty()) {
      if (OB_ISNULL(db_name_node = new_terminal_node(allocator, T_IDENT))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("make db name T_IDENT node failed", K(ret));
      } else {
        func_udf->children_[2] = db_name_node;
        db_name_node->str_value_ = parse_strndup(db_name.ptr(), db_name.length(), allocator);
        if (OB_ISNULL(db_name_node->str_value_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("copy db name failed", K(ret));
        } else {
          db_name_node->str_len_ = db_name.length();
        }
      }
    }

    // assign pkg name node
    if (!pkg_name.empty()) {
      if (OB_ISNULL(pkg_name_node = new_terminal_node(allocator, T_IDENT))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("make pkg name T_IDENT node failed", K(ret));
      } else {
        func_udf->children_[3] = pkg_name_node;
        pkg_name_node->str_value_ = parse_strndup(pkg_name.ptr(), pkg_name.length(), allocator);
        if (OB_ISNULL(pkg_name_node->str_value_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("copy pkg name failed", K(ret));
        } else {
          pkg_name_node->str_len_ = pkg_name.length();
        }
      }
    }

    if (OB_SUCC(ret)) {
      // sys node and udf node have the same memory life cycle
      // we share the func name and param node;
      func_udf->children_[0] = func_sys->children_[0];  // func name
      if (2 == func_sys->num_child_) {
        func_udf->children_[1] = func_sys->children_[1];  // func param
      } else {
        func_udf->children_[1] = NULL;
      }
    }
  }
  return ret;
}

int ObResolverUtils::get_columns_name_from_index_table_schema(
    const ObTableSchema& index_table_schema, ObIArray<ObString>& index_columns_name)
{
  int ret = OB_SUCCESS;
  const ObColumnSchemaV2* index_col = NULL;
  const ObIndexInfo& index_info = index_table_schema.get_index_info();
  index_columns_name.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < index_info.get_size(); ++i) {
    if (OB_ISNULL(index_col = index_table_schema.get_column_schema(index_info.get_column(i)->column_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index column schema failed", K(ret));
    } else if (index_col->is_hidden() || index_col->is_shadow_column()) {
      // do nothing
    } else if (OB_FAIL(index_columns_name.push_back(index_col->get_column_name()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("push back index column failed", K(ret));
    } else {
    }  // do nothing
  }
  return ret;
}

int ObResolverUtils::resolve_string(const ParseNode* node, ObString& string)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node should not be null");
  } else if (OB_UNLIKELY(T_VARCHAR != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node type is not T_VARCHAR", "type", get_type_name(node->type_));
  } else if (OB_UNLIKELY(node->str_len_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty string");
  } else {
    string = ObString(node->str_len_, node->str_value_);
  }
  return ret;
}

int ObResolverUtils::resolve_external_param_info(ObIArray<std::pair<ObRawExpr*, ObConstRawExpr*>>& param_infos,
    ObRawExprFactory& expr_factory, int64_t& prepare_param_count, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  int64_t same_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && OB_INVALID_INDEX == same_idx && i < param_infos.count(); ++i) {
    ObRawExpr* original_expr = param_infos.at(i).first;
    if (OB_ISNULL(original_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (original_expr->same_as(*expr)) {
      same_idx = i;
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_INVALID_INDEX != same_idx) {
      expr = param_infos.at(same_idx).second;
    } else {
      std::pair<ObRawExpr*, ObConstRawExpr*> param_info;
      ObRawExpr* original_ref = expr;
      if (OB_FAIL(ObRawExprUtils::create_param_expr(expr_factory, prepare_param_count++, expr))) {
        LOG_WARN("create param expr failed", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("access idxs is empty", K(ret));
      } else {
        ObConstRawExpr* param_expr = static_cast<ObConstRawExpr*>(expr);
        const_cast<sql::ObExprResType&>(param_expr->get_result_type()).set_param(param_expr->get_value());
        param_info.first = original_ref;
        param_info.second = param_expr;
        if (OB_FAIL(param_infos.push_back(param_info))) {
          LOG_WARN("push_back error", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::uv_check_basic(ObSelectStmt& stmt, const bool is_insert)
{
  int ret = OB_SUCCESS;
  if (stmt.get_table_items().count() == 0) {
    // create view as select 1 a;
    ret = lib::is_mysql_mode() ? (is_insert ? OB_ERR_NON_INSERTABLE_TABLE : OB_ERR_NON_UPDATABLE_TABLE)
                               : OB_ERR_ILLEGAL_VIEW_UPDATE;
    LOG_WARN("no table in select", K(ret));
  } else {
    if (lib::is_mysql_mode()) {
      if (stmt.has_group_by() || stmt.has_having() || stmt.get_aggr_item_size() > 0 || stmt.has_window_function() ||
          stmt.is_distinct() || stmt.has_set_op() || stmt.has_limit()) {
        ret = is_insert ? OB_ERR_NON_INSERTABLE_TABLE : OB_ERR_NON_UPDATABLE_TABLE;
        LOG_WARN("not updatable", K(ret));
      }
    } else if (stmt.has_fetch()) {
      ret = OB_ERR_VIRTUAL_COL_NOT_ALLOWED;
      LOG_WARN("subquery with fetch can't occur in insert/update/delete stmt", K(ret));
    } else {
      bool has_rownum = false;
      if (OB_FAIL(stmt.has_rownum(has_rownum))) {
        LOG_WARN("check select stmt has rownum failed", K(ret));
      }
      if (stmt.has_window_function() || stmt.has_set_op() || has_rownum ||
          (!is_insert && (stmt.has_group_by() || stmt.has_having() || stmt.get_aggr_item_size() > 0))) {
        ret = OB_ERR_ILLEGAL_VIEW_UPDATE;
        LOG_WARN("not updatable", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_select_item_subquery(ObSelectStmt& stmt, bool& has_subquery, bool& has_dependent_subquery,
    const uint64_t base_tid, bool& ref_update_table)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObQueryRefRawExpr*, 1> query_exprs;
  FOREACH_CNT_X(item, stmt.get_select_items(), OB_SUCC(ret))
  {
    if (OB_ISNULL(item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(item->expr_, query_exprs))) {
      LOG_WARN("extract sub query expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && query_exprs.count() > 0) {
    has_subquery = true;
    FOREACH_CNT_X(expr, query_exprs, OB_SUCC(ret) && !has_dependent_subquery)
    {
      if (OB_ISNULL(*expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else {
        ObSelectStmt* ref_stmt = (*expr)->get_ref_stmt();
        if (NULL != ref_stmt) {
          FOREACH_CNT_X(item, ref_stmt->get_table_items(), OB_SUCC(ret) && !ref_update_table)
          {
            if (OB_ISNULL(*item)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL table item", K(ret));
            } else if ((*item)->ref_id_ == base_tid) {
              ref_update_table = true;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObTransformUtils::is_ref_outer_block_relation(
                         ref_stmt, ref_stmt->get_current_level(), has_dependent_subquery))) {
            LOG_WARN("check subquery ref outer relation failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObResolverUtils::set_direction_by_mode(const ParseNode& sort_node, OrderItem& order_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sort_node.children_) || sort_node.num_child_ < 2 || OB_ISNULL(sort_node.children_[1])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (T_SORT_ASC == sort_node.children_[1]->type_) {
    if (share::is_oracle_mode()) {
      if (1 == sort_node.children_[1]->value_) {
        order_item.order_type_ = NULLS_LAST_ASC;
      } else if (2 == sort_node.children_[1]->value_) {
        order_item.order_type_ = NULLS_FIRST_ASC;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value for null position", K(ret), K(sort_node.children_[1]->value_));
      }
    } else {
      order_item.order_type_ = NULLS_FIRST_ASC;
    }
  } else if (T_SORT_DESC == sort_node.children_[1]->type_) {
    if (share::is_oracle_mode()) {
      if (1 == sort_node.children_[1]->value_) {
        order_item.order_type_ = NULLS_LAST_DESC;
      } else if (2 == sort_node.children_[1]->value_) {
        order_item.order_type_ = NULLS_FIRST_DESC;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid value for null position", K(ret), K(sort_node.children_[1]->value_));
      }
    } else {
      order_item.order_type_ = NULLS_LAST_DESC;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sort type", K(ret), K(sort_node.children_[1]->type_));
  }
  return ret;
}

int ObResolverUtils::uv_check_select_item_subquery(
    const TableItem& table_item, bool& has_subquery, bool& has_dependent_subquery, bool& ref_update_table)
{
  int ret = OB_SUCCESS;
  const TableItem* item = &table_item;
  const uint64_t base_tid = table_item.get_base_table_item().ref_id_;
  while (OB_SUCC(ret) && NULL != item && item->is_generated_table() && !has_dependent_subquery) {
    if (OB_ISNULL(item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query is NULL for generate table", K(ret));
    } else if (OB_FAIL(check_select_item_subquery(
                   *item->ref_query_, has_subquery, has_dependent_subquery, base_tid, ref_update_table))) {
      LOG_WARN("check select item subquery failed", K(ret));
    } else {
      item = item->view_base_item_;
    }
  }
  return ret;
}

int ObResolverUtils::check_table_referred(ObSelectStmt& stmt, const uint64_t base_tid, bool& referred)
{
  int ret = OB_SUCCESS;
  OZ(check_stack_overflow());
  FOREACH_CNT_X(t, stmt.get_table_items(), OB_SUCC(ret) && !referred)
  {
    if (OB_ISNULL(*t)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is NULL", K(ret));
    } else {
      if ((*t)->ref_id_ == base_tid) {
        referred = true;
      } else if ((*t)->is_generated_table() && NULL != (*t)->ref_query_) {
        if (OB_FAIL(check_table_referred(*(*t)->ref_query_, base_tid, referred))) {
          LOG_WARN("check table referred failed", K(ret));
        }
      }
    }
  }

  FOREACH_CNT_X(expr, stmt.get_subquery_exprs(), OB_SUCC(ret) && !referred)
  {
    if (OB_ISNULL(*expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (NULL != (*expr)->get_ref_stmt()) {
      if (OB_FAIL(check_table_referred(*(*expr)->get_ref_stmt(), base_tid, referred))) {
        LOG_WARN("check table referred in subquery failed", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::uv_check_where_subquery(const TableItem& table_item, bool& ref_update_table)
{
  int ret = OB_SUCCESS;
  const TableItem* item = &table_item;
  uint64_t update_tid = table_item.get_base_table_item().ref_id_;
  ObSEArray<ObQueryRefRawExpr*, 1> query_exprs;
  while (OB_SUCC(ret) && NULL != item && item->is_generated_table()) {
    if (OB_ISNULL(item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query is NULL for generate table", K(ret));
    } else {
      FOREACH_CNT_X(expr, item->ref_query_->get_condition_exprs(), OB_SUCC(ret))
      {
        if (OB_ISNULL(*expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret));
        } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(*expr, query_exprs))) {
          LOG_WARN("extract subquery failed", K(ret));
        }
      }
    }
    item = item->view_base_item_;
  }

  FOREACH_CNT_X(expr, query_exprs, OB_SUCC(ret) && !ref_update_table)
  {
    if (OB_ISNULL(*expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (NULL != (*expr)->get_ref_stmt()) {
      if (OB_FAIL(check_table_referred(*(*expr)->get_ref_stmt(), update_tid, ref_update_table))) {
        LOG_WARN("check table referred in subquery failed", K(ret));
      }
    }
  }
  return ret;
}

int ObResolverUtils::check_has_non_inner_join(ObSelectStmt& stmt, bool& has_non_inner_join)
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(join, stmt.get_joined_tables(), !has_non_inner_join)
  {
    if (OB_ISNULL(*join)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join table is NULL", K(ret));
    } else if (!(*join)->is_inner_join()) {
      has_non_inner_join = true;
    }
  }
  return ret;
}

int ObResolverUtils::uv_check_has_non_inner_join(const TableItem& table_item, bool& has_non_inner_join)
{
  int ret = OB_SUCCESS;
  const TableItem* item = &table_item;
  while (OB_SUCC(ret) && NULL != item && item->is_generated_table() && !has_non_inner_join) {
    if (OB_ISNULL(item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ref query is NULL for generate table", K(ret));
    } else if (OB_FAIL(check_has_non_inner_join(*item->ref_query_, has_non_inner_join))) {
      LOG_WARN("check select item subquery failed", K(ret));
    } else {
      item = item->view_base_item_;
    }
  }
  return ret;
}

int ObResolverUtils::uv_check_dup_base_col(const TableItem& table_item, bool& has_dup, bool& has_non_col_ref)
{
  int ret = OB_SUCCESS;
  has_dup = false;
  has_non_col_ref = false;
  if (table_item.is_generated_table() && NULL != table_item.ref_query_) {
    const uint64_t base_tid = table_item.get_base_table_item().ref_id_;
    const ObIArray<SelectItem>& select_items = table_item.ref_query_->get_select_items();
    ObSEArray<int64_t, 32> cids;
    FOREACH_CNT_X(si, select_items, OB_SUCC(ret) && !has_dup)
    {
      if (si->implicit_filled_) {
        continue;
      }
      ObRawExpr* expr = si->expr_;
      if (T_REF_ALIAS_COLUMN == expr->get_expr_type()) {
        expr = static_cast<ObAliasRefRawExpr*>(expr)->get_ref_expr();
      }
      if (T_REF_COLUMN != expr->get_expr_type()) {
        has_non_col_ref = true;
      } else {
        ColumnItem* col_item =
            table_item.ref_query_->get_column_item_by_id(static_cast<ObColumnRefRawExpr*>(expr)->get_table_id(),
                static_cast<ObColumnRefRawExpr*>(expr)->get_column_id());
        if (OB_ISNULL(col_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column item by id failed", K(ret), K(*expr));
        } else if (base_tid == col_item->base_tid_) {
          FOREACH_X(c, cids, !has_dup)
          {
            if (*c == col_item->base_cid_) {
              has_dup = true;
            }
          }
          if (OB_FAIL(cids.push_back(col_item->base_cid_))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// mysql insertable view:
// 1. must be inner join
// 2. all join components:
//    - must be view
//    - can not reference the update table
//    - pass uv_check_basic
int ObResolverUtils::uv_mysql_insertable_join(const TableItem& table_item, const uint64_t base_tid, bool& insertable)
{
  int ret = OB_SUCCESS;
  if (table_item.is_generated_table() && NULL != table_item.ref_query_) {
    OZ(check_stack_overflow());
    FOREACH_CNT_X(join, table_item.ref_query_->get_joined_tables(), OB_SUCC(ret) && insertable)
    {
      if (OB_ISNULL(*join)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("join table is NULL", K(ret));
      } else if (!(*join)->is_inner_join()) {
        insertable = false;
      }
    }

    FOREACH_CNT_X(it, table_item.ref_query_->get_table_items(), OB_SUCC(ret) && insertable)
    {
      const TableItem* item = *it;
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null join table item", K(ret));
      } else if (item->is_basic_table()) {
        if (table_item.view_base_item_ != item && item->ref_id_ == base_tid) {
          LOG_DEBUG("reference to insert table");
          insertable = false;
        }
      } else if (item->is_generated_table()) {
        if (OB_ISNULL(item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ref query is NULL", K(ret));
        }

        if (OB_SUCC(ret) && insertable) {
          if (!item->is_view_table_) {
            insertable = false;
          }
        }

        if (OB_SUCC(ret) && insertable && item != table_item.view_base_item_) {
          bool ref = false;
          if (OB_FAIL(check_table_referred(*item->ref_query_, base_tid, ref))) {
            LOG_WARN("check table referred failed", K(ret));
          } else if (ref) {
            insertable = false;
          }
        }

        if (OB_SUCC(ret) && insertable) {
          const bool is_insert = true;
          int tmp_ret = uv_check_basic(*item->ref_query_, is_insert);
          if (OB_SUCCESS != tmp_ret) {
            if (tmp_ret == OB_ERR_NON_INSERTABLE_TABLE) {
              insertable = false;
            } else {
              ret = tmp_ret;
              LOG_WARN("check basic updatable view failed", K(ret));
            }
          }
        }

        if (OB_SUCC(ret) && insertable) {
          if (OB_FAIL(uv_mysql_insertable_join(*item, base_tid, insertable))) {
            LOG_WARN("check insertable join failed", K(ret));
          }
        }
      }
    }  // end FOREACH
  }
  return ret;
}

int ObResolverUtils::uv_check_oracle_distinct(
    const TableItem& table_item, ObSQLSessionInfo& session_info, ObSchemaChecker& schema_checker, bool& has_distinct)
{
  int ret = OB_SUCCESS;
  has_distinct = false;
  const TableItem* item = &table_item;
  ObSEArray<ObRawExpr*, 16> select_exprs;
  while (NULL != item && item->is_generated_table() && NULL != item->ref_query_ && !has_distinct) {
    if (item->ref_query_->has_distinct()) {
      bool unique = false;
      select_exprs.reuse();
      // Can not call ObOptimizerUtil::get_select_exprs(),
      // since we need ignore implicit filled select items.
      FOREACH_CNT_X(si, item->ref_query_->get_select_items(), OB_SUCC(ret))
      {
        if (OB_ISNULL(si->expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL in select item", K(ret));
        } else {
          if (!si->implicit_filled_) {
            if (OB_FAIL(select_exprs.push_back(si->expr_))) {
              LOG_WARN("array push back failed", K(ret));
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(item->ref_query_,
                     &session_info,
                     &schema_checker,
                     select_exprs,
                     true /* strict */,
                     unique,
                     FLAGS_IGNORE_DISTINCT /* ignore distinct */))) {
        LOG_WARN("check stmt unique failed", K(ret));
      } else {
        // distinct will be removed latter if unique.
        has_distinct = !unique;
      }
    }
    item = item->view_base_item_;
  }
  return ret;
}

ObString ObResolverUtils::get_stmt_type_string(stmt::StmtType stmt_type)
{
  return ((stmt::T_NONE <= stmt_type && stmt_type <= stmt::T_MAX) ? stmt_type_string[stmt::get_stmt_type_idx(stmt_type)]
                                                                  : ObString::make_empty_string());
}

ParseNode* ObResolverUtils::get_select_into_node(const ParseNode& node)
{
  ParseNode* into_node = NULL;
  if (OB_LIKELY(node.type_ == T_SELECT)) {
    if (NULL != node.children_[PARSE_SELECT_INTO]) {
      into_node = node.children_[PARSE_SELECT_INTO];
    } else {
      into_node = node.children_[PARSE_SELECT_INTO_EXTRA];
    }
  }
  return into_node;
}

int ObResolverUtils::parse_interval_ym_type(char* cstr, ObDateUnitType& part_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cstr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is null", K(ret));
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_YEAR)))) {
    part_type = DATE_UNIT_YEAR;
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_MONTH)))) {
    part_type = DATE_UNIT_MONTH;
  } else {
    part_type = DATE_UNIT_MAX;
  }
  return ret;
}

int ObResolverUtils::parse_interval_ds_type(char* cstr, ObDateUnitType& part_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cstr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data is null", K(ret));
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_DAY)))) {
    part_type = DATE_UNIT_DAY;
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_HOUR)))) {
    part_type = DATE_UNIT_HOUR;
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_MINUTE)))) {
    part_type = DATE_UNIT_MINUTE;
  } else if (OB_NOT_NULL(strcasestr(cstr, ob_date_unit_type_str(DATE_UNIT_SECOND)))) {
    part_type = DATE_UNIT_SECOND;
  } else {
    part_type = DATE_UNIT_MAX;
  }
  return ret;
}

int ObResolverUtils::parse_interval_precision(char* cstr, int16_t& precision, int16_t default_precision)
{
  int ret = OB_SUCCESS;
  const char* brackt_pos1 = strchr(cstr, '(');
  const char* brackt_pos2 = strchr(cstr, ')');
  if (OB_NOT_NULL(brackt_pos1) && OB_NOT_NULL(brackt_pos2)) {
    precision = atoi(brackt_pos1 + 1);
  } else {
    precision = default_precision;
  }
  return ret;
}

int ObResolverUtils::get_user_var_value(const ParseNode* node, ObSQLSessionInfo* session_info, ObObj& value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(node), K(session_info));
  } else if (OB_UNLIKELY(1 != node->num_child_) || OB_ISNULL(node->children_) || OB_ISNULL(node->children_[0])) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid node children for get user_val", K(ret), K(node->num_child_));
  } else {
    ObString str = ObString(static_cast<int32_t>(node->children_[0]->str_len_), node->children_[0]->str_value_);
    ObSessionVariable osv;
    ret = session_info->get_user_variable(str, osv);
    if (OB_SUCC(ret)) {
      value = osv.value_;
      value.set_meta_type(osv.meta_);
    } else if (OB_ERR_USER_VARIABLE_UNKNOWN == ret) {
      value.set_null();
      value.set_collation_level(CS_LEVEL_IMPLICIT);
      ret = OB_SUCCESS;  // always return success no matter found or not
    } else {
      LOG_WARN("Unexpected ret code", K(ret), K(str), K(osv));
    }
  }
  return ret;
}

int ObResolverUtils::check_duplicated_column(ObSelectStmt& select_stmt, bool can_skip /*default false*/)
{
  int ret = OB_SUCCESS;
  if (!can_skip) {
    for (int64_t i = 1; OB_SUCC(ret) && i < select_stmt.get_select_item_size(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
        if (ObCharset::case_compat_mode_equal(
                select_stmt.get_select_item(i).alias_name_, select_stmt.get_select_item(j).alias_name_)) {
          if (share::is_oracle_mode()) {
            ret = OB_NON_UNIQ_ERROR;
            ObString scope_name = ObString::make_string(get_scope_name(T_FIELD_LIST_SCOPE));
            LOG_USER_ERROR(OB_NON_UNIQ_ERROR,
                select_stmt.get_select_item(i).alias_name_.length(),
                select_stmt.get_select_item(i).alias_name_.ptr(),
                scope_name.length(),
                scope_name.ptr());
          } else {
            ret = OB_ERR_COLUMN_DUPLICATE;
            LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE,
                select_stmt.get_select_item(i).alias_name_.length(),
                select_stmt.get_select_item(i).alias_name_.ptr());
          }
          break;
        }
      }
    }
  }

  // mark all need_check_dup_name_ if neccessary
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt.get_select_item_size(); i++) {
    if (select_stmt.get_select_item(i).is_real_alias_ ||
        0 == select_stmt.get_select_item(i).paramed_alias_name_.length() ||
        select_stmt.get_select_item(i).need_check_dup_name_) {
      // do nothing
    } else {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < select_stmt.get_select_item_size(); j++) {
        if (select_stmt.get_select_item(i).is_real_alias_ ||
            0 == select_stmt.get_select_item(i).paramed_alias_name_.length() ||
            select_stmt.get_select_item(j).need_check_dup_name_) {
          // do nothing
        } else if (ObCharset::case_compat_mode_equal(select_stmt.get_select_item(i).paramed_alias_name_,
                       select_stmt.get_select_item(j).paramed_alias_name_)) {
          select_stmt.get_select_item(i).need_check_dup_name_ = true;
          select_stmt.get_select_item(j).need_check_dup_name_ = true;
        }
      }
    }
  }
  return ret;
}

void ObResolverUtils::escape_char_for_oracle_mode(ObString& str)
{
  if (str.length() == 2 && '\\' == str[0]) {
    int with_back_slash = 0;
    unsigned char escpaed_char = escaped_char(str[1], &with_back_slash);
    if (1 != with_back_slash) {
      str.ptr()[0] = static_cast<char>(escpaed_char);
      str.assign_ptr(str.ptr(), 1);
    }
  }
}

}  // namespace sql
}  // namespace oceanbase
