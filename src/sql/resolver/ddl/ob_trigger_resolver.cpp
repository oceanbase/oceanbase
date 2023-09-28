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
#include "sql/resolver/ddl/ob_trigger_resolver.h"
#include "sql/resolver/ddl/ob_trigger_stmt.h"
#include "pl/parser/ob_pl_parser.h"
#include "pl/ob_pl_resolver.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_create_routine_resolver.h"
#include "pl/parser/parse_stmt_item_type.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_compile.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
using namespace pl;

int ObTriggerResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObItemType stmt_type = parse_tree.type_;
  switch (stmt_type) {
  case T_TG_CREATE: {
    ObCreateTriggerStmt *stmt = create_stmt<ObCreateTriggerStmt>();
    OV (OB_NOT_NULL(stmt), OB_ALLOCATE_MEMORY_FAILED);
    OZ (resolve_create_trigger_stmt(parse_tree, stmt->get_trigger_arg()));
    break;
  }
  case T_TG_DROP: {
    ObDropTriggerStmt *stmt = create_stmt<ObDropTriggerStmt>();
    OV (OB_NOT_NULL(stmt), OB_ALLOCATE_MEMORY_FAILED);
    OZ (resolve_drop_trigger_stmt(parse_tree, stmt->get_trigger_arg()));
    break;
  }
  case T_TG_ALTER: {
    ObAlterTriggerStmt *stmt = create_stmt<ObAlterTriggerStmt>();
    OV (OB_NOT_NULL(stmt), OB_ALLOCATE_MEMORY_FAILED);
    OZ (resolve_alter_trigger_stmt(parse_tree, stmt->get_trigger_arg()));
    break;
  }
  default:
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid stmt type", K(ret), K(stmt_type));
  }
  return ret;
}

int ObTriggerResolver::resolve_sp_definer(const ParseNode *parse_node,
                                          ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(schema_checker_));
  CK(OB_NOT_NULL(schema_checker_->get_schema_guard()));
  CK(OB_NOT_NULL(session_info_));
  CK(OB_NOT_NULL(allocator_));
  ObString user_name, host_name;
  ObString cur_user_name, cur_host_name;
  cur_user_name = session_info_->get_user_name();
  cur_host_name = session_info_->get_host_name();
  if (OB_NOT_NULL(parse_node)) {
    CK(T_USER_WITH_HOST_NAME == parse_node->type_);
    if (OB_SUCC(ret)) {
      const ParseNode *user_node = parse_node->children_[0];
      const ParseNode *host_node = parse_node->children_[1];

      if (OB_ISNULL(user_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user must be specified", K(ret));
      } else {
        // 需要检查当前用户是否有超级权限或者set user id的权限
        if (!session_info_->has_user_super_privilege()) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("no privilege", K(ret));
        } else {
          user_name.assign_ptr(user_node->str_value_, static_cast<int32_t>(user_node->str_len_));
          // 得区分current_user和“current_user”, 前者需要获取当前用户和host，后者是作为用户名存在
          if (0 == user_name.case_compare("current_user") && T_IDENT == user_node->type_) {
            user_name = cur_user_name;
            host_name = cur_host_name;
          } else if (OB_ISNULL(host_node)) {
            host_name.assign_ptr("%", 1);
          } else {
            host_name.assign_ptr(host_node->str_value_, static_cast<int32_t>(host_node->str_len_));
          }
        }
        if (OB_SUCC(ret)) {
          // 检查user@host是否在mysql.user表中
          const ObUserInfo* user_info = nullptr;
          if (OB_FAIL(schema_checker_->get_schema_guard()->get_user_info(session_info_->get_effective_tenant_id(),
                                                                         user_name,
                                                                         host_name,
                                                                         user_info))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get_user_info", K(ret));
          } else if (OB_ISNULL(user_info)) {
            LOG_USER_WARN(OB_ERR_USER_NOT_EXIST);
            pl::ObPL::insert_error_msg(OB_ERR_USER_NOT_EXIST);
            ret = OB_SUCCESS;
          }
        }
      }
    }
  } else if (lib::is_mysql_mode()) {
    // 不指定definer时，默认为当前用户和host
    user_name = cur_user_name;
    host_name = cur_host_name;
  }
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    //user@host作为一个整体存储到priv_user字段
    char tmp_buf[common::OB_MAX_USER_NAME_LENGTH + common::OB_MAX_HOST_NAME_LENGTH + 2] = {};
    snprintf(tmp_buf, sizeof(tmp_buf), "%.*s@%.*s", user_name.length(), user_name.ptr(),
                                                    host_name.length(), host_name.ptr());

    ObString priv_user(tmp_buf);
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
              *allocator_, session_info_->get_dtc_params(), priv_user))) {
      LOG_WARN("fail to convert charset", K(ret));
    } else if (OB_FAIL(trigger_arg.trigger_info_.set_trigger_priv_user(priv_user))) {
      LOG_WARN("failed to set priv user", K(ret));
    }
  }

  return ret;
}

int ObTriggerResolver::resolve_create_trigger_stmt(const ParseNode &parse_node,
                                                   ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  bool is_ora = lib::is_oracle_mode();
  OV (parse_node.type_ == T_TG_CREATE, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (parse_node.num_child_ == (is_ora ? 1 : 2), OB_ERR_UNEXPECTED, parse_node.num_child_);
  OV (OB_NOT_NULL(parse_node.children_));
  OV (OB_NOT_NULL(parse_node.children_[is_ora ? 0 : 1]));    // trigger source.
  OV (OB_NOT_NULL(session_info_));
  if (OB_SUCC(ret) && parse_node.int32_values_[1] == 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "editionable in create trigger");
  }
  OX (trigger_arg.with_replace_ = (parse_node.int32_values_[0] != 0));
  OX (trigger_arg.trigger_info_.set_tenant_id(session_info_->get_effective_tenant_id()));
  OX (trigger_arg.trigger_info_.set_owner_id(session_info_->get_user_id()));
  OZ (resolve_sp_definer(is_ora ? nullptr : parse_node.children_[0], trigger_arg));
  OZ (resolve_trigger_source(*parse_node.children_[is_ora ? 0 : 1], trigger_arg));
  if (OB_SUCC(ret)) {
    ObErrorInfo &error_info = trigger_arg.error_info_;
    error_info.collect_error_info(&(trigger_arg.trigger_info_));
  }
  return ret;
}

int ObTriggerResolver::resolve_drop_trigger_stmt(const ParseNode &parse_node,
                                                 ObDropTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  OV (parse_node.type_ == T_TG_DROP, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (parse_node.num_child_ == 1, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OV (OB_NOT_NULL(parse_node.children_));
  OV (OB_NOT_NULL(parse_node.children_[0]));    // trigger name.
  OV (OB_NOT_NULL(session_info_));
  OX (trigger_arg.tenant_id_ = session_info_->get_effective_tenant_id());
  OZ (resolve_schema_name(*parse_node.children_[0], trigger_arg.trigger_database_, trigger_arg.trigger_name_));
  OV (OB_NOT_NULL(schema_checker_));
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(
                            session_info_->get_effective_tenant_id(),
                            session_info_->get_priv_user_id(),
                            trigger_arg.trigger_database_,
                            stmt::T_DROP_TRIGGER,
                            session_info_->get_enable_role_array()));
  }
  OX (trigger_arg.if_exist_ = parse_node.value_);
  return ret;
}

int ObTriggerResolver::resolve_alter_trigger_stmt(const ParseNode &parse_node,
                                                  ObAlterTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  ObString trigger_db_name;
  ObString trigger_name;
  const ObTriggerInfo *old_tg_info = NULL;
  ObTriggerInfo new_tg_info;
  OV (parse_node.type_ == T_TG_ALTER, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (parse_node.num_child_ == 2, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OV (OB_NOT_NULL(parse_node.children_));
  OV (OB_NOT_NULL(parse_node.children_[0]));  //trigger name.
  OV (OB_NOT_NULL(parse_node.children_[1]));  //alter clause.
  OV (OB_NOT_NULL(session_info_) && OB_NOT_NULL(schema_checker_));
  OZ (resolve_schema_name(*parse_node.children_[0], trigger_db_name, trigger_name));
  OZ (schema_checker_->get_trigger_info(session_info_->get_effective_tenant_id(), trigger_db_name,
                                        trigger_name, old_tg_info));
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ(schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                           session_info_->get_priv_user_id(),
                                           trigger_db_name, stmt::T_ALTER_TRIGGER,
                                           session_info_->get_enable_role_array()));
  }
  if (OB_SUCC(ret) && OB_ISNULL(old_tg_info)) {
    ret = OB_ERR_TRIGGER_NOT_EXIST;
    LOG_ORACLE_USER_ERROR(OB_ERR_TRIGGER_NOT_EXIST, trigger_name.length(), trigger_name.ptr());
  }
  OZ (new_tg_info.deep_copy(*old_tg_info));
  OZ (resolve_alter_clause(*parse_node.children_[1], new_tg_info, trigger_db_name,
                           trigger_arg.is_set_status_, trigger_arg.is_alter_compile_));
  OZ (trigger_arg.trigger_infos_.push_back(new_tg_info));
  return ret;
}

int ObTriggerResolver::resolve_trigger_source(const ParseNode &parse_node,
                                              ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  ObString trigger_name;
  ObString trigger_body;
  OV (parse_node.type_ == T_TG_SOURCE, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (parse_node.num_child_ == 2, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OV (OB_NOT_NULL(parse_node.children_));
  OV (OB_NOT_NULL(parse_node.children_[0]));    // trigger name.
  OV (OB_NOT_NULL(parse_node.children_[1]));    // trigger definition.
  OZ (resolve_schema_name(*parse_node.children_[0], trigger_arg.trigger_database_, trigger_name));
  OV (OB_NOT_NULL(session_info_));
  OV (OB_NOT_NULL(schema_checker_));
  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    OZ (schema_checker_->check_ora_ddl_priv(
                            session_info_->get_effective_tenant_id(),
                            session_info_->get_priv_user_id(),
                            trigger_arg.trigger_database_,
                            stmt::T_CREATE_TRIGGER,
                            session_info_->get_enable_role_array()));
  }
  OZ (trigger_arg.trigger_info_.set_trigger_name(trigger_name), trigger_name);
  trigger_body = ObString(parse_node.children_[1]->str_len_, parse_node.children_[1]->str_value_);
  OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
        *allocator_, session_info_->get_dtc_params(), trigger_body));
  OZ (trigger_arg.trigger_info_.set_trigger_body(trigger_body));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (T_TG_SIMPLE_DML == parse_node.children_[1]->type_) {
    OX (trigger_arg.trigger_info_.set_simple_dml_type());
    OZ (resolve_simple_dml_trigger(*parse_node.children_[1], trigger_arg));
  } else if (T_TG_INSTEAD_DML == parse_node.children_[1]->type_) {
    OX (trigger_arg.trigger_info_.set_instead_dml_type());
    OZ (resolve_instead_dml_trigger(*parse_node.children_[1], trigger_arg));
  } else if (T_TG_COMPOUND_DML == parse_node.children_[1]->type_) {
    OX (trigger_arg.trigger_info_.set_compound_dml_type());
    OZ (resolve_compound_dml_trigger(*parse_node.children_[1], trigger_arg));
  }
  if (OB_SUCC(ret) && parse_node.value_ != 0 && is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "default collation in create trigger");
  }
  return ret;
}

int ObTriggerResolver::resolve_instead_dml_trigger(const ParseNode &parse_node,
                                                   ObCreateTriggerArg &trigger_arg)
{
  // An INSTEAD OF trigger is always a row-level trigger.
  int ret = OB_SUCCESS;
  LOG_DEBUG("resolve instead of trigger");
  OV (T_TG_INSTEAD_DML == parse_node.type_, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (parse_node.num_child_ == 5, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OV (OB_NOT_NULL(parse_node.children_));
  OV (OB_NOT_NULL(parse_node.children_[0]));    // dml event.
  // when-clause not supported in oracle
  OV (OB_ISNULL(parse_node.children_[3]), OB_ERR_WHEN_CLAUSE_IN_TRI);
  OV (OB_NOT_NULL(parse_node.children_[4]));    // trigger body.
  OX (trigger_arg.trigger_info_.add_before_row()); // instead of trigger is always before row.
  OX (trigger_arg.trigger_info_.add_instead_row());
  OZ (resolve_dml_event_option(*parse_node.children_[0], trigger_arg));
  OZ (resolve_reference_names(parse_node.children_[1], trigger_arg));
  OZ (resolve_trigger_status(parse_node.int16_values_[1], trigger_arg));
  OZ (resolve_order_clause(parse_node.children_[2], trigger_arg));
  OZ (resolve_trigger_body(*parse_node.children_[4], trigger_arg));
  OZ (fill_package_info(trigger_arg.trigger_info_));
  return ret;
} 

int ObTriggerResolver::resolve_simple_dml_trigger(const ParseNode &parse_node,
                                                  ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  bool is_ora = lib::is_oracle_mode();
  OV (parse_node.type_ == T_TG_SIMPLE_DML, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (parse_node.num_child_ == (is_ora ? 5 : 4), OB_ERR_UNEXPECTED, parse_node.num_child_);
  OV (OB_NOT_NULL(parse_node.children_));
  OV (OB_NOT_NULL(parse_node.children_[0]));    // dml event.
  OV (OB_NOT_NULL(parse_node.children_[is_ora ? 4 : 3]));    // simple trigger body.
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_ora) {
    OX (LOG_DEBUG("TRIGGER", K(parse_node.int16_values_[0]), K(parse_node.int16_values_[1]),
                  K(parse_node.int16_values_[2])));
    OZ (resolve_timing_point(parse_node.int16_values_[0], parse_node.int16_values_[1], trigger_arg));
    OZ (resolve_dml_event_option(*parse_node.children_[0], trigger_arg));
    OZ (resolve_reference_names(parse_node.children_[1], trigger_arg));
    OZ (resolve_trigger_status(parse_node.int16_values_[2], trigger_arg));
    OZ (resolve_when_condition(parse_node.children_[3], trigger_arg));
  } else {
    OX (LOG_DEBUG("TRIGGER", K(parse_node.int16_values_[0])));
    OV (OB_NOT_NULL(parse_node.children_[1])); // mysql mode, trigger_name
    if (OB_SUCC(ret)) {
      if (T_BEFORE == parse_node.int16_values_[0]) {
        trigger_arg.trigger_info_.add_before_row();
      } else if (T_AFTER == parse_node.int16_values_[0]) {
        trigger_arg.trigger_info_.add_after_row();
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("parse_node timing points is invalid", K(parse_node.int16_values_[0]), K(ret));
      }
      if (OB_SUCC(ret)) {
        switch (parse_node.children_[0]->type_)
        {
        case T_INSERT:
          trigger_arg.trigger_info_.add_insert_event();
          break;
        case T_UPDATE:
          trigger_arg.trigger_info_.add_update_event();
          break;
        case T_DELETE:
          trigger_arg.trigger_info_.add_delete_event();
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("parse_node type is invalid", K(ret), K(parse_node.children_[0]->type_));
          break;
        }
      }
      OX (trigger_arg.trigger_info_.set_enable());
      OZ (trigger_arg.trigger_info_.set_ref_old_name(REF_OLD));
      OZ (trigger_arg.trigger_info_.set_ref_new_name(REF_NEW));
      OZ (trigger_arg.trigger_info_.set_ref_parent_name(REF_PARENT));
    }
    OZ (resolve_schema_name(*parse_node.children_[1],
                            trigger_arg.base_object_database_, trigger_arg.base_object_name_));
    OZ (resolve_base_object(trigger_arg, false));
  }
  OZ (resolve_order_clause(parse_node.children_[2], trigger_arg));
  OZ (resolve_trigger_body(*parse_node.children_[is_ora ? 4 : 3], trigger_arg));
  OZ (fill_package_info(trigger_arg.trigger_info_));
  return ret;
}

int ObTriggerResolver::resolve_compound_dml_trigger(const ParseNode &parse_node,
                                                    ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  OV (T_TG_COMPOUND_DML == parse_node.type_, OB_ERR_UNEXPECTED, parse_node.type_);
  CK (OB_NOT_NULL(parse_node.children_[4]));
  OZ (resolve_dml_event_option(*parse_node.children_[0], trigger_arg));
  OZ (resolve_reference_names(parse_node.children_[1], trigger_arg));
  OZ (resolve_order_clause(parse_node.children_[2], trigger_arg));
  OZ (resolve_when_condition(parse_node.children_[3], trigger_arg));
  OZ (resolve_trigger_status(static_cast<int16_t>(parse_node.value_), trigger_arg));
  OZ (resolve_compound_timing_point(*parse_node.children_[4]->children_[1], trigger_arg));
  OZ (resolve_compound_trigger_body(*parse_node.children_[4], trigger_arg));
  OZ (fill_package_info(trigger_arg.trigger_info_));
  return ret;
}

int ObTriggerResolver::resolve_compound_timing_point(const ParseNode &parse_node,
                                                     ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
  CK (OB_NOT_NULL(schema_guard));
  OZ (schema_guard->get_table_schema(trigger_arg.trigger_info_.get_tenant_id(),
                                     trigger_arg.trigger_info_.get_base_object_id(),
                                     table_schema));
  CK (OB_NOT_NULL(table_schema));
#define LABEL_NOT_MATCH(ident1, ident2) \
  ret = OB_ERR_END_LABEL_NOT_MATCH; \
  LOG_USER_ERROR(OB_ERR_END_LABEL_NOT_MATCH, ident1.length(), ident1.ptr(), ident2.length(), ident2.ptr()); \
  LOG_WARN("END identifier must match START identifier", K(ident1), K(ident2), K(ret));

#define CHECK_DUPLICATE_OR_SET_POINT(timing) \
  if (trigger_arg.trigger_info_.has_##timing##_point()) {              \
    ret = OB_ERR_DUPLICATE_TRIGGER_SECTION;                            \
    LOG_WARN("duplicate Compound Triggers section after row", K(ret)); \
  } else {                                                             \
    trigger_arg.trigger_info_.add_##timing();                          \
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < parse_node.num_child_; i++) {
    CK (OB_NOT_NULL(parse_node.children_[i]));
    if (OB_SUCC(ret)) {
      const int16_t header_timing = parse_node.children_[i]->int16_values_[0];
      const int16_t header_level = parse_node.children_[i]->int16_values_[1];
      const int16_t tail_timing = parse_node.children_[i]->int16_values_[2];
      const int16_t tail_level = parse_node.children_[i]->int16_values_[3];
      if (T_INSTEAD == header_timing) {
        if (T_INSTEAD != tail_timing) {
          ret = OB_ERR_PARSE_PLSQL;
          LOG_WARN("unexpected symbol", K(ret));
          if (T_AFTER == tail_timing) {
            LOG_USER_ERROR(OB_ERR_PARSE_PLSQL, "\"AFTER\"", "instead");
          } else {
            LOG_USER_ERROR(OB_ERR_PARSE_PLSQL, "\"BEFORE\"", "instead");
          }
        } else if (!table_schema->is_user_view()) {
          ret = OB_ERR_INVALID_SECTION;
          LOG_WARN("invalid section for this type of Compound Trigger", K(ret));
        } else {
          trigger_arg.trigger_info_.add_instead_row();
          trigger_arg.trigger_info_.add_before_row(); // instead of trigger 在before row时机执行
        }
      } else {
        if (T_BEFORE == header_timing) {
          if (T_BEFORE != tail_timing) {
            LABEL_NOT_MATCH(ObString("BEFORE"), ObString("AFTER"));
          } else if (T_TP_STATEMENT == header_level) {
            if (T_TP_STATEMENT != tail_level) {
              LABEL_NOT_MATCH(ObString("STATEMENT"), ObString("ROW"));
            } else {
              CHECK_DUPLICATE_OR_SET_POINT(before_stmt);
            }
          } else {
            if (T_TP_EACH_ROW != tail_level) {
              LABEL_NOT_MATCH(ObString("ROW"), ObString("STATEMENT"));
            } else {
              CHECK_DUPLICATE_OR_SET_POINT(before_row);
            }
          }
        } else if (T_AFTER == header_timing) {
          if (T_AFTER != tail_timing) {
            LABEL_NOT_MATCH(ObString("AFTER"), ObString("BEFORE"));
          } else if (T_TP_STATEMENT == header_level) {
            if (T_TP_STATEMENT != tail_level) {
              LABEL_NOT_MATCH(ObString("STATEMENT"), ObString("ROW"));
            } else {
              CHECK_DUPLICATE_OR_SET_POINT(after_stmt);
            }
          } else {
            if (T_TP_EACH_ROW != tail_level) {
              LABEL_NOT_MATCH(ObString("ROW"), ObString("STATEMENT"));
            } else {
              CHECK_DUPLICATE_OR_SET_POINT(after_row);
            }
          }
        }
        if (OB_SUCC(ret) && !table_schema->is_user_table()) {
          ret = OB_ERR_INVALID_SECTION;
          LOG_WARN("invalid section for this type of Compound Trigger", K(ret));
        } 
      }
    }
  }
#undef CHECK_DUPLICATE_OR_SET_POINT
#undef LABEL_NOT_MATCH

  return ret;
}

int ObTriggerResolver::resolve_timing_point(int16_t before_or_after, int16_t stmt_or_row,
                                            ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  pl::ObPLParser pl_parser(*allocator_, session_info_->get_charsets4parser(), session_info_->get_sql_mode());
  ParseResult parse_result;
  const ObStmtNodeTree *parse_tree = NULL;
  bool is_include_old_new_in_trigger = false;
  const ObString &trigger_body = trigger_arg.trigger_info_.get_trigger_body();
  parse_result.is_for_trigger_ = 1;
  parse_result.mysql_compatible_comment_ = 0;
  parse_result.is_dynamic_sql_ = 0;
  OZ (pl_parser.parse(trigger_body, trigger_body, parse_result, true));
  CK (OB_NOT_NULL(parse_tree = parse_result.result_tree_));
  CK (T_STMT_LIST == parse_tree->type_);
  CK (1 == parse_tree->num_child_);
  CK (OB_NOT_NULL(parse_tree = parse_tree->children_[0]));
  OX (is_include_old_new_in_trigger
    = (1 == parse_result.is_include_old_new_in_trigger_) ? true : false);
  if (OB_SUCC(ret) && T_SP_PRE_STMTS == parse_tree->type_) {
    CK (OB_NOT_NULL(params_.allocator_));
    CK (OB_NOT_NULL(params_.schema_checker_));
    CK (OB_NOT_NULL(params_.session_info_));
    OZ (pl::ObPLResolver::resolve_condition_compile(
      *(params_.allocator_),
      params_.session_info_,
      params_.schema_checker_->get_schema_guard(),
      NULL, /*package_guard*/
      params_.sql_proxy_, /*sql_proxy*/
      &(trigger_arg.trigger_info_.get_package_exec_env()),
      parse_tree,
      parse_tree,
      true /*inner_parse*/,
      true /*for trigger*/,
      false /*for dynamic*/,
      &(is_include_old_new_in_trigger)
    ));
  }
  if (OB_FAIL(ret)) {
  } else if (is_include_old_new_in_trigger && T_TP_EACH_ROW != stmt_or_row) {
    ret = OB_ERR_NEW_OLD_REFERENCES;
    LOG_WARN("NEW or OLD references not allowed in table level triggers", K(ret),
             K(trigger_arg.trigger_info_.get_trigger_body()));
  } else if (T_BEFORE == before_or_after && T_TP_STATEMENT == stmt_or_row) {
    trigger_arg.trigger_info_.add_before_stmt();
  } else if (T_AFTER == before_or_after && T_TP_STATEMENT == stmt_or_row) {
    trigger_arg.trigger_info_.add_after_stmt();
  } else if (T_BEFORE == before_or_after && T_TP_EACH_ROW == stmt_or_row) {
    trigger_arg.trigger_info_.add_before_row();
  } else if (T_AFTER == before_or_after && T_TP_EACH_ROW == stmt_or_row) {
    trigger_arg.trigger_info_.add_after_row();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_node timing points is invalid", K(ret),
             K(before_or_after), K(stmt_or_row));
  }
  return ret;
}

int ObTriggerResolver::resolve_dml_event_option(const ParseNode &parse_node,
                                                ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  OV (parse_node.type_ == T_TG_DML_EVENT_OPTION, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (3 == parse_node.num_child_, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OV (OB_NOT_NULL(parse_node.children_));
  OV (OB_NOT_NULL(parse_node.children_[0]));    // dml event list.
  OV (OB_NOT_NULL(parse_node.children_[2]));    // base object name.
  if (OB_SUCC(ret) && OB_NOT_NULL(parse_node.children_[1])) {
    if (trigger_arg.trigger_info_.is_simple_dml_type()) {
      ret = OB_ERR_NESTED_TABLE_IN_TRI;
      LOG_WARN("nested table not allowed here", K(ret));
    } else if (trigger_arg.trigger_info_.is_instead_dml_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("nested table cluase not supported now", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested table cluase");
    }
  }
  OZ (resolve_schema_name(*parse_node.children_[2],
                          trigger_arg.base_object_database_, trigger_arg.base_object_name_));
  OZ (resolve_base_object(trigger_arg, NULL == parse_node.children_[2]->children_[0]));
  OZ (resolve_dml_event_list(*parse_node.children_[0], trigger_arg));
  return ret;
}

int ObTriggerResolver::resolve_reference_names(const ParseNode *parse_node,
                                               ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  ObString ref_old_name = REF_OLD;
  ObString ref_new_name = REF_NEW;
  ObString ref_parent_name = REF_PARENT;
  if (parse_node != NULL) {
    /*
     * CREATE OR REPLACE TRIGGER insert_trigger1
     * BEFORE INSERT OR UPDATE
     * ON employees
     * REFERENCING old AS old new AS new old AS old111 new AS new222
     * FOR EACH ROW
     * WHEN (NEW222.dep_id = 101)
     * BEGIN
     *   if (INSERTING and :NEW222.pk is NULL) then
     *     :NEW222.pk := 1;
     *   end if;
     * END;
     *
     * later ref name will overwrite previous name.
     */
    OV (parse_node->type_ == T_TG_REF_LIST, OB_ERR_UNEXPECTED, parse_node->type_);
    for (int32_t i = 0; OB_SUCC(ret) && i < parse_node->num_child_; i++) {
      const ParseNode *ref_node = parse_node->children_[i];
      OV (OB_NOT_NULL(ref_node), OB_ERR_UNEXPECTED, i);
      OV (ref_node->type_ == T_IDENT, OB_ERR_UNEXPECTED, ref_node->type_);
      OV (OB_NOT_NULL(ref_node->str_value_), OB_ERR_UNEXPECTED, i)
      OX (switch (ref_node->value_) {
          case T_TG_REF_OLD:
            OX (ref_old_name.assign_ptr(ref_node->str_value_, ref_node->str_len_));
            break;
          case T_TG_REF_NEW:
            OX (ref_new_name.assign_ptr(ref_node->str_value_, ref_node->str_len_));
            break;
          case T_TG_REF_PARENT:
            ret = OB_ERR_TRIGGER_INVALID_REF_NAME;
            LOG_WARN("invalid REFERENCING name", K(ret));
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("referencing type is invalid", K(ref_node->value_));
            break;
      })
    }
  }
  OZ (trigger_arg.trigger_info_.set_ref_old_name(ref_old_name));
  OZ (trigger_arg.trigger_info_.set_ref_new_name(ref_new_name));
  OZ (trigger_arg.trigger_info_.set_ref_parent_name(ref_parent_name));
  return ret;
}

int ObTriggerResolver::resolve_dml_event_list(const ParseNode &parse_node,
                                              ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  ObIAllocator *allocator = trigger_arg.trigger_info_.get_allocator();
  const ParseNode *event_node = NULL;
  OV (parse_node.type_ == T_TG_DML_EVENT_LIST, OB_ERR_UNEXPECTED, parse_node.type_);
  OV (parse_node.num_child_ > 0, OB_ERR_UNEXPECTED, parse_node.num_child_);
  OV (OB_NOT_NULL(parse_node.children_));
  OV (OB_NOT_NULL(allocator));
// update columns.
#define RESOLVE_UPDATE_COLUMN_LIST(event, tg_arg, col_arr)                                        \
{                                                                                                 \
  if (NULL != event->str_value_) {                                                                \
    if (tg_arg.trigger_info_.is_instead_dml_type() || !tg_arg.trigger_info_.has_update_event()) { \
      ret = OB_ERR_COL_LIST_IN_TRI;                                                               \
      LOG_WARN("column list not valid for instead of trigger type", K(ret));                      \
    } else {                                                                                      \
      const ObString new_columns(event->str_len_, event->str_value_);                             \
      static const char *UPDATE_OF_STR = "UPDATE OF ";                                            \
      char *buf = NULL;                                                                           \
      int64_t buf_len = new_columns.length() + STRLEN(UPDATE_OF_STR);                             \
      for (int64_t i = 0; OB_SUCC(ret) && i < event->num_child_; ++i) {                           \
        ObString col(event->children_[i]->str_value_);                                            \
        for (int64_t j = 0; OB_SUCC(ret) && j < col_arr.count(); ++j) {                           \
          if (0 == col_arr.at(j).case_compare(col)) {                                             \
            ret = OB_ERR_FIELD_SPECIFIED_TWICE;                                                   \
            LOG_WARN("duplicate column name", K(col), K(ret));                                    \
          }                                                                                       \
        }                                                                                         \
        OZ (col_arr.push_back(col));                                                              \
      }                                                                                           \
      OV (OB_NOT_NULL(buf = static_cast<char *>(allocator->alloc(buf_len))),                      \
          OB_ALLOCATE_MEMORY_FAILED, buf_len);                                                    \
      OX (MEMCPY(buf, UPDATE_OF_STR, STRLEN(UPDATE_OF_STR)));                                     \
      OX (MEMCPY(buf + buf_len - new_columns.length(), new_columns.ptr(),                         \
                 new_columns.length()));                                                          \
      OX (tg_arg.trigger_info_.assign_update_columns(buf, buf_len));                              \
    }                                                                                             \
  }                                                                                               \
}
  /*
   * CREATE OR REPLACE TRIGGER simple_trigger
   *   BEFORE INSERT OR
   *          INSERT OR
   *          UPDATE OF department_id, salary OR
   *          UPDATE OF employee_name
   *   ON employees
   *   FOR EACH ROW
   * BEGIN
   *   NULL;
   * END;
   * /
   * duplicate event is OK, just combine them.
   */
  ObArray<ObString> col_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < parse_node.num_child_; i++) {
    OV (OB_NOT_NULL(event_node = parse_node.children_[i]));
    switch (event_node->type_) {
    case T_INSERT: {
      OX (trigger_arg.trigger_info_.add_insert_event());
      RESOLVE_UPDATE_COLUMN_LIST(event_node, trigger_arg, col_array);
      break;
    }
    case T_UPDATE: {
      OX (trigger_arg.trigger_info_.add_update_event());
      RESOLVE_UPDATE_COLUMN_LIST(event_node, trigger_arg, col_array);
      break;
    }
    case T_DELETE: {
      OX (trigger_arg.trigger_info_.add_delete_event());
      RESOLVE_UPDATE_COLUMN_LIST(event_node, trigger_arg, col_array);
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("parse_node type is invalid", K(ret), K(event_node->type_));
    }
  }
  if (OB_SUCC(ret)) {
    const ObTableSchema *table_schema = NULL;
    ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
    OZ (schema_guard->get_table_schema(trigger_arg.trigger_info_.get_tenant_id(),
                                       trigger_arg.base_object_database_,
                                       trigger_arg.base_object_name_,
                                      false/*is_index*/, table_schema));
    CK (OB_NOT_NULL(table_schema));
    for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(col_array.at(i));
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_WARN("column not exist", K(ret), K(col_array.at(i)));
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, col_array.at(i).length(), col_array.at(i).ptr());
      }
    }
  }
  return ret;
}

int ObTriggerResolver::resolve_trigger_status(int16_t enable_or_disable,
                                              ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  if (enable_or_disable == T_ENABLE) {
    trigger_arg.trigger_info_.set_enable();
  } else if (enable_or_disable == T_DISABLE) {
    trigger_arg.trigger_info_.set_disable();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("parse_node timing points is invalid", K(ret), K(enable_or_disable));
  }
  return ret;
}

int ObTriggerResolver::resolve_when_condition(const ParseNode *parse_node,
                                              ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  if (NULL != parse_node) {
    ObString when_condition;
    OV (trigger_arg.trigger_info_.has_before_row_point()
        || trigger_arg.trigger_info_.has_after_row_point()
        || trigger_arg.trigger_info_.is_compound_dml_type(), OB_ERR_WHEN_CLAUSE);
    OV (parse_node->type_ == T_TG_WHEN_CONDITION, OB_ERR_UNEXPECTED, parse_node->type_);
    OV (OB_NOT_NULL(parse_node->str_value_) && parse_node->str_len_ > 0);
    OX (when_condition.assign_ptr(parse_node->str_value_, static_cast<int32_t>(parse_node->str_len_)));
    OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
          *allocator_, session_info_->get_dtc_params(), when_condition));
    OZ (trigger_arg.trigger_info_.set_when_condition(when_condition), when_condition);
  }
  return ret;
}

int ObTriggerResolver::resolve_trigger_body(const ParseNode &parse_node,
                                            ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  ObTriggerInfo &trigger_info = trigger_arg.trigger_info_;
  ObString tg_body;
  if (lib::is_oracle_mode()) {
    OV (parse_node.type_ == T_SP_BLOCK_CONTENT || parse_node.type_ == T_SP_LABELED_BLOCK);
  }
  CK (OB_NOT_NULL(session_info_));
  OV (OB_NOT_NULL(parse_node.str_value_) && parse_node.str_len_ > 0);
  OX (tg_body.assign_ptr(parse_node.str_value_,
                         static_cast<int32_t>(parse_node.str_len_)));
  OX (LOG_DEBUG("TRIGGER", K(tg_body)));
  OZ (trigger_info.gen_package_source(trigger_arg.base_object_database_,
                                      trigger_arg.base_object_name_, parse_node,
                                      session_info_->get_dtc_params()));
  if (OB_SUCC(ret) && lib::is_mysql_mode()) {
    ObString procedure_source;
    pl::ObPLParser parser(*allocator_, session_info_->get_charsets4parser(), session_info_->get_sql_mode());
    ObStmtNodeTree *parse_tree = NULL;
    CHECK_COMPATIBILITY_MODE(session_info_);
    OZ (trigger_info.gen_procedure_source(trigger_arg.base_object_database_,
                                          trigger_arg.base_object_name_,
                                          parse_node,
                                          session_info_->get_dtc_params(),
                                          procedure_source));
    OZ (parser.parse_package(procedure_source, parse_tree, session_info_->get_dtc_params(), NULL, true));
    if (OB_SUCC(ret)) {
      params_.tg_timing_event_ = static_cast<int64_t>(trigger_info.get_timing_event());
      HEAP_VAR(ObCreateProcedureResolver, resolver, params_) {
        bool saved_trigger_flag = session_info_->is_for_trigger_package();
        session_info_->set_for_trigger_package(true);
        if (OB_FAIL(resolver.resolve(*parse_tree->children_[0]))) {
          LOG_WARN("resolve trigger procedure failed", K(parse_tree->children_[0]->type_), K(ret));
        }
        //无论是否执行成功都要恢复该变量原值
        session_info_->set_for_trigger_package(saved_trigger_flag);
      }
    }
  }
  return ret;
}

int ObTriggerResolver::resolve_compound_trigger_body(const ParseNode &parse_node,
                                                     ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  ObTriggerInfo &trigger_info = trigger_arg.trigger_info_;
  CK (OB_NOT_NULL(session_info_));
  CK (T_TG_COMPOUND_BODY == parse_node.type_);
  CK (3 == parse_node.num_child_);
  if (OB_SUCC(ret) && OB_NOT_NULL(parse_node.children_[2])) {
    ObString tail_name(parse_node.children_[2]->str_len_, parse_node.children_[2]->str_value_);
    if (0 != tail_name.case_compare(trigger_arg.trigger_info_.get_trigger_name())) {
      ret = OB_ERR_END_LABEL_NOT_MATCH;
      LOG_WARN("END identifier must match START identifier", K(ret), K(tail_name));
      LOG_USER_ERROR(OB_ERR_END_LABEL_NOT_MATCH, tail_name.length(), tail_name.ptr(),
                     trigger_arg.trigger_info_.get_trigger_name().length(),
                     trigger_arg.trigger_info_.get_trigger_name().ptr());
    }
  }
  OZ (trigger_info.gen_package_source(trigger_arg.base_object_database_,
                                      trigger_arg.base_object_name_,
                                      parse_node,
                                      session_info_->get_dtc_params()));
  return ret;
}

int ObTriggerResolver::resolve_schema_name(const ParseNode &parse_node,
                                           ObString &database_name,
                                           ObString &schema_name)
{
  int ret = OB_SUCCESS;
  OV (OB_NOT_NULL(session_info_));
  OZ (ObResolverUtils::resolve_sp_name(*session_info_, parse_node, database_name, schema_name));
  return ret;
}

int ObTriggerResolver::resolve_alter_clause(const ParseNode &alter_clause,
                                            ObTriggerInfo &tg_info,
                                            const ObString &db_name,
                                            bool &is_set_status,
                                            bool &is_alter_compile)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(OB_NOT_NULL(schema_checker_)));
  CK (OB_LIKELY(OB_NOT_NULL(schema_checker_->get_schema_guard())));
  CK (OB_LIKELY(OB_NOT_NULL(allocator_)));
  CK (OB_LIKELY(T_TG_ALTER_OPTIONS == alter_clause.type_));
  if (FAILEDx(TRIGGER_ALTER_IF_EDITIONABLE == alter_clause.int16_values_[0])) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("alter editionable is not supported yet!", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter editionable");
  } else if (TRIGGER_ALTER_IF_ENABLE == alter_clause.int16_values_[0]) {
    is_set_status = true;
    is_alter_compile = false;
    if (T_ENABLE == static_cast<ObItemType>(alter_clause.int16_values_[1])) {
      tg_info.set_enable();
    } else {
      tg_info.set_disable();
    }
#ifdef OB_BUILD_ORACLE_PL
  } else if (TRIGGER_ALTER_COMPILE == alter_clause.int16_values_[0]) {
    is_set_status = false;
    is_alter_compile = true;
    if (1 == alter_clause.int16_values_[2]) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("alter trigger with reuse_setting not supported yet!", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter trigger with reuse setting");
    } else {
      share::schema::ObErrorInfo error_info;
      ObSEArray<ObDependencyInfo, 1> dep_infos;
      if (OB_FAIL(analyze_trigger(*schema_checker_->get_schema_guard(),
                                  session_info_,
                                  params_.sql_proxy_,
                                  *allocator_,
                                  tg_info,
                                  db_name,
                                  dep_infos,
                                  true))) {
        LOG_USER_WARN(OB_ERR_TRIGGER_COMPILE_ERROR, "TRIGGER",
                        db_name.length(), db_name.ptr(),
                        tg_info.get_trigger_name().length(), tg_info.get_trigger_name().ptr());
        ret = OB_SUCCESS;
        error_info.handle_error_info(&tg_info);
      } else {
        OZ (error_info.delete_error(&tg_info));
      }
    }
  } else if (TRIGGER_ALTER_RENAME == alter_clause.int16_values_[0]) {
    is_set_status = false;
    is_alter_compile = false;
    CK (1 == alter_clause.num_child_);
    CK (OB_NOT_NULL(alter_clause.children_[0]))
    OZ (resolve_rename_trigger(*alter_clause.children_[0], *schema_checker_->get_schema_guard(), tg_info, *allocator_));
#endif
  }
  return ret;
}

int ObTriggerResolver::fill_package_info(ObTriggerInfo &trigger_info)

{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t pos = 0;
  ObString pl_exec_env;
  OV (OB_NOT_NULL(session_info_));
  OZ (ObExecEnv::gen_exec_env(*session_info_, buf, OB_MAX_PROC_ENV_LENGTH, pos));
  OX (pl_exec_env.assign_ptr(buf, static_cast<int32_t>(pos)));
  OX (trigger_info.set_package_flag(0));
  OX (trigger_info.set_package_comp_flag(0));
  OZ (trigger_info.set_package_exec_env(pl_exec_env));
  OX (trigger_info.set_sql_mode(session_info_->get_sql_mode()));
  return ret;
}

int ObTriggerResolver::resolve_base_object(ObCreateTriggerArg &tg_arg,
                                           bool search_public_schema) {
  int ret = OB_SUCCESS;
  uint64_t tg_db_id = OB_INVALID_ID; 
  ObTriggerInfo &tg_info = tg_arg.trigger_info_;
  uint64_t tenant_id = tg_info.get_tenant_id();
  ObSchemaGetterGuard *schema_guard = schema_checker_->get_schema_guard();
  const ObTableSchema *table_schema = NULL;
  OV (OB_NOT_NULL(schema_guard));
  OZ (schema_checker_->get_database_id(tenant_id, tg_arg.trigger_database_, tg_db_id));
  OZ (schema_guard->get_table_schema(tenant_id, tg_arg.base_object_database_,
                                     tg_arg.base_object_name_,
                                     false/*is_index*/, table_schema));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table_schema)) {
    if (lib::is_oracle_mode()) {
      uint64_t base_db_id = OB_INVALID_ID;
      uint64_t object_db_id = OB_INVALID_ID;
      ObSynonymChecker synonym_checker;
      ObString object_db_name;
      ObString object_name;
      const ObDatabaseSchema *object_db_schema = NULL;
      bool exist = false;
      OZ (schema_checker_->get_database_id(tenant_id, tg_arg.base_object_database_, base_db_id));
      OZ (ObResolverUtils::resolve_synonym_object_recursively(*schema_checker_, synonym_checker,
                                                              tenant_id, base_db_id,
                                                              tg_arg.base_object_name_,
                                                              object_db_id, object_name, exist,
                                                              search_public_schema));
      if (OB_FAIL(ret)) {
      } else if (!exist) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("synonym not exist", K(tenant_id), K(base_db_id),
                 K(tg_arg.base_object_name_), K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id, object_db_id, object_name,
                                                        false/*is_index*/, table_schema))) {
        LOG_WARN("get table schema failed", K(object_name), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table schema is null", KP(table_schema), K(ret));
      } else {
        // oracle mode, 前面设置的base_object_database_可能不正确,此处兜底
        tg_arg.base_object_name_ = object_name;
        object_db_id = table_schema->get_database_id();
        OZ (schema_checker_->get_database_schema(tenant_id, object_db_id, object_db_schema));
        CK (OB_NOT_NULL(object_db_schema));
        OX (tg_arg.base_object_database_ = object_db_schema->get_database_name());
      }
    } else {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table or view does not exist", K(tenant_id), K(tg_db_id),
               K(tg_arg.base_object_name_), K(ret));
      LOG_MYSQL_USER_ERROR(OB_TABLE_NOT_EXIST, tg_arg.base_object_database_.ptr(),
                           tg_arg.base_object_name_.ptr());
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_BAD_TABLE;
    LOG_WARN("table schema is invalid", K(ret));
  } else if (table_schema->is_in_recyclebin()) {
    ret = OB_ERR_OPERATION_ON_RECYCLE_OBJECT;
    LOG_WARN("table is in recyclebin", K(ret));
  } else if (tg_info.is_instead_dml_type()) {
    if (!table_schema->is_user_view()) {
      ret = OB_ERR_INSTEAD_TRI_ON_TABLE;
      LOG_WARN("instead of trigger only support on user view", K(ret));
    } else if (table_schema->is_read_only()) {
      ret = OB_ERR_TRIGGER_CANT_CRT_ON_RO_VIEW;
      LOG_WARN("cannot CREATE INSTEAD OF trigger on a read-only view",
               K(table_schema->get_table_name_str()), K(ret));
    }
  } else if (tg_info.is_simple_dml_type()) {
    if (!table_schema->is_user_table()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("simple dml trigger only support on user table", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "simple dml trigger isn't used on user table");
    } else if (lib::is_mysql_mode()) {
      uint64_t trigger_id = OB_INVALID_ID;
      const ObTriggerInfo *trigger_info = NULL;
      const uint64_t tenant_id = table_schema->get_tenant_id();
      const ObIArray<uint64_t> &trigger_list = table_schema->get_trigger_list();
      if (tg_db_id != table_schema->get_database_id()) {
        ret = OB_ERR_TRIGGER_IN_WRONG_SCHEMA;
        LOG_WARN("trigger database must same as table database", K(tg_db_id),
                 K(table_schema->get_database_id()), K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < trigger_list.count(); i++) {
        OX (trigger_id = trigger_list.at(i));
        OZ (schema_guard->get_trigger_info(tenant_id, trigger_id, trigger_info), trigger_id);
        OV (OB_NOT_NULL(trigger_info), OB_ERR_UNEXPECTED, trigger_id);
      }
    }
  }
  if (OB_SUCC(ret)) {
    tg_info.set_database_id(tg_db_id);
    tg_info.set_base_object_type(table_schema->is_user_table() ? TABLE_SCHEMA : VIEW_SCHEMA);
    tg_info.set_base_object_id(table_schema->get_table_id());
  }
  return ret;
}

int ObTriggerResolver::resolve_order_clause(const ParseNode *parse_node, ObCreateTriggerArg &trigger_arg)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("resolve trigger order clause start", K(ret));
  if (OB_NOT_NULL(parse_node)) {
    bool is_oracle_mode = lib::is_oracle_mode();
    ObTriggerInfo &trg_info = trigger_arg.trigger_info_;
    OV (T_TG_ORDER == parse_node->type_ && 1 == parse_node->num_child_ && NULL != parse_node->children_[0]);
    if (OB_SUCC(ret)) {
      ObString ref_trg_db_name;
      ObString ref_trg_name;
      trg_info.set_order_type(static_cast<ObTriggerInfo::OrderType>(parse_node->value_));
      if (OB_SUCC(ret) && trg_info.is_order_precedes() && is_oracle_mode) {
        ret = OB_ERR_CANNOT_SPECIFY_PRECEDES_IN_TRG;
        LOG_WARN("cannot specify PRECEDES clause", K(ret));
      }
      OZ (resolve_schema_name(*parse_node->children_[0], ref_trg_db_name, ref_trg_name), ref_trg_db_name, ref_trg_name);
      OZ (trg_info.set_ref_trg_db_name(ref_trg_db_name));
      OZ (trg_info.set_ref_trg_name(ref_trg_name));
      if (OB_SUCC(ret) && !ref_trg_db_name.empty()) {
        const ObTriggerInfo *ref_trg_info = NULL;
        OV (OB_NOT_NULL(schema_checker_));
        OV (!ref_trg_name.empty(), OB_ERR_UNEXPECTED, ref_trg_db_name, ref_trg_name);
        if (OB_SUCC(ret) && is_oracle_mode) {
          uint64_t ref_db_id = OB_INVALID_ID;
          OZ (schema_checker_->get_database_id(trg_info.get_tenant_id(), ref_trg_db_name, ref_db_id));
          if (OB_SUCC(ret) && (trg_info.get_database_id() == ref_db_id)
              && (trg_info.get_trigger_name() == ref_trg_name)) {
            ret = OB_ERR_REF_CYCLIC_IN_TRG;
            LOG_WARN("ORA-25023: cyclic trigger dependency is not allowed", K(ret));
          }
        }
        OZ (schema_checker_->get_trigger_info(trg_info.get_tenant_id(), ref_trg_db_name, ref_trg_name, ref_trg_info));
        if (OB_FAIL(ret)) {
        } else if (NULL == ref_trg_info) {
          ret = OB_ERR_TRG_ORDER;
          LOG_WARN("ref_trg_info is NULL", K(ref_trg_db_name), K(ref_trg_name), K(ret));
          if (is_oracle_mode) {
            // example: "WEBBER"."TRI"
            ObString trg_full_name;
            uint64_t buf_len = ref_trg_db_name.length() + ref_trg_name.length() + 6;
            char buf[buf_len];
            trg_full_name.assign_buffer(buf, buf_len);
            trg_full_name.write("\"", 1);
            trg_full_name.write(ref_trg_db_name.ptr(), ref_trg_db_name.length());
            trg_full_name.write("\".\"", 3);
            trg_full_name.write(ref_trg_name.ptr(), ref_trg_name.length());
            trg_full_name.write("\"\0", 2);
            LOG_ORACLE_USER_ERROR(OB_ERR_TRG_ORDER, trg_full_name.ptr());
          } else {
            LOG_MYSQL_USER_ERROR(OB_ERR_TRG_ORDER, ref_trg_name.ptr());
          }
        } else {
          if (is_oracle_mode) {
            if (trg_info.get_base_object_id() != ref_trg_info->get_base_object_id()) {
              ret = OB_ERR_REF_ANOTHER_TABLE_IN_TRG;
              LOG_WARN("ORA-25021: cannot reference a trigger defined on another table", K(ret));
            } else if (trg_info.is_simple_dml_type() && !ref_trg_info->is_compound_dml_type()) {
              if (!(trg_info.is_row_level_before_trigger() && ref_trg_info->is_row_level_before_trigger())
                  && !(trg_info.is_row_level_after_trigger() && ref_trg_info->is_row_level_after_trigger())
                  && !(trg_info.is_stmt_level_before_trigger() && ref_trg_info->is_stmt_level_before_trigger())
                  && !(trg_info.is_stmt_level_after_trigger() && ref_trg_info->is_stmt_level_after_trigger())) {
                ret = OB_ERR_REF_TYPE_IN_TRG;
                LOG_WARN("cannot reference a trigger of a different type", K(ref_trg_db_name), K(ref_trg_name), K(ret));
              }
            }
          } else {
            if (!ObTriggerInfo::is_same_timing_event(trg_info, *ref_trg_info)
                || trg_info.get_base_object_id() != ref_trg_info->get_base_object_id()) {
              ret = OB_ERR_TRG_ORDER;
              LOG_WARN("trigger order invalid", K(ref_trg_db_name), K(ref_trg_name), K(ret));
              LOG_MYSQL_USER_ERROR(OB_ERR_TRG_ORDER, ref_trg_name.ptr());
            }
          }
        }
      }
    }
  }
  LOG_DEBUG("resolve trigger order clause end", K(ret));
  return ret;
}

int ObTriggerResolver::analyze_trigger(ObSchemaGetterGuard &schema_guard,
                                       ObSQLSessionInfo *session_info,
                                       ObMySQLProxy *sql_proxy,
                                       ObIAllocator &allocator,
                                       const ObTriggerInfo &trigger_info,
                                       const ObString &db_name,
                                       ObIArray<ObDependencyInfo> &dep_infos,
                                       bool is_alter_compile)
{
  int ret = OB_SUCCESS;
  CK (OB_LIKELY(OB_NOT_NULL(session_info)));
  CK (OB_LIKELY(OB_NOT_NULL(sql_proxy)));
  if (OB_SUCC(ret)) {
    HEAP_VARS_2((ObPLPackageAST, package_spec_ast, allocator),
                  (ObPLPackageAST, package_body_ast, allocator)) {
      ObPLPackageGuard package_guard(PACKAGE_RESV_HANDLE);
      const ObString &pkg_name = trigger_info.get_package_body_info().get_package_name();
      ObString source;
      ObPLCompiler compiler(allocator, *session_info, schema_guard, package_guard, *sql_proxy);
      const ObPackageInfo &package_spec_info = trigger_info.get_package_spec_info();
      if (!trigger_info.get_update_columns().empty()) {
        ObPLParser parser(allocator, session_info->get_charsets4parser(), session_info->get_sql_mode());
        ObStmtNodeTree *column_list = NULL;
        ParseResult parse_result;
        OZ (parser.parse(trigger_info.get_update_columns(), trigger_info.get_update_columns(), parse_result, true));
        CK (OB_NOT_NULL(parse_result.result_tree_) && 1 == parse_result.result_tree_->num_child_);
        CK (OB_NOT_NULL(column_list = parse_result.result_tree_->children_[0]));
        CK (column_list->type_ == T_TG_COLUMN_LIST);
        if (OB_SUCC(ret)) {
          const ObTableSchema *table_schema = NULL;
          OZ (schema_guard.get_table_schema(trigger_info.get_tenant_id(),
                                            trigger_info.get_base_object_id(),
                                            table_schema));
          CK (OB_NOT_NULL(table_schema));
          for (int64_t i = 0; OB_SUCC(ret) && i < column_list->num_child_; i++) {
            const ParseNode *column_node = column_list->children_[i];
            const ObColumnSchemaV2 *column_schema = NULL;
            OV (column_node != NULL);
            OV (column_node->type_ == T_IDENT, OB_ERR_UNEXPECTED, column_node->type_);
            OV (column_node->str_value_ != NULL && column_node->str_len_ > 0);
            OX (column_schema = table_schema->get_column_schema(column_node->str_value_));
            if (OB_SUCC(ret) && OB_ISNULL(column_schema)) {
              ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
              LOG_WARN("column not exist", K(ret), K(trigger_info.get_update_columns()), K(i));
              LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, (int32_t)column_node->str_len_, column_node->str_value_);
            }
          }
        }
      }
      OZ (package_spec_ast.init(db_name,
                                package_spec_info.get_package_name(),
                                PL_PACKAGE_SPEC,
                                package_spec_info.get_database_id(),
                                package_spec_info.get_package_id(),
                                package_spec_info.get_schema_version(),
                                NULL));
      OZ (ObTriggerInfo::gen_package_source(trigger_info.get_tenant_id(),
                                            trigger_info.get_trigger_spec_package_id(trigger_info.get_trigger_id()),
                                            source, true, schema_guard, allocator));
      OZ (compiler.analyze_package(source, NULL, package_spec_ast, true));
      OZ (package_body_ast.init(db_name,
                                pkg_name,
                                PL_PACKAGE_BODY,
                                trigger_info.get_package_body_info().get_database_id(),
                                trigger_info.get_package_body_info().get_package_id(),
                                trigger_info.get_package_body_info().get_schema_version(),
                                &package_spec_ast));
      OZ (ObTriggerInfo::gen_package_source(trigger_info.get_tenant_id(),
                                            trigger_info.get_trigger_body_package_id(trigger_info.get_trigger_id()),
                                            source, false, schema_guard, allocator));
      OZ (compiler.analyze_package(source,
                                   &(package_spec_ast.get_body()->get_namespace()),
                                   package_body_ast,
                                   true));
      if (OB_SUCC(ret)) {
        uint64_t data_version = 0;
        if (OB_FAIL(GET_MIN_DATA_VERSION(trigger_info.get_tenant_id(), data_version))) {
          LOG_WARN("failed to get data version", K(ret));
        } else if (data_version < DATA_VERSION_4_2_0_0) {
          // do nothing
        } else {
          OX (const_cast<ObTriggerInfo&>(trigger_info).set_analyze_flag(package_body_ast.get_analyze_flag()));
        }
      }
      if (OB_SUCC(ret) && lib::is_oracle_mode()) {
        if (is_alter_compile) {
          OZ (ObPLCompiler::update_schema_object_dep_info(package_body_ast.get_dependency_table(),
                                                          trigger_info.get_tenant_id(),
                                                          trigger_info.get_owner_id(),
                                                          trigger_info.get_trigger_id(),
                                                          trigger_info.get_schema_version(),
                                                          trigger_info.get_object_type()));
        } else {
          ObString dep_attr;
          OZ (ObDependencyInfo::collect_dep_infos(package_body_ast.get_dependency_table(),
                                                  dep_infos,
                                                  trigger_info.get_object_type(),
                                                  0, dep_attr, dep_attr));
        }
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObTriggerResolver::resolve_rename_trigger(const ParseNode &rename_clause,
                                              ObSchemaGetterGuard &schema_guard,
                                              ObTriggerInfo &trigger_info,
                                              common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (2 != rename_clause.num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num child error", K(rename_clause.num_child_));
  } else if (OB_NOT_NULL(rename_clause.children_[0])) {
    ret = OB_ERR_CMD_NOT_PROPERLY_ENDED;
    LOG_WARN("database name must null", K(ret));
  } else {
    ObString mock_db_name;
    ObString new_trg_name;
    if (OB_FAIL(resolve_schema_name(rename_clause, mock_db_name, new_trg_name))) {
      LOG_WARN("resolve schema name failed", K(new_trg_name), K(ret));
    } else if (0 == trigger_info.get_trigger_name().case_compare(new_trg_name)) {
      ret = OB_OBJ_ALREADY_EXIST;
      LOG_WARN("name is already exist", K(trigger_info.get_trigger_name()), K(new_trg_name), K(ret));
    } else {
      const ObTriggerInfo *other_trg_info = NULL;
      if (OB_FAIL(schema_guard.get_trigger_info(trigger_info.get_tenant_id(),
                                                trigger_info.get_database_id(),
                                                new_trg_name,
                                                other_trg_info))) {
        LOG_WARN("get trigger info failed", K(trigger_info.get_database_id()), K(new_trg_name), K(ret));
      } else if (OB_NOT_NULL(other_trg_info)) {
        ret = OB_OBJ_ALREADY_EXIST;
        LOG_WARN("name is already exist", KPC(other_trg_info), K(new_trg_name), K(ret));
      } else if (OB_FAIL(trigger_info.set_trigger_name(new_trg_name))) {
        LOG_WARN("set trigger name failed", K(trigger_info), K(new_trg_name), K(ret));
      } else if (OB_FAIL(ObTriggerInfo::replace_trigger_name_in_body(trigger_info, alloc, schema_guard))) {
        LOG_WARN("rebuild trigger body failed", K(trigger_info), K(ret));
      }
    }
  }
  return ret;
}
#endif

const ObString ObTriggerResolver::REF_OLD = "OLD";
const ObString ObTriggerResolver::REF_NEW = "NEW";
const ObString ObTriggerResolver::REF_PARENT = "PARENT";

} // namespace sql
} // namespace oceanbase

