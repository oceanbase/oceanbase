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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_trigger_info.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/parser/ob_parser.h"
#include "sql/ob_sql_utils.h"
#include "pl/ob_pl_resolver.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
namespace share
{
namespace schema
{

OB_SERIALIZE_MEMBER((ObTriggerInfo, ObSimpleTriggerSchema),
//                  tenant_id_,
//                  trigger_id_,
                    owner_id_,
//                  database_id_,
//                  schema_version_,
                    base_object_id_,
                    base_object_type_,
                    trigger_type_,
                    trigger_events_.bit_value_,
                    timing_points_.bit_value_,
                    trigger_flags_.bit_value_,
//                  trigger_name_,
                    update_columns_,
                    reference_names_[RT_OLD],
                    reference_names_[RT_NEW],
                    reference_names_[RT_PARENT],
                    when_condition_,
                    trigger_body_,
                    package_spec_info_,
                    package_body_info_,
                    sql_mode_,
                    priv_user_,
                    order_type_,
                    ref_trg_db_name_,
                    ref_trg_name_,
                    action_order_,
                    analyze_flag_);

ObTriggerInfo &ObTriggerInfo::operator =(const ObTriggerInfo &other)
{
  if (this != &other) {
    reset();
    int &ret = error_ret_;
    OZ (deep_copy(other));
  }
  return *this;
}

int ObTriggerInfo::assign(const ObTriggerInfo &other)
{
  int ret = OB_SUCCESS;
  this->operator=(other);
  ret = this->error_ret_;
  return ret;
}

void ObTriggerInfo::reset()
{
//tenant_id_ = OB_INVALID_TENANT_ID;
//trigger_id_ = OB_INVALID_ID;
  owner_id_ = OB_INVALID_ID;
//database_id_ = OB_INVALID_ID;
//schema_version_ = common::OB_INVALID_VERSION;;
  base_object_id_ = OB_INVALID_ID;
  base_object_type_ = OB_MAX_SCHEMA;
  trigger_type_ = TT_INVALID;
  trigger_events_.reset();
  timing_points_.reset();
  trigger_flags_.reset();
//reset_string(trigger_name_);
  reset_string(update_columns_);
  reset_string(reference_names_[RT_OLD]);
  reset_string(reference_names_[RT_NEW]);
  reset_string(reference_names_[RT_PARENT]);
  reset_string(when_condition_);
  reset_string(trigger_body_);
  // pl_flag / pl_comp_flag / pl_exec_env are reset below.
  package_spec_info_.reset();
  package_body_info_.reset();
  reset_string(priv_user_);
  sql_mode_ = 0;
  order_type_ = OT_INVALID;
  reset_string(ref_trg_db_name_);
  reset_string(ref_trg_name_);
  action_order_ = 0;
  ObSimpleTriggerSchema::reset();
  analyze_flag_ = 0;
}

bool ObTriggerInfo::is_valid_for_create() const
{
  return ObSchema::is_valid() &&
         tenant_id_ != OB_INVALID_TENANT_ID &&
         trigger_type_ != TT_INVALID &&
         trigger_events_.get_value() != 0 &&
         timing_points_.get_value() != 0 &&
         !reference_names_[RT_OLD].empty() &&
         !reference_names_[RT_NEW].empty() &&
         !reference_names_[RT_PARENT].empty() &&
         !trigger_name_.empty();
}

bool ObTriggerInfo::is_valid() const
{
  return ObSimpleTriggerSchema::is_valid() &&
//       tenant_id_ != OB_INVALID_TENANT_ID &&
         trigger_type_ != TT_INVALID &&
         trigger_events_.get_value() != 0 &&
         timing_points_.get_value() != 0 &&
         !reference_names_[RT_OLD].empty() &&
         !reference_names_[RT_NEW].empty() &&
         !reference_names_[RT_PARENT].empty();
//       !trigger_name_.empty();
}

int ObTriggerInfo::deep_copy(const ObTriggerInfo &other)
{
  int ret = OB_SUCCESS;
  OZ (ObSimpleTriggerSchema::deep_copy(other));
//OX (set_tenant_id(other.get_tenant_id()));
//OX (set_trigger_id(other.get_trigger_id()));
  OX (set_owner_id(other.get_owner_id()));
//OX (set_database_id(other.get_database_id()));
//OX (set_schema_version(other.get_schema_version()));
  OX (set_base_object_id(other.get_base_object_id()));
  OX (set_base_object_type(other.get_base_object_type()));
  OX (set_trigger_type(other.get_trigger_type()));
  OX (set_trigger_events(other.get_trigger_events()));
  OX (set_timing_points(other.get_timing_points()));
  OX (set_trigger_flags(other.get_trigger_flags()));
//OZ (set_trigger_name(other.get_trigger_name()));
  OZ (set_update_columns(other.get_update_columns()));
  OZ (set_ref_old_name(other.get_ref_old_name()));
  OZ (set_ref_new_name(other.get_ref_new_name()));
  OZ (set_ref_parent_name(other.get_ref_parent_name()));
  OZ (set_when_condition(other.get_when_condition()));
  OZ (set_trigger_body(other.get_trigger_body()));
  OZ (set_package_spec_source(other.get_package_spec_source()));
  OZ (set_package_body_source(other.get_package_body_source()));
  OX (set_package_flag(other.get_package_flag()));
  OX (set_package_comp_flag(other.get_package_comp_flag()));
  OZ (set_package_exec_env(other.get_package_exec_env()));
  OX (set_sql_mode(other.get_sql_mode()));
  OZ (set_trigger_priv_user(other.get_trigger_priv_user()));
  OX (set_order_type(other.get_order_type()));
  OZ (set_ref_trg_db_name(other.get_ref_trg_db_name()));
  OZ (set_ref_trg_name(other.get_ref_trg_name()));
  OX (set_action_order(other.get_action_order()));
  OX (set_analyze_flag(other.get_analyze_flag()));
  return ret;
}

int64_t ObTriggerInfo::get_convert_size() const
{
  int64_t convert_size = ObSimpleTriggerSchema::get_convert_size() +
                         sizeof(ObTriggerInfo) +
//                       trigger_name_.length() + 1 +
                         update_columns_.length() + 1 +
                         reference_names_[RT_OLD].length() + 1 +
                         reference_names_[RT_NEW].length() + 1 +
                         reference_names_[RT_PARENT].length() + 1 +
                         when_condition_.length() + 1 +
                         trigger_body_.length() + 1 +
                         priv_user_.length() + 1 +
                         package_spec_info_.get_convert_size() +
                         package_body_info_.get_convert_size() +
                         ref_trg_db_name_.length() + 1 +
                         ref_trg_name_.length();
  convert_size -= (sizeof(ObSimpleTriggerSchema) + sizeof(ObPackageInfo) * 2);
  return convert_size;
}

#define SPEC_BEGIN \
  "PACKAGE %c%.*s%c AS\n"
#define SPEC_CALC_WHEN \
  "FUNCTION calc_when(%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE, %.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE) RETURN BOOL;\n"
#define SPEC_BEFORE_STMT \
  "PROCEDURE before_stmt;\n"
// 在 instead of trigger 中, 第二个参数的第二个%.*s 传入的是 "IN", 其他情况传入的是 "IN OUT"
#define SPEC_BEFORE_ROW \
  "PROCEDURE before_row(:%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE, :%.*s %.*s %c%.*s%c.%c%.*s%c%%ROWTYPE);\n"
#define SPEC_AFTER_ROW \
  "PROCEDURE after_row(:%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE, :%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE);\n"
#define SPEC_AFTER_STMT \
  "PROCEDURE after_stmt;\n"
#define SPEC_END \
  "END;\n"

#define PACKAGE_SPEC_FMT \
  SPEC_BEGIN \
  SPEC_CALC_WHEN \
  SPEC_BEFORE_STMT \
  SPEC_BEFORE_ROW \
  SPEC_AFTER_ROW \
  SPEC_AFTER_STMT \
  SPEC_END

#define BODY_BEGIN \
  "PACKAGE BODY %c%.*s%c AS\n"
#define BODY_CALC_WHEN \
  "FUNCTION calc_when(%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE, %.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE) RETURN BOOL IS\n" \
  "BEGIN\n" \
  "  RETURN (%.*s);\n" \
  "END;\n"
#define BODY_BEFORE_STMT \
  "PROCEDURE before_stmt IS\n" \
  "%.*s" \
  "BEGIN\n" \
  "%.*s" \
  "END;\n"
// 在 instead of trigger 中, 第二个参数的第二个%.*s 传入的是 "IN", 其他情况传入的是 "IN OUT"
#define BODY_BEFORE_ROW \
  "PROCEDURE before_row(:%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE, :%.*s %.*s %c%.*s%c.%c%.*s%c%%ROWTYPE) IS\n" \
  "%.*s" \
  "BEGIN\n" \
  "%.*s" \
  "END;\n"
#define BODY_AFTER_ROW \
  "PROCEDURE after_row(:%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE, :%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE) IS\n" \
  "%.*s" \
  "BEGIN\n" \
  "%.*s" \
  "END;\n"
#define BODY_AFTER_STMT \
  "PROCEDURE after_stmt IS\n" \
  "%.*s" \
  "BEGIN\n" \
  "%.*s" \
  "END;\n"
#define BODY_END \
  "END;\n"

#define PACKAGE_BODY_FMT \
  BODY_BEGIN \
  BODY_CALC_WHEN \
  BODY_BEFORE_STMT \
  BODY_BEFORE_ROW \
  BODY_AFTER_ROW \
  BODY_AFTER_STMT \
  BODY_END

#define WHEN_TRUE \
  "TRUE"

#define EMPTY_BODY \
  "NULL;\n"

/************************* oracle compound trigger *************************/
#define BODY_DECLARE_COMPOUND \
"%.*s\n"
#define BODY_BEFORE_STMT_COMPOUND \
  "PROCEDURE before_stmt IS\n" \
  "%.*s;\n"
#define BODY_BEFORE_ROW_COMPOUND \
  "PROCEDURE before_row(:%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE, :%.*s %.*s %c%.*s%c.%c%.*s%c%%ROWTYPE) IS\n" \
  "%.*s;\n"
#define BODY_AFTER_ROW_COMPOUND \
  "PROCEDURE after_row(:%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE, :%.*s IN %c%.*s%c.%c%.*s%c%%ROWTYPE) IS\n" \
  "%.*s;\n"
#define BODY_AFTER_STMT_COMPOUND \
  "PROCEDURE after_stmt IS\n" \
  "%.*s;\n"

#define EMPTY_BODY_COMPOUND \
  "BEGIN\n" \
  "NULL;\n" \
  "END"

#define PACKAGE_BODY_FMT_COMPOUND \
  BODY_BEGIN \
  BODY_CALC_WHEN \
  BODY_DECLARE_COMPOUND \
  BODY_BEFORE_STMT_COMPOUND \
  BODY_BEFORE_ROW_COMPOUND \
  BODY_AFTER_ROW_COMPOUND \
  BODY_AFTER_STMT_COMPOUND \
  BODY_END
/************************* oracle compound trigger *************************/

#define MODE_DELIMITER  (lib::is_oracle_mode() ? '"' : '`')

/************************* mysql mode package *************************/
#define SPEC_BEGIN_MYSQL \
  "PACKAGE %c%.*s%c \n"
#define SPEC_BEFORE_ROW_MYSQL \
  "PROCEDURE before_row(IN OLD %c%.*s%c.%c%.*s%c%%ROWTYPE, INOUT NEW %c%.*s%c.%c%.*s%c%%ROWTYPE);\n"
#define SPEC_AFTER_ROW_MYSQL \
  "PROCEDURE after_row(IN OLD %c%.*s%c.%c%.*s%c%%ROWTYPE, IN NEW %c%.*s%c.%c%.*s%c%%ROWTYPE);\n"
#define SPEC_END_MYSQL \
  "END;\n"

#define PACKAGE_SPEC_FMT_MYSQL \
  SPEC_BEGIN_MYSQL \
  SPEC_BEFORE_ROW_MYSQL \
  SPEC_AFTER_ROW_MYSQL \
  SPEC_END_MYSQL

#define BODY_BEGIN_MYSQL \
  "PACKAGE BODY %c%.*s%c \n"
#define BODY_BEFORE_ROW_MYSQL \
  "PROCEDURE before_row(IN OLD %c%.*s%c.%c%.*s%c%%ROWTYPE, INOUT NEW %c%.*s%c.%c%.*s%c%%ROWTYPE) \n" \
  "BEGIN\n" \
  "%.*s" \
  "END;\n"
#define BODY_AFTER_ROW_MYSQL \
  "PROCEDURE after_row(IN OLD %c%.*s%c.%c%.*s%c%%ROWTYPE, IN NEW %c%.*s%c.%c%.*s%c%%ROWTYPE) \n" \
  "BEGIN\n" \
  "%.*s" \
  "END;\n"
#define BODY_END_MYSQL \
  "END;\n"

#define PACKAGE_BODY_FMT_MYSQL \
  BODY_BEGIN_MYSQL \
  BODY_BEFORE_ROW_MYSQL \
  BODY_AFTER_ROW_MYSQL \
  BODY_END_MYSQL  \
/************************* mysql mode package *************************/

/************************* mysql mode procedure *************************/
#define TRIGGER_PROCEDURE_MYSQL \
  "CREATE PROCEDURE %c%.*s%c(IN OLD %c%.*s%c.%c%.*s%c%%ROWTYPE, %.*s NEW %c%.*s%c.%c%.*s%c%%ROWTYPE) \n" \
  "%.*s \n"
/************************* mysql mode procedure *************************/

int ObTriggerInfo::gen_package_source(const uint64_t tenant_id,
                                      const uint64_t tg_package_id,
                                      common::ObString &source,
                                      bool is_header,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ParseResult parse_result;
  ParseNode *stmt_list_node = NULL;
  const ParseNode *trigger_source_node = NULL;
  const ParseNode *trigger_define_node = NULL;
  const ParseNode *trigger_body_node = NULL;
  const ObTriggerInfo *trigger_info = NULL;
  OZ (schema_guard.get_trigger_info(tenant_id, get_package_trigger_id(tg_package_id), trigger_info));
  CK (OB_NOT_NULL(trigger_info));
  if (OB_SUCC(ret)) {
    ObParser parser(alloc, trigger_info->get_sql_mode());
    OZ (parser.parse(trigger_info->get_trigger_body(), parse_result,
                     TRIGGER_MODE, false, false, true),
        trigger_info->get_trigger_body());
    // stmt list node.
    OV (OB_NOT_NULL(stmt_list_node = parse_result.result_tree_));
    OV (stmt_list_node->type_ == T_STMT_LIST, OB_ERR_UNEXPECTED, stmt_list_node->type_);
    OV (stmt_list_node->num_child_ == 1, OB_ERR_UNEXPECTED, stmt_list_node->num_child_);
    OV (OB_NOT_NULL(stmt_list_node->children_));
    // trigger source node.
    OV (OB_NOT_NULL(trigger_source_node = stmt_list_node->children_[0]));
    if (OB_FAIL(ret)){
      // do nothing
    } else {
      const ObSimpleTableSchemaV2 *table_schema = NULL;
      const ObDatabaseSchema *base_db_schema = NULL;
      ObString spec_source;
      ObString body_source;
      if (OB_SUCC(ret) && T_SP_PRE_STMTS == trigger_source_node->type_) {
        OZ (pl::ObPLResolver::resolve_condition_compile(alloc,
                                                        NULL,
                                                        &schema_guard,
                                                        NULL,
                                                        NULL,
                                                        &(trigger_info->get_package_exec_env()),
                                                        trigger_source_node,
                                                        trigger_source_node,
                                                        true /*inner_parse*/,
                                                        true /*is_trigger*/));
        CK (OB_NOT_NULL(trigger_source_node));
      }
      OV (T_TG_SOURCE == trigger_source_node->type_, trigger_source_node->type_);
      OV (OB_NOT_NULL(trigger_define_node = trigger_source_node->children_[1]));
      if (OB_FAIL(ret)) {
#ifdef OB_BUILD_ORACLE_PL
      } else if (lib::is_oracle_mode()) {
        OV (5 == trigger_define_node->num_child_);
        OV (OB_NOT_NULL(trigger_body_node = trigger_define_node->children_[4]));
#endif
      } else {
        OV (4 == trigger_define_node->num_child_);
        OV (OB_NOT_NULL(trigger_body_node = trigger_define_node->children_[3]));
      }
      OZ (schema_guard.get_simple_table_schema(tenant_id, trigger_info->get_base_object_id(), table_schema));
      CK (OB_NOT_NULL(table_schema));
      OZ (schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), base_db_schema));
      CK (OB_NOT_NULL(base_db_schema));
      if (OB_SUCC(ret) && trigger_info->is_compound_dml_type()) {
        OZ (gen_package_source_compound(*trigger_info, base_db_schema->get_database_name_str(),
                                        table_schema->get_table_name_str(), *trigger_body_node,
                                        ObDataTypeCastParams(), spec_source, body_source,
                                        alloc, is_header ? SPEC_ONLY : BODY_ONLY));
      } else {
        OZ (gen_package_source_simple(*trigger_info, base_db_schema->get_database_name_str(),
                                      table_schema->get_table_name_str(), *trigger_body_node,
                                      ObDataTypeCastParams(), spec_source, body_source,
                                      alloc, is_header ? SPEC_ONLY : BODY_ONLY));
      }
      OX (source = is_header ? spec_source : body_source);
    }
  }
  LOG_INFO("generate trigger package end", K(source), K(ret));
  return ret;
}

int ObTriggerInfo::gen_package_source(const ObString &base_object_database,
                                      const ObString &base_object_name,
                                      const ParseNode &parse_node,
                                      const ObDataTypeCastParams &dtc_params)
{

  int ret = OB_SUCCESS;
  ObString spec_source;
  ObString body_source;
  OV (OB_NOT_NULL(get_allocator()));
  if (is_compound_dml_type()) {
    OZ (gen_package_source_compound(*this, base_object_database, base_object_name,
                                    parse_node, dtc_params,
                                    spec_source, body_source, *get_allocator()));
  } else {
    OZ (gen_package_source_simple(*this, base_object_database, base_object_name,
                                  parse_node, dtc_params,
                                  spec_source, body_source, *get_allocator()));
  }
  OX (package_spec_info_.set_type(PACKAGE_TYPE));
  OX (package_spec_info_.assign_source(spec_source));
  OX (package_body_info_.set_type(PACKAGE_BODY_TYPE));
  OX (package_body_info_.assign_source(body_source));
  return ret;
}

int ObTriggerInfo::gen_package_source_simple(const ObTriggerInfo &trigger_info,
                                             const ObString &base_object_database,
                                             const ObString &base_object_name,
                                             const ParseNode &parse_node,
                                             const ObDataTypeCastParams &dtc_params,
                                             ObString &spec_source,
                                             ObString &body_source,
                                             ObIAllocator &alloc,
                                             const PackageSouceType type)
{
  int ret = OB_SUCCESS;
  const ParseNode *block_node = NULL;
  const ParseNode *declare_node = NULL;
  const ParseNode *execute_node = NULL;
  ObString *declare_str = NULL;
  ObString *execute_str = NULL;
  ObString *tg_body = NULL;
  TriggerContext trigger_ctx;
  int64_t spec_size = 0;
  int64_t body_size = 0;

  bool is_ora = lib::is_oracle_mode();
  if (is_ora) {
    if (parse_node.type_ == T_SP_LABELED_BLOCK) {
      OV (parse_node.num_child_ == 3, OB_ERR_UNEXPECTED, parse_node.num_child_);
      OV (OB_NOT_NULL(parse_node.children_));
      OV (OB_NOT_NULL(block_node = parse_node.children_[1]));
    } else if (parse_node.type_ == T_SP_BLOCK_CONTENT) {
      block_node = &parse_node;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parse_node.type is invalid", K(ret), K(parse_node.type_));
    }
    OV (block_node->type_ == T_SP_BLOCK_CONTENT, OB_ERR_UNEXPECTED, block_node->type_);
  } else {
    block_node = &parse_node;
  }
  OV (OB_NOT_NULL(block_node->children_));
  if (is_ora) {
    OV (block_node->num_child_ >= 1, OB_ERR_UNEXPECTED, block_node->num_child_);
    OX (declare_node = (block_node->num_child_ > 1) ? block_node->children_[0] : NULL);
    OX (execute_node = (block_node->num_child_ > 1) ? block_node->children_[1] : block_node->children_[0]);
    OV (OB_NOT_NULL(execute_node));
  }
  OX (trigger_ctx.dispatch_decalare_execute(trigger_info, declare_str, execute_str, tg_body));
  OX (LOG_DEBUG("TRIGGER", K(*declare_str), K(*execute_str)));
  OV (OB_NOT_NULL(declare_str) && OB_NOT_NULL(execute_str) && OB_NOT_NULL(tg_body));

  // trigger body
  if (!is_ora) {
    OV (OB_NOT_NULL(block_node->str_value_) && block_node->str_len_ > 0);
    OX (tg_body->assign_ptr(block_node->str_value_, static_cast<int32_t>(block_node->str_len_)));
    OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(alloc, dtc_params, *tg_body));
    OX (LOG_DEBUG("TRIGGER", K(*tg_body)));
  }

  // declare node is optional.
  if (declare_node != NULL) {
    OV (OB_NOT_NULL(declare_node->str_value_) && declare_node->str_len_ > 0);
    OX (declare_str->assign_ptr(declare_node->str_value_,
                                static_cast<int32_t>(declare_node->str_len_)));
    OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(alloc, dtc_params, *declare_str));
    OX (LOG_DEBUG("TRIGGER", K(*declare_str)));
  }
  //oracle execute node is necessary, mysql is optional.
  if (execute_node != NULL) {
    OV (OB_NOT_NULL(execute_node->str_value_) && execute_node->str_len_ > 0);
    OX (execute_str->assign_ptr(execute_node->str_value_,
                                static_cast<int32_t>(execute_node->str_len_)));
    OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(alloc, dtc_params, *execute_str));
    OX (LOG_DEBUG("TRIGGER", K(*execute_str)));
  }
  OX (calc_package_source_size(trigger_info, base_object_database, base_object_name, spec_size, body_size));
  if (BODY_ONLY != type) {
    OZ (fill_package_spec_source(trigger_info, base_object_database, base_object_name,
                                 spec_size, spec_source, alloc));
  }
  if (SPEC_ONLY != type) {
    OZ (fill_package_body_source(trigger_info, base_object_database, base_object_name,
                                 body_size, trigger_ctx, body_source, alloc));
  }
  OX (LOG_INFO("TRIGGER", K(spec_source), K(body_source)));
  return ret;
}

int ObTriggerInfo::gen_package_source_compound(const ObTriggerInfo &trigger_info,
                                               const ObString &base_object_database,
                                               const ObString &base_object_name,
                                               const ParseNode &parse_node,
                                               const ObDataTypeCastParams &dtc_params,
                                               ObString &spec_source,
                                               ObString &body_source,
                                               ObIAllocator &alloc,
                                               const PackageSouceType type)
{
  int ret = OB_SUCCESS;
  TriggerContext trigger_ctx;
  const ParseNode *decl_node = parse_node.children_[0];
  ObString *decl_str = &trigger_ctx.compound_declare_;
  ObString *before_stmt_str = &trigger_ctx.before_stmt_execute_;
  ObString *after_stmt_str = &trigger_ctx.after_stmt_execute_;
  ObString *before_row_str = &trigger_ctx.before_row_execute_;
  ObString *after_row_str = &trigger_ctx.after_row_execute_;
  int64_t spec_size = 0;
  int64_t body_size = 0;
  if (NULL != decl_node) {
    OV (OB_NOT_NULL(decl_node->str_value_) && decl_node->str_len_ > 0);
    OX (decl_str->assign_ptr(decl_node->str_value_, static_cast<int32_t>(decl_node->str_len_)));
    OX (LOG_DEBUG("compound trigger declare", KPC(decl_str)));
  }
  CK (OB_NOT_NULL(parse_node.children_[1]));
  for (int64_t i = 0; OB_SUCC(ret) && i < parse_node.children_[1]->num_child_; i++) {
    CK (OB_NOT_NULL(parse_node.children_[1]->children_[i]));
    if (OB_SUCC(ret)) {
      const int16_t timing = parse_node.children_[1]->children_[i]->int16_values_[0];
      const int16_t level = parse_node.children_[1]->children_[i]->int16_values_[1];
      const ParseNode *point_section = parse_node.children_[1]->children_[i]->children_[0];
      OV (OB_NOT_NULL(point_section), OB_ERR_UNEXPECTED, i);
      OV (OB_NOT_NULL(point_section->str_value_) && point_section->str_len_ > 0, OB_ERR_UNEXPECTED, i);
      if (OB_SUCC(ret)) {
        if (T_BEFORE == timing && T_TP_STATEMENT == level) {
          before_stmt_str->assign_ptr(point_section->str_value_, static_cast<int32_t>(point_section->str_len_));
        } else if (T_BEFORE == timing && T_TP_EACH_ROW == level) {
          before_row_str->assign_ptr(point_section->str_value_, static_cast<int32_t>(point_section->str_len_));
        } else if (T_AFTER == timing && T_TP_EACH_ROW == level) {
          after_row_str->assign_ptr(point_section->str_value_, static_cast<int32_t>(point_section->str_len_));
        } else if (T_AFTER == timing && T_TP_STATEMENT == level) {
          after_stmt_str->assign_ptr(point_section->str_value_, static_cast<int32_t>(point_section->str_len_));
        } else if (T_INSTEAD == timing) {
          before_row_str->assign_ptr(point_section->str_value_, static_cast<int32_t>(point_section->str_len_));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("timing point is error", K(ret));
        }
        OX (LOG_DEBUG("compound trigger point section", KPC(before_stmt_str), KPC(before_row_str),
            KPC(after_row_str), KPC(after_stmt_str)));
      }
    }
  }
  OX (calc_package_source_size(trigger_info, base_object_database, base_object_name, spec_size, body_size));
  if (BODY_ONLY != type) {
    OZ (fill_package_spec_source(trigger_info, base_object_database, base_object_name,
                                 spec_size, spec_source, alloc));
  }
  if (SPEC_ONLY != type) {
    OZ (fill_package_body_source(trigger_info, base_object_database, base_object_name,
                                 body_size, trigger_ctx, body_source, alloc));
  }
  OX (LOG_INFO("TRIGGER", K(spec_source), K(body_source)));
  return ret;
}

int ObTriggerInfo::fill_compound_declare_body(const char *body_fmt,
                                              const common::ObString &body_declare,
                                              char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OV (OB_NOT_NULL(body_fmt) && OB_NOT_NULL(buf));
  OZ (BUF_PRINTF(body_fmt, body_declare.length(), body_declare.ptr()));
  return ret;
}

void ObTriggerInfo::calc_package_source_size(const ObTriggerInfo &trigger_info,
                                             const ObString &base_object_database,
                                             const ObString &base_object_name,
                                             int64_t &spec_size, int64_t &body_size)
{
  bool is_ora = lib::is_oracle_mode();
  int64_t spec_params_size = 0;
  int64_t body_params_size = 0;
  if (is_ora) {
    // instead trigger before row 第二个参数属性为 "IN", 其他类型trigger的属性为"IN OUT"
    int64_t in_out_size = (trigger_info.is_instead_dml_type() 
                           || (trigger_info.is_compound_dml_type() && trigger_info.has_instead_row())) ? 2 : 6;
    spec_params_size = trigger_info.get_trigger_name().length() +
                       // when / before row / after row / instead row.
                       trigger_info.get_ref_old_name().length() * 3 +
                       trigger_info.get_ref_new_name().length() * 3 +
                       trigger_info.get_ref_parent_name().length() * 3 +
                       base_object_database.length() * 6 +
                       base_object_name.length() * 6 + in_out_size;
    body_params_size = spec_params_size +
                       trigger_info.get_when_condition().length() +
                       trigger_info.get_trigger_body().length() + in_out_size;

  } else {
    spec_params_size = trigger_info.get_trigger_name().length() +
                       base_object_database.length() * 4 +
                       base_object_name.length() * 4;
    body_params_size = spec_params_size + trigger_info.get_trigger_body().length();
  }
  spec_size = STRLEN(lib::is_oracle_mode() ? PACKAGE_SPEC_FMT : PACKAGE_SPEC_FMT_MYSQL)
              + spec_params_size;
  body_size = STRLEN(lib::is_oracle_mode() ? (trigger_info.is_compound_dml_type() ? PACKAGE_BODY_FMT_COMPOUND 
                                                                                  : PACKAGE_BODY_FMT) 
                                           : PACKAGE_BODY_FMT_MYSQL)
              + body_params_size;
  return;
}

int ObTriggerInfo::fill_package_spec_source(const ObTriggerInfo &trigger_info,
                                            const ObString &base_object_database,
                                            const ObString &base_object_name,
                                            const int64_t spec_size,
                                            ObString &spec_source,
                                            ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  const ObString &trigger_name = trigger_info.get_trigger_name();
  char delimiter = MODE_DELIMITER;
  char *buf = static_cast<char *>(alloc.alloc(spec_size));
  int64_t buf_len = spec_size;
  int64_t pos = 0;
  bool is_ora = lib::is_oracle_mode();
  OV (OB_NOT_NULL(buf));
  OZ (BUF_PRINTF(is_ora ? SPEC_BEGIN : SPEC_BEGIN_MYSQL,
                 delimiter, trigger_name.length(), trigger_name.ptr(), delimiter));
  if (is_ora) {
    OZ (fill_row_routine_spec(SPEC_CALC_WHEN, trigger_info,
                              base_object_database, base_object_name,
                              buf, buf_len, pos, false));
    OZ (BUF_PRINTF(SPEC_BEFORE_STMT));
  }
  OZ (fill_row_routine_spec(is_ora ? SPEC_BEFORE_ROW : SPEC_BEFORE_ROW_MYSQL,
                            trigger_info, base_object_database,
                            base_object_name, buf, buf_len, pos, true));
  OZ (fill_row_routine_spec(is_ora ? SPEC_AFTER_ROW : SPEC_AFTER_ROW_MYSQL,
                            trigger_info, base_object_database,
                            base_object_name, buf, buf_len, pos, false));
  if (is_ora) {
    OZ (BUF_PRINTF(SPEC_AFTER_STMT));
  }
  OZ (BUF_PRINTF(is_ora ? SPEC_END : SPEC_END_MYSQL));
  OX (spec_source.assign_ptr(buf, static_cast<int32_t>(pos)));
  OX (LOG_DEBUG("TRIGGER", K(spec_source)));
  return ret;
}

int ObTriggerInfo::fill_package_body_source(const ObTriggerInfo &trigger_info,
                                            const ObString &base_object_database,
                                            const ObString &base_object_name,
                                            const int64_t body_size,
                                            const TriggerContext &trigger_ctx,
                                            ObString &body_source,
                                            ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  const ObString &trigger_name = trigger_info.get_trigger_name();
  const ObString &when_condition = trigger_info.get_when_condition();
  char delimiter = MODE_DELIMITER;
  char *buf = static_cast<char *>(alloc.alloc(body_size));
  int64_t buf_len = body_size;
  int64_t pos = 0;
  bool is_ora = lib::is_oracle_mode();
  OV (OB_NOT_NULL(buf));
  OZ (BUF_PRINTF(is_ora ? BODY_BEGIN : BODY_BEGIN_MYSQL,
                 delimiter, trigger_name.length(), trigger_name.ptr(), delimiter));
  if (is_ora) {
    OZ (fill_when_routine_body(BODY_CALC_WHEN, trigger_info,
                               base_object_database, base_object_name,
                               when_condition.empty() ?
                                WHEN_TRUE : when_condition,
                               buf, buf_len, pos));
    if (OB_SUCC(ret) && trigger_info.is_compound_dml_type()) {
      OZ (fill_compound_declare_body(BODY_DECLARE_COMPOUND, trigger_ctx.compound_declare_, buf, buf_len, pos));
    }
    OZ (fill_stmt_routine_body(trigger_info, trigger_ctx, buf, buf_len, pos, true));
  }
  OZ (fill_row_routine_body(trigger_info, base_object_database, base_object_name,
                            trigger_ctx, buf, buf_len, pos, true));
  OZ (fill_row_routine_body(trigger_info, base_object_database, base_object_name,
                            trigger_ctx, buf, buf_len, pos, false));
  if (is_ora) {
    OZ (fill_stmt_routine_body(trigger_info, trigger_ctx, buf, buf_len, pos, false));
  }
  OZ (BUF_PRINTF(is_ora ? BODY_END : BODY_END_MYSQL));
  OX (body_source.assign_ptr(buf, static_cast<int32_t>(pos)));
  LOG_DEBUG("TRIGGER", K(body_source));
  return ret;
}

int ObTriggerInfo::fill_row_routine_spec(const char *spec_fmt,
                                         const ObTriggerInfo &trigger_info,
                                         const ObString &base_object_database,
                                         const ObString &base_object_name,
                                         char *buf, int64_t buf_len, int64_t &pos,
                                         const bool is_before_row)
{
  int ret = OB_SUCCESS;
  char delimiter = MODE_DELIMITER;
  OV (OB_NOT_NULL(spec_fmt));
  OV (OB_NOT_NULL(buf));
  OV (!base_object_database.empty());
  OV (!base_object_name.empty());
  if (lib::is_oracle_mode()) {
    if (!is_before_row) {
      OZ (BUF_PRINTF(spec_fmt,
                     trigger_info.get_ref_old_name().length(),
                     trigger_info.get_ref_old_name().ptr(),
                     delimiter, base_object_database.length(),
                     base_object_database.ptr(), delimiter,
                     delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                     trigger_info.get_ref_new_name().length(),
                     trigger_info.get_ref_new_name().ptr(),
                     delimiter, base_object_database.length(),
                     base_object_database.ptr(), delimiter,
                     delimiter, base_object_name.length(), base_object_name.ptr(), delimiter));
    } else {
      bool is_instead = trigger_info.is_instead_dml_type() 
                        || (trigger_info.is_compound_dml_type() && trigger_info.has_instead_row());
      OZ (BUF_PRINTF(spec_fmt,
                     trigger_info.get_ref_old_name().length(),
                     trigger_info.get_ref_old_name().ptr(),
                     delimiter, base_object_database.length(),
                     base_object_database.ptr(), delimiter,
                     delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                     trigger_info.get_ref_new_name().length(),
                     trigger_info.get_ref_new_name().ptr(),
                     is_instead ? 2 : 6,
                     is_instead ? "IN" : "IN OUT",
                     delimiter, base_object_database.length(),
                     base_object_database.ptr(), delimiter,
                     delimiter, base_object_name.length(), base_object_name.ptr(), delimiter));
    }
  } else {
    OZ (BUF_PRINTF(spec_fmt,
                   delimiter, base_object_database.length(), base_object_database.ptr(), delimiter,
                   delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                   delimiter, base_object_database.length(), base_object_database.ptr(), delimiter,
                   delimiter, base_object_name.length(), base_object_name.ptr(), delimiter));
  }
  return ret;
}

int ObTriggerInfo::fill_when_routine_body(const char *body_fmt,
                                          const ObTriggerInfo &trigger_info,
                                          const ObString &base_object_database,
                                          const ObString &base_object_name,
                                          const ObString &body_execute,
                                          char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  char delimiter = MODE_DELIMITER;
  OV (OB_NOT_NULL(body_fmt));
  OV (OB_NOT_NULL(buf));
  OV (!base_object_database.empty());
  OV (!base_object_name.empty());
  OZ (BUF_PRINTF(body_fmt,
                 trigger_info.get_ref_old_name().length(), trigger_info.get_ref_old_name().ptr(),
                 delimiter, base_object_database.length(), base_object_database.ptr(), delimiter,
                 delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                 trigger_info.get_ref_new_name().length(), trigger_info.get_ref_new_name().ptr(),
                 delimiter, base_object_database.length(), base_object_database.ptr(), delimiter,
                 delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                 body_execute.length(), body_execute.ptr()));
  return ret;
}

int ObTriggerInfo::fill_row_routine_body(const ObTriggerInfo &trigger_info,
                                         const ObString &base_object_database,
                                         const ObString &base_object_name,
                                         const TriggerContext &trigger_ctx,
                                         char *buf, int64_t buf_len, int64_t &pos,
                                         const bool is_before_row)
{
  int ret = OB_SUCCESS;
  bool is_compound_trigger = trigger_info.is_compound_dml_type();
  bool is_oracle_mode = lib::is_oracle_mode();
  char delimiter = MODE_DELIMITER;
  const char *body_fmt = NULL;
  if (is_oracle_mode && is_before_row) {
    body_fmt = is_compound_trigger ? BODY_BEFORE_ROW_COMPOUND : BODY_BEFORE_ROW;
  } else if (is_oracle_mode) {
    body_fmt = is_compound_trigger ? BODY_AFTER_ROW_COMPOUND : BODY_AFTER_ROW;
  } else {
    body_fmt = is_before_row ? BODY_BEFORE_ROW_MYSQL : BODY_AFTER_ROW_MYSQL;
  }
  OV (OB_NOT_NULL(body_fmt));
  OV (OB_NOT_NULL(buf));
  OV (!base_object_database.empty());
  OV (!base_object_name.empty());
  if (OB_FAIL(ret)) {

  } else if (is_oracle_mode) {
    const char *empty_body = is_compound_trigger ? EMPTY_BODY_COMPOUND : EMPTY_BODY;
    const ObString &body_execute = is_before_row ? (trigger_ctx.before_row_execute_.empty() ?
                                                    empty_body : trigger_ctx.before_row_execute_)
                                                 : (trigger_ctx.after_row_execute_.empty() ?
                                                    empty_body : trigger_ctx.after_row_execute_);
    if (!is_before_row) {
      if (is_compound_trigger) {
        OZ (BUF_PRINTF(body_fmt,
                       trigger_info.get_ref_old_name().length(),
                       trigger_info.get_ref_old_name().ptr(),
                       delimiter, base_object_database.length(),
                       base_object_database.ptr(), delimiter,
                       delimiter, base_object_name.length(),
                       base_object_name.ptr(), delimiter,
                       trigger_info.get_ref_new_name().length(),
                       trigger_info.get_ref_new_name().ptr(),
                       delimiter, base_object_database.length(),
                       base_object_database.ptr(), delimiter,
                       delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                       body_execute.length(), body_execute.ptr()));
      } else {
        OZ (BUF_PRINTF(body_fmt,
                       trigger_info.get_ref_old_name().length(),
                       trigger_info.get_ref_old_name().ptr(),
                       delimiter, base_object_database.length(),
                       base_object_database.ptr(), delimiter,
                       delimiter, base_object_name.length(),
                       base_object_name.ptr(), delimiter,
                       trigger_info.get_ref_new_name().length(),
                       trigger_info.get_ref_new_name().ptr(),
                       delimiter, base_object_database.length(),
                       base_object_database.ptr(), delimiter,
                       delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                       trigger_ctx.after_row_declare_.length(), trigger_ctx.after_row_declare_.ptr(),
                       body_execute.length(), body_execute.ptr()));
      }
    } else {
      bool is_instead = trigger_info.is_instead_dml_type() 
                        || (trigger_info.is_compound_dml_type() && trigger_info.has_instead_row());
      if (is_compound_trigger) {
        OZ (BUF_PRINTF(body_fmt,
                       trigger_info.get_ref_old_name().length(),
                       trigger_info.get_ref_old_name().ptr(),
                       delimiter, base_object_database.length(),
                       base_object_database.ptr(), delimiter,
                       delimiter, base_object_name.length(),
                       base_object_name.ptr(), delimiter,
                       trigger_info.get_ref_new_name().length(),
                       trigger_info.get_ref_new_name().ptr(),
                       is_instead ? 2 : 6,
                       is_instead ? "IN" : "IN OUT",
                       delimiter, base_object_database.length(),
                       base_object_database.ptr(), delimiter,
                       delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                       body_execute.length(), body_execute.ptr()));
      } else {
        OZ (BUF_PRINTF(body_fmt,
                       trigger_info.get_ref_old_name().length(),
                       trigger_info.get_ref_old_name().ptr(),
                       delimiter, base_object_database.length(),
                       base_object_database.ptr(), delimiter,
                       delimiter, base_object_name.length(),
                       base_object_name.ptr(), delimiter,
                       trigger_info.get_ref_new_name().length(),
                       trigger_info.get_ref_new_name().ptr(),
                       is_instead ? 2 : 6,
                       is_instead ? "IN" : "IN OUT",
                       delimiter, base_object_database.length(),
                       base_object_database.ptr(), delimiter,
                       delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                       trigger_ctx.before_row_declare_.length(), trigger_ctx.before_row_declare_.ptr(),
                       body_execute.length(), body_execute.ptr()));
      }
    }
  } else {
    const ObString &tg_body = is_before_row ? (trigger_info.has_before_row_point() ? trigger_ctx.trigger_body_ : "")
                                            : (trigger_info.has_after_row_point() ? trigger_ctx.trigger_body_ : "");
    OZ (BUF_PRINTF(body_fmt,
                   delimiter, base_object_database.length(), base_object_database.ptr(), delimiter,
                   delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                   delimiter, base_object_database.length(), base_object_database.ptr(), delimiter,
                   delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                   tg_body.length(), tg_body.ptr()));
  }
  return ret;
}

int ObTriggerInfo::fill_stmt_routine_body(const ObTriggerInfo &trigger_info,
                                          const TriggerContext &trigger_ctx,
                                          char *buf, int64_t buf_len, int64_t &pos,
                                          const bool is_before)
{
  int ret = OB_SUCCESS;
  bool is_compound_trigger = trigger_info.is_compound_dml_type();
  const char *body_fmt = is_before ? (is_compound_trigger ? BODY_BEFORE_STMT_COMPOUND : BODY_BEFORE_STMT)
                                   : (is_compound_trigger ? BODY_AFTER_STMT_COMPOUND : BODY_AFTER_STMT);
  const char *empty_body = is_compound_trigger ? EMPTY_BODY_COMPOUND : EMPTY_BODY;
  const ObString &body_execute = is_before ? (trigger_ctx.before_stmt_execute_.empty() 
                                              ? empty_body : trigger_ctx.before_stmt_execute_)
                                           : (trigger_ctx.after_stmt_execute_.empty() 
                                              ? empty_body : trigger_ctx.after_stmt_execute_);
  const ObString &body_declare = is_before ? trigger_ctx.before_stmt_declare_ : trigger_ctx.after_stmt_declare_;
  OV (OB_NOT_NULL(body_fmt) && OB_NOT_NULL(buf));
  if (is_compound_trigger) {
    OZ (BUF_PRINTF(body_fmt, body_execute.length(), body_execute.ptr()));
  } else {
    OZ (BUF_PRINTF(body_fmt, body_declare.length(), body_declare.ptr(), body_execute.length(), body_execute.ptr()));
  }
  return ret;
}

void ObTriggerInfo::TriggerContext::dispatch_decalare_execute(const ObTriggerInfo &trigger_info,
                                                              ObString *&simple_declare,
                                                              ObString *&simple_execute,
                                                              ObString *&tg_body)
{
  if (trigger_info.has_before_stmt_point()) {
    simple_declare = &before_stmt_declare_;
    simple_execute = &before_stmt_execute_;
  } else if (trigger_info.has_before_row_point()) {
    simple_declare = &before_row_declare_;
    simple_execute = &before_row_execute_;
  } else if (trigger_info.has_after_row_point()) {
    simple_declare = &after_row_declare_;
    simple_execute = &after_row_execute_;
  } else if (trigger_info.has_after_stmt_point()) {
    simple_declare = &after_stmt_declare_;
    simple_execute = &after_stmt_execute_;
  }
  tg_body = &trigger_body_;
}

int ObTriggerInfo::gen_procedure_source(const common::ObString &base_object_database,
                                        const common::ObString &base_object_name,
                                        const ParseNode &parse_node,
                                        const ObDataTypeCastParams &dtc_params,
                                        ObString &procedure_source)
{
  int ret = OB_SUCCESS;
  ObString proc_source;
  int64_t proc_size = 0;
  int64_t proc_params_size;
  ObString tg_body;
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  char delimiter = MODE_DELIMITER;
  ObIAllocator *alloc = get_allocator();
  int32_t param_new_inout_len = has_after_row_point() ? 2 : 5; // IN or INOUT
  OV (lib::is_mysql_mode() && OB_NOT_NULL(alloc));
  OV (OB_NOT_NULL(parse_node.str_value_) && parse_node.str_len_ > 0);
  OX (tg_body.assign_ptr(parse_node.str_value_, static_cast<int32_t>(parse_node.str_len_)));
  // OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(*alloc, dtc_params, tg_body));
  if (OB_SUCC(ret)) {
    proc_params_size = get_trigger_name().length() +
                       base_object_database.length() * 2 +
                       base_object_name.length() * 2 +
                       param_new_inout_len;
    proc_size = proc_params_size + tg_body.length() + STRLEN(TRIGGER_PROCEDURE_MYSQL);
    buf = static_cast<char *>(alloc->alloc(proc_size));
    buf_len = proc_size;
    OV (OB_NOT_NULL(buf));
    OZ (BUF_PRINTF(TRIGGER_PROCEDURE_MYSQL,
                   delimiter, get_trigger_name().length(), get_trigger_name().ptr(), delimiter,
                   delimiter, base_object_database.length(), base_object_database.ptr(), delimiter,
                   delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                   param_new_inout_len, has_after_row_point() ? "IN" : "INOUT",
                   delimiter, base_object_database.length(), base_object_database.ptr(), delimiter,
                   delimiter, base_object_name.length(), base_object_name.ptr(), delimiter,
                   tg_body.length(), tg_body.ptr()));
    OX (procedure_source.assign_ptr(buf, static_cast<int32_t>(pos)));
    LOG_DEBUG("TRIGGER PROCEDURE", K(procedure_source));
  }
  return ret;
}

bool ObTriggerInfo::ActionOrderComparator::operator()(const ObTriggerInfo *left, const ObTriggerInfo *right)
{
  bool bool_ret = false;
  if (OB_UNLIKELY(OB_SUCCESS != ret_)) {
    // ignore
  } else if (OB_UNLIKELY(NULL == left) || OB_UNLIKELY(NULL == right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_,   "invalid argument", K(ret_), KP(left), KP(right));
  } else {
    bool_ret = (left->get_action_order() < right->get_action_order());
  }
  return bool_ret;
}

#ifdef OB_BUILD_ORACLE_PL
// for alter trigger rename
int ObTriggerInfo::replace_trigger_name_in_body(ObTriggerInfo &trigger_info,
                                                common::ObIAllocator &alloc,
                                                ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObParser parser(alloc, trigger_info.get_sql_mode());
  ParseResult parse_result;
  ParseNode *stmt_list_node = NULL;
  const ParseNode *trigger_source_node = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  OZ (schema_guard.get_database_schema(trigger_info.get_tenant_id(), trigger_info.get_database_id(), db_schema));
  OV (OB_NOT_NULL(db_schema));
  OZ (parser.parse(trigger_info.get_trigger_body(), parse_result, TRIGGER_MODE,
                   false, false, true),
      trigger_info.get_trigger_body());
  // stmt list node
  OV (OB_NOT_NULL(stmt_list_node = parse_result.result_tree_));
  OV (stmt_list_node->type_ == T_STMT_LIST, OB_ERR_UNEXPECTED, stmt_list_node->type_);
  OV (stmt_list_node->num_child_ == 1, OB_ERR_UNEXPECTED, stmt_list_node->num_child_);
  OV (OB_NOT_NULL(stmt_list_node->children_));
  // trigger source node
  OV (OB_NOT_NULL(trigger_source_node = stmt_list_node->children_[0]));
  if (OB_SUCC(ret)) {
#define RENAME_TRIGGER_FMT \
  "TRIGGER \"%.*s\".\"%.*s\" %.*s"
    buf_len = STRLEN(RENAME_TRIGGER_FMT) + db_schema->get_database_name_str().length()
              + trigger_info.get_trigger_name().length() + trigger_source_node->str_len_;
    buf = static_cast<char*>(alloc.alloc(buf_len));
    OV (OB_NOT_NULL(buf), OB_ALLOCATE_MEMORY_FAILED);
    OZ (BUF_PRINTF(RENAME_TRIGGER_FMT,
                   db_schema->get_database_name_str().length(),
                   db_schema->get_database_name_str().ptr(),
                   trigger_info.get_trigger_name().length(),
                   trigger_info.get_trigger_name().ptr(),
                   (int)trigger_source_node->str_len_,
                   trigger_source_node->str_value_));
    OZ (trigger_info.set_trigger_body(ObString(buf)));
#undef RENAME_TRIGGER_FMT
  }
  return ret;
}
#endif

// for rebuild trigger body due to rename table
int ObTriggerInfo::replace_table_name_in_body(ObTriggerInfo &trigger_info,
                                              common::ObIAllocator &alloc,
                                              const common::ObString &base_object_database,
                                              const common::ObString &base_object_name,
                                              bool is_oracle_mode)
{
  UNUSED(base_object_database);
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObParser parser(alloc, trigger_info.get_sql_mode());
  ParseResult parse_result;
  ParseNode *stmt_list_node = NULL;
  const ParseNode *trg_source_node = NULL;

  const ParseNode *trg_def_node = NULL;
  const ParseNode *dml_event_node = NULL;
  const ParseNode *base_schema_node = NULL;
  const ParseNode *base_object_node = NULL;

  OZ (parser.parse(trigger_info.get_trigger_body(), parse_result, TRIGGER_MODE,
                   false, false, true),
      trigger_info.get_trigger_body());
  // stmt list node
  OV (OB_NOT_NULL(stmt_list_node = parse_result.result_tree_));
  OV (stmt_list_node->type_ == T_STMT_LIST, OB_ERR_UNEXPECTED, stmt_list_node->type_);
  OV (stmt_list_node->num_child_ == 1, OB_ERR_UNEXPECTED, stmt_list_node->num_child_);
  OV (OB_NOT_NULL(stmt_list_node->children_));
  // trigger source node
  OV (OB_NOT_NULL(trg_source_node = stmt_list_node->children_[0]));

  OV (2 == trg_source_node->num_child_);
  OV (OB_NOT_NULL(trg_def_node = trg_source_node->children_[1]));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (is_oracle_mode) {
    OV (5 == trg_def_node->num_child_);
    OV (OB_NOT_NULL(dml_event_node = trg_def_node->children_[0]));
    OV (3 == dml_event_node->num_child_);
    OV (OB_NOT_NULL(base_schema_node = dml_event_node->children_[2]));
  } else {
    OV (4 == trg_def_node->num_child_);
    OV (OB_NOT_NULL(base_schema_node = trg_def_node->children_[1]));
  }
  OV (2 == base_schema_node->num_child_);
  OV (OB_NOT_NULL(base_object_node = base_schema_node->children_[1]));

  if (OB_SUCC(ret)) {
    buf_len = trg_def_node->str_len_ - base_object_node->str_len_ + base_object_name.length() + 3;
    buf = static_cast<char*>(alloc.alloc(buf_len));
    bool has_delimiter_already = false;
    int trg_header_len = (int)base_object_node->pl_str_off_;
    const char *trg_tail_str = (trg_def_node->str_value_ + base_object_node->pl_str_off_ + base_object_node->str_len_);
    if (is_oracle_mode) {
      // '\"' is included in base_object_node->str_value_ in oracle mode,
      // but is not included in base_object_node->str_len_
      has_delimiter_already = ('\"' == trg_def_node->str_value_[base_object_node->pl_str_off_]);
    } else {
      has_delimiter_already = ('`' == trg_def_node->str_value_[base_object_node->pl_str_off_]);
    }
    if (has_delimiter_already) {
      // base object database
      if (NULL == base_schema_node->children_[0] && !is_oracle_mode) {
        trg_header_len = trg_header_len - 1;
      }
      trg_tail_str = trg_tail_str + 2;
    }
    OV (OB_NOT_NULL(buf), OB_ALLOCATE_MEMORY_FAILED);
    OZ (BUF_PRINTF(is_oracle_mode ? "%.*s\"%.*s\"%.*s" : "%.*s`%.*s`%.*s",
                   trg_header_len,
                   trg_def_node->str_value_,
                   base_object_name.length(),
                   base_object_name.ptr(),
                   int(trg_def_node->str_len_ - (base_object_node->pl_str_off_ + base_object_node->str_len_)),
                   trg_tail_str));
    OZ (trigger_info.set_trigger_body(ObString(buf)));
  }
  LOG_INFO("rebuild trigger body end", K(trigger_info), K(base_object_name), K(lbt()), K(ret));
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
