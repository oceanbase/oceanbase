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

#define USING_LOG_PREFIX PL

#include "pl/ob_pl_package.h"
#include "pl/ob_pl_exception_handling.h"
#include "lib/oblog/ob_log_module.h"
#include "share/schema/ob_package_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/plan_cache/ob_cache_object_factory.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share::schema;

namespace pl
{

int ObPLPackageAST::init(const ObString &db_name,
                         const ObString &package_name,
                         ObPackageType package_type,
                         uint64_t database_id,
                         uint64_t package_id,
                         int64_t package_version,
                         ObPLPackageAST *parent_package_ast)
{
  int ret = OB_SUCCESS;
  const ObPLUserTypeTable *parent_user_type_table = NULL;
  ObPLRoutineTable *parent_routine_table = NULL;
  ObPLConditionTable *parent_condition_table = NULL;
  db_name_ = db_name;
  name_ = package_name;
  id_ = package_id;
  database_id_ = database_id;
  package_type_ = package_type;
  version_ = package_version;
  if (OB_NOT_NULL(parent_package_ast)) {
    compile_flag_ = parent_package_ast->get_compile_flag();
    serially_reusable_ = parent_package_ast->get_serially_reusable();
    parent_user_type_table = &parent_package_ast->get_user_type_table();
    parent_routine_table = &parent_package_ast->get_routine_table();
    parent_condition_table = &parent_package_ast->get_condition_table();
  }
  if (OB_NOT_NULL(parent_user_type_table)) {
    user_type_table_.set_type_start_gen_id(parent_user_type_table->get_type_start_gen_id());
  }
  if (OB_FAIL(routine_table_.init(parent_routine_table))) {
    LOG_WARN("routine info table init failed", K(ret));
  }
  if (OB_NOT_NULL(parent_condition_table)) {
    OZ (condition_table_.init(*parent_condition_table));
  }

  if (OB_SUCC(ret)
      && parent_package_ast != NULL
      && !ObTriggerInfo::is_trigger_package_id(package_id)
      && PL_PACKAGE_BODY == package_type) {
    ObSchemaObjVersion obj_version;
    obj_version.object_id_ = parent_package_ast->get_id();
    obj_version.version_ = parent_package_ast->get_version();
    obj_version.object_type_ = DEPENDENCY_PACKAGE;
    if (OB_FAIL(add_dependency_object(obj_version))) {
      LOG_WARN("add dependency table failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSchemaObjVersion obj_version;
    if (ObTriggerInfo::is_trigger_package_id(package_id)) {
      obj_version.object_id_ = ObTriggerInfo::get_package_trigger_id(package_id);
      obj_version.object_type_ = DEPENDENCY_TRIGGER;
    } else if (PL_UDT_OBJECT_SPEC == package_type || PL_UDT_OBJECT_BODY == package_type) {
      obj_version.object_id_ = package_id;
      obj_version.object_type_
        = PL_UDT_OBJECT_SPEC  == package_type ? DEPENDENCY_TYPE : DEPENDENCY_TYPE_BODY;
    } else {
      obj_version.object_id_ = package_id;
      obj_version.object_type_ = (PL_PACKAGE_SPEC == package_type) ? DEPENDENCY_PACKAGE : DEPENDENCY_PACKAGE_BODY;
    }
    obj_version.version_ = package_version;
    if (OB_FAIL(add_dependency_object(obj_version))) {
      LOG_WARN("add dependency table failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

int ObPLPackageAST::process_generic_type()
{
  int ret = OB_SUCCESS;
  static const char *generic_type_table[PL_GENERIC_MAX] = {
    "", // INVALID

    "<ADT_1>",
    "<RECORD_1>",
    "<TUPLE_1>",
    "<VARRAY_1>",
    "<V2_TABLE_1>",
    "<TABLE_1>",
    "<COLLECTION_1>",
    "<REF_CURSOR_1>",

    "<TYPED_TABLE>",
    "<ADT_WITH_OID>",
    " SYS$INT_V2TABLE",
    " SYS$BULK_ERROR_RECORD",
    " SYS$REC_V2TABLE",
    "<ASSOC_ARRAY_1>"
  };
  if (0 == get_name().case_compare("STANDARD")
      && 0 == get_db_name().case_compare(OB_SYS_DATABASE_NAME)) {
    const ObPLUserTypeTable &user_type_table = get_user_type_table();
    const ObUserDefinedType *user_type = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < user_type_table.get_count(); ++i) {
      if (OB_NOT_NULL(user_type = user_type_table.get_type(i))) {
        for (int64_t j = 0; OB_SUCC(ret) && j < PL_GENERIC_MAX; ++j) {
          if (0 == user_type->get_name().case_compare(generic_type_table[j])) {
            (const_cast<ObUserDefinedType *>(user_type))
              ->set_generic_type(static_cast<ObPLGenericType>(j));
          }
        }
      }
    }
  }
  return ret;
}

ObPLPackage::~ObPLPackage()
{
  for (int64_t i = 0; i < var_table_.count(); ++i) {
    if (OB_NOT_NULL(var_table_.at(i))) {
      var_table_.at(i)->~ObPLVar();
    }
  }
  for (int64_t i = 0; i < type_table_.count(); ++i) {
    if (OB_NOT_NULL(type_table_.at(i))) {
      type_table_.at(i)->~ObUserDefinedType();
    }
  }
}

int ObPLPackage::init(const ObPLPackageAST &package_ast)
{
  int ret = OB_SUCCESS;
  database_id_ = package_ast.get_database_id();
  id_ = package_ast.get_id();
  version_ = package_ast.get_version();
  package_type_ = package_ast.get_package_type();
  serially_reusable_ = package_ast.get_serially_reusable();
  if (OB_FAIL(ob_write_string(get_allocator(), const_cast<ObString &>(package_ast.get_db_name()), db_name_))) {
    LOG_WARN("copy db name failed", "db name", package_ast.get_db_name(), K(ret));
  } else if (OB_FAIL(ob_write_string(get_allocator(), const_cast<ObString &>(package_ast.get_name()), name_))) {
    LOG_WARN("copy package name failed", "package name", package_ast.get_name(), K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObPLPackage::instantiate_package_state(const ObPLResolveCtx &resolve_ctx,
                                           ObExecContext &exec_ctx,
                                           ObPLPackageState &package_state)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObObj value;
  ARRAY_FOREACH(var_table_, var_idx) {
    const ObPLVar *var = var_table_.at(var_idx);
    const ObPLDataType &var_type = var->get_type();
    const ObObj *ser_value = NULL;
    key.reset();
    value.reset();
    if (OB_ISNULL(var)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("var is null", K(ret), K(var), K(var_idx));
    } else {
      if (var_type.is_cursor_type()
          && OB_FAIL(resolve_ctx.session_info_.init_cursor_cache())) {
        LOG_WARN("failed to init cursor cache", K(ret));
      } else if (OB_FAIL(var_type.init_session_var(resolve_ctx,
                                            var_type.is_cursor_type() ?
                                              package_state.get_pkg_cursor_allocator()
                                              : package_state.get_pkg_allocator(),
                                            exec_ctx,
                                            var->is_formal_param() ? NULL : get_default_expr(var->get_default()),
                                            var->is_default_construct(),
                                            value))) {
        LOG_WARN("init sesssion var failed", K(ret));
      } else if (value.is_null_oracle() && var_type.is_not_null()) {
        ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
        LOG_WARN("cannot assign null to var with not null attribution", K(ret));
      } else if (OB_FAIL(package_state.make_pkg_var_kv_key(resolve_ctx.allocator_, var_idx, VARIABLE, key))) {
        LOG_WARN("make package var name failed", K(ret));
      } else if (OB_NOT_NULL(ser_value = resolve_ctx.session_info_.get_user_variable_value(key))) {
        if (package_state.get_serially_reusable()) {
          // do not sync serially package variable !
        } else {
          if (var_type.is_cursor_type()) {
            OV (ser_value->is_tinyint() || ser_value->is_number(),
                OB_ERR_UNEXPECTED, KPC(ser_value), K(lbt()));
            if (OB_SUCC(ret)
                && ser_value->is_tinyint() ? ser_value->get_bool() : !ser_value->is_zero_number()) {
              ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo *>(value.get_ext());
              CK (OB_NOT_NULL(cursor));
              OX (cursor->set_sync_cursor());
            }
          } else {
            // sync other server modify for this server! (from porxy or distribute plan)
            OZ (var_type.deserialize(resolve_ctx,
                                    var_type.is_cursor_type() ?
                                      package_state.get_pkg_cursor_allocator()
                                      : package_state.get_pkg_allocator(),
                                    ser_value->get_hex_string().ptr(),
                                    ser_value->get_hex_string().length(),
                                    value));
          }
          // record sync variable, avoid to sync tiwce!
          if (OB_NOT_NULL(resolve_ctx.session_info_.get_pl_sync_pkg_vars())) {
            OZ (resolve_ctx.session_info_.get_pl_sync_pkg_vars()->set_refactored(key));
          }
        }
      }
      //NOTE: do not remove package user variable! distribute plan will sync it to remote if needed!
      OZ (package_state.add_package_var_val(value, var_type.get_type()));
    }
  }
  if (OB_SUCC(ret) && !resolve_ctx.is_sync_package_var_) {
    if (OB_FAIL(execute_init_routine(resolve_ctx.allocator_, exec_ctx))) {
      LOG_WARN("execute init routine failed", K(ret));
    }
  }
  return ret;
}

int ObPLPackage::execute_init_routine(ObIAllocator &allocator, ObExecContext &exec_ctx)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  ObPLFunction *init_routine = routine_table_.at(ObPLRoutineTable::INIT_ROUTINE_IDX);
  if (OB_NOT_NULL(init_routine)) {
    pl::ObPL *pl_engine = NULL;
    CK (OB_NOT_NULL(exec_ctx.get_my_session()));
    CK (OB_NOT_NULL(pl_engine = exec_ctx.get_my_session()->get_pl_engine()));

    if (OB_SUCC(ret)) {
      ParamStore params;
      ObSEArray<int64_t, 2> nocopy_param;
      ObObj result;
      int status;
      ObSEArray<int64_t, 2> subp_path;
      OZ (pl_engine->execute(exec_ctx,
                             exec_ctx.get_allocator(),
                             init_routine->get_package_id(),
                             init_routine->get_routine_id(),
                             subp_path,
                             params,
                             nocopy_param,
                             result,
                             &status,
                             false,
                             init_routine->is_function()));
    }
  } else {
    LOG_DEBUG("package init routine function is null",
              K(init_routine), K(get_name()), K(get_db_name()),
              K(get_id()), K(get_version()), K(get_package_type()));
  }
  return ret;
}


int ObPLPackage::add_var(ObPLVar *var)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(var_table_.push_back(var))) {
    LOG_WARN("add symbol table failed", K(ret));
  }
  return ret;
}

int ObPLPackage::get_var(const ObString &var_name, const ObPLVar *&var, int64_t &var_idx) const
{
  int ret = OB_SUCCESS;
  var = NULL;
  var_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_ISNULL(var) && i < var_table_.count(); ++i) {
    ObPLVar *tmp_var = var_table_.at(i);
    if (!tmp_var->is_formal_param()
        && ObCharset::case_insensitive_equal(var_name, tmp_var->get_name())) {
      if (tmp_var->is_dup_declare()) {
        ret = OB_ERR_DECL_MORE_THAN_ONCE;
        LOG_WARN("package var dup", K(ret), K(var_idx));
        LOG_USER_ERROR(OB_ERR_DECL_MORE_THAN_ONCE, tmp_var->get_name().length(), tmp_var->get_name().ptr());
      } else {
        var = tmp_var;
        var_idx = i;
      }
    }
  }
  return ret;
}

int ObPLPackage::get_var(int64_t var_idx, const ObPLVar *&var) const
{
  int ret = OB_SUCCESS;
  var = NULL;
  if (var_idx < 0 || var_idx >= var_table_.count()) {
     LOG_WARN("var index invalid", K(var_idx), K(ret));
  } else {
    var = var_table_.at(var_idx);
  }
  return ret;
}

int ObPLPackage::add_condition(ObPLCondition *value)
{
  int ret = OB_SUCCESS;
  OZ (condition_table_.push_back(value));
  return ret;
}

int ObPLPackage::get_condition(const ObString &condition_name, const ObPLCondition *&value)
{
  int ret = OB_SUCCESS;
  value = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < condition_table_.count(); ++i) {
    const ObPLCondition *tmp = condition_table_.at(i);
    if (OB_ISNULL(tmp)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition is null", K(ret), K(tmp));
    } else if (ObCharset::case_insensitive_equal(condition_name, tmp->get_name())) {
      value = tmp;
      break;
    }
  }
  return ret;
}

int ObPLPackage::get_cursor(int64_t cursor_idx, const ObPLCursor *&cursor) const
{
  int ret = OB_SUCCESS;
  cursor = NULL;
  if (cursor_table_.get_count() > cursor_idx && cursor_idx >= 0) {
    cursor = cursor_table_.get_cursor(cursor_idx);
    CK (OB_NOT_NULL(cursor));
  }
  return ret;
}

int ObPLPackage::get_cursor(
  int64_t package_id, int64_t routine_id, int64_t index, const ObPLCursor *&cursor) const
{
  int ret = OB_SUCCESS;
  cursor = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < cursor_table_.get_count(); ++i) {
    const ObPLCursor *it = cursor_table_.get_cursor(i);
    CK (OB_NOT_NULL(it));
    if (it->get_package_id() == package_id
        && it->get_routine_id() == routine_id
        && it->get_index() == index) {
      cursor = it;
      break;
    }
  }
  return ret;
}

int ObPLPackage::get_cursor(
  const ObString &cursor_name, const ObPLCursor *&cursor, int64_t &cursor_idx) const
{
  int ret = OB_SUCCESS;
  cursor = NULL;
  cursor_idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < cursor_table_.get_count(); ++i) {
    const ObPLCursor *it = cursor_table_.get_cursor(i);
    const ObPLVar *v = NULL;
    CK (OB_NOT_NULL(it));
    CK (OB_NOT_NULL(v = get_var_table().at(it->get_index())));
    if (OB_SUCC(ret) && 0 == v->get_name().case_compare(cursor_name)) {
      cursor = it;
      cursor_idx = i;
      break;
    }
  }
  return ret;
}

int ObPLPackage::add_type(ObUserDefinedType *type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(type_table_.push_back(type))) {
    LOG_WARN("add user type table failed", K(ret));
  }
  return ret;
}

int ObPLPackage::get_type(const common::ObString type_name, const ObUserDefinedType *&type) const
{
  int ret = OB_SUCCESS;
  type = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < type_table_.count(); ++i) {
    const ObUserDefinedType *tmp_type = type_table_.at(i);
    if (ObCharset::case_insensitive_equal(type_name, tmp_type->get_name())) {
      if (OB_NOT_NULL(type)) {
        ret = OB_ERR_DECL_MORE_THAN_ONCE;
        LOG_USER_ERROR(OB_ERR_DECL_MORE_THAN_ONCE, type_name.length(), type_name.ptr());
      } else {
        type = tmp_type;
      }
    }
  }
  return ret;
}

int ObPLPackage::get_type(uint64_t type_id, const ObUserDefinedType *&type) const
{
  int ret = OB_SUCCESS;
  type = NULL;
  if (OB_INVALID_ID == type_id) {
    LOG_WARN("type id invalid", K(type_id), K(ret));
  } else {
    for (int64_t i = 0; OB_ISNULL(type) && i < type_table_.count(); ++i) {
      const ObUserDefinedType *tmp_type = type_table_.at(i);
      if (OB_ISNULL(tmp_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user type invalid", K(type_id), K(ret));
      } else {
        if (tmp_type->get_user_type_id() == type_id) {
          type = tmp_type;
        }
      }
    }
  }
  return ret;
}
} // end namespace pl
} // end namespace oceanbase





