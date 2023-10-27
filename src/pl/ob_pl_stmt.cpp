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

#include "ob_pl_stmt.h"
#include "ob_pl_resolver.h"
#include "lib/charset/ob_charset.h"
#include "common/ob_smart_call.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_user_type.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_udt_object_manager.h"
#endif

namespace oceanbase {
using namespace common;
using namespace share;
using namespace sql;
using namespace share::schema;
namespace pl {

int ObPLVar::deep_copy(const ObPLVar &var, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(&var != this)) {
    OZ (ob_write_string(allocator, var.get_name(), name_));
    if (OB_SUCC(ret)) {
      new (&type_) ObPLDataType(var.get_type());
      is_readonly_ = var.is_readonly();
      is_not_null_ = var.is_not_null();
      is_default_construct_ = var.is_default_construct();
      default_ = var.get_default();
      is_dup_declare_ = var.is_dup_declare();
      is_formal_param_ = var.is_formal_param();
      is_referenced_ = var.is_referenced();
    }
  }
  return ret;
}

int ObPLVarDebugInfo::deep_copy(ObIAllocator &allocator, const ObPLVarDebugInfo &other)
{
  int ret = OB_SUCCESS;
  OZ (ob_write_string(allocator, other.name_, name_));
  OX (type_ = other.type_);
  OX (scope_ = other.scope_);
  return ret;
}

int ObPLSymbolTable::add_symbol(const ObString &name,
                                const ObPLDataType &type,
                                const int64_t default_idx,
                                const bool read_only,
                                const bool not_null,
                                const bool default_construct,
                                const bool is_formal_param,
                                const bool is_dup_declare)
{
  int ret = OB_SUCCESS;
  ObPLVar var;
  var.set_name(name);
  var.set_type(type);
  var.set_default(default_idx);
  var.set_readonly(read_only);
  var.set_not_null(not_null);
  var.set_default_construct(default_construct);
  var.set_is_formal_param(is_formal_param);
  var.set_dup_declare(is_dup_declare);
  OZ (variables_.push_back(var), var, variables_.count());
  return ret;
}

int ObPLSymbolTable::delete_symbol(int64_t symbol_idx)
{
  int ret = OB_SUCCESS;
  OZ (variables_.remove(symbol_idx));
  return ret;
}

int ObPLUserTypeTable::add_type(ObUserDefinedType *user_defined_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(user_types_.push_back(user_defined_type))) {
    LOG_WARN("user type push back failed", K(ret));
  }
  return ret;
}

const ObUserDefinedType *ObPLUserTypeTable::get_type(const ObString &type_name) const
{
  const ObUserDefinedType *user_defined_type = NULL;
  for (int64_t i = 0; i < user_types_.count(); ++i) {
    if (user_types_.at(i) != NULL
        && 0 == type_name.case_compare(user_types_.at(i)->get_name())) {
      user_defined_type = user_types_.at(i);
    }
  }
  return user_defined_type;
}

const ObUserDefinedType *ObPLUserTypeTable::get_type(uint64_t type_id) const
{
  const ObUserDefinedType *user_defined_type = NULL;
#ifdef OB_BUILD_ORACLE_PL
  if (type_id == sys_refcursor_type_.get_user_type_id()) {
    user_defined_type = &sys_refcursor_type_;
  }
#endif
  for (int64_t i = 0; NULL == user_defined_type && i < user_types_.count(); ++i) {
    if (user_types_.at(i) != NULL && user_types_.at(i)->get_user_type_id() == type_id) {
      user_defined_type = user_types_.at(i);
    }
  }
  return user_defined_type;
}

const ObUserDefinedType *ObPLUserTypeTable::get_type(int64_t idx) const
{
  return idx < 0 || idx >= user_types_.count() ? NULL : user_types_.at(idx);
}

const ObUserDefinedType *ObPLUserTypeTable::get_external_type(uint64_t type_id) const
{
  const ObUserDefinedType *user_defined_type = NULL;
#ifdef OB_BUILD_ORACLE_PL
  if (type_id == sys_refcursor_type_.get_user_type_id()) {
    user_defined_type = &sys_refcursor_type_;
  }
#endif
  for (int64_t i = 0; NULL == user_defined_type && i < external_user_types_.count(); ++i) {
    if (external_user_types_.at(i) != NULL && external_user_types_.at(i)->get_user_type_id() == type_id) {
      user_defined_type = external_user_types_.at(i);
    }
  }
  return user_defined_type;
}

int ObPLUserTypeTable::add_external_type(const ObUserDefinedType *user_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid user type to add", K(user_type), K(ret));
  } else {
    int64_t count = external_user_types_.count();
    int64_t i = 0;
    for (; i < count; i++) {
      if (OB_ISNULL(external_user_types_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("external user type is NULL", K(external_user_types_), K(i), K(ret));
      } else if (external_user_types_.at(i)->get_user_type_id() == user_type->get_user_type_id()) {
        LOG_DEBUG("type id already exist",
                  K(ret), K(external_user_types_.at(i)->get_user_type_id()), K(i));
        break;
      }
    }
    if (OB_SUCC(ret) && i == count) {
      if (OB_FAIL(external_user_types_.push_back(user_type))) {
        LOG_WARN("external user type push back failed", K(ret), K(i), K(count), KPC(user_type));
      } else {
        LOG_DEBUG("add external type",
                  K(ret),
                  K(user_type->get_user_type_id()),
                  K(external_user_types_),
                  K(&external_user_types_),
                  KPC(user_type),
                  K(i), K(count),
                  K(external_user_types_.count()),
                  K(lbt()));
      }
    }
  }
  return ret;
}

int ObPLUserTypeTable::add_external_types(ObIArray<const ObUserDefinedType *> &user_types)
{
  int ret = OB_SUCCESS;
  int64_t count = external_user_types_.count();
  int64_t add_count = user_types.count();
  int64_t i = 0;
  int64_t j = 0;
  for (i=0; i<add_count; i++) {
    for (j=0; j<count; j++) {
      if (user_types.at(i) == external_user_types_.at(j)) {
        LOG_WARN("external type already exist" ,K(ret));
        break;
      }
    }
    if (j == count) {
      if (OB_FAIL(external_user_types_.push_back(user_types.at(i)))) {
        LOG_WARN("external type push back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLLabelTable::add_label(const common::ObString &name,
                              const ObPLLabelTable::ObPLLabelType &type,
                              ObPLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (count_ < 0 || count_ >= FUNC_MAX_LABELS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid condition count in condition table", K(get_count()), K(FUNC_MAX_LABELS), K(ret));
  } else {
    labels_[count_].label_ = name;
    labels_[count_].type_ = type;
    labels_[count_].next_stmt_ = stmt;
    count_++;
  }
  return ret;
}

int ObPLCondition::deep_copy(const ObPLCondition &condition, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(&condition != this)) {
    ObString sql_state;
    OZ (ob_write_string(allocator, condition.get_name(), name_));
    OZ (ob_write_string(allocator,
                        ObString(condition.get_value().str_len_, condition.get_value().sql_state_),
                        sql_state));
    OX (value_.sql_state_ = sql_state.ptr());
    OX (value_.type_ = condition.get_value().type_);
    OX (value_.error_code_ = condition.get_value().error_code_);
    OX (value_.str_len_ = condition.get_value().str_len_);
    OX (value_.stmt_id_ = condition.get_value().stmt_id_);
    OX (value_.signal_ = condition.get_value().signal_);
    OX (value_.duplicate_ = condition.get_value().duplicate_);
  }
  return ret;
}

int ObPLConditionTable::init(ObPLConditionTable &parent_condition_table)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < parent_condition_table.get_count(); ++i) {
    const ObPLCondition *condition = parent_condition_table.get_condition(i);
    CK (OB_NOT_NULL(condition));
    OZ (add_condition(condition->get_name(), condition->get_value()));
  }
  return ret;
}

int ObPLConditionTable::add_condition(const common::ObString &name, const ObPLConditionValue &value)
{
  int ret = OB_SUCCESS;
  if (count_ < 0 || count_ >= FUNC_MAX_CONDITIONS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid condition count in condition table", K(get_count()), K(FUNC_MAX_CONDITIONS), K(ret));
  } else {
    conditions_[count_].set_name(name);
    conditions_[count_].set_value(value);
    count_++;
  }
  return ret;
}

int ObPLCursor::set(const ObString &sql,
               const ObIArray<int64_t> &expr_idxs,
               const ObString &ps_sql,
               sql::stmt::StmtType type,
               bool for_update,
               ObRecordType *record_type,
               const ObPLDataType &cursor_type,
               CursorState state,
               const ObIArray<share::schema::ObSchemaObjVersion> &ref_objects,
               const common::ObIArray<int64_t> &params,
               bool has_dup_column_name
               )
{
  int ret = OB_SUCCESS;
  set_sql(sql);
  set_sql_params(expr_idxs);
  set_ps_sql(ps_sql, type);
  set_for_update(for_update);
  set_row_desc(record_type);
  set_cursor_type(cursor_type);
  set_state(state);
  if (has_dup_column_name) {
    set_dup_column();
  }
  OZ (set_ref_objects(ref_objects));
  OZ (set_formal_params(params));
  return ret;
}

int ObPLCursorTable::add_cursor(uint64_t pkg_id,
                                uint64_t routine_id,
                                int64_t idx,
                                const ObString &sql,
                                const ObIArray<int64_t> &sql_params,
                                const ObString &ps_sql,
                                sql::stmt::StmtType stmt_type,
                                bool for_update,
                                bool has_hidden_rowid,
                                uint64_t rowid_table_id,
                                const common::ObIArray<ObSchemaObjVersion> &ref_objects,
                                const ObRecordType* row_desc, //sql返回的行描述(record)
                                const ObPLDataType& cursor_type, // cursor返回值类型(record)
                                const common::ObIArray<int64_t> &formal_params, //cursor的形参
                                ObPLCursor::CursorState state,
                                bool has_dup_column_name)
{
  int ret = OB_SUCCESS;
  // CK (OB_LIKELY(cursors_.count() < FUNC_MAX_CURSORS));
  if (OB_SUCC(ret)) {
    ObPLCursor *cursor = NULL;
    if (OB_ISNULL(cursor = static_cast<ObPLCursor*>(allocator_.alloc(sizeof(ObPLCursor))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc cursor memory", K(ret));
    } else {
      new (cursor) ObPLCursor(allocator_);
      cursor->set_package_id(pkg_id);
      cursor->set_routine_id(routine_id);
      cursor->set_index(idx);
      cursor->set_sql(sql);
      cursor->set_sql_params(sql_params);
      cursor->set_ps_sql(ps_sql, stmt_type);
      cursor->set_for_update(for_update);
      cursor->set_hidden_rowid(has_hidden_rowid);
      cursor->set_row_desc(row_desc);
      cursor->set_cursor_type(cursor_type);
      cursor->set_state(state);
      cursor->set_rowid_table_id(rowid_table_id);
      if (has_dup_column_name) {
        cursor->set_dup_column();
      }
      OZ (cursor->set_ref_objects(ref_objects));
      OZ (cursor->set_formal_params(formal_params));
      OZ (cursors_.push_back(cursor));
    }
  }
  return ret;
}

const ObPLCursor *ObPLCursorTable::get_cursor(uint64_t pkg_id, uint64_t routine_id,
                                              int64_t idx) const
{
  const ObPLCursor *cursor = NULL;
  for (int64_t i = 0; NULL == cursor && i < get_count(); ++i) {
    if (NULL != get_cursor(i)
        && get_cursor(i)->get_package_id() == pkg_id
        && get_cursor(i)->get_routine_id() == routine_id
        && get_cursor(i)->get_index() == idx) {
      cursor = get_cursor(i);
    }
  }
  return cursor;
}

void ObPLRoutineParam::reset()
{
  if (NULL != name_.ptr()) {
    free(name_.ptr());
  }
  name_.reset();
  type_.reset();
  mode_ = PL_PARAM_IN;
  default_value_.reset();
  type_owner_ = OB_INVALID_ID;
  extern_type_ = SP_EXTERN_INVALID;
  type_name_.reset();
  type_subname_.reset();
  obj_version_.reset();
}

bool ObPLRoutineParam::operator ==(const ObPLRoutineParam &other) const
{
  bool is_equal = false;
  is_equal = get_name() == other.get_name()
    && get_type() == other.get_type()
    // && get_type().get_type() == other.get_type().get_type()
    // && get_type().get_obj_type() == other.get_type().get_obj_type()
    && get_mode() == other.get_mode()
    && get_extern_type() == other.get_extern_type()
    && get_type_name() == other.get_type_name()
    && get_type_subname() == other.get_type_subname();
  if (is_equal && ObLobType == get_type().get_obj_type()) {
    is_equal = get_type().get_meta_type()->get_collation_type()
                == other.get_type().get_meta_type()->get_collation_type();
  }
  return is_equal;
}

ObPLRoutineInfo::~ObPLRoutineInfo()
{
  ARRAY_FOREACH_NORET(params_, param_idx) {
    ObPLRoutineParam *routine_param = params_.at(param_idx);
    if (OB_NOT_NULL(routine_param)) {
      routine_param->~ObPLRoutineParam();
      allocator_.free(routine_param);
      params_.at(param_idx) = NULL;
    }
  }
}

int ObPLRoutineInfo::get_idx(int64_t &idx) const
{
  int ret = OB_SUCCESS;
  if (NESTED_PROCEDURE == type_ || NESTED_FUNCTION == type_) {
    if (subprogram_path_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("idx is invalid", K(*this), K(ret));
    } else {
      idx = subprogram_path_.at(subprogram_path_.count() - 1);
    }
  } else if (!subprogram_path_.empty()) {
    idx = subprogram_path_.at(subprogram_path_.count() - 1);
  } else {
    idx = id_;
  }
  return ret;
}

int ObPLRoutineInfo::set_idx(int64_t idx)
{
  int ret = OB_SUCCESS;
  if (NESTED_PROCEDURE == type_ || NESTED_FUNCTION == type_) {
    OZ (subprogram_path_.push_back(idx));
  } else {
    id_ = idx;
  }
  return ret;
}

int ObPLRoutineInfo::get_routine_param(int64_t idx, ObPLRoutineParam *&param) const
{
  int ret = OB_SUCCESS;
  ObIRoutineParam *iparam = NULL;
  OZ (get_routine_param(idx, iparam));
  CK (OB_NOT_NULL(iparam));
  CK (OB_NOT_NULL(param = static_cast<ObPLRoutineParam*>(iparam)));
  return ret;
}

int ObPLRoutineInfo::get_routine_param(int64_t idx, ObIRoutineParam*& param) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx > get_params().count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("idx is invalid", K(idx), K(ret));
  } else {
    param = get_params().at(idx);
  }
  return ret;
}

int ObPLRoutineInfo::find_param_by_name(const ObString &name, int64_t &position) const
{
  int ret = OB_SUCCESS;
  position = -1;
  if (name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalue param name", K(ret), K(name));
  } else {
    int64_t i = 0;
    for (; i < get_params().count(); ++i) {
      if (0 == get_params().at(i)->get_name().case_compare(name)) {
        position = i;
      }
    }
    if (i != get_params().count()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("param name is not exists", K(ret), K(*this), K(name));
    }
  }
  return ret;
}

int ObPLRoutineInfo::make_routine_param(ObIAllocator &allocator,
                                        const ObDataTypeCastParams &dtc_params,
                                        const ObString &param_name,
                                        const ObPLDataType &param_type,
                                        ObPLRoutineParamMode param_mode,
                                        bool is_nocopy,
                                        const ObString &default_value,
                                        bool default_cast,
                                        const ObPLExternTypeInfo &extern_type_info,
                                        ObPLRoutineParam *&param)
{
  int ret = OB_SUCCESS;
  param = static_cast<ObPLRoutineParam *>(allocator.alloc(sizeof(ObPLRoutineParam)));
  if (OB_ISNULL(param)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  }
  OX (new (param) ObPLRoutineParam());
  OX (param->set_type(param_type));
  OX (param->set_param_mode(param_mode));
  OX (param->set_is_nocopy(is_nocopy));
  OZ (ob_write_string(allocator, param_name, const_cast<ObString &>(param->get_name())), param_name);
  OX (param->get_default_value() = default_value);
  OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(allocator,
                                                         dtc_params,
                                                         param->get_default_value(),
                                                         ObCharset::COPY_STRING_ON_SAME_CHARSET));
  OX (param->set_default_cast(default_cast));
  OX (param->set_extern_type(extern_type_info.flag_));
  OX (param->set_type_owner(extern_type_info.type_owner_));
  OX (param->set_obj_version(extern_type_info.obj_version_));
  OZ (ob_write_string(allocator, extern_type_info.type_name_,
                      const_cast<ObString &>(param->get_type_name())), extern_type_info);
  OZ (ob_write_string(allocator, extern_type_info.type_subname_,
                      const_cast<ObString&>(param->get_type_subname())), extern_type_info);

#ifdef OB_BUILD_ORACLE_PL
  if (OB_SUCC(ret)) {
    if (0 == param_name.case_compare("SELF")) {
      param->set_is_self_param(true);
    } else {
      param->set_is_self_param(false);
    }
  }
#endif

  LOG_DEBUG("make call routine param", K(ret), K(extern_type_info), K(param_type), K(lbt()));

  if (OB_FAIL(ret) && OB_NOT_NULL(param)) {
    param->~ObPLRoutineParam();
    allocator.free(param);
    param = NULL;
  }
  return ret;
}

bool ObPLRoutineInfo::has_self_param() const
{
  bool bret = false;
  ObPLRoutineParam *param = NULL;
  for (int64_t i = 0; !bret && i < params_.count(); ++i) {
    param = params_.at(i);
    if (OB_ISNULL(param)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "get unexpected null param", K(i));
      break;
    } else {
      bret = bret || param->get_is_self_param();
    }
  }
  return bret;
}

int64_t ObPLRoutineInfo::get_self_param_pos() const
{
  bool stop = false;
  int64_t pos = OB_INVALID_INDEX;
  ObPLRoutineParam *param = NULL;
  for (int64_t i = 0; !stop && i < params_.count(); ++i) {
    param = params_.at(i);
    if (OB_ISNULL(param)) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "get unexpected null param", K(i));
      break;
    } else if (param->get_is_self_param()) {
      pos = i;
      stop = true;
    }
  }
  return pos;
}

int ObPLRoutineInfo::add_param(ObPLRoutineParam *param)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(param));
  for (int64_t i = 0; OB_SUCC(ret) && i < params_.count(); ++i) {
    if (0 == param->get_name().case_compare(params_.at(i)->get_name())) {
      ret = OB_ERR_DUPLICATE_FILED;
      LOG_WARN("duplicate fields in argument list are not permitted!", K(ret), K(i), KPC(param));
    }
  }
  OZ (params_.push_back(param));
  if (OB_SUCC(ret) && param->get_is_self_param()) {
    if (1 < params_.count()) {
      std::rotate(params_.begin(),
                  params_.end() - 1,
                  params_.end());
    }
  }
  return ret;
}

int ObPLRoutineInfo::is_equal(const ObPLRoutineInfo* other, bool &equal) const
{
  int ret = OB_SUCCESS;
  equal = true;
  if (OB_ISNULL(other)
      || type_ != other->type_
      || name_ != other->name_
      || params_.count() != other->params_.count()) {
    equal = false;
  }
  if (equal && (PACKAGE_FUNCTION == type_ || NESTED_FUNCTION == type_)) {
    CK (OB_NOT_NULL(ret_info_));
    CK (OB_NOT_NULL(other->ret_info_));
    CK (OB_LIKELY(ret_info_->get_default_value().empty()));
    CK (OB_LIKELY(other->ret_info_->get_default_value().empty()))
    OX (equal = *ret_info_ == *(other->ret_info_));
  }
  if (equal) {
    for (int64_t i = 0; OB_SUCC(ret) && equal && i < params_.count(); ++i) {
      CK (OB_NOT_NULL(params_.at(i)));
      CK (OB_NOT_NULL(other->params_.at(i)));
      OX (equal = *(params_.at(i)) == *(other->params_.at(i)));
    }
  }
  return ret;
}

bool ObPLRoutineInfo::has_generic_type() const
{
  bool result = false;
  if (OB_NOT_NULL(ret_info_)) {
    result = ret_info_->get_type().is_generic_type();
  }
  if (!result) {
    for (int64_t i = 0; !result && i < params_.count(); ++i) {
      if (OB_NOT_NULL(params_.at(i))) {
        result = params_.at(i)->get_type().is_generic_type();
      }
    }
  }
  return result;
}

ObPLRoutineTable::~ObPLRoutineTable()
{
  ARRAY_FOREACH_NORET(routine_infos_, routine_info_idx) {
    ObPLRoutineInfo *routine_info = routine_infos_.at(routine_info_idx);
    if (OB_NOT_NULL(routine_info)) {
      routine_info->~ObPLRoutineInfo();
      allocator_.free(routine_info);
      routine_infos_.at(routine_info_idx) = NULL;
    }
  }
  ARRAY_FOREACH_NORET(routine_asts_, routine_ast_idx) {
    ObPLFunctionAST *routine_ast = routine_asts_.at(routine_ast_idx);
    if (OB_NOT_NULL(routine_ast)) {
      routine_ast->~ObPLFunctionAST();
      allocator_.free(routine_ast);
      routine_asts_.at(routine_ast_idx) = NULL;
    }
  }
}

int ObPLRoutineTable::init(ObPLRoutineTable *parent_routine_table)
{
  int ret = OB_SUCCESS;
  int64_t pre_alloc_count = OB_ISNULL(parent_routine_table)?INIT_ROUTINE_COUNT:parent_routine_table->get_count();
  if (OB_FAIL(routine_infos_.prepare_allocate(pre_alloc_count))) {
    LOG_WARN("pre allocate routine infos failed", K(ret));
  } else if (OB_FAIL(routine_asts_.prepare_allocate(pre_alloc_count))) {
    LOG_WARN("pre allocate routine asts failed", K(ret));
  } else {
    ARRAY_FOREACH(routine_infos_, routine_info_idx) {
      routine_infos_.at(routine_info_idx) = NULL;
    }
    ARRAY_FOREACH(routine_asts_, routine_ast_idx) {
      routine_asts_.at(routine_ast_idx) = NULL;
    }
  }
  return ret;
}

int ObPLRoutineTable::make_routine_info(ObIAllocator &allocator,
                                        const ObString &name,
                                        ObProcType type,
                                        const ObString &decl_str,
                                        uint64_t database_id,
                                        uint64_t pkg_id,
                                        uint64_t id,
                                        const ObIArray<int64_t> &subprogram_path,
                                        ObPLRoutineInfo *&routine_info) const
{
  int ret = OB_SUCCESS;
  routine_info = static_cast<ObPLRoutineInfo *>(allocator.alloc(sizeof(ObPLRoutineInfo)));
  if (OB_ISNULL(routine_info)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    ObString dst_name;
    ObString dst_decl_str;
    new (routine_info) ObPLRoutineInfo(allocator);
    routine_info->set_type(type);
    routine_info->set_tenant_id(get_tenant_id_by_object_id(pkg_id));
    routine_info->set_db_id(database_id);
    routine_info->set_pkg_id(pkg_id);
    if (NESTED_PROCEDURE == type || NESTED_FUNCTION == type) {
      routine_info->set_parent_id(id);
      routine_info->set_id(OB_INVALID_ID == id ? subprogram_path.count() + 1 : subprogram_path.count() + 1 + id);
    } else {
      routine_info->set_id(id);
    }
    if (OB_FAIL(ob_write_string(allocator, name, dst_name))) {
      LOG_WARN("copy routine name failed", K(name), K(ret));
    } else if (OB_FAIL(ob_strip_space(allocator, decl_str, dst_decl_str))) {
      LOG_WARN("strip routine decl str space failed", K(decl_str), K(ret));
    } else {
      routine_info->set_name(dst_name);
      routine_info->set_decl_str(dst_decl_str);
    }
    if (OB_SUCC(ret) && (NESTED_PROCEDURE == type || NESTED_FUNCTION == type)) {
      OZ (routine_info->set_subprogram_path(subprogram_path));
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(routine_info)) {
    routine_info->~ObPLRoutineInfo();
    allocator.free(routine_info);
    routine_info = NULL;
  }
  return ret;
}

int ObPLRoutineTable::make_routine_ast(ObIAllocator &allocator,
                                       const ObString &db_name,
                                       const ObString &package_name,
                                       uint64_t package_version,
                                       const ObPLRoutineInfo &routine_info,
                                       ObPLFunctionAST *&routine_ast)
{
  int ret = OB_SUCCESS;
  routine_ast = static_cast<ObPLFunctionAST *>(allocator.alloc(sizeof(ObPLFunctionAST)));
  if (OB_ISNULL(routine_ast)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    new (routine_ast) ObPLFunctionAST(allocator);
    //routine info and routine ast have same memory life, it is safe to do shadow copy
    routine_ast->set_name(routine_info.get_name());
    routine_ast->set_proc_type(routine_info.get_type());
    routine_ast->set_package_id(routine_info.get_pkg_id());
    routine_ast->set_package_version(package_version);
    routine_ast->set_id(
      NESTED_PROCEDURE == routine_info.get_type() || NESTED_FUNCTION == routine_info.get_type()
        ? routine_info.get_parent_id() : routine_info.get_id());
    routine_ast->set_subprogram_id(routine_info.get_id());
    if (routine_info.is_pipelined()) {
      routine_ast->set_pipelined();
    }
    routine_ast->set_compile_flag(routine_info.get_compile_flag());
    OZ (routine_ast->set_subprogram_path(routine_info.get_subprogram_path()));
    if (OB_FAIL(ob_write_string(allocator, db_name, const_cast<ObString &>(routine_ast->get_db_name())))) {
      LOG_WARN("copy routine db name failed", K(db_name), K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, package_name, routine_ast->get_package_name()))) {
      LOG_WARN("copy routine package name failed", K(package_name), K(ret));
    } else {
      if (PACKAGE_FUNCTION == routine_info.get_type() || NESTED_FUNCTION == routine_info.get_type()) {
        const ObPLRoutineParam *ret_param = static_cast<const ObPLRoutineParam *>(routine_info.get_ret_info());
        CK (OB_NOT_NULL(ret_param));
        if (OB_SUCC(ret) && ret_param->get_obj_version().is_valid()) {
          OZ (routine_ast->add_dependency_object(ret_param->get_obj_version()));
        }
        OX (routine_ast->set_ret_type(ret_param->get_type()));
        if (OB_SUCC(ret) && ret_param->get_type().is_valid_type() && ret_param->get_type().is_obj_type()) {
          CK (OB_NOT_NULL(ret_param->get_type().get_data_type()));
          if (OB_SUCC(ret)
              && ob_is_enum_or_set_type(ret_param->get_type().get_data_type()->get_obj_type())) {
            OZ (routine_ast->set_ret_type_info(ret_param->get_type().get_type_info()));
          }
        }
      }
      for (int64_t param_idx = 0; OB_SUCC(ret) && param_idx < routine_info.get_params().count(); ++param_idx) {
        const ObPLRoutineParam *param = routine_info.get_params().at(param_idx);
        CK (OB_NOT_NULL(param));
        if (OB_SUCC(ret) && param->get_obj_version().is_valid()) {
          OZ (routine_ast->add_dependency_object(param->get_obj_version()));
        }
        if (OB_SUCC(ret) && !param->get_type().is_valid_type()) {
          ret = OB_ERR_SP_UNDECLARED_TYPE;
          LOG_WARN("undeclare type", K(ret), KPC(param));
        }
        OZ (routine_ast->add_argument(param->get_name(),
                                      param->get_type(),
                                      NULL,
                                      &(param->get_type().get_type_info()),
                                      param->is_in_param(),
                                      param->is_self_param()));
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(routine_ast)) {
    routine_ast->~ObPLFunctionAST();
    allocator.free(routine_ast);
    routine_ast = NULL;
  }
  return ret;
}

int ObPLRoutineTable::get_routine_info(int64_t routine_idx, const ObPLRoutineInfo *&routine_info) const
{
  int ret = OB_SUCCESS;
  routine_info = NULL;
  if (routine_idx < 0 || routine_idx >= get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package routine idx invalid", K(routine_idx), K(ret));
  } else {
    routine_info = routine_infos_[routine_idx];
  }
  return ret;
}

int ObPLRoutineTable::get_routine_info(int64_t routine_idx, ObPLRoutineInfo *&routine_info)
{
  int ret = OB_SUCCESS;
  routine_info = NULL;
  if (routine_idx < 0 || routine_idx >= get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package routine idx invalid", K(routine_idx), K(ret));
  } else {
    routine_info = routine_infos_[routine_idx];
  }
  return ret;
}

int ObPLRoutineTable::get_routine_info(const ObPLRoutineInfo *routine_info,
                                       const ObPLRoutineInfo *&info) const
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(routine_info));
  info = NULL;
  for (int64_t i = ObPLRoutineTable::NORMAL_ROUTINE_START_IDX;
       OB_SUCC(ret) && i < routine_infos_.count(); ++i) {
    bool is_equal = false;
    OZ (routine_info->is_equal(routine_infos_.at(i), is_equal));
    if (is_equal) {
      info = routine_infos_.at(i);
      break;
    }
  }
  return ret;
}

int ObPLRoutineTable::get_routine_info(const ObString &routine_decl_str, ObPLRoutineInfo *&routine_info) const
{
  int ret = OB_SUCCESS;
  routine_info = NULL;
  ObString routine_decl_str_without_blank;
  if (OB_FAIL(ob_strip_space(allocator_, routine_decl_str, routine_decl_str_without_blank))) {
    LOG_WARN("strip routine decl str space failed", K(routine_decl_str), K(ret));
  } else {
    uint64_t routine_count = get_count();
    for (int64_t i = ObPLRoutineTable::NORMAL_ROUTINE_START_IDX; OB_SUCC(ret) && i < routine_count; ++i) {
      ObPLRoutineInfo *tmp_routine_info = routine_infos_.at(i);
      if (OB_NOT_NULL(tmp_routine_info)) {
        if (ObCharset::case_insensitive_equal(tmp_routine_info->get_decl_str(), routine_decl_str_without_blank)) {
          routine_info = tmp_routine_info;
          break;
        }
      }
    }
  }
  return ret;
}

int ObPLRoutineTable::add_routine_info(ObPLRoutineInfo *routine_info)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(routine_info));
  // CK (get_count() < PACKAGE_MAX_ROUTINES);
  OZ (routine_infos_.push_back(routine_info));
  OZ (routine_asts_.push_back(NULL)); //reserve place for def
  OZ (routine_info->set_idx(get_count() - 1));
  return ret;
}

int ObPLRoutineTable::set_routine_info(int64_t routine_idx, ObPLRoutineInfo *routine_info)
{
  int ret = OB_SUCCESS;
  CK (routine_idx >= 0 && routine_idx < get_count());
  CK (OB_ISNULL(routine_infos_.at(routine_idx)));
  OX (routine_infos_.at(routine_idx) = routine_info);
  OZ (routine_info->set_idx(routine_idx));
  return ret;
}

int ObPLRoutineTable::set_routine_ast(int64_t routine_idx, ObPLFunctionAST *routine_ast)
{
  int ret = OB_SUCCESS;
  CK (routine_idx >= 0 && routine_idx < get_count());
  CK (OB_ISNULL(routine_asts_.at(routine_idx)));
  OX (routine_asts_.at(routine_idx) = routine_ast);
  return ret;
}

int ObPLRoutineTable::get_routine_ast(int64_t routine_idx, ObPLFunctionAST *&routine_ast) const
{
  int ret = OB_SUCCESS;
  if (routine_idx < 0 || routine_idx >= get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package routine idx invalid", K(routine_idx), K(ret));
  } else {
    routine_ast = routine_asts_[routine_idx];
  }
  return ret;
}

int ObPLUDTNS::get_user_type(uint64_t type_id,
                             const ObUserDefinedType *&user_type,
                             ObIAllocator *allocator) const
{
  int ret = OB_SUCCESS;
  const ObUDTTypeInfo *udt_info = NULL;
  const uint64_t tenant_id = get_tenant_id_by_object_id(type_id);
  CK (OB_NOT_NULL(allocator));
  OZ (schema_guard_.get_udt_info(tenant_id, type_id, udt_info));
  CK (OB_NOT_NULL(udt_info));
  OZ (udt_info->transform_to_pl_type(*allocator, user_type));
  CK (OB_NOT_NULL(user_type));
  return ret;
}

int ObPLBlockNS::get_user_type(uint64_t type_id,
                               const ObUserDefinedType *&user_type,
                               ObIAllocator *allocator) const
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  if (OB_FAIL(get_pl_data_type_by_id(type_id, user_type))) {
    LOG_WARN("failed to get user type", K(type_id), K(ret));
  }
  return ret;
}

int ObPLBlockNS::add_type(ObUserDefinedType *type)
{
  int ret = OB_SUCCESS;
  bool is_dup = false;
  const void *dup_type = NULL;
  if (OB_ISNULL(type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("type is NULL", K(ret));
  } else if (OB_ISNULL(get_type_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("symbol table is NULL", K(ret));
  } else if (OB_FAIL(check_dup_type(type->get_name(), is_dup, dup_type))) {
    LOG_WARN("failed to check dup", K(type->get_name()), K(ret));
  } else if (is_dup && !type->is_subtype()) {
    ret = OB_ERR_SP_DUP_TYPE;
    LOG_USER_ERROR(OB_ERR_SP_DUP_TYPE, type->get_name().length(), type->get_name().ptr());
  } else {
    uint64_t type_id = get_type_table()->generate_user_type_id(package_id_);
    type->set_user_type_id(type_id);
    if (OB_FAIL(types_.push_back(get_type_table()->get_count()))) {
      LOG_WARN("failed to add user defined type", K(ret));
    } else if (OB_FAIL(get_type_table()->add_type(type))) {
      LOG_WARN("failed to add type to user defined type table", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLBlockNS::add_symbol(const ObString &name,
                            const ObPLDataType &type,
                            const ObRawExpr *expr,
                            const bool read_only,
                            const bool not_null,
                            const bool default_construct,
                            const bool is_formal_param)
{
  int ret = OB_SUCCESS;
  bool is_dup = false;
  if (OB_ISNULL(get_symbol_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("symbol table is NULL", K(ret));
  } else if (!name.empty() && OB_FAIL(check_dup_symbol(name, type, is_dup))) {
    LOG_WARN("failed to check dup", K(name), K(ret));
  } else if (is_dup && lib::is_mysql_mode()) {
    ret = OB_ERR_SP_DUP_VAR;
    LOG_USER_ERROR(OB_ERR_SP_DUP_VAR, name.length(), name.ptr());
  } else {
    OZ (symbols_.push_back(get_symbol_table()->get_count()));
    CK (OB_NOT_NULL(exprs_));
    OZ (get_symbol_table()->add_symbol(name,
                                       type,
                                       OB_NOT_NULL(expr) ? exprs_->count() : -1,
                                       read_only,
                                       not_null,
                                       default_construct,
                                       is_formal_param,
                                       is_dup));
    if (OB_SUCC(ret) && OB_NOT_NULL(expr)) {
      OZ (exprs_->push_back(const_cast<ObRawExpr*>(expr)));
    }
  }
  return ret;
}

int ObPLBlockNS::delete_symbols()
{
  int ret = OB_SUCCESS;
  for (int64_t i = symbols_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    CK (symbols_.at(i) == get_symbol_table()->get_count() - 1);
    OZ (get_symbol_table()->delete_symbol(symbols_.at(i)));
  }
  return ret;
}

int ObPLBlockNS::add_label(const ObString &name,
                           const ObPLLabelTable::ObPLLabelType &type,
                           ObPLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_label_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("label table is NULL", K(ret));
  } else if (name.length() > LABEL_MAX_SIZE) {
    ret = OB_ERR_IDENTIFIER_TOO_LONG;
    LOG_WARN("label name is too long.", K(ret), K(name));
    LOG_USER_ERROR(OB_ERR_IDENTIFIER_TOO_LONG, name.length(), name.ptr());
  } else {
    bool is_dup = false;
    if (OB_FAIL(check_dup_label(name, is_dup))) {
      LOG_INFO("check dup label fail. ", K(ret), K(name));
    } else if (is_dup) {
      ret = OB_ERR_REDEFINE_LABEL;
      LOG_WARN("redefining label ", K(name), K(ret));
    } else if (OB_FAIL(labels_.push_back(get_label_table()->get_count()))) {
      LOG_WARN("failed to add symbol", K(ret));
    } else if (OB_FAIL(get_label_table()->add_label(name, type, stmt))) {
      LOG_WARN("failed to add variable to sysbol table", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLBlockNS::add_condition(const common::ObString &name,
                               const ObPLConditionValue &value,
                               bool exception_init)
{
  int ret = OB_SUCCESS;
  bool is_dup = false;
  const void *dup_cond = NULL;
  if (OB_ISNULL(get_condition_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("condition table is NULL", K(ret));
  } else if (OB_FAIL(check_dup_condition(name, is_dup, dup_cond))) {
    LOG_WARN("failed to check dup", K(name), K(ret));
  } else if (is_dup) {
    if (OB_ISNULL(external_ns_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("external ns is null", K(ret));
    } else if (lib::is_oracle_mode()) {
      if (OB_ISNULL(dup_cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dup cond is null", K(ret));
      } else {
        ObPLCondition *temp_dup_cond = static_cast<ObPLCondition *>(const_cast<void *>(dup_cond));
        // ORACLE模式如果多次EXECPTION_INIT同一个USER EXCEPTION, 后面的覆盖前面的
        if (exception_init) {
          bool duplicate = temp_dup_cond->get_duplicate();
          CK (ERROR_CODE == value.type_ && value.error_code_ < 0);
          OX (temp_dup_cond->set_value(value));
          OX (duplicate ? temp_dup_cond->set_duplicate() : void(NULL));
        } else { // Oracle模式下多次声明, 只要不被引用就不需要报错, 因此声明的时候记录下
          OX (temp_dup_cond->set_duplicate());
        }
      }
    } else {
      ret = OB_ERR_SP_DUP_CONDITION;
      LOG_WARN("duplicate condition declare", K(ret), K(name));
      LOG_USER_ERROR(OB_ERR_SP_DUP_CONDITION, name.length(), name.ptr());
    }
  } else if (lib::is_oracle_mode() && exception_init) {
    // PRAGMA EXCEPTION_INIT not got exception define.
    ret = OB_ERR_UNKNOWN_EXCEPTION;
    LOG_USER_ERROR(OB_ERR_UNKNOWN_EXCEPTION, name.length(), name.ptr());
  } else {
    if (OB_FAIL(conditions_.push_back(get_condition_table()->get_count()))) {
      LOG_WARN("failed to add condition", K(ret));
    } else if (OB_FAIL(get_condition_table()->add_condition(name, value))) {
      LOG_WARN("failed to add condition to condition table", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObPLBlockNS::add_questionmark_cursor(const int64_t symbol_idx)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> dummy_params;
  ObString dummy_sql;
  sql::stmt::StmtType dummy_stmt_type = sql::stmt::T_NONE;
  bool dummy_for_update = false;
  bool dummy_hidden_rowid = false;
  common::ObArray<ObSchemaObjVersion> dummy_ref_objects;
  const ObPLDataType dummy_return_type;
  const ObArray<int64_t> dummy_formal_params;
  if (OB_FAIL(cursors_.push_back(get_cursor_table()->get_count()))) {
    LOG_WARN("failed to add condition", K(ret));
  } else if (OB_FAIL(get_cursor_table()->add_cursor(get_package_id(),
                                                    get_routine_id(),
                                                    symbol_idx,
                                                    "?",
                                                    dummy_params,
                                                    dummy_sql,
                                                    dummy_stmt_type,
                                                    false,
                                                    false,
                                                    OB_INVALID_ID,
                                                    dummy_ref_objects,
                                                    NULL,
                                                    dummy_return_type,
                                                    dummy_formal_params,
                                                    ObPLCursor::PASSED_IN,
                                                    false))) {
    LOG_WARN("failed to add condition to condition table", K(ret));
  }
  return ret;
}

int ObPLBlockNS::add_cursor(const ObString &name,
                            const ObPLDataType &type,
                            const ObString &sql,
                            const ObIArray<int64_t> &sql_params,
                            const ObString &ps_sql,
                            sql::stmt::StmtType stmt_type,
                            bool for_update,
                            bool has_hidden_rowid,
                            uint64_t rowid_table_id,
                            const common::ObIArray<ObSchemaObjVersion> &ref_objects,
                            const ObRecordType *row_desc,
                            const ObPLDataType &cursor_type, // cursor返回值类型(record)
                            const common::ObIArray<int64_t> &formal_params,
                            ObPLCursor::CursorState state,
                            bool has_dup_column_name,
                            int64_t &index)
{
  int ret = OB_SUCCESS;
  bool is_dup = false;
  if (OB_FAIL(check_dup_cursor(name, is_dup))) {
    LOG_WARN("failed to check dup", K(name), K(ret));
  } else if (is_dup) {
    ret = OB_ERR_SP_DUP_CURSOR;
    LOG_USER_ERROR(OB_ERR_SP_DUP_CURSOR, name.length(), name.ptr());
  } else if (OB_FAIL(add_symbol(name, type))) {
    LOG_WARN("failed to add cursor to symbol table", K(ret));
  } else if (OB_ISNULL(get_cursor_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cursor table is NULL", K(ret));
  } else {
    if (OB_FAIL(cursors_.push_back(get_cursor_table()->get_count()))) {
      LOG_WARN("failed to add condition", K(ret));
    } else if (OB_FAIL(get_cursor_table()->add_cursor(get_package_id(),
                                                      get_routine_id(),
                                                      get_symbol_table()->get_count() - 1,
                                                      sql,
                                                      sql_params,
                                                      ps_sql,
                                                      stmt_type,
                                                      for_update,
                                                      has_hidden_rowid,
                                                      rowid_table_id,
                                                      ref_objects,
                                                      row_desc,
                                                      cursor_type,
                                                      formal_params,
                                                      state,
                                                      has_dup_column_name))) {
      LOG_WARN("failed to add condition to condition table", K(ret));
    } else {
      index = cursors_.at(cursors_.count() - 1);
    }
  }
  return ret;
}

#define CHECK_DUP(name_type) \
  int ObPLBlockNS::check_dup_##name_type(const ObString &name, bool &is_dup, const void *&dup_item) const \
  { \
    int ret = OB_SUCCESS; \
    is_dup = false; \
    dup_item = NULL; \
    if (OB_ISNULL(name_type##_table_)) { \
      ret = OB_ERR_UNEXPECTED; \
      LOG_WARN("table is NULL", K(ret)); \
    } else { \
      for (int64_t i = 0; OB_SUCC(ret) && i < name_type##s_.count(); ++i) { \
        if (OB_ISNULL(name_type##_table_->get_##name_type(name_type##s_.at(i)))) { \
          ret = OB_ERR_UNEXPECTED; \
          LOG_WARN("element is NULL", K(i), K(name_type##s_.at(i)), K(ret)); \
        } else if (name_type##_table_->get_##name_type(name_type##s_.at(i))->get_name() == name) { \
          is_dup = true; \
          dup_item = static_cast<const void *>(name_type##_table_->get_##name_type(name_type##s_.at(i))); \
        } else { /*do nothing*/ } \
      } \
    } \
    if (BLOCK_PACKAGE_BODY == type_) { \
      if (!is_dup && !OB_ISNULL(external_ns_->get_parent_ns())) { \
        if (OB_FAIL(external_ns_->get_parent_ns()->check_dup_##name_type(name, is_dup, dup_item))) { \
          LOG_WARN("check dup failed", K(ret)); \
        } \
      } \
    } \
    return ret; \
  }

CHECK_DUP(condition)
CHECK_DUP(type)

int ObPLBlockNS::check_dup_symbol(const ObString &name, const ObPLDataType &type, bool &is_dup) const
{
  int ret = OB_SUCCESS;
  is_dup = false;
  if (OB_ISNULL(symbol_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < symbols_.count(); ++i) {
      if (OB_ISNULL(symbol_table_->get_symbol(symbols_.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("element is NULL", K(i), K(symbols_.at(i)), K(ret));
      } else if ((lib::is_mysql_mode() && 0 == name.case_compare(symbol_table_->get_symbol(symbols_.at(i))->get_name())) ||
                 (lib::is_oracle_mode() && symbol_table_->get_symbol(symbols_.at(i))->get_name() == name)) {
        if (lib::is_mysql_mode() &&
            type.get_type() != symbol_table_->get_symbol(symbols_.at(i))->get_type().get_type()) {
          /* do nothing */
        } else {
          //名字相同，且类型相同才认为是相同
          is_dup = true;
          ObPLVar *pl_var = const_cast<ObPLVar *>(symbol_table_->get_symbol(symbols_.at(i)));
          pl_var->set_dup_declare(is_dup);
          if (pl_var->is_referenced()) {
            ret = OB_ERR_DECL_MORE_THAN_ONCE;
            LOG_USER_ERROR(OB_ERR_DECL_MORE_THAN_ONCE, name.length(), name.ptr());
          }
        }
      } else { /*do nothing*/ }
    }
  }
  if (BLOCK_PACKAGE_BODY == type_) {
    if (!is_dup && !OB_ISNULL(external_ns_->get_parent_ns())) {
      if (OB_FAIL(external_ns_->get_parent_ns()->check_dup_symbol(name, type, is_dup))) {
        LOG_WARN("check dup failed", K(ret));
      }
    }
  }
  return ret;
}

int ObPLBlockNS::check_dup_label(const ObString &name, bool &is_dup) const
{
  int ret = OB_SUCCESS;
  is_dup = false;
  if (lib::is_oracle_mode()) {
    // do nothing
  } else {
    const ObPLBlockNS *ns = this;
    while (NULL != ns && !is_dup) {
      for (int64_t i = 0; OB_SUCC(ret) && !is_dup && i < ns->get_labels().count(); ++i) {
        if (OB_ISNULL(ns->get_label_table())
            || OB_ISNULL(ns->get_label_table()->get_label(ns->get_labels().at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("label is NULL", K(i), K(ns->get_labels().at(i)), K(ret));
        } else if (*ns->get_label_table()->get_label(ns->get_labels().at(i)) == name
                    && ns->get_label_table()->is_ended(ns->get_labels().at(i)) == false) {
          is_dup = true;
        }
      }
      if (!is_dup && !ns->stop_search_label()) {
        ns = ns->get_pre_ns();
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObPLBlockNS::check_dup_goto_label(const ObString &name, bool &is_dup) const
{
  // label 'name' has been resolved successfully in former logic
  // so the search space will be nested pre-ns, instead of only current ns
  // (reference the logic in resolve_label)
  int ret = OB_SUCCESS;
  is_dup = false;
  if (OB_ISNULL(label_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("label table is NULL", K(ret));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < labels_.count(); ++i) {
      if (OB_ISNULL(label_table_->get_label(labels_.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("label is NULL", K(i), K(labels_.at(i)), K(ret));
      } else if (*label_table_->get_label(labels_.at(i)) == name) {
        if (!found) {
          found = true;
        } else {
          is_dup = true;
          break;
        }
      } else { /*do nothing*/ }
    }

    if (OB_SUCC(ret) && !found) {
      const ObPLBlockNS *pre_ns = get_pre_ns();
      if (stop_search_label() || NULL == pre_ns) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("resolve goto label succeesfully in resolve_label while failed in dup checking", K(ret));
      } else {
        OZ (SMART_CALL(pre_ns->check_dup_goto_label(name, is_dup)));
      }
    }
  }
  return ret;
}

int ObPLBlockNS::check_dup_cursor(const ObString &name, bool &is_dup) const
{
  int ret = OB_SUCCESS;
  is_dup = false;
  if (OB_ISNULL(cursor_table_) || OB_ISNULL(symbol_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cursor table or symbol table is NULL", K(cursor_table_), K(symbol_table_), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cursors_.count(); ++i) {
      if (OB_ISNULL(cursor_table_->get_cursor(cursors_.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cursor is NULL", K(i), K(cursors_.at(i)), K(ret));
      } else if (OB_ISNULL(symbol_table_->get_symbol(cursor_table_->get_cursor(cursors_.at(i))->get_index()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition is NULL", K(i), K(cursors_.at(i)), K(ret));
      } else if (symbol_table_->get_symbol(cursor_table_->get_cursor(cursors_.at(i))->get_index())->get_name() == name) {
        is_dup = true;
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPLExternalNS::add_dependency_object(const ObSchemaObjVersion &obj_version) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(get_dependency_table())) {
    OZ (ObPLCompileUnitAST::add_dependency_object_impl(*get_dependency_table(), obj_version));
  }
  return ret;
}

int ObPLExternalNS::search_in_standard_package(const common::ObString &name,
                                               ExternalType &type,
                                               ObPLDataType &data_type,
                                               uint64_t &parent_id,
                                               int64_t &var_idx) const
{
  int ret = OB_SUCCESS;
  uint64_t standard_package_id = OB_INVALID_ID;
  ObSQLSessionInfo &session_info = resolve_ctx_.session_info_;
  ObSchemaGetterGuard &schema_guard = resolve_ctx_.schema_guard_;
  ObPLPackageManager &package_manager = session_info.get_pl_engine()->get_package_manager();
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  const ObPLVar *var = NULL;
  bool exist = false;

  CK (OB_INVALID_INDEX == var_idx);
  CK (OB_INVALID_INDEX == parent_id);
  OX (type = ObPLExternalNS::INVALID_VAR);
  CK (!name.empty());
  OZ (schema_guard.check_package_exist(OB_SYS_TENANT_ID,
                                       OB_SYS_DATABASE_ID,
                                       ObString("STANDARD"),
                                       share::schema::PACKAGE_TYPE,
                                       compatible_mode,
                                       exist));
  if (OB_SUCC(ret) && exist) {
    OX (data_type.reset());
    OZ (schema_guard.get_package_id(OB_SYS_TENANT_ID,
                                    OB_SYS_DATABASE_ID,
                                    ObString("STANDARD"),
                                    share::schema::PACKAGE_TYPE,
                                    compatible_mode,
                                    standard_package_id));
    CK (standard_package_id != OB_INVALID_ID);
    // first try standard package constant var!
    OZ (package_manager.get_package_var(resolve_ctx_, standard_package_id, name, var, var_idx));
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(var)) { // then try standard package type!
      const ObUserDefinedType *user_type = NULL;
      if (OB_FAIL(package_manager.get_package_type(resolve_ctx_,
                                                   standard_package_id,
                                                   name,
                                                   user_type,
                                                   false))) {
        if (OB_ERR_SP_UNDECLARED_TYPE == ret) {
          LOG_INFO("get standard package type not exist!", K(ret), K(standard_package_id), K(name));
          type = ObPLExternalNS::INVALID_VAR;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("faild to get standard package type!", K(ret), K(standard_package_id), K(name));
        }
      } else if (OB_ISNULL(user_type)) {
        type = ObPLExternalNS::INVALID_VAR;
        LOG_WARN("package element not found", K(ret), K(standard_package_id), K(name));
      } else {
        var_idx = user_type->get_user_type_id();
        parent_id = standard_package_id;
        type = ObPLExternalNS::PKG_TYPE;
      }
    } else {
      parent_id = standard_package_id;
      data_type = var->get_type();
      type = ObPLExternalNS::PKG_VAR;
    }
  }

  if (OB_SUCC(ret) && var_idx != OB_INVALID_INDEX && OB_NOT_NULL(dependency_table_)) {
    const share::schema::ObPackageInfo *package_info_resolve = NULL;
    ObSchemaObjVersion obj_version;
    const uint64_t tenant_id = get_tenant_id_by_object_id(parent_id);
    CK (parent_id != OB_INVALID_INDEX);
    OZ (schema_guard.get_package_info(tenant_id, parent_id, package_info_resolve));
    CK (OB_NOT_NULL(package_info_resolve));
    OX (obj_version.object_id_ = parent_id);
    OX (obj_version.object_type_ = DEPENDENCY_PACKAGE);
    OX (obj_version.version_ = package_info_resolve->get_schema_version());
    OZ (add_dependency_object(obj_version));
  }
  return ret;
}

int ObPLExternalNS::resolve_synonym(uint64_t object_db_id,
                                    const ObString &object_name,
                                    ExternalType &type,
                                    uint64_t &parent_id,
                                    int64_t &var_idx,
                                    const ObString &synonym_name,
                                    const uint64_t cur_db_id) const
{
  int ret = OB_SUCCESS;
  uint64_t object_id = OB_INVALID_ID;
  uint64_t tenant_id = is_oceanbase_sys_database_id(object_db_id) ?
                       OB_SYS_TENANT_ID :
                       resolve_ctx_.session_info_.get_effective_tenant_id();
  ObSchemaGetterGuard &schema_guard = resolve_ctx_.schema_guard_;
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  if (OB_FAIL(schema_guard.get_table_id(
                  tenant_id, object_db_id, object_name, false /*is_index*/,
                  schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, object_id))
      || object_id == OB_INVALID_ID) {
    if (OB_FAIL(schema_guard.get_package_id(tenant_id, object_db_id, object_name,
                                            PACKAGE_TYPE, compatible_mode, object_id))
        || OB_INVALID_ID == object_id) {
      if (OB_FAIL(schema_guard.get_udt_id(
                  tenant_id, object_db_id, OB_INVALID_ID/*package_id*/, object_name, object_id))
          || OB_INVALID_ID == object_id) {
        // try dblink synonym
        ObString tmp_name;
        uint64_t dblink_id = OB_INVALID_ID;
        if (OB_FAIL(ob_write_string(resolve_ctx_.allocator_, object_name, tmp_name))) {
          LOG_WARN("write string failed", K(ret));
        } else {
          ObString full_object_name = tmp_name.split_on('@');
          bool exist = false;
          if (!full_object_name.empty()) {
            ObString obj_name;
            // object_id is the synonym id
            if (OB_FAIL(schema_guard.get_dblink_id(tenant_id, tmp_name, dblink_id))
                || OB_INVALID_ID == dblink_id) {
              LOG_WARN("resolve synonym failed!", K(ret), K(object_db_id), K(tmp_name));
            } else if (OB_FAIL(schema_guard.get_object_with_synonym(tenant_id, cur_db_id, synonym_name, object_db_id,
                                                                    object_id, obj_name, exist, true))) {
              LOG_WARN("get synonym schema failed", K(ret), K(cur_db_id), K(synonym_name), K(object_name));
            } else if (!exist || OB_INVALID_ID == object_id) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("synonym not exist", K(ret), K(tenant_id), K(object_db_id), K(object_name), K(synonym_name));
            } else {
              type = DBLINK_PKG_NS;
            }
          } else {
            LOG_WARN("resolve synonym failed!", K(ret), K(object_db_id), K(object_name));
          }
        }
      } else {
        type = UDT_NS;
      }
    } else {
      type = PKG_NS;
    }
  } else {
    type = TABLE_NS;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_ID == object_id) {
    type = ExternalType::INVALID_VAR;
  } else {
    var_idx = static_cast<int64_t>(object_id);
    parent_id = object_db_id;
  }
  return ret;
}

int ObPLExternalNS::resolve_external_symbol(const common::ObString &name,
                                            ExternalType &type,
                                            ObPLDataType &data_type,
                                            uint64_t &parent_id,
                                            int64_t &var_idx) const
{
  int ret = OB_SUCCESS;
  SET_LOG_CHECK_MODE();
  data_type.reset();
  ObSQLSessionInfo &session_info = resolve_ctx_.session_info_;
  ObSchemaGetterGuard &schema_guard = resolve_ctx_.schema_guard_;
  common::ObDataType obj_type;
  switch (type) {
  case INVALID_VAR: {
    //first search package header var
    if (OB_NOT_NULL(parent_ns_)) {
      if (OB_FAIL(
          SMART_CALL(parent_ns_->resolve_symbol(name, type, data_type, parent_id, var_idx)))) {
        LOG_WARN("resolve package symbol failed", K(ret));
      } else if (type == ObPLExternalNS::LOCAL_VAR) {
        type =
          ObPLBlockNS::BLOCK_ROUTINE == parent_ns_->get_block_type() ? SUBPROGRAM_VAR : PKG_VAR;
        parent_id =
          SUBPROGRAM_VAR == type
            ? reinterpret_cast<uint64_t>(parent_ns_) : parent_ns_->get_package_id();
      }
    }
    if (OB_ISNULL(parent_ns_)
        || OB_ISNULL(parent_ns_->get_external_ns())) {
      //search standard package first
      if (OB_SUCC(ret)
          && lib::is_oracle_mode()
          && OB_INVALID_INDEX == var_idx && OB_INVALID_INDEX == parent_id) {
        OZ (search_in_standard_package(name, type, data_type, parent_id, var_idx));
      }
      //then package name
      if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx) {
        uint64_t tenant_id = session_info.get_effective_tenant_id();
        int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                        : COMPATIBLE_MYSQL_MODE;
        uint64_t db_id = OB_INVALID_ID;
        uint64_t package_id = OB_INVALID_ID;
        if (parent_id != OB_INVALID_INDEX) {
          db_id = parent_id;
        } else if (OB_FAIL(session_info.get_database_id(db_id))) {
          LOG_WARN("failed to get session database id", K(ret), K(db_id));
        }

        if (OB_SUCC(ret) && OB_INVALID_ID != db_id) {
          if (OB_FAIL(schema_guard.get_package_id(
              tenant_id, db_id, name, share::schema::PACKAGE_TYPE, compatible_mode, package_id))) {
            LOG_WARN("get package id failed", K(ret));
          } else if (OB_INVALID_ID == package_id
                    && (OB_INVALID_INDEX == parent_id
                        || is_oracle_sys_database_id(parent_id)
                        || is_oceanbase_sys_database_id(parent_id))) {
            if (OB_FAIL(schema_guard.get_package_id(OB_SYS_TENANT_ID,
                                                    OB_SYS_DATABASE_ID,
                                                    name,
                                                    share::schema::PACKAGE_TYPE,
                                                    compatible_mode,
                                                    package_id))) {
              LOG_WARN("get package id failed", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_INVALID_ID != package_id) {
            type = PKG_NS;
            parent_id = db_id;
            var_idx = static_cast<int64_t>(package_id);
          }
        }
      }
      //then database name
      if (OB_SUCC(ret) && is_mysql_mode() && OB_INVALID_INDEX == var_idx && OB_INVALID_INDEX == parent_id) {
        uint64_t tenant_id = session_info.get_effective_tenant_id();
        uint64_t db_id = OB_INVALID_ID;
        if (OB_FAIL(schema_guard.get_database_id(tenant_id, name, db_id))) {
          LOG_WARN("get database id failed", K(ret));
        } else if (OB_INVALID_ID == db_id) {
          type = ObPLExternalNS::INVALID_VAR;
        } else {
          type = DB_NS;
          parent_id = OB_INVALID_INDEX;
          var_idx = db_id;
        }
      }
      //then table name
      if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx) {
        uint64_t tenant_id = session_info.get_effective_tenant_id();
        uint64_t db_id = OB_INVALID_ID;
        uint64_t table_id = OB_INVALID_ID;
        if (parent_id != OB_INVALID_INDEX) {
          db_id = parent_id;
        } else {
          OZ (session_info.get_database_id(db_id));
        }

        if (OB_SUCC(ret) && OB_INVALID_ID != db_id) {
          OZ (schema_guard.get_table_id(tenant_id, db_id, name, false,
                                        schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id));
          if (OB_FAIL(ret)) {
          } else if (OB_INVALID_ID == table_id
                    && ObSQLUtils::is_oracle_sys_view(name)
                    && lib::is_oracle_mode()) {
            // try sys view
            OZ (schema_guard.get_table_id(tenant_id, ObString("SYS"), name, false,
                                          schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id));
          }
          if (OB_FAIL(ret)) {
          } else if (OB_INVALID_ID == table_id) {
            type = ObPLExternalNS::INVALID_VAR;
          } else {
            type = TABLE_NS;
            parent_id = db_id;
            var_idx = table_id;
          }
        }
      }
      // then udt type
      if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx && lib::is_oracle_mode()) {
        uint64_t tenant_id = session_info.get_effective_tenant_id();
        uint64_t db_id = OB_INVALID_ID;
        uint64_t udt_id = OB_INVALID_ID;
        if (parent_id != OB_INVALID_INDEX) {
          db_id = parent_id;
        } else {
          OZ (session_info.get_database_id(db_id));
        }

        if (OB_SUCC(ret) && OB_INVALID_ID != db_id) {
          const ObUDTTypeInfo *udt_info = NULL;
          OZ (schema_guard.get_udt_info(tenant_id, db_id, OB_INVALID_ID, name, udt_info));
          // 尝试去系统租户下查找
          if (NULL == udt_info
              && (OB_INVALID_ID == parent_id
                  || is_oracle_sys_database_id(parent_id)
                  || is_oceanbase_sys_database_id(parent_id))) {
            OZ (schema_guard.get_udt_info(OB_SYS_TENANT_ID,
                OB_SYS_DATABASE_ID, OB_INVALID_ID, name, udt_info));
            if (OB_SUCC(ret) && udt_info != NULL) {
              parent_id = OB_SYS_DATABASE_ID;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (NULL == udt_info) {
            type = ObPLExternalNS::INVALID_VAR;
          } else {
            udt_id = udt_info->get_type_id();
            type = ObPLExternalNS::ExternalType::UDT_NS;
            parent_id = parent_id == OB_INVALID_ID ? db_id : parent_id;
            var_idx = udt_id;

            if (OB_SUCC(ret)) {
              if (OB_NOT_NULL(dependency_table_)) {
                ObSchemaObjVersion obj_version;
                OX (obj_version.object_id_ = udt_id);
                OX (obj_version.object_type_ = DEPENDENCY_TYPE);
                OX (obj_version.version_ = udt_info->get_schema_version());
                OZ (add_dependency_object(obj_version));
              }
            }
          }
        }
      }
      // then routine
      if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx) {
        uint64_t tenant_id = session_info.get_effective_tenant_id();
        uint64_t db_id = OB_INVALID_ID;
        uint64_t udt_id = OB_INVALID_ID;
        if (parent_id != OB_INVALID_INDEX) {
          db_id = parent_id;
        } else {
          OZ (session_info.get_database_id(db_id));
        }
        const ObRoutineInfo *routine_info = NULL;
        OZ (schema_guard.get_standalone_procedure_info(tenant_id, db_id, name, routine_info));
        if (NULL == routine_info) {
          ret = OB_SUCCESS;
          OZ (schema_guard.get_standalone_function_info(tenant_id, db_id, name, routine_info));
        }
        if (NULL == routine_info) {
          ret = OB_SUCCESS;
          type = ObPLExternalNS::INVALID_VAR;
        } else {
          // udf/procedure will resolve later, here only avoid to resolve synonym
          type = ObPLExternalNS::INVALID_VAR;
          var_idx = routine_info->get_routine_id();
        }
      }
      //then synonym
      if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx) {
        bool exist = false;
        uint64_t tenant_id = session_info.get_effective_tenant_id();
        uint64_t db_id = OB_INVALID_ID;
        uint64_t object_db_id = OB_INVALID_ID;
        ObString object_name;
        ObSchemaChecker schema_checker;
        ObSynonymChecker synonym_checker;
        if (parent_id != OB_INVALID_INDEX) {
          db_id = parent_id;
        } else {
          OZ (session_info.get_database_id(db_id));
        }

        if (OB_SUCC(ret) && OB_INVALID_ID != db_id) {
          OZ (schema_checker.init(schema_guard, session_info.get_sessid()));
          OZ (ObResolverUtils::resolve_synonym_object_recursively(
            schema_checker, synonym_checker,
            tenant_id, db_id, name, object_db_id, object_name, exist, OB_INVALID_INDEX == parent_id));
          if (exist) {
            OZ (resolve_synonym(object_db_id, object_name, type, parent_id, var_idx, name, db_id));
          }
        }
      }
      //then database name
      if (OB_SUCC(ret) && is_oracle_mode() && OB_INVALID_INDEX == var_idx && OB_INVALID_INDEX == parent_id) {
        uint64_t tenant_id = session_info.get_effective_tenant_id();
        uint64_t db_id = OB_INVALID_ID;
        if (OB_FAIL(schema_guard.get_database_id(tenant_id, name, db_id))) {
          LOG_WARN("get database id failed", K(ret));
        } else if (OB_INVALID_ID == db_id) {
          type = ObPLExternalNS::INVALID_VAR;
        } else {
          type = DB_NS;
          parent_id = OB_INVALID_INDEX;
          var_idx = db_id;
        }
      }
      // 尝试看是不是系统变量的特殊写法，如 set SQL_MODE='ONLY_FULL_GROUP_BY';
      if (OB_SUCC(ret)
          && !resolve_ctx_.is_sql_scope_  // 纯SQL语境过来的表达式解析不需要尝试解析为SESSION VAR
          && ObPLExternalNS::INVALID_VAR == type && lib::is_mysql_mode()) {
        type = SESSION_VAR;
        if (OB_FAIL(
            SMART_CALL(resolve_external_symbol(name, type, data_type, parent_id, var_idx)))) {
          LOG_WARN("failed to resolve external symbol as global var", K(ret));
        }
      }
    }
  }
    break;
  case PKG_NS: {
    int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                    : COMPATIBLE_MYSQL_MODE;
    uint64_t tenant_id = session_info.get_effective_tenant_id();
    uint64_t package_id = OB_INVALID_ID;

    if (OB_FAIL(schema_guard.get_package_id(tenant_id, parent_id, name, share::schema::PACKAGE_TYPE,
                                            compatible_mode, package_id))) {
      LOG_WARN("get package id failed", K(ret));
    } else if (OB_INVALID_ID == package_id) {
      if (is_oceanbase_sys_database_id(parent_id)) {
        if (OB_FAIL(schema_guard.get_package_id(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID,
            name, share::schema::PACKAGE_TYPE, compatible_mode, package_id))) {
          LOG_WARN("get package id failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_INVALID_ID == package_id) {
        type = ObPLExternalNS::INVALID_VAR;
        LOG_WARN("package not exist", K(ret), K(parent_id), K(name));
      } else {
        var_idx = static_cast<int64_t>(package_id);
      }
    }
  }
    break;
  case PKG_VAR: {
    if (lib::is_mysql_mode()
        && get_tenant_id_by_object_id(parent_id) != OB_SYS_TENANT_ID
        && session_info.get_effective_tenant_id() != OB_SYS_TENANT_ID) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("package is not supported in Mysql mode", K(type), K(ret));
    } else {
      const share::schema::ObPackageInfo *package_info_resolve = NULL;
      const uint64_t tenant_id = get_tenant_id_by_object_id(parent_id);
      if (OB_FAIL(schema_guard.get_package_info(tenant_id, parent_id, package_info_resolve))) {
        LOG_WARN("get package info resolve failed", K(ret), K(tenant_id));
      } else if (NULL == package_info_resolve) {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        LOG_WARN("self or resolve package not exist", K(ret));
      } else {
        if (OB_NOT_NULL(parent_ns_)
            && ObCharset::case_compat_mode_equal(parent_ns_->get_package_name(), package_info_resolve->get_package_name())) {
          if (OB_FAIL(
              SMART_CALL(parent_ns_->resolve_symbol(name, type, data_type, parent_id, var_idx)))) {
            LOG_WARN("resolve package symbol failed", K(ret));
          } else if (OB_INVALID_INDEX == var_idx) {
            type = ObPLExternalNS::INVALID_VAR;
            LOG_WARN("package var not found", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx) {
          ObPLPackageManager &package_manager = session_info.get_pl_engine()->get_package_manager();
          const ObPLVar *var = NULL;
          const ObUserDefinedType *user_type = NULL;
          if (OB_FAIL(package_manager.get_package_var(resolve_ctx_, parent_id, name, var, var_idx))) {
            LOG_WARN("get package var failed", K(ret), K(parent_id), K(name));
          } else if (OB_ISNULL(var)) { // 不是PackageVar尝试下是不是PackageType
            if (OB_FAIL(package_manager.get_package_type(resolve_ctx_,
                                                         parent_id,
                                                         name,
                                                         user_type,
                                                         false))) {
              LOG_WARN("failed to get package type", K(ret), K(parent_id), K(name));
              if (OB_ERR_SP_UNDECLARED_TYPE == ret) {
                type = ObPLExternalNS::INVALID_VAR;
                ret = OB_SUCCESS;
              }
            } else if (OB_ISNULL(user_type)) {
              type = ObPLExternalNS::INVALID_VAR;
              LOG_WARN("package element not found", K(ret), K(parent_id), K(name));
            } else {
              var_idx = user_type->get_user_type_id();
              type = ObPLExternalNS::PKG_TYPE;
            }
          } else {
            OX (data_type = var->get_type());
          }
          if (OB_SUCC(ret) && type != ObPLExternalNS::INVALID_VAR) {
            if (OB_NOT_NULL(dependency_table_)) {
              ObSchemaObjVersion obj_version;
              OX (obj_version.object_id_ = parent_id);
              OX (obj_version.object_type_ = DEPENDENCY_PACKAGE);
              OX (obj_version.version_ = package_info_resolve->get_schema_version());
              OZ (add_dependency_object(obj_version));
            }
          }
        }
      }
    }
  }
    break;
  case TABLE_COL: {
    if (lib::is_mysql_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Table Column is not supported in Mysql mode now", K(type), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Table Column in Mysql mode");
    } else {
      const ObTableSchema* table_info = NULL;
      ObRecordType *record_type = NULL;
      const ObPLDataType *member_type = NULL;
      const uint64_t tenant_id = session_info.get_effective_tenant_id();
      OZ (schema_guard.get_table_schema(tenant_id, parent_id, table_info));
      CK (OB_NOT_NULL(table_info));
      OZ (ObPLResolver::build_record_type_by_schema(resolve_ctx_, table_info, record_type));
      CK (OB_NOT_NULL(record_type));
      int64_t i = 0;
      for (; OB_SUCC(ret) && i < record_type->get_member_count(); ++i) {
        const ObString *record_name = record_type->get_record_member_name(i);
        CK (OB_NOT_NULL(record_name));
        if (OB_SUCC(ret) && 0 == record_name->case_compare(name)) {
          CK (OB_NOT_NULL(member_type = record_type->get_record_member_type(i)));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(member_type)) {
          type = ObPLExternalNS::INVALID_VAR;
          LOG_WARN("table column not found", K(ret), K(name), K(parent_id));
        } else {
          ObSchemaObjVersion obj_version;
          ObDataType col_type;
          bool is_view = table_info->is_view_table() && !table_info->is_materialized_view();
          if (OB_NOT_NULL(dependency_table_)) {
            OX (obj_version.object_id_ = parent_id);
            OX (obj_version.object_type_ = is_view ? DEPENDENCY_VIEW : DEPENDENCY_TABLE);
            OX (obj_version.version_ = table_info->get_schema_version());
            OZ (add_dependency_object(obj_version));
          }
          OX (var_idx = i);
          OX (data_type = *member_type);
        }
      }
    }
  }
    break;
  case USER_VAR: {
    if (lib::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("User Variable is not supported in Oracle mode now", K(type), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "User Variable in Oracle mode");
    } else {
      if (NULL == (session_info.get_user_variable(name))) {
        //用户变量找不到可能是正常的，跳出 why? guangang.gg
      } else {
        obj_type.set_obj_type(session_info.get_user_variable(name)->meta_.get_type());
        data_type.set_data_type(obj_type);
      }
    }
  }
    break;
  case SESSION_VAR:
  case GLOBAL_VAR: {
    if (lib::is_oracle_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Sys var is not supported in Oracle mode now", K(type), K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "System Variable in Oracle mode");
    } else {
      ObBasicSysVar *sys_var = NULL;
        if (OB_FAIL(session_info.get_sys_variable_by_name(name, sys_var))) {
          if (OB_ERR_SYS_VARIABLE_UNKNOWN == ret) {
            //没找到退出即可，这里不报错，外面根据ObPLExternalNS::INVALID_VAR判断报错
            type = ObPLExternalNS::INVALID_VAR;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("Failed to get_sys_variable_by_name", K(name), K(ret));
          }
        } else {
          ObObj val = sys_var->get_value();
          obj_type.set_obj_type(val.get_type());
          data_type.set_data_type(obj_type);
        }
      }
    }
    break;
  default: {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid type", K(type), K(ret));
  }
    break;
  }
  if (OB_FAIL(ret) && !resolve_ctx_.is_sql_scope_) {
    // only reset in pl
    // udf in sql do not reset this error
    ObWarningBuffer *buf = common::ob_get_tsi_warning_buffer();
    if (NULL != buf) {
      buf->reset();
    }
  }
  CANCLE_LOG_CHECK_MODE();
  return ret;
}

int ObPLExternalNS::resolve_external_type_by_name(const ObString &db_name, const ObString &org_package_name,
                                                  const ObString &org_type_name, const ObUserDefinedType *&user_type,
                                                  bool try_synonym = true)
{
  int ret = OB_SUCCESS;
  user_type = NULL;
  const ObPackageInfo *package_info = NULL;
  if (OB_NOT_NULL(parent_ns_)) {
    OZ (SMART_CALL(parent_ns_->get_pl_data_type_by_name(
                    resolve_ctx_, db_name, org_package_name, org_type_name, user_type)),
      org_package_name, org_type_name);
  }
  if (OB_SUCC(ret) && OB_ISNULL(user_type)) {
    uint64_t tenant_id = resolve_ctx_.session_info_.get_effective_tenant_id();
    int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                    : COMPATIBLE_MYSQL_MODE;
    uint64_t db_id = OB_INVALID_ID;
    ObString package_name = org_package_name;
    ObString type_name = org_type_name;
    if (db_name.empty()) {
      if (OB_FAIL(resolve_ctx_.session_info_.get_database_id(db_id))) {
        LOG_WARN("get db id failed", K(ret));
      } else if (OB_INVALID_ID == db_id) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("database not valid", K(ret), K(db_id));
      }
    } else {
      if (OB_FAIL(resolve_ctx_.schema_guard_.get_database_id(tenant_id, db_name, db_id))) {
        LOG_WARN("get database id failed", K(ret));
      } else if (OB_INVALID_ID == db_id) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_WARN("db name not found", K(ret));
      }
    }
    if (OB_SUCC(ret) && !package_name.empty()) {
      // search package
      if (OB_FAIL(resolve_ctx_.schema_guard_.get_package_info(tenant_id, db_id, package_name,
                                                              PACKAGE_TYPE, compatible_mode,
                                                              package_info))) {
         LOG_WARN("get package id failed", K(ret));
      }
    }
    // Determine whether the package_name is the 'SYS' user in oracle mode
    bool is_oracle_sys_user = !package_name.empty() && OB_ISNULL(package_info)
                            && 0 == package_name.case_compare(OB_ORA_SYS_SCHEMA_NAME)
                            && lib::is_oracle_mode();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!is_oracle_sys_user && !package_name.empty()) {
      // search for package type
      if (OB_ISNULL(package_info)) {
        // try system package
        if (db_name.empty() || 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)) {
          if (OB_FAIL(resolve_ctx_.schema_guard_.get_package_info(OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID,
              package_name, PACKAGE_TYPE, compatible_mode, package_info))) {
            LOG_WARN("get package id failed", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_ISNULL(package_info)) {
          ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
          LOG_WARN("package not exist", K(package_name), K(ret));
          LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE",
                         db_name.length(), db_name.ptr(),
                         package_name.length(), package_name.ptr());
        }
      }
      if (OB_SUCC(ret)) {
        ObPLPackageManager &package_manager = resolve_ctx_.session_info_.get_pl_engine()->get_package_manager();
        uint64_t package_id = package_info->get_package_id();
        const ObUserDefinedType *package_user_type = NULL;
        ObPLDataType *copy_pl_type = NULL;
        if (OB_FAIL(package_manager.get_package_type(resolve_ctx_, package_id, type_name, package_user_type))) {
          LOG_WARN("get package type failed", K(ret), K(package_id), K(type_name), K(package_user_type));
        } else if (OB_ISNULL(package_user_type)) {
          ret = OB_ERR_SP_UNDECLARED_TYPE;
          LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE, type_name.length(), type_name.ptr());
        }
        CK (package_user_type->is_collection_type()
            || package_user_type->is_record_type()
            || package_user_type->is_subtype()
            || package_user_type->is_cursor_type());

        OZ (ObPLDataType::deep_copy_pl_type(
          resolve_ctx_.allocator_, *package_user_type, copy_pl_type));
        CK (OB_NOT_NULL(copy_pl_type));
        CK (OB_NOT_NULL(user_type = static_cast<ObUserDefinedType *>(copy_pl_type)));

        if (OB_SUCC(ret)) {
          ObSchemaObjVersion obj_version;
          obj_version.object_id_ = package_id;
          obj_version.object_type_ = DEPENDENCY_PACKAGE;
          obj_version.version_ = package_info->get_schema_version();
          if (OB_FAIL(add_dependency_object(obj_version))) {
            LOG_WARN("add dependency object failed", K(package_id), K(ret));
          }
        }
      }
    } else { // search for udt type
      const ObUDTTypeInfo *udt_info = NULL;
      const ObUserDefinedType *type = NULL;
      if (OB_SUCC(ret) && !is_oracle_sys_user) {
        bool exist = false;
        uint64_t object_db_id = OB_INVALID_ID;
        ObString object_name;
        ObSchemaChecker schema_checker;
        ObSynonymChecker synonym_checker;
        OZ (schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid()));
        OZ (resolve_ctx_.schema_guard_.get_udt_info(tenant_id, db_id, OB_INVALID_ID, type_name, udt_info));
      }
      if (OB_SUCC(ret) && (is_oracle_sys_user || OB_ISNULL(udt_info))) {
        // try system udt
        if (db_name.empty() || 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)) {
          OZ (resolve_ctx_.schema_guard_.get_udt_info(
            OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID, OB_INVALID_ID,
            type_name, udt_info));
        }
        if (OB_SUCC(ret) && OB_ISNULL(udt_info)) {
          ret = OB_ERR_SP_UNDECLARED_TYPE;
          LOG_WARN("failed to get udt info",
                   K(ret), K(db_name), K(type_name), K(db_id), K(package_name));
          LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE, type_name.length(), type_name.ptr());
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(udt_info->transform_to_pl_type(resolve_ctx_.allocator_, type))) {
        LOG_WARN("failed to transform to pl type from udt info", K(ret));
      } else if (OB_ISNULL(type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("user type is unexpected null", K(ret), K(user_type));
      } else {
        user_type = type;
        ObSchemaObjVersion obj_version;
        obj_version.object_id_ = udt_info->get_type_id();
        obj_version.object_type_ = DEPENDENCY_TYPE;
        obj_version.version_ = udt_info->get_schema_version();
        if (OB_FAIL(add_dependency_object(obj_version))) {
          LOG_WARN("add dependency object failed", K(obj_version), K(*udt_info), K(ret));
        }
      }
    }
    // schema object, will try synonym
    if (OB_SUCC(ret) && OB_ISNULL(user_type) && try_synonym) {
      bool exist = false;
      uint64_t object_db_id = OB_INVALID_ID;
      ObString object_name;
      ObSchemaChecker schema_checker;
      ObSynonymChecker synonym_checker;
      if (OB_FAIL(schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid()))) {
        LOG_WARN("failed to init schema checker for resolve synonym", K(ret));
      } else if (!package_name.empty()) {
        if (OB_FAIL(ObResolverUtils::resolve_synonym_object_recursively(schema_checker,
                                                                        synonym_checker,
                                                                        tenant_id,
                                                                        db_id,
                                                                        package_name,
                                                                        object_db_id,
                                                                        object_name,
                                                                        exist,
                                                                        db_name.empty()))) {
          LOG_WARN("failed to resolve synonym object", K(ret), K(package_name), K(db_id), K(tenant_id));
        } else if (exist) {
          package_name = object_name;
          db_id = object_db_id;
        }
      } else if (!type_name.empty()) {
        if (OB_FAIL(ObResolverUtils::resolve_synonym_object_recursively(schema_checker,
                                                                        synonym_checker,
                                                                        tenant_id,
                                                                        db_id,
                                                                        type_name,
                                                                        object_db_id,
                                                                        object_name,
                                                                        exist,
                                                                        db_name.empty()))) {
          LOG_WARN("failed to resolve synonym object", K(ret), K(package_name), K(db_id), K(tenant_id));
        } else if (exist) {
          type_name = object_name;
          db_id = object_db_id;
        }
      }
      if (OB_SUCC(ret) && exist) {
        const share::schema::ObDatabaseSchema *db_schema = NULL;
        OZ (schema_checker.get_database_schema(tenant_id, db_id, db_schema));
        CK (OB_NOT_NULL(db_schema));
        OZ (resolve_external_type_by_name(db_schema->get_database_name_str(), package_name, type_name, user_type, false));
      }
    }
  }
  return ret;
}

int ObPLExternalNS::resolve_external_type_by_id(uint64_t type_id, const ObUserDefinedType *&user_type)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parent_ns_)) {
    OZ (SMART_CALL(parent_ns_->get_pl_data_type_by_id(type_id, user_type)));
  }
  if (OB_SUCC(ret) && OB_ISNULL(user_type)) {
    OZ (resolve_ctx_.get_user_type(type_id, user_type, &(resolve_ctx_.allocator_)), type_id);
    if (OB_SUCC(ret) && OB_ISNULL(user_type)) {
      ret = OB_ERR_SP_UNDECLARED_TYPE;
      LOG_WARN("can not resolve external type by user type id", K(ret), K(type_id), K(user_type));
    }
  }
  if (OB_SUCC(ret)
      && OB_NOT_NULL(user_type)
      && user_type->is_record_type()) {
    ObPLDataType *copy_pl_type = NULL;
    OZ (ObPLDataType::deep_copy_pl_type(resolve_ctx_.allocator_, *user_type, copy_pl_type));
    OV (OB_NOT_NULL(copy_pl_type), OB_ERR_UNEXPECTED, KPC(user_type), KPC(copy_pl_type));
    OX (user_type = static_cast<const ObUserDefinedType *>(copy_pl_type));
  }
  return ret;
}

int ObPLExternalNS::resolve_external_routine(const ObString &db_name,
                                             const ObString &package_name,
                                             const ObString &routine_name,
                                             const common::ObIArray<ObRawExpr *> &expr_params,
                                             ObProcType &routine_type,
                                             ObIArray<const ObIRoutineInfo *> &routine_infos) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(parent_ns_)) {
    if (OB_FAIL(SMART_CALL(parent_ns_->resolve_routine(resolve_ctx_,
                                                       db_name,
                                                       package_name,
                                                       routine_name,
                                                       expr_params,
                                                       routine_type,
                                                       routine_infos)))) {
      LOG_WARN("resolve routine failed", K(routine_name), K(ret));
    }
  }
  if (OB_SUCC(ret) && routine_infos.empty()) {
    const ObRoutineInfo *schema_routine_info = NULL;
    ObRoutineType schema_routine_type =
      is_procedure(routine_type) ? ROUTINE_PROCEDURE_TYPE : ROUTINE_FUNCTION_TYPE;
    if (OB_FAIL(ObResolverUtils::get_routine(resolve_ctx_,
                                          resolve_ctx_.session_info_.get_effective_tenant_id(),
                                          resolve_ctx_.session_info_.get_database_name(),
                                          db_name,
                                          package_name,
                                          routine_name,
                                          schema_routine_type,
                                          expr_params,
                                          schema_routine_info))) {
      LOG_WARN("failed to get routine info",
               K(ret), K(db_name), K(package_name), K(routine_name));
    } else {
      // todo: dependency on udt functions
      ObSchemaObjVersion obj_version;
      obj_version.object_id_ = schema_routine_info->get_routine_id();
      obj_version.object_type_ = is_procedure(routine_type) ? DEPENDENCY_PROCEDURE : DEPENDENCY_FUNCTION;
      obj_version.version_ = schema_routine_info->get_schema_version();
      if (OB_FAIL(add_dependency_object(obj_version))) {
        LOG_WARN("add dependency object failed", "package_id", schema_routine_info->get_package_id(), K(ret));
      } else {
        OZ (routine_infos.push_back(schema_routine_info));
      }
    }
  }
  return ret;
}

int ObPLExternalNS::check_routine_exists(const ObString &db_name,
                                          const ObString &package_name,
                                          const ObString &routine_name,
                                          const share::schema::ObRoutineType routine_type,
                                          bool &exists,
                                          pl::ObProcType &proc_type,
                                          uint64_t udt_id) const
{
  int ret = OB_SUCCESS;
  exists = false;
  proc_type = INVALID_PROC_TYPE;
  if (OB_NOT_NULL(parent_ns_)) {
    if (OB_FAIL(parent_ns_->check_routine_exists(db_name, package_name, routine_name,
             routine_type, exists, proc_type, udt_id))) {
      LOG_WARN("resolve routine failed",
               K(db_name), K(package_name), K(routine_name),
               K(routine_type), K(exists), K(proc_type), K(udt_id), K(ret));
    }
  }
  if (OB_SUCC(ret) && !exists) {
    ObSchemaChecker schema_checker;
    if (OB_FAIL(schema_checker.init(resolve_ctx_.schema_guard_, resolve_ctx_.session_info_.get_sessid()))) {
      LOG_WARN("schema checker init failed", K(ret));
    } else if (OB_FAIL(ObResolverUtils::check_routine_exists(schema_checker, resolve_ctx_.session_info_, db_name,
      package_name, routine_name, routine_type, exists, udt_id))) {
      LOG_WARN("resolve routine failed",
               K(db_name), K(package_name), K(routine_name),
               K(routine_type), K(exists), K(proc_type), K(udt_id), K(ret));
    } else if (exists) {
      proc_type = ROUTINE_PROCEDURE_TYPE == routine_type ? STANDALONE_PROCEDURE : STANDALONE_FUNCTION;
    } else { /*do nothing*/ }
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
#define SET_ACCESS(idx, data_type) \
    do { \
      if (OB_SUCC(ret)) { \
        var_idx = idx +1; \
        ObPLDataType property_type; \
        property_type.set_data_type(data_type); \
        new(&access_idx)ObObjAccessIdx(property_type, ObObjAccessIdx::IS_PROPERTY, \
                                       attr_name, property_type, var_idx); \
      } \
    } while(0)

#define SET_ACCESS_AA(idx, data_type) \
    do { \
      if (OB_SUCC(ret)) { \
        if (user_type.is_associative_array_type()) {\
          const ObAssocArrayType &assoc_type = static_cast<const ObAssocArrayType&>(user_type);\
          const ObDataType *dt= assoc_type.get_index_type().get_data_type();\
          CK (OB_NOT_NULL(dt));\
          SET_ACCESS(IDX_COLLECTION_FIRST, *dt);\
        } else {\
          SET_ACCESS(IDX_COLLECTION_FIRST, data_type);\
        }\
      } \
    } while(0)

int ObPLBlockNS::add_column_conv_for_coll_func(
  ObSQLSessionInfo &session_info,
  ObRawExprFactory &expr_factory,
  const ObUserDefinedType *user_type,
  const ObString &attr_name,
  ObRawExpr *&expr) const
{
  int ret = OB_SUCCESS;
  ObDataType expected_type;
  CK (OB_NOT_NULL(user_type));
  CK (OB_NOT_NULL(expr));
  if (OB_FAIL(ret)) {
  } else if (0 == attr_name.case_compare("prior")
      || 0 == attr_name.case_compare("next")
      || 0 == attr_name.case_compare("exists")
      || 0 == attr_name.case_compare("delete")) {
    if (user_type->is_associative_array_type()) {
      const ObAssocArrayType *assoc_type = static_cast<const ObAssocArrayType *>(user_type);
      CK (OB_NOT_NULL(assoc_type));
      if (OB_SUCC(ret)
          && assoc_type->get_index_type().get_data_type() != NULL) {
        expected_type = *(assoc_type->get_index_type().get_data_type());
      }
    } else {
      expected_type.set_obj_type(ObInt32Type);
    }
  } else if (0 == attr_name.case_compare("extend")
              || 0 == attr_name.case_compare("trim")) {
    expected_type.set_obj_type(ObInt32Type);
  }
  if (OB_SUCC(ret) && expected_type.get_obj_type() != ObNullType) {
    OZ (ObRawExprUtils::build_column_conv_expr(&session_info,
                                               expr_factory,
                                               expected_type.get_obj_type(),
                                               expected_type.get_collation_type(),
                                               expected_type.get_accuracy_value(),
                                               true,
                                               NULL,
                                               NULL,
                                               expr,
                                               true));
  }
  return ret;
}
#endif

int ObPLBlockNS::find_sub_attr_by_name(const ObUserDefinedType &user_type,
                                       const ObObjAccessIdent &access_ident,
                                       ObSQLSessionInfo &session_info,
                                       ObRawExprFactory &expr_factory,
                                       ObPLCompileUnitAST &func,
                                       ObObjAccessIdx &access_idx,
                                       ObPLDataType &data_type,
                                       uint64_t &package_id,
                                       int64_t &var_idx) const
{
  int ret = OB_SUCCESS;
  const ObString attr_name = access_ident.access_name_;
  if (user_type.is_record_type()) {
    const ObRecordType &record_type = static_cast<const ObRecordType &>(user_type);
    int64_t member_index = record_type.get_record_member_index(attr_name);
    if (member_index != OB_INVALID_INDEX) {
      if (OB_ISNULL(record_type.get_record_member_type(member_index))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("type is invalid", K(ret));
      } else {
        new(&access_idx)ObObjAccessIdx(*record_type.get_record_member_type(member_index),
            ObObjAccessIdx::IS_CONST, attr_name, *record_type.get_record_member_type(member_index),
            member_index);
      }
    } else {
      ret = OB_ERR_SP_UNDECLARED_VAR;
      LOG_WARN("PLS-00302: component 'A' must be declared", K(ret), K(access_ident), K(user_type));
    }
#ifdef OB_BUILD_ORACLE_PL
  } else if (user_type.is_collection_type()) {
    ObPLExternalNS::ExternalType type = ObPLExternalNS::INVALID_VAR;
    // declare v vvv, last number; val number; begin val = v.last; end; vvv 是 varray
    // 这种场景下，last会被识别为一个local，所以collection的类型需要设置type id，
    // 确保在resolve symbol的时候不会找错。
    if (OB_INVALID_ID == package_id) {
      package_id = user_type.get_user_type_id();
    }
    if (OB_FAIL(resolve_symbol(attr_name, type, data_type, package_id, var_idx))) {
      LOG_WARN("get var index by name failed", K(ret));
    } else if (ObPLExternalNS::INVALID_VAR == type) {
      ObDataType data_type;
      data_type.set_int();
      if (0 == attr_name.case_compare("count")) { // TODO: bug!!! @ryan.ly @yuchen.wyc

        SET_ACCESS(IDX_COLLECTION_COUNT, data_type);

      } else  if (0 == attr_name.case_compare("first")) {

        SET_ACCESS_AA(IDX_COLLECTION_FIRST, data_type);

      } else if (0 == attr_name.case_compare("last")) {

        SET_ACCESS_AA(IDX_COLLECTION_LAST, data_type);

      } else if (0 == attr_name.case_compare("limit")){

        SET_ACCESS(IDX_VARRAY_CAPACITY, data_type);

      } else if (0 == attr_name.case_compare("prior")
                 || 0 == attr_name.case_compare("next")
                 || 0 == attr_name.case_compare("exists")) {
        if (0 == attr_name.case_compare("exists")) {
          data_type.set_obj_type(ObTinyIntType);
          SET_ACCESS(IDX_COLLECTION_PLACEHOLD, data_type);
        } else {
          SET_ACCESS_AA(IDX_COLLECTION_PLACEHOLD, data_type);
        }
        if (OB_SUCC(ret) && access_ident.params_.count() > 1) {
          ret = OB_ERR_CALL_WRONG_ARG;
          LOG_WARN("call collection method with wrong parameter",
                    K(ret), K(access_ident.params_), K(attr_name));
          LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, attr_name.length(), attr_name.ptr());
        }
        ARRAY_FOREACH(access_ident.params_, idx) {
          ObRawExpr *param_expr = access_ident.params_.at(idx).first;
          int64_t expr_idx = OB_INVALID_INDEX;
          OZ (add_column_conv_for_coll_func(
            session_info, expr_factory, &user_type, attr_name, param_expr));
          if (OB_FAIL(ret)) {
          } else if (!has_exist_in_array(func.get_exprs(), param_expr, &expr_idx)) {
            OZ (func.add_expr(param_expr));
            OX (expr_idx = func.get_exprs().count() - 1);
          }
          OZ (access_idx.type_method_params_.push_back(expr_idx));
        }

      } else if (0 == attr_name.case_compare("extend")
                 || 0 == attr_name.case_compare("delete")
                 || 0 == attr_name.case_compare("trim")) {
        ObPLDataType invalid_pl_data_type;
        new(&access_idx)ObObjAccessIdx(
          invalid_pl_data_type, ObObjAccessIdx::IS_TYPE_METHOD, attr_name, invalid_pl_data_type);
        if (access_ident.params_.count() > 2) {
          ret = OB_ERR_CALL_WRONG_ARG;
          LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, attr_name.length(), attr_name.ptr());
        }
        ARRAY_FOREACH(access_ident.params_, idx) {
          ObRawExpr *param_expr = access_ident.params_.at(idx).first;
          int64_t expr_idx = OB_INVALID_INDEX;
          OZ (add_column_conv_for_coll_func(
              session_info, expr_factory, &user_type, attr_name, param_expr));
          if (OB_FAIL(ret)) {
          } else if (!has_exist_in_array(func.get_exprs(), param_expr, &expr_idx)) {
            OZ (func.add_expr(param_expr));
            OX (expr_idx = func.get_exprs().count() - 1);
          }
          OZ (access_idx.type_method_params_.push_back(expr_idx));
        }
      } else {
        ret = OB_ERR_SP_UNDECLARED_VAR;
        LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR, attr_name.length(), attr_name.ptr());
      }
    } else {
      const ObCollectionType &collection_type = static_cast<const ObCollectionType &>(user_type);
      new(&access_idx)ObObjAccessIdx(collection_type.get_element_type(),
                                     static_cast<ObObjAccessIdx::AccessType>(type),
                                     attr_name,
                                     data_type,
                                     var_idx);
    }
#endif
  } else {
    ret = OB_ERR_COMPONENT_UNDECLARED;
    LOG_USER_ERROR(OB_ERR_COMPONENT_UNDECLARED, attr_name.length(), attr_name.ptr());
  }
  return ret;
}

int ObPLBlockNS::resolve_local_symbol(const ObString &name,
                                      ObPLExternalNS::ExternalType &type,
                                      ObPLDataType &data_type,
                                      int64_t &var_idx) const
{
  int ret = OB_SUCCESS;
  var_idx = OB_INVALID_ID;
  for (int64_t i = 0;
       OB_SUCC(ret) && OB_INVALID_ID == var_idx && i < get_symbols().count(); ++i) {
    const ObPLVar *local_var = symbol_table_->get_symbol(get_symbols().at(i));
    if (OB_ISNULL(local_var)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current local var is NULL", K(ret), K(local_var), K(i), K(get_symbols().at(i)));
    } else if (ObCharset::case_compat_mode_equal(local_var->get_name(), name)) {
      data_type = local_var->get_type();
      var_idx = get_symbols().at(i);
      type = get_block_type() != ObPLBlockNS::BlockType::BLOCK_ROUTINE
              ? ObPLExternalNS::PKG_VAR : ObPLExternalNS::LOCAL_VAR;
    }
  }
  // 尝试匹配为当前NS的TYPE
  if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_types().count(); ++i) {
      const ObUserDefinedType* user_type = type_table_->get_type(get_types().at(i));
      CK (OB_NOT_NULL(user_type));
      if (OB_FAIL(ret)) {
      } else if (ObCharset::case_compat_mode_equal(name, user_type->get_name())) {
        if (var_idx != OB_INVALID_INDEX) {
          ret = OB_ERR_DECL_MORE_THAN_ONCE;
          LOG_USER_ERROR(OB_ERR_DECL_MORE_THAN_ONCE, name.length(), name.ptr());
        } else {
          var_idx = user_type->get_user_type_id();
          type = get_block_type() != ObPLBlockNS::BlockType::BLOCK_ROUTINE
                  ? ObPLExternalNS::PKG_TYPE : ObPLExternalNS::LOCAL_TYPE;
        }
      }
    }
  }
  return ret;
}

int ObPLBlockNS::resolve_local_label(const ObString &name,
                                     ObPLExternalNS::ExternalType &type,
                                     int64_t &var_idx) const
{
  int ret = OB_SUCCESS;
  var_idx = OB_INVALID_ID;
  for (int64_t i = 0;
       OB_SUCC(ret) && OB_INVALID_ID == var_idx && i < get_labels().count(); ++i) {
    const ObString *label = label_table_->get_label(get_labels().at(i));
    if (OB_ISNULL(label)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current label is NULL", K(ret), K(label), K(i), K(get_labels().at(i)));
    } else if (ObCharset::case_compat_mode_equal(*label, name)) {
      var_idx = reinterpret_cast<int64_t>(this);
      type = ObPLExternalNS::LABEL_NS;
    }
  }
  return ret;
}

int ObPLBlockNS::search_parent_next_ns(const ObPLBlockNS *parent_ns,
                                       const ObPLBlockNS *current_ns,
                                       const ObPLBlockNS *&next_ns)
{
  int ret = OB_SUCCESS;
  const ObPLBlockNS *current_explicit_ns = current_ns->explicit_block() ? current_ns : NULL;
  const ObPLBlockNS *iter_ns = current_ns;
  while (OB_NOT_NULL(iter_ns)) {
    if (OB_NOT_NULL(iter_ns->pre_ns_)) {
      if (iter_ns->pre_ns_ == parent_ns) {
        next_ns = current_explicit_ns;
        break;
      } else {
        iter_ns = iter_ns->pre_ns_;
        current_explicit_ns = iter_ns->explicit_block() ? iter_ns : current_explicit_ns;
      }
    } else if (OB_NOT_NULL(iter_ns->get_external_ns())
              && OB_NOT_NULL(iter_ns->get_external_ns()->get_parent_ns())) {
      if (iter_ns->get_external_ns()->get_parent_ns() == parent_ns) {
        next_ns = current_explicit_ns;
        break;
      } else {
        iter_ns = iter_ns->get_external_ns()->get_parent_ns();
        current_explicit_ns = iter_ns->explicit_block() ? iter_ns : current_explicit_ns;
      }
    } else {
      break;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(next_ns)) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_WARN("can not found parent ns in current ns search", K(ret));
  }
  return ret;
}

int ObPLBlockNS::search_parent_next_ns(const ObPLBlockNS *parent_ns,
                                       const ObPLBlockNS *&next_ns) const
{
  int ret = OB_SUCCESS;
  next_ns = NULL;
  if (parent_ns == this) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_WARN("can not found parent next ns, cause current ns is already is parent ns",
             K(ret));
  } else if (OB_NOT_NULL(pre_ns_)) {
    if (pre_ns_ == parent_ns) {
        next_ns = this;
    } else {
      OZ (pre_ns_->search_parent_next_ns(parent_ns, next_ns));
    }
  } else if (OB_NOT_NULL(external_ns_)) {
    if (OB_NOT_NULL(external_ns_->get_parent_ns())) {
      if (external_ns_->get_parent_ns() == parent_ns) {
        next_ns = this;
      } else {
        OZ (external_ns_->get_parent_ns()->search_parent_next_ns(parent_ns, next_ns));
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(next_ns)) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_WARN("can not found parent ns in current ns search", K(ret));
  }
  return ret;
}

int ObPLBlockNS::resolve_label_symbol(const ObString &name,
                                      ObPLExternalNS::ExternalType &type,
                                      ObPLDataType &data_type,
                                      const ObPLBlockNS *&parent_ns,
                                      int64_t &var_idx) const
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(parent_ns));
  const ObPLBlockNS *next_ns = NULL;
  if (OB_FAIL(ret)) {
  } else if (ObPLBlockNS::BLOCK_ROUTINE == parent_ns->get_block_type()) {
    OZ (ObPLBlockNS::search_parent_next_ns(parent_ns, this, next_ns));
    CK (OB_NOT_NULL(next_ns));
    OZ (next_ns->resolve_local_symbol(name, type, data_type, var_idx));
  } else {
    // self package variable, search package spec first.
    OX (next_ns = parent_ns);
    CK (ObPLBlockNS::BLOCK_PACKAGE_SPEC == next_ns->get_block_type());
    OZ (next_ns->resolve_local_symbol(name, type, data_type, var_idx));
    if (OB_SUCC(ret) && ObPLExternalNS::ExternalType::INVALID_VAR == type) {
      // then searh package body.
      OZ (ObPLBlockNS::search_parent_next_ns(parent_ns, this, next_ns));
      CK (OB_NOT_NULL(next_ns));
      if (OB_SUCC(ret) && ObPLBlockNS::BLOCK_PACKAGE_BODY == next_ns->get_block_type()) {
        OZ (next_ns->resolve_local_symbol(name, type, data_type, var_idx));
      }
    }
  }
  if (OB_SUCC(ret)
      && (ObPLExternalNS::ExternalType::LOCAL_VAR == type
          || ObPLExternalNS::ExternalType::LOCAL_TYPE == type
          || ObPLExternalNS::ExternalType::PKG_VAR == type
          || ObPLExternalNS::ExternalType::PKG_TYPE == type)) {
    if (next_ns->get_block_type() != ObPLBlockNS::BlockType::BLOCK_ROUTINE) {
      parent_ns = next_ns; // record package correct namespace for package id.
    } else if (next_ns->get_symbol_table() != get_symbol_table()) {
      if (ObPLExternalNS::ExternalType::LOCAL_VAR == type) {
        type = ObPLExternalNS::ExternalType::SUBPROGRAM_VAR;
      } else {
        // TODO: SUBPROGRAM_TYPE
      }
    }
  }
  if (OB_SUCC(ret) && ObPLExternalNS::ExternalType::INVALID_VAR == type) {
    OZ (next_ns->resolve_local_label(name, type, var_idx));
  }
  if (OB_SUCC(ret) && ObPLExternalNS::ExternalType::INVALID_VAR == type) {
    parent_ns = next_ns;
    OZ (ObPLBlockNS::search_parent_next_ns(parent_ns, this, next_ns));
    OX (next_ns = next_ns->get_pre_ns());
    while (OB_SUCC(ret)
          && ObPLExternalNS::ExternalType::INVALID_VAR == type
          && next_ns != parent_ns && OB_NOT_NULL(next_ns)) {
      OZ (next_ns->resolve_local_label(name, type, var_idx));
      if (OB_SUCC(ret) && ObPLExternalNS::ExternalType::INVALID_VAR == type) {
        next_ns = next_ns->get_pre_ns();
      }
    }
  }
  if (OB_SUCC(ret)
      && ObPLExternalNS::ExternalType::INVALID_VAR == type
      && ObPLBlockNS::BLOCK_ROUTINE == parent_ns->get_block_type()) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_WARN("failed to resolve label symbol", K(ret), K(name), K(type));
  }
  return ret;
}

int ObPLBlockNS::resolve_symbol(const ObString &var_name,
                                ObPLExternalNS::ExternalType &type,
                                ObPLDataType &data_type,
                                uint64_t &parent_id,
                                int64_t &var_idx,
                                bool resolve_external) const
{
  int ret = OB_SUCCESS;
  data_type.reset();
  if (OB_ISNULL(symbol_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("symbol table is null", K(symbol_table_), K(ret));
  } else if (ObPLExternalNS::INVALID_VAR != type) {
    if (OB_INVALID_INDEX == var_idx && OB_NOT_NULL(external_ns_)) {
      OZ (SMART_CALL(
        external_ns_->resolve_external_symbol(var_name, type, data_type, parent_id, var_idx)));
      if (OB_SUCC(ret) && data_type.is_composite_type()) { // 来自外部的user_type需要在本namespace中展开
        const ObUserDefinedType *user_type = NULL;
        ObSEArray<ObDataType, 8> types;
        OZ (get_pl_data_type_by_id(data_type.get_user_type_id(), user_type));
        CK (OB_NOT_NULL(user_type));
        OZ (expand_data_type(user_type, types));
        OZ (type_table_->add_external_type(user_type));
      }
    }
  } else {
    // 尝试匹配为当前NS的VAR
    for (int64_t i = 0;
         OB_SUCC(ret)
         && OB_INVALID_INDEX == var_idx
         && OB_INVALID_INDEX == parent_id
         && i < get_symbols().count(); ++i) {
      ObPLVar *pl_var = const_cast<ObPLVar *>(symbol_table_->get_symbol(get_symbols().at(i)));
      if (OB_ISNULL(pl_var)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("PL var ns is null", K(i), K(get_symbols().at(i)), K(ret));
      } else if (ObCharset::case_compat_mode_equal(var_name, pl_var->get_name())) {
        bool is_referenced = true;
        pl_var->set_is_referenced(is_referenced);
        if (pl_var->is_dup_declare()) {
          ret = OB_ERR_DECL_MORE_THAN_ONCE;
          LOG_USER_ERROR(OB_ERR_DECL_MORE_THAN_ONCE, var_name.length(), var_name.ptr());
        } else {
          data_type = pl_var->get_type();
          parent_id = package_id_;
          var_idx = get_symbols().at(i);
          type = BLOCK_ROUTINE == get_block_type()
                                  ? ObPLExternalNS::LOCAL_VAR : ObPLExternalNS::PKG_VAR;
        }
      } else { /*do nothing*/ }
    }
    // 尝试匹配为当前NS的TYPE
    if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx && OB_INVALID_INDEX == parent_id) {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_types().count(); ++i) {
        const ObUserDefinedType* user_type = type_table_->get_type(get_types().at(i));
        CK (OB_NOT_NULL(user_type));
        if (OB_FAIL(ret)) {
        } else if (ObCharset::case_compat_mode_equal(var_name, user_type->get_name())) {
          if (var_idx != OB_INVALID_INDEX) {
            ret = OB_ERR_DECL_MORE_THAN_ONCE;
            LOG_USER_ERROR(OB_ERR_DECL_MORE_THAN_ONCE, var_name.length(), var_name.ptr());
          } else {
            parent_id = package_id_;
            var_idx = user_type->get_user_type_id();
            type = BLOCK_ROUTINE == get_block_type()
                    ? ObPLExternalNS::LOCAL_TYPE : ObPLExternalNS::PKG_TYPE;
          }
        }
      }
      if (OB_SUCC(ret)
          && OB_INVALID_INDEX == var_idx
          && OB_INVALID_INDEX == parent_id
          && BLOCK_OBJECT_SPEC == get_block_type()
          && ObCharset::case_compat_mode_equal(var_name, get_package_name())) {
        parent_id = get_database_id();
      }
    }
    // 尝试匹配为当前NS的Label
    for (int64_t i = 0;
         OB_SUCC(ret)
         && OB_INVALID_INDEX == var_idx
         && OB_INVALID_INDEX == parent_id
         && i < get_labels().count(); ++i) {
      const ObString *label = label_table_->get_label(get_labels().at(i));
      if (OB_ISNULL(label)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("PL Label is NULL", K(ret), K(i), K(get_labels().at(i)));
      } else if (ObCharset::case_compat_mode_equal(var_name, *label)) {
        var_idx = reinterpret_cast<int64_t>(this);
        type = ObPLExternalNS::LABEL_NS;
      }
    }
    // 尝试匹配为父NS的VAR或者TYPE
    if (OB_SUCC(ret)
        && OB_INVALID_INDEX == var_idx
        && OB_INVALID_INDEX == parent_id) {
      if (OB_NOT_NULL(pre_ns_)) {
        if (OB_FAIL(SMART_CALL(pre_ns_->resolve_symbol(var_name, type, data_type, parent_id, var_idx)))) {
          LOG_WARN("get var index by name failed", K(var_name), K(ret));
        }
      }
    }
    // try attribute of self argument
    if (OB_SUCC(ret)
        && OB_NOT_NULL(symbol_table_->get_self_param())
        && OB_INVALID_INDEX == var_idx
        && OB_INVALID_INDEX == parent_id) {
      const ObPLDataType &pl_type = symbol_table_->get_self_param()->get_type();
      const ObUserDefinedType *user_type = NULL;
      const ObRecordType *record_type = NULL;
      if (!pl_type.is_udt_type() || pl_type.is_opaque_type()) {
        // type is invalid when create udt & opaque type has not attribute, so do nothing ...
      } else if (OB_FAIL(get_user_type(pl_type.get_user_type_id(), user_type))) {
        LOG_WARN("failed to get user type", K(ret), KPC(user_type));
      } else if (OB_ISNULL(user_type) || !user_type->is_object_type()) {
        // do nothing ...
      } else if (OB_ISNULL(record_type = static_cast<const ObRecordType*>(user_type))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected record type", K(ret), KPC(record_type), KPC(user_type));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < record_type->get_record_member_count(); ++i) {
          if (OB_ISNULL(record_type->get_record_member_name(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected record member name", K(ret), K(i), KPC(record_type));
          } else if (ObCharset::case_compat_mode_equal(var_name, *record_type->get_record_member_name(i))) {
            type = ObPLExternalNS::SELF_ATTRIBUTE;
            var_idx = i;
            parent_id = user_type->get_user_type_id();
            break;
          }
        }
      }
    }
    // 尝试解析为外部符号
    if (OB_SUCC(ret) && OB_INVALID_INDEX == var_idx && resolve_external) {
      if (OB_NOT_NULL(external_ns_)) {
        OZ (SMART_CALL(
          external_ns_->resolve_external_symbol(var_name, type, data_type, parent_id, var_idx)));
        if (OB_SUCC(ret) && data_type.is_composite_type()) { // 来自外部的user_type需要在本namespace中展开
          const ObUserDefinedType *user_type = NULL;
          ObSEArray<ObDataType, 8> types;
          OZ (get_pl_data_type_by_id(data_type.get_user_type_id(), user_type), K(data_type));
          CK (OB_NOT_NULL(user_type));
          OZ (expand_data_type(user_type, types));
          OZ (type_table_->add_external_type(user_type));
        }
      }
    }

  }
  return ret;
}

const ObPLBlockNS* ObPLBlockNS::get_udt_routine_ns() const
{
  const ObPLBlockNS *result = NULL;
  if (is_udt_routine()) {
    result = this;
  } else if (OB_NOT_NULL(pre_ns_)) {
    result = pre_ns_->get_udt_routine_ns();
  } else if (OB_NOT_NULL(external_ns_)
              && OB_NOT_NULL(external_ns_->get_parent_ns())) {
    result = external_ns_->get_parent_ns()->get_udt_routine_ns();
  }
  return result;
}

int ObPLBlockNS::resolve_udt_symbol(
                                uint64_t udt_id,
                                const ObString &var_name,
                                ObPLExternalNS::ExternalType &type,
                                ObPLDataType &data_type,
                                uint64_t &parent_id,
                                int64_t &var_idx,
                                ObString &parent_udt_name) const
{
  UNUSED(parent_udt_name);
  int ret = OB_SUCCESS;
  // 尝试udt object的属性
  if (OB_INVALID_ID == var_idx) {
    bool is_inside_udt_func = is_udt_routine();
    while (!is_inside_udt_func) {
      if (OB_NOT_NULL(pre_ns_)) {
        is_inside_udt_func = pre_ns_->is_udt_routine();
      } else {
        break;
      }
    }
    if (is_inside_udt_func) {
      if (OB_INVALID_ID == udt_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("udt id is invalid while routine is udt function", K(udt_id),
                                                                    K(get_package_name()));
      } else {
        const ObUserDefinedType *user_type = NULL;
        if (OB_FAIL(get_pl_data_type_by_id(udt_id, user_type))) {
          LOG_WARN("failed to get pl user type", K(ret));
        } else if (OB_NOT_NULL(user_type)) {
          if (user_type->is_record_type() && user_type->is_udt_type()) {
          const ObRecordType *record_type = static_cast<const ObRecordType *>(user_type);
          int64_t member_index = record_type->get_record_member_index(var_name);
          if (member_index != OB_INVALID_INDEX) {
            if (OB_ISNULL(record_type->get_record_member_type(member_index))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("type is invalid", K(ret));
            } else {
              parent_id = udt_id;
              type = static_cast<ObPLExternalNS::ExternalType>(ObObjAccessIdx::IS_CONST);
              data_type = (*(record_type->get_record_member_type(member_index)));
              var_idx = member_index;
            }
          }
          }
        }
      }
    }
  }
  return ret;
}

int ObPLBlockNS::get_routine_info(int64_t routine_idx, const ObPLRoutineInfo *&routine) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(routine_table_->get_routine_info(routine_idx, routine))) {
    LOG_WARN("get package routine failed", K(routine_idx), K(ret));
  }
  return ret;
}

int ObPLBlockNS::get_routine_info(const ObString &routine_decl_str, ObPLRoutineInfo *&routine) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(routine_table_->get_routine_info(routine_decl_str, routine))) {
    LOG_WARN("get package routine failed", K(routine_decl_str), K(ret));
  }
  return ret;
}

int ObPLBlockNS::get_routine_info(const ObPLRoutineInfo *routine_info,
                                  const ObPLRoutineInfo *&info) const
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < routines_.count(); ++i) {
    const ObPLRoutineInfo *tmp_info = NULL;
    bool is_equal = false;
    OZ (routine_table_->get_routine_info(routines_.at(i), tmp_info));
    CK (OB_NOT_NULL(tmp_info));
    OZ (routine_info->is_equal(tmp_info, is_equal));
    if (is_equal) {
      info = tmp_info;
      break;
    }
  }
  return ret;
}

int ObPLBlockNS::add_routine_info(ObPLRoutineInfo *routine_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(routine_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("routine table is NULL", K(ret));
  } else if (OB_FAIL(routine_table_->add_routine_info(routine_info))) {
    LOG_WARN("add routine signature to routine table failed", K(ret));
  } else if (OB_FAIL(routines_.push_back(routine_table_->get_count() - 1))) {
    LOG_WARN("failed to add routine info to routines", K(ret));
  }
  return ret;
}

int ObPLBlockNS::set_routine_info(int64_t routine_idx, ObPLRoutineInfo *routine_info)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(routine_table_));
  OZ (routine_table_->set_routine_info(routine_idx, routine_info));
  return ret;
}

bool ObPLBlockNS::search_routine_local(const ObString &db_name,
                                       const ObString &package_name) const
{
  bool search_local = false;
  if (db_name.empty() && package_name.empty()) {
    search_local = true;
  } else if (!db_name.empty() && !package_name.empty()) {
    if (ObCharset::case_compat_mode_equal(db_name_, db_name)
        && ObCharset::case_compat_mode_equal(package_name_, package_name)) {
      search_local = true;
    }
  } else if (db_name.empty() && !package_name.empty()) {
    if (ObCharset::case_compat_mode_equal(package_name_, package_name)) {
      search_local = true;
    }
  } else if (!db_name.empty() && package_name.empty()) {
    if (ObCharset::case_compat_mode_equal(db_name_, db_name)
        && package_name_.empty()) {
      search_local = true;
    }
  }
  return search_local;
}

int ObPLBlockNS::resolve_routine(const ObPLResolveCtx &resolve_ctx,
                                 const ObString &db_name,
                                 const ObString &package_name,
                                 const ObString &routine_name,
                                 const common::ObIArray<ObRawExpr *> &expr_params,
                                 ObProcType &routine_type,
                                 const ObIRoutineInfo *&routine_info) const
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObIRoutineInfo*, 8> routine_infos;
  routine_info = NULL;
  OZ (resolve_routine(
    resolve_ctx, db_name, package_name, routine_name, expr_params, routine_type, routine_infos));
  if (OB_SUCC(ret) && routine_infos.count() > 0) {
    OZ (ObResolverUtils::pick_routine(
      resolve_ctx, expr_params, routine_infos, routine_info));
    LOG_DEBUG("debug for pick routine info",
           K(ret), K(routine_name), K(db_name), K(package_name), K(expr_params),
           K(routine_type), KPC(routine_info));
  }
  return ret;
}

int ObPLBlockNS::resolve_routine(const ObPLResolveCtx &resolve_ctx,
                                 const ObString &db_name,
                                 const ObString &package_name,
                                 const ObString &routine_name,
                                 const common::ObIArray<ObRawExpr *> &expr_params,
                                 ObProcType &routine_type,
                                 ObIArray<const ObIRoutineInfo *> &routine_infos) const
{
  int ret = OB_SUCCESS;
  if (STANDALONE_PROCEDURE != routine_type && STANDALONE_FUNCTION != routine_type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("routine type invalid", K(routine_type), K(ret));
  } else {
    LOG_DEBUG("debug for call resolve routine",
              K(db_name),
              K(package_name),
              K(routine_name),
              K(search_routine_local(db_name, package_name)),
              KP(routine_table_));
    if (OB_NOT_NULL(routine_table_)
        && get_routines().count() > 0
        && search_routine_local(db_name, package_name)) {
      bool is_pkg_scope = ObPLBlockNS::BLOCK_PACKAGE_SPEC == type_
                          || ObPLBlockNS::BLOCK_PACKAGE_BODY == type_;
      bool is_obj_scope = ObPLBlockNS::BLOCK_OBJECT_SPEC == type_
                          || ObPLBlockNS::BLOCK_OBJECT_BODY == type_;
      const ObPLRoutineTable *routine_table = get_routine_table();
      CK (OB_NOT_NULL(routine_table));
      for (int64_t i = 0; OB_SUCC(ret) && i < get_routines().count(); ++i) {
        const ObPLRoutineInfo *info = NULL;
        OZ (routine_table->get_routine_info(get_routines().at(i), info));
        if (OB_SUCC(ret)
            && OB_NOT_NULL(info)
            && ObCharset::case_compat_mode_equal(routine_name, info->get_name())
            && is_procedure(routine_type) == is_procedure(info->get_type())) {
          LOG_DEBUG("fit routine info ",
                    K(routine_name), K(db_name), K(package_name), K(ret), KPC(info));
          OZ (routine_infos.push_back(info));
        }
      }
      // for package body, we need to search public routine also.
      if (OB_SUCC(ret) && ObPLBlockNS::BLOCK_PACKAGE_BODY == type_) {
        const ObPLRoutineTable *routine_table = NULL;
        CK (OB_NOT_NULL(external_ns_));
        CK (OB_NOT_NULL(external_ns_->get_parent_ns()));
        CK (routine_table = external_ns_->get_parent_ns()->get_routine_table());
        for (int64_t i = 0; OB_SUCC(ret) && i < routine_table->get_count(); ++i) {
          const ObPLRoutineInfo *info = NULL;
          OZ (routine_table->get_routine_info(i, info));
          if (OB_SUCC(ret)
              && OB_NOT_NULL(info)
              && ObCharset::case_compat_mode_equal(routine_name, info->get_name())
              && is_procedure(routine_type) == is_procedure(info->get_type())) {
            LOG_DEBUG("fit routine info ",
                      K(routine_name), K(db_name), K(package_name), K(ret), KPC(info));
            OZ (routine_infos.push_back(info));
          }
        }
      }
      // adjust routine type
      if (OB_SUCC(ret) && routine_infos.count() > 0) {
        const ObPLRoutineInfo *info = static_cast<const ObPLRoutineInfo *>(routine_infos.at(0));
        CK (OB_NOT_NULL(info));
        OX (routine_type = !(is_pkg_scope || is_obj_scope) ? info->get_type() :
              (STANDALONE_PROCEDURE == routine_type ?
               is_pkg_scope ? PACKAGE_PROCEDURE : UDT_PROCEDURE
             : is_pkg_scope ? PACKAGE_FUNCTION : UDT_FUNCTION));
      }
    }
    if (OB_SUCC(ret) && routine_infos.empty()) {
      if (OB_NOT_NULL(pre_ns_)) {
        if (OB_FAIL(SMART_CALL(pre_ns_->resolve_routine(resolve_ctx,
                                                        db_name,
                                                        package_name,
                                                        routine_name,
                                                        expr_params,
                                                        routine_type,
                                                        routine_infos)))) {
          LOG_WARN("resolve routine failed", K(db_name), K(package_name), K(routine_name),
                   K(expr_params), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && routine_infos.empty()) {
      if (OB_NOT_NULL(external_ns_)) {
        bool need_clear = false;
        if (OB_ISNULL(external_ns_->get_resolve_ctx().params_.secondary_namespace_)
            && OB_NOT_NULL(resolve_ctx.params_.secondary_namespace_)) {
          need_clear = true;
          (const_cast<ObPLResolveCtx &>(external_ns_->get_resolve_ctx())).params_.secondary_namespace_
          = resolve_ctx.params_.secondary_namespace_;
        }
        if (OB_FAIL(SMART_CALL(external_ns_->resolve_external_routine(
                                                        db_name,
                                                        package_name,
                                                        routine_name,
                                                        expr_params,
                                                        routine_type,
                                                        routine_infos)))) {
          LOG_WARN("resolve routine failed", K(db_name), K(package_name), K(routine_name),
                   K(expr_params), K(ret));
        }
        if (need_clear) {
          (const_cast<ObPLResolveCtx &>(external_ns_->get_resolve_ctx())).params_.secondary_namespace_ = NULL;
        }
      }
    }
  }
  return ret;
}

int ObPLBlockNS::try_resolve_udt_name(const ObString &udt_var_name,
                                      ObString &udt_type_name,
                                      uint64_t &udt_id,
                                      pl::ObPLExternalNS::ExternalType external_type,
                                      uint64_t parent_id) const
{
  int ret = OB_SUCCESS;

  pl::ObPLDataType type;
  int64_t var_idx = OB_INVALID_INDEX;
  udt_id = OB_INVALID_ID;
  const pl::ObUserDefinedType *user_type = NULL;

  OX (udt_id = OB_INVALID_ID);
  OZ (resolve_symbol(
    udt_var_name, external_type, type, parent_id, var_idx));

  if (OB_SUCC(ret)
      && var_idx != OB_INVALID_INDEX
      && (type.is_record_type() || type.is_opaque_type())
      && type.is_udt_type()) {

    OX (udt_id = type.get_user_type_id());
    CK (OB_NOT_NULL(get_type_table()));
    OX (user_type = get_type_table()->get_type(udt_id));
    if (OB_SUCC(ret) && OB_ISNULL(user_type)) {
      CK (OB_NOT_NULL(user_type = get_type_table()->get_external_type(udt_id)));
    }
    OX (udt_type_name = user_type->get_name());
  }
  return ret;
}

int ObPLBlockNS::check_routine_exists(const ObString &db_name,
                                      const ObString &package_name,
                                      const ObString &routine_name,
                                      const share::schema::ObRoutineType routine_type,
                                      bool &exists,
                                      pl::ObProcType &proc_type,
                                      uint64_t udt_id) const
{
  int ret = OB_SUCCESS;
  exists = false;
  proc_type = INVALID_PROC_TYPE;
  if (routine_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("routine name empty", K(routine_name), K(ret));
  } else if (ROUTINE_PROCEDURE_TYPE != routine_type && ROUTINE_FUNCTION_TYPE != routine_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("routine type invalid", K(routine_type), K(ret));
  } else {
    if (search_routine_local(db_name, package_name)
        && OB_NOT_NULL(routine_table_)) {
      const ObPLRoutineTable *routine_table = get_routine_table();
      uint64_t routine_count = routine_table->get_count();
      ObProcType search_routine_type = BLOCK_ROUTINE != get_block_type() ?
          (ROUTINE_PROCEDURE_TYPE == routine_type ? PACKAGE_PROCEDURE : PACKAGE_FUNCTION) :
          (ROUTINE_PROCEDURE_TYPE == routine_type ? NESTED_PROCEDURE : NESTED_FUNCTION);
      int64_t routine_idx = BLOCK_ROUTINE != get_block_type() ?
                            ObPLRoutineTable::NORMAL_ROUTINE_START_IDX : 0;
      for (; OB_SUCC(ret) && !exists && routine_idx < routine_count; ++routine_idx) {
        const ObPLRoutineInfo *pl_routine_info = NULL;
        if (OB_FAIL(routine_table->get_routine_info(routine_idx, pl_routine_info))) {
          LOG_WARN("get package routine failed", K(ret));
        } else if (OB_NOT_NULL(pl_routine_info)) {
          if (ObCharset::case_compat_mode_equal(routine_name, pl_routine_info->get_name())
              && search_routine_type == pl_routine_info->get_type()) {
            exists = true;
            proc_type = search_routine_type;
          }
        } else {  /* null is possible */ }
      }
    }
    if (OB_SUCC(ret) && !exists) {
      if (OB_NOT_NULL(pre_ns_)) {
        if (OB_FAIL(pre_ns_->check_routine_exists(db_name, package_name,
                               routine_name, routine_type, exists, proc_type, udt_id))) {
          LOG_WARN("resolve routine failed",
                   K(db_name), K(package_name), K(routine_name),
                   K(routine_type), K(exists), K(proc_type), K(udt_id), K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !exists) {
      if (OB_NOT_NULL(external_ns_)) {
        if (OB_FAIL(external_ns_->check_routine_exists(db_name, package_name,
                                 routine_name, routine_type, exists, proc_type, udt_id))) {
          LOG_WARN("resolve routine failed",
                   K(db_name), K(package_name), K(routine_name),
                   K(routine_type), K(exists), K(proc_type), K(udt_id), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPLBlockNS::find_sub_attr_by_index(const ObUserDefinedType &user_type, int64_t attr_index, const ObRawExpr *func_expr, ObObjAccessIdx &access_idx) const
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  if (user_type.is_nested_table_type()) {
    const ObNestedTableType &table_type = static_cast<const ObNestedTableType &>(user_type);
    ObString empty_name;
    new(&access_idx)ObObjAccessIdx(table_type.get_element_type(),
                                   NULL == func_expr ? ObObjAccessIdx::IS_CONST : ObObjAccessIdx::IS_EXPR,
                                       empty_name,
                                       table_type.get_element_type(),
                                       reinterpret_cast<int64_t>(func_expr));
    access_idx.var_index_ = attr_index;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid user type", K(user_type), K(ret));
  }
#endif
  return ret;
}

int ObPLBlockNS::get_pl_data_type_by_name(const ObPLResolveCtx &resolve_ctx,
                                          const ObString &db_name, const ObString &package_name,
                                          const ObString &type_name, const ObUserDefinedType *&user_type) const
{
  int ret = OB_SUCCESS;
  SET_LOG_CHECK_MODE();
  user_type = NULL;
  if (OB_ISNULL(type_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user_type_table_ is null");
  } else {
    if (search_routine_local(db_name, package_name)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < get_types().count(); ++i) {
        const ObUserDefinedType *type = type_table_->get_type(get_types().at(i));
        if (OB_ISNULL(type)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user type is null", K(i), K(get_types().at(i)), K(ret));
        } else if (ObCharset::case_compat_mode_equal(type_name, type->get_name())) {
          user_type = type;
          break;
        } else { /*do nothing*/ }
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(user_type)) {
      if (OB_NOT_NULL(pre_ns_)) {
        if (OB_FAIL(SMART_CALL(pre_ns_->get_pl_data_type_by_name(
                               resolve_ctx, db_name, package_name, type_name, user_type)))) {
          LOG_WARN("get pl data type by name in pre ns failed",
                   K(ret), K(db_name), K(package_name), K(type_name));
        }
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(user_type)) {
      if (OB_NOT_NULL(external_ns_)) {
        ObSEArray<ObDataType, 8> types;
        if (OB_FAIL(SMART_CALL(external_ns_->
            resolve_external_type_by_name(db_name, package_name, type_name, user_type)))) {
          LOG_WARN("resolve external type failed",
                   K(ret), K(db_name), K(package_name), K(type_name));
        } else if (OB_ISNULL(user_type)) {
          ret = OB_ERR_SP_UNDECLARED_TYPE;
          LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_TYPE, type_name.length(), type_name.ptr());
        } else if (OB_FAIL(expand_data_type(user_type, types))) {
          LOG_WARN("failed to expand data type", K(ret), KPC(user_type));
        } else if (OB_FAIL(type_table_->add_external_type(user_type))) {
          LOG_WARN("add external type failed", K(ret), K(db_name), K(package_name), K(type_name));
        } else { /*do nothing*/ }
      }
    }
#ifdef OB_BUILD_ORACLE_PL
    //所有的type表都找不到，看是否是SYS_REFCURSOR
    if ((OB_SUCC(ret) && OB_ISNULL(user_type)) || OB_ERR_SP_UNDECLARED_TYPE == ret) {
      if (db_name.empty() && package_name.empty() && ObCharset::case_insensitive_equal(type_name, "SYS_REFCURSOR")) {
        user_type = &type_table_->get_sys_refcursor_type();
        ret = OB_SUCCESS;
      }
    }
#endif
    if (OB_SUCC(ret) && OB_NOT_NULL(user_type)) {
      if (OB_FAIL(user_type->get_all_depended_user_type(resolve_ctx, *this))) {
        LOG_WARN("get all depended user type failed", K(ret));
      }
    }
  }
  CANCLE_LOG_CHECK_MODE();
  return ret;
}

#ifdef OB_BUILD_ORACLE_PL
int ObPLBlockNS::get_subtype(uint64_t type_id, const ObUserDefinedSubType *&subtype)
{
  int ret = OB_SUCCESS;
  const ObUserDefinedType *type = NULL;
  OZ (get_pl_data_type_by_id(type_id, type));
  CK (OB_NOT_NULL(type));
  CK (type->is_subtype());
  CK (OB_NOT_NULL(subtype = static_cast<const ObUserDefinedSubType *>(type)));
  return ret;
}
#endif

int ObPLBlockNS::get_subtype_actually_basetype(ObPLDataType &pl_type)
{
  int ret = OB_SUCCESS;
  if (pl_type.is_subtype()) {
    const ObPLDataType *actually_type = NULL;
    OZ (get_subtype_actually_basetype(&pl_type, actually_type));
    if (OB_SUCC(ret)
        && OB_NOT_NULL(actually_type)
        && actually_type != &pl_type) {
      pl_type = *actually_type;
    }
  }
  return ret;
}

int ObPLBlockNS::get_subtype_actually_basetype(const ObPLDataType *pl_type,
                                               const ObPLDataType *&actually_type)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  ret = OB_NOT_SUPPORTED;
  LOG_WARN("get_subtype_actually_basetype is not supported in mysql mode", K(ret));
#else
  const ObUserDefinedSubType *subtype = NULL;
  if (OB_NOT_NULL(pl_type) && pl_type->is_subtype()) {
    OZ (get_subtype(pl_type->get_user_type_id(), subtype));
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(subtype)) {
    CK (OB_NOT_NULL(actually_type = subtype->get_base_type()));
    OZ (get_subtype_actually_basetype(actually_type, actually_type));
  }
#endif
  return ret;
}

int ObPLBlockNS::get_subtype_not_null_constraint(const ObPLDataType *pl_type, bool &not_null)
{
  int ret = OB_SUCCESS;
  UNUSEDx(pl_type, not_null);
  return ret;
}

int ObPLBlockNS::get_subtype_range_constraint(const ObPLDataType *pl_type,
                                             bool &has_range, int64_t &lower, int64_t &upper)
{
  int ret = OB_SUCCESS;
  UNUSEDx(pl_type, has_range, lower, upper);
  return ret;
}

int ObPLBlockNS::extract_external_record_default_expr(ObRawExpr &expr) const
{
  int ret = OB_SUCCESS;
  if (expr.is_obj_access_expr()) {
    ObObjAccessRawExpr &access_expr = static_cast<ObObjAccessRawExpr&>(expr);
    ObIArray<ObObjAccessIdx> &access_idxs = access_expr.get_access_idxs();
    OZ (add_var_to_array_no_dup(*obj_access_exprs_, &expr));
    for (int64_t i = 0; OB_SUCC(ret) && i < access_idxs.count(); ++i) {
      if (access_idxs.at(i).elem_type_.is_user_type()) {
        const ObUserDefinedType *user_type = NULL;
        OZ (SMART_CALL(get_pl_data_type_by_id(
          access_idxs.at(i).elem_type_.get_user_type_id(), user_type)));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    CK (OB_NOT_NULL(expr.get_param_expr(i)));
    OZ (SMART_CALL(extract_external_record_default_expr(*expr.get_param_expr(i))));
  }
  return ret;
}

int ObPLBlockNS::get_pl_data_type_by_id(uint64_t type_id, const ObUserDefinedType *&user_type) const
{
  int ret = OB_SUCCESS;
  user_type = NULL;
  if (OB_ISNULL(type_table_)) { //type id不会重复，所以不用像name那样逐层查找
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user type table is null");
  } else if (OB_ISNULL(user_type = type_table_->get_type(type_id))) {
    if (OB_ISNULL(user_type = type_table_->get_external_type(type_id))) {
      ObSEArray<ObDataType, 8> types;
      if (NULL == external_ns_) {
        //external_ns_为空说明已经到了package最上层名称空间，返回NULL即可
      } else if (OB_FAIL(
          SMART_CALL(external_ns_->resolve_external_type_by_id(type_id, user_type)))) {
        LOG_WARN("resolve external type by id failed", K(ret), K(type_id));
      } else if (OB_FAIL(expand_data_type(user_type, types))) {
        LOG_WARN("failed to expand data type", K(ret), K(type_id));
      } else if (OB_FAIL(type_table_->add_external_type(user_type))) {
        LOG_WARN("add external type failed", K(ret), K(type_id));
      } else if (user_type->is_record_type()) {
        const ObRecordType *rec_type = static_cast<const ObRecordType *>(user_type);
        for (int64_t i = 0; OB_SUCC(ret) && i < rec_type->get_member_count(); ++i) {
          ObRecordMember *member = const_cast<ObRecordMember *>(rec_type->get_record_member(i));
          CK (OB_NOT_NULL(member));
          if (OB_SUCC(ret)
              && OB_INVALID_INDEX != member->get_default()
              && OB_NOT_NULL(member->get_default_expr())) {
            CK (OB_NOT_NULL(exprs_));
            CK (OB_NOT_NULL(obj_access_exprs_));
            OZ (extract_external_record_default_expr(*member->get_default_expr()));
            OZ (exprs_->push_back(member->get_default_expr()));
            OX (member->set_default(exprs_->count() - 1));
          }
        }
      }
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return ret;
}

int ObPLBlockNS::get_cursor(uint64_t pkg_id, uint64_t routine_id, int64_t idx,
                            const ObPLCursor *&cursor) const
{
  int ret = OB_SUCCESS;
  cursor = NULL;
  if (OB_ISNULL(cursor_table_)) { //id不会重复，所以不用像name那样逐层查找
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user type table is null");
  } else if (OB_ISNULL(cursor = cursor_table_->get_cursor(pkg_id, routine_id, idx))) {
    if (NULL == external_ns_ || NULL == external_ns_->get_parent_ns()) {
      //external_ns_为空说明已经到了package最上层名称空间，返回NULL即可
    } else if (OB_FAIL(external_ns_->get_parent_ns()->get_cursor(pkg_id, routine_id, idx,
                                                                 cursor))) {
      LOG_WARN("resolve external type by id failed", K(ret), K(pkg_id), K(routine_id), K(idx));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return ret;
}

int ObPLBlockNS::get_cursor_var(uint64_t pkg_id, uint64_t routine_id, int64_t idx,
                   const ObPLVar *&var) const
{
  int ret = OB_SUCCESS;
  var = NULL;
  const ObPLCursor *cursor = NULL;
  if (OB_FAIL(get_cursor(pkg_id, routine_id, idx, cursor))) {
    LOG_WARN("failed to get cursor", K(pkg_id), K(routine_id), K(idx), K(ret));
  } else if (NULL == cursor || pkg_id  != package_id_ || routine_id != routine_id_) {
    if (NULL == external_ns_ || NULL == external_ns_->get_parent_ns()) {
      //do nothing
    } else if (OB_FAIL(external_ns_->get_parent_ns()->get_cursor_var(pkg_id, routine_id, idx,
                                                                     var))) {
      LOG_WARN("failed to get cursor", K(pkg_id), K(routine_id), K(idx), K(ret));
    } else { /*do nothing*/ }
  } else if (OB_ISNULL(get_symbol_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("symbol table is null");
  } else {
    var = get_symbol_table()->get_symbol(idx);
  }
  return ret;
}

int ObPLBlockNS::get_cursor_by_name(const ObExprResolveContext &ctx,
                                    const ObString &database_name, 
                                    const ObString &package_name, 
                                    const ObString &cursor_name, 
                                    const ObPLCursor *&cursor) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  cursor = NULL;
  if (package_name.empty() && !database_name.empty()) {
    ret = OB_ERR_SP_UNDECLARED_VAR;
    LOG_USER_ERROR(OB_ERR_SP_UNDECLARED_VAR, cursor_name.length(), cursor_name.ptr());
    LOG_WARN("undeclare cursor of ", K(cursor_name));
  } else if ((package_name.empty() && database_name.empty())
              || (0 == package_name.case_compare(get_package_name())
                   && (database_name.empty() || 0 == database_name.case_compare(get_db_name())))) {
    // local cursor
    CK (OB_NOT_NULL(get_cursor_table()));
    CK (OB_NOT_NULL(get_symbol_table()));
    for (int64_t i = 0; OB_SUCC(ret) && i < get_cursor_table()->get_count() && !found; i++) {
      const ObPLCursor *cur = get_cursor_table()->get_cursor(i);
      if (OB_NOT_NULL(cur)) {
        const ObPLVar *var = get_symbol_table()->get_symbol(cur->get_index());
        if (OB_NOT_NULL(var) && 0 == var->get_name().case_compare(cursor_name)) {
          cursor = cur;
          found = true;
        }
      }
    }
    if (OB_SUCC(ret) && !found && NULL != pre_ns_) {
      OZ (pre_ns_->get_cursor_by_name(ctx, database_name, package_name, cursor_name, cursor));
    }
    // maybe local package cursor
    if (OB_SUCC(ret) && !found && NULL != external_ns_ && NULL != external_ns_->get_parent_ns()) {
      OZ (external_ns_->get_parent_ns()->get_cursor_by_name(
            ctx, database_name, package_name, cursor_name, cursor));
    }
  } else {
    // package cursor
    CK (OB_NOT_NULL(ctx.session_info_));
    CK (OB_NOT_NULL(ctx.secondary_namespace_));
    CK (OB_NOT_NULL(ctx.session_info_->get_pl_engine()));
    CK (OB_NOT_NULL(ctx.secondary_namespace_->get_external_ns()));
    if (OB_SUCC(ret)) {
      pl::ObPLPackageGuard package_guard(ctx.session_info_->get_effective_tenant_id());
      const pl::ObPLResolveCtx &resolve_ctx = ctx.secondary_namespace_->get_external_ns()
                                                ->get_resolve_ctx();
      uint64_t tenant_id = ctx.session_info_->get_effective_tenant_id();
      int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                      : COMPATIBLE_MYSQL_MODE;
      const ObPackageInfo *package_info = NULL;
      ObPLPackageManager &package_manager =
        ctx.session_info_->get_pl_engine()->get_package_manager();
      ObString db_name =
        database_name.empty() ? ctx.session_info_->get_database_name() : database_name;
      uint64_t database_id = OB_INVALID_ID;
      int64_t idx = OB_INVALID_INDEX;
      CK (!package_name.empty());
      CK (!cursor_name.empty());
      OX (cursor = NULL);
      OZ (resolve_ctx.schema_guard_.get_database_id(tenant_id, db_name, database_id));
      OZ (resolve_ctx.schema_guard_.get_package_info(
          tenant_id, database_id, package_name, PACKAGE_TYPE, compatible_mode, package_info));
      if (OB_SUCC(ret)
          && OB_ISNULL(package_info) && db_name.case_compare(OB_SYS_DATABASE_NAME)) {
        OZ (resolve_ctx.schema_guard_.get_package_info(
          OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID,
          package_name, PACKAGE_TYPE, compatible_mode, package_info));
      }
      if (OB_SUCC(ret) && OB_ISNULL(package_info)) {
        ret = OB_ERR_PACKAGE_DOSE_NOT_EXIST;
        LOG_WARN("package not exist", K(ret), K(package_name), K(db_name));
        LOG_USER_ERROR(OB_ERR_PACKAGE_DOSE_NOT_EXIST, "PACKAGE",
                              db_name.length(), db_name.ptr(),
                              package_name.length(), package_name.ptr());
      }
      OZ (package_manager.get_package_cursor(
            resolve_ctx, package_info->get_package_id(), cursor_name, cursor, idx));
    }
  }
  return ret;
}

int ObPLBlockNS::expand_data_type_once(const ObUserDefinedType *user_type,
                                      ObIArray<ObDataType> &types,
                                      ObIArray<bool> *not_null_flags,
                                      ObIArray<int64_t> *pls_ranges) const
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(user_type));
  for (int64_t i = 0; OB_SUCC(ret) && i < user_type->get_member_count(); ++i) {
    const ObPLDataType *member = user_type->get_member(i);
    CK (OB_NOT_NULL(member));
    if (OB_FAIL(ret)) {
    } else if (member->is_obj_type()) {
      CK (OB_NOT_NULL(member->get_data_type()));
      OZ (types.push_back(*member->get_data_type()), i);
      if (OB_NOT_NULL(not_null_flags)) {
        OZ (not_null_flags->push_back(member->get_not_null()));
      }
      if (OB_NOT_NULL(pls_ranges)) {
        ObPLIntegerRange range;
        OZ (pls_ranges->push_back(member->is_pl_integer_type() ? member->get_range() : range.range_));
      }
    } else {
      ObDataType ext_type;
      ext_type.set_obj_type(ObExtendType);
      OZ (types.push_back(ext_type), i);
      if (OB_NOT_NULL(not_null_flags)) {
        OZ (not_null_flags->push_back(false));
      }
      if (OB_NOT_NULL(pls_ranges)) {
        ObPLIntegerRange range;
        OZ (pls_ranges->push_back(range.range_));
      }
    }
  }
  return ret;
}

int ObPLBlockNS::expand_data_type(const ObUserDefinedType *user_type,
                                  ObIArray<ObDataType> &types,
                                  ObIArray<bool> *not_null_flags,
                                  ObIArray<int64_t> *pls_ranges) const
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(user_type));
  for (int64_t i = 0; OB_SUCC(ret) && i < user_type->get_member_count(); ++i) {
    const ObPLDataType *member = user_type->get_member(i);
    CK (OB_NOT_NULL(member));
    if (OB_FAIL(ret)) {
    } else if (member->is_obj_type()) {
      CK (OB_NOT_NULL(member->get_data_type()));
      OZ (types.push_back(*member->get_data_type()), i);
      if (OB_NOT_NULL(not_null_flags)) {
        OZ (not_null_flags->push_back(member->get_not_null()));
      }
      if (OB_NOT_NULL(pls_ranges)) {
        ObPLIntegerRange range;
        OZ (pls_ranges->push_back(member->is_pl_integer_type() ? member->get_range() : range.range_));
      }
    } else {
      const ObUserDefinedType *l_user_type = NULL;
      OZ (SMART_CALL(get_pl_data_type_by_id(member->get_user_type_id(), l_user_type)));
      OZ (SMART_CALL(expand_data_type(l_user_type, types, not_null_flags, pls_ranges)));
    }
  }
  return ret;
}

int ObPLBlockNS::get_package_var(const ObPLResolveCtx &resolve_ctx,
                                 uint64_t package_id,
                                 const ObString &var_name,
                                 const ObPLVar *&var,
                                 int64_t &var_idx) const
{
  int ret = OB_SUCCESS;

#define GET_VAR(sym_tbl) \
do { \
      CK (OB_NOT_NULL(sym_tbl)); \
      const ObPLVar *item = NULL; \
      for (int64_t i = 0; OB_SUCC(ret) && i < sym_tbl->get_count(); ++i) { \
        item = sym_tbl->get_symbol(i); \
        CK (OB_NOT_NULL(item)); \
        if (OB_SUCC(ret)) { \
          if (0 == item->get_name().case_compare(var_name)) { \
            var = item; \
            var_idx = i; \
          } \
        } \
      }\
}while(0)

 if (OB_INVALID_ID == package_id) {
    // 这种场景是在resolve package body，但是package body还没有写入schema，所以pkg id是invalid

    if (OB_NOT_NULL(external_ns_)
         && OB_NOT_NULL(external_ns_->get_parent_ns())
         && OB_NOT_NULL(external_ns_->get_parent_ns()->get_symbol_table())) {
      const ObPLSymbolTable *symbol_tbl = external_ns_->get_parent_ns()->get_symbol_table();
      GET_VAR(symbol_tbl);
      // 不应该找spec中的var 内容, 因为id 是invalid表示它是定义在body中的变量，否则id部位invalid，
      // spec走else中的路径
      // if (OB_ISNULL(var)) {
      //   const ObPLBlockNS *spec_ns = external_ns_->get_parent_ns();
      //   if (OB_NOT_NULL(spec_ns)
      //      && OB_NOT_NULL(spec_ns->get_external_ns())
      //      && OB_NOT_NULL(spec_ns->get_external_ns()->get_parent_ns())
      //      && OB_NOT_NULL(spec_ns->get_external_ns()->get_parent_ns()->get_symbol_table())) {
      // var = spec_ns->get_external_ns()->get_parent_ns()->get_symbol_table()->get_symbol(var_idx);
      //   }
      // }
    }
  } else {
    // block may be a package function block;
    const ObPackageInfo *pkg_info = NULL;
    const uint64_t tenant_id = get_tenant_id_by_object_id(package_id);
    if (OB_FAIL(resolve_ctx.schema_guard_.get_package_info(tenant_id, package_id, pkg_info))) {
      LOG_WARN("failed to get package info", K(ret), K(tenant_id));
    } else if (OB_ISNULL(pkg_info)) {
    } else {
      if (0 == get_package_name().case_compare(pkg_info->get_package_name())) {

        // find var in package spec
        if (pkg_info->is_package()) {
            const ObPLBlockNS *spec_ns = external_ns_->get_parent_ns();
            if (OB_NOT_NULL(spec_ns)
               && OB_NOT_NULL(spec_ns->get_external_ns())
               && OB_NOT_NULL(spec_ns->get_external_ns()->get_parent_ns())
               && OB_NOT_NULL(spec_ns->get_external_ns()->get_parent_ns()->get_symbol_table())) {
              const ObPLSymbolTable *symbol_tbl =
                                   spec_ns->get_external_ns()->get_parent_ns()->get_symbol_table();
              GET_VAR(symbol_tbl);
            }
        } else {
          // 这儿应该不需要，这个场景处理的就是找spec中的var
          // if (OB_ISNULL(var)) {
          //   if (OB_NOT_NULL(external_ns_)
          //     && OB_NOT_NULL(external_ns_->get_parent_ns())
          //     && OB_NOT_NULL(external_ns_->get_parent_ns()->get_symbol_table())) {
          //     var = external_ns_->get_parent_ns()->get_symbol_table()->get_symbol(var_idx);
          //   }
          // }
        }
      } else {
        OZ (resolve_ctx.session_info_.get_pl_engine()->get_package_manager().get_package_var(
            resolve_ctx, package_id, var_idx, var));
      }
    }
  }
  return ret;
}
int ObPLBlockNS::get_package_var(const ObPLResolveCtx &resolve_ctx,
                                 uint64_t package_id,
                                 int64_t var_idx,
                                 const ObPLVar *&var) const
{
  int ret = OB_SUCCESS;

  // Step1: found local package namespace.
  const ObPLBlockNS *package_ns = this;

  while (OB_NOT_NULL(package_ns)
        && package_ns->get_block_type() != ObPLBlockNS::BLOCK_PACKAGE_SPEC
        && package_ns->get_block_type() != ObPLBlockNS::BLOCK_PACKAGE_BODY
        && package_ns->get_block_type() != ObPLBlockNS::BLOCK_OBJECT_SPEC
        && package_ns->get_block_type() != ObPLBlockNS::BLOCK_OBJECT_BODY) {
    if (OB_NOT_NULL(package_ns->get_pre_ns())) {
      package_ns = package_ns->get_pre_ns();
    } else if (OB_NOT_NULL(package_ns->get_external_ns())) {
      package_ns = package_ns->get_external_ns()->get_parent_ns();
    }
  }

  // Step2: try local package namespace.
  if (OB_NOT_NULL(package_ns)
      && (OB_INVALID_ID == package_id || package_ns->get_package_id() == package_id)) {
    CK (OB_NOT_NULL(package_ns->get_symbol_table()));
    OX (var = package_ns->get_symbol_table()->get_symbol(var_idx));
    CK (OB_NOT_NULL(var));
  } else if (OB_NOT_NULL(package_ns) // Step3: try local package spec namespace
             && (package_ns->get_block_type() == ObPLBlockNS::BLOCK_OBJECT_BODY
                 || package_ns->get_block_type() == ObPLBlockNS::BLOCK_PACKAGE_BODY)
             && OB_NOT_NULL(package_ns->get_external_ns())
             && OB_NOT_NULL(package_ns->get_external_ns()->get_parent_ns())
             && package_ns->get_external_ns()->get_parent_ns()->get_package_id() == package_id) {
    const ObPLBlockNS *package_spec_ns = package_ns->get_external_ns()->get_parent_ns();
    CK (OB_NOT_NULL(package_spec_ns->get_symbol_table()));
    CK (OB_NOT_NULL(var = package_spec_ns->get_symbol_table()->get_symbol(var_idx)));
  } else { // Step4: try external package.
    OZ (resolve_ctx.session_info_.get_pl_engine()->get_package_manager().get_package_var(
            resolve_ctx, package_id, var_idx, var));
  }
  return ret;
}

int ObPLBlockNS::get_subprogram_var(
  uint64_t package_id, uint64_t routine_id, int64_t var_idx, const ObPLVar *&var) const
{
  int ret = OB_SUCCESS;
  if (get_package_id() == package_id && get_routine_id() == routine_id) {
    CK (OB_NOT_NULL(get_symbol_table()));
    CK (OB_NOT_NULL(var = get_symbol_table()->get_symbol(var_idx)));
  } else if (OB_NOT_NULL(get_external_ns()) && get_external_ns()->get_parent_ns()) {
    OZ (get_external_ns()->get_parent_ns()
        ->get_subprogram_var(package_id, routine_id, var_idx, var));
  }
  return ret;
}

int ObPLStmtBlock::add_stmt(ObPLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(stmts_.push_back(stmt))) {
    LOG_WARN("failed push stmt", K(ret));
  } else {
    if (OB_NOT_NULL(stmt) && PL_CURSOR_FOR_LOOP == stmt->get_type()) {
      if (OB_FAIL(forloop_cursor_stmts_.push_back(stmt))) {
        LOG_WARN("failed to push forloop stmt", K(ret));
      }
    }
  }
  return ret;
}

bool ObPLStmtBlock::is_contain_stmt(const ObPLStmt *stmt) const
{
  bool bool_ret = false;
  if (OB_NOT_NULL(stmt)) {
    for (int64_t i = 0; i < stmts_.count(); ++i) {
      if (stmts_.at(i)->get_stmt_id() == stmt->get_stmt_id()) {
        bool_ret = true;
        break;
      }
    }
  }
  return bool_ret;
}


int64_t ObPLBlockNS::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
  } else {
    databuff_printf(buf, buf_len, pos,
      "TYPE:%d, DB_NAME:%s, PKG_NAME: %s, PKG_ID: %ld," \
        "PKG_VERSION: %ld, ROUTINE_ID: %ld EXPLICT_BLOCK: %d\n",
      type_, db_name_.ptr(), package_name_.ptr(),
      package_id_, package_version_, routine_id_, explicit_block_);
    databuff_printf(buf, buf_len, pos, "\t TYPES(%ld): \t\t[", types_.count());
    for (int64_t i = 0; i < types_.count(); ++i) {
      databuff_printf(buf, buf_len, pos, "%ld,", types_.at(i));
    }
    databuff_printf(buf, buf_len, pos, "]\n\t SYMBOLS(%ld): \t\t[", symbols_.count());
    for (int64_t i = 0; i < symbols_.count(); ++i) {
      databuff_printf(buf, buf_len, pos, "%ld,", symbols_.at(i));
    }
    databuff_printf(buf, buf_len, pos, "]\n\t LABLES(%ld): \t\t[", labels_.count());
    for (int64_t i = 0; i < labels_.count(); ++i) {
      databuff_printf(buf, buf_len, pos, "%ld,", labels_.at(i));
    }
    databuff_printf(buf, buf_len, pos, "]\n\t CONDITIONS(%ld): \t[", conditions_.count());
    for (int64_t i = 0; i < conditions_.count(); ++i) {
      databuff_printf(buf, buf_len, pos, "%ld,", conditions_.at(i));
    }
    databuff_printf(buf, buf_len, pos, "]\n\t CURSORS(%ld): \t\t[", cursors_.count());
    for (int64_t i = 0; i < cursors_.count(); ++i) {
      databuff_printf(buf, buf_len, pos, "%ld,", cursors_.at(i));
    }
    databuff_printf(buf, buf_len, pos, "]\n\t ROUTINES(%ld): \t\t[", routines_.count());
    for (int64_t i = 0; i < routines_.count(); ++i) {
      databuff_printf(buf, buf_len, pos, "%ld,", routines_.at(i));
    }
    databuff_printf(buf, buf_len, pos, "]\n");
    if (OB_NOT_NULL(pre_ns_)) {
      databuff_printf(buf, buf_len, pos, "PARENT NAMESPACE:\n");
      pos = pos + pre_ns_->to_string(buf + pos, buf_len - pos);
    } else {
      if (OB_NOT_NULL(symbol_table_)) {
        databuff_printf(buf, buf_len, pos, "GLOBAL SYMBOLS:\n");
        pos = pos + symbol_table_->to_string(buf + pos, buf_len - pos);
      }
      if (OB_NOT_NULL(label_table_)) {
        databuff_printf(buf, buf_len, pos, "\nGLOBAL LABELS:\n");
        pos = pos + label_table_->to_string(buf + pos, buf_len - pos);
      }
    }
  }
  return pos;
}

int ObPLInto::generate_into_variable_info(ObPLBlockNS &ns, const ObRawExpr &expr)
{
  int ret = OB_SUCCESS;

  pl::ObPLDataType final_type;
  pl::ObPLDataType pl_data_type;
  // T_OBJ_ACCESS_REF expr, access obj type (not user defined type)
  bool access_obj_type = false;
  if (expr.is_obj_access_expr()) {
    // case:
    //   type num_table is table of number;
    //   ...
    //   num_table nums;
    //   ...
    //   fetch c_cursor into nums(1);
    //
    // We got `num(1)` by T_OBJ_ACCESS_REF expression for write. T_OBJ_ACCESS_REF return
    // the address of ObObj by extend value in execution, so %expr's result type is set to
    // ObExtendType. We get the obj type from final type of T_OBJ_ACCESS_REF here.
    const auto &access_expr = static_cast<const ObObjAccessRawExpr &>(expr);
    OZ(access_expr.get_final_type(final_type));
    OX(access_obj_type = !final_type.is_user_type());
    if (bulk_ && !access_obj_type && final_type.is_collection_type()) {
      const pl::ObUserDefinedType *user_type = NULL;
      OZ (ns.get_pl_data_type_by_id(final_type.get_user_type_id(), user_type));
      CK (OB_NOT_NULL(user_type));
      if (OB_SUCC(ret)) {
        const ObCollectionType *coll_type = static_cast<const ObCollectionType*>(user_type);
        CK (OB_NOT_NULL(coll_type));
        OX (final_type = coll_type->get_element_type());
        OX(access_obj_type = !final_type.is_user_type());
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (expr.get_result_type().is_ext() && !access_obj_type) {
    const pl::ObUserDefinedType *user_type = NULL;
    if (expr.is_obj_access_expr()) {
      // do nothing, %final_type already fetched
    } else if (T_QUESTIONMARK == expr.get_expr_type()) {
      CK (OB_NOT_NULL(ns.get_symbol_table()));
      int64_t var_index = static_cast<const ObConstRawExpr&>(expr).get_value().get_int();
      const ObPLVar *var = ns.get_symbol_table()->get_symbol(var_index);
      CK (OB_NOT_NULL(var));
      OX (final_type = var->get_pl_data_type());
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Invalid expr type used in INTO clause", K(expr), K(ret));
    }

    if (OB_SUCC(ret)) {
      if (final_type.is_cursor_type() || final_type.is_opaque_type()) {
        ObDataType ext_type;
        ext_type.set_obj_type(ObExtendType);
        if (final_type.is_opaque_type()) {
          ext_type.set_udt_id(final_type.get_user_type_id());
        }
        OZ (data_type_.push_back(ext_type));
        OZ (not_null_flags_.push_back(false));
        ObPLIntegerRange range;
        OZ (pl_integer_ranges_.push_back(range.range_));
        OX (pl_data_type.set_data_type(ext_type));
        OX (pl_data_type.set_type(PL_OBJ_TYPE));
        OZ (into_data_type_.push_back(pl_data_type));
      } else {
        if (final_type.is_type_record()) {
          ObArray<ObDataType> basic_types;
          ObArray<bool> not_null_flags;
          ObArray<int64_t> pls_ranges;
          is_type_record_ = true;
          OZ (ns.get_pl_data_type_by_id(final_type.get_user_type_id(), user_type));
          CK (OB_NOT_NULL(user_type));
          // 只能展一层
          OZ (ns.expand_data_type_once(user_type, basic_types, &not_null_flags, &pls_ranges));
          OZ (append(data_type_, basic_types));
          OZ (append(not_null_flags_, not_null_flags));
          OZ (append(pl_integer_ranges_, pls_ranges));
        } else {
          ObDataType ext_type;
          ObDataType type;
          ObPLIntegerRange range;
          ext_type.set_obj_type(ObExtendType);
          OZ (data_type_.push_back(ext_type));
          OZ (not_null_flags_.push_back(false));
          OZ (pl_integer_ranges_.push_back(range.range_));
        }
        OZ (into_data_type_.push_back(final_type));
      }
    }
  } else {
    ObDataType type;
    ObPLIntegerRange range;
    bool flag = false;
    if (access_obj_type) {
      type.set_meta_type(final_type.get_data_type()->get_meta_type());
      type.set_accuracy(final_type.get_data_type()->get_accuracy());
    } else {
      type.set_meta_type(expr.get_result_type().get_obj_meta());
      type.set_accuracy(expr.get_result_type().get_accuracy());
    }
    OZ (calc_type_constraint(expr, ns, flag, range), expr);
    OZ (data_type_.push_back(type), type);
    OZ (not_null_flags_.push_back(flag), type, flag);
    OZ (pl_integer_ranges_.push_back(range.range_));
    OX (pl_data_type.set_data_type(type));
    OX (pl_data_type.set_type(PL_OBJ_TYPE));
    OZ (into_data_type_.push_back(pl_data_type));
  }

  return ret;
}

int ObPLInto::add_into(int64_t idx, ObPLBlockNS &ns, const ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(into_.push_back(idx))) {
    LOG_WARN("Failed to add into", K(idx), K(ret));
  } else {
    OZ (generate_into_variable_info(ns, expr));
  }
  return ret;
}

int ObPLInto::calc_type_constraint(const sql::ObRawExpr &expr,
                                   const ObPLBlockNS &ns,
                                   bool &flag,
                                   ObPLIntegerRange &range) const
{
  int ret = OB_SUCCESS;
#define GET_CONST_EXPR_VALUE(expr, val) \
do {  \
  const ObConstRawExpr *c_expr = static_cast<const ObConstRawExpr*>(expr); \
  CK (OB_NOT_NULL(c_expr)); \
  CK (c_expr->get_value().is_uint64() \
      || c_expr->get_value().is_int() \
      || c_expr->get_value().is_unknown()); \
  OX (val = c_expr->get_value().is_uint64() ? c_expr->get_value().get_uint64() \
        : c_expr->get_value().is_int() ? c_expr->get_value().get_int() \
        : c_expr->get_value().get_unknown()); \
} while (0)

  if (T_QUESTIONMARK == expr.get_expr_type()) {
    const ObConstRawExpr &const_expr = static_cast<const ObConstRawExpr&>(expr);
    OZ (ObPLResolver::get_local_variable_constraint(
      ns, const_expr.get_value().get_unknown(), flag, range));
  } else if (T_OBJ_ACCESS_REF == expr.get_expr_type()) {
    const ObObjAccessRawExpr &obj_access_expr = static_cast<const ObObjAccessRawExpr&>(expr);
    pl::ObPLDataType final_type;
    OZ (obj_access_expr.get_final_type(final_type), obj_access_expr);
    OX (flag = final_type.get_not_null());
    OX (final_type.is_pl_integer_type() ? range.set_range(final_type.get_range()) : void(NULL));
  } else if (T_OP_GET_PACKAGE_VAR == expr.get_expr_type()) {
    const ObSysFunRawExpr &sys_expr = static_cast<const ObSysFunRawExpr&>(expr);
    uint64_t pkg_id = OB_INVALID_ID;
    int64_t var_idx = OB_INVALID_ID;
    const ObPLVar *var = NULL;
    GET_CONST_EXPR_VALUE(sys_expr.get_param_expr(0), pkg_id);
    GET_CONST_EXPR_VALUE(sys_expr.get_param_expr(1), var_idx);
    if (OB_SUCC(ret)) {
      const ObPLBlockNS *root_ns = &ns;
      while(OB_NOT_NULL(root_ns->get_pre_ns())) {
        root_ns = root_ns->get_pre_ns();
      }
      ObPLExternalNS *extern_ns = const_cast<ObPLExternalNS *>(root_ns->get_external_ns());
      if (OB_NOT_NULL(extern_ns)) {
        const ObPLResolveCtx &ctx = extern_ns->get_resolve_ctx();
        // ret = ctx.session_info_.get_pl_engine()->get_package_manager().get_package_var(
        //   ctx, pkg_id, var_idx, var);
        ret = ns.get_package_var(ctx, pkg_id, var_idx, var);
        if (OB_SUCC(ret) && OB_NOT_NULL(var)) {
          flag = var->is_not_null();
          if (var->get_type().is_pl_integer_type()) {
            range.set_range(var->get_type().get_range());
          }
        }
      }
    }
  } else if (T_OP_GET_SUBPROGRAM_VAR == expr.get_expr_type()) {
    const ObSysFunRawExpr *f_expr = static_cast<const ObSysFunRawExpr *>(&expr);
    uint64_t subprogram_id = OB_INVALID_ID;
    int64_t var_idx = OB_INVALID_ID;
    ObPLBlockNS *subprogram_ns = NULL;
    CK (OB_NOT_NULL(f_expr) && f_expr->get_param_count() >= 3);
    GET_CONST_EXPR_VALUE(f_expr->get_param_expr(1), subprogram_id);
    GET_CONST_EXPR_VALUE(f_expr->get_param_expr(2), var_idx);
    OZ (ObPLResolver::get_subprogram_ns(
        const_cast<ObPLBlockNS&>(ns), subprogram_id, subprogram_ns));
    CK (OB_NOT_NULL(subprogram_ns));
    OZ (ObPLResolver::get_local_variable_constraint(*subprogram_ns, var_idx, flag, range));
  } else if (T_OP_GET_USER_VAR == expr.get_expr_type()) {
    flag = false;
  } else if (T_OP_GET_SYS_VAR == expr.get_expr_type()) {
    flag = false;
  } else {
    // do nothing
  }
#undef GET_CONST_EXPR_VALUE

  return ret;
}

int ObPLInto::set_into(const common::ObIArray<int64_t> &idxs, ObPLBlockNS &ns, const common::ObIArray<ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (idxs.count() != exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idx count is not equal to exprs count", K(idxs), K(exprs), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < idxs.count(); ++i) {
      if (OB_ISNULL(exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(i), K(exprs), K(ret));
      } else if (OB_FAIL(add_into(idxs.at(i), ns, *exprs.at(i)))) {
        LOG_WARN("Failed to add into", K(i), K(idxs.at(i)), K(*exprs.at(i)), K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObPLInto::check_into(ObPLFunctionAST &func, ObPLBlockNS &ns, bool is_bulk)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_into().count(); ++i) {
    CK (OB_NOT_NULL(func.get_expr(get_into(i))));
    if (OB_FAIL(ret)) {
    } else if (!is_bulk //Restriction on variable: variable cannot have the data type BOOLEAN!
               && ObTinyIntType == func.get_expr(get_into(i))->get_result_type().get_type()
               && lib::is_oracle_mode()) {
      ret = OB_ERR_EXPRESSION_WRONG_TYPE;
      LOG_WARN("PLS-00382: expression is of wrong type", K(ret), K(i));
    } else if (is_bulk //Bulk variables: variable must be a collection!
               && !func.get_expr(get_into(i))->is_obj_access_expr()) {
      ret = OB_ERR_MIX_SINGLE_MULTI;
      LOG_WARN("PLS-00497: cannot mix between single row and multi-row (BULK) in INTO list",
               K(ret), K(i));
    } else if (func.get_expr(get_into(i))->is_obj_access_expr()) {
      const ObObjAccessRawExpr *access_expr
        = static_cast<const ObObjAccessRawExpr*>(func.get_expr(get_into(i)));
      ObPLDataType type;
      CK (OB_NOT_NULL(access_expr));
      OZ (access_expr->get_final_type(type));
      if (OB_FAIL(ret)) {
      } else if (is_bulk && !type.is_collection_type()) {
        ret = OB_ERR_MIX_SINGLE_MULTI;
        LOG_WARN("PLS-00497: cannot mix between single row and multi-row (BULK) in INTO list",
                 K(ret), K(i));
#ifdef OB_BUILD_ORACLE_PL
      } else if (!is_bulk && type.is_collection_type()) {
        //ret = OB_ERR_INTO_EXPR_ILLEGAL;
        //LOG_WARN("PLS-00597: expression 'string' in the INTO list is of wrong type", K(ret), K(i));
      } else if (is_bulk && type.is_associative_array_type()) {
        const ObUserDefinedType *user_type = NULL;
        const ObAssocArrayType *assoc_type = NULL;
        OZ (ns.get_pl_data_type_by_id(type.get_user_type_id(), user_type));
        CK (OB_NOT_NULL(assoc_type = static_cast<const ObAssocArrayType*>(user_type)));
        CK (OB_NOT_NULL(assoc_type->get_index_type().get_data_type()));
        if (OB_FAIL(ret)) {
        } else if (ObStringTC == assoc_type->get_index_type().get_data_type()->get_type_class()) {
          ret = OB_ERR_BULK_SQL_RESTRICTION;
          LOG_WARN("PLS-00657: Implementation restriction:"
                   " bulk SQL with associative arrays with VARCHAR2 key is not supported.",
                   K(ret), K(i), KPC(assoc_type));
        }
#endif
      }
    }
  }

  return ret;
}

int ObPLDeclareHandlerStmt::DeclareHandler::compare_condition(ObPLConditionType type1, int64_t level1, ObPLConditionType type2, int64_t level2)
{
  int ret = 0;
  if (level1 > level2) {
    ret = 1;
  } else if (level1 < level2) {
    ret = -1;
  } else {
    if (type1 > type2) {
      ret = -1;
    } else if (type1 < type2) {
      ret = 1;
    } else {
      ret = 0;
    }
  }
  return ret;
}

int ObPLDeclareHandlerStmt::add_handler(ObPLDeclareHandlerStmt::DeclareHandler &handler)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && handlers_.count() > 0) {
    DeclareHandler &last = handlers_.at(handlers_.count() - 1);
    DeclareHandler::HandlerDesc *desc = last.get_desc();
    for (int64_t i = 0;
          OB_SUCC(ret) && OB_NOT_NULL(desc) && i < desc->get_conditions().count(); ++i) {
      if (ObPLConditionType::OTHERS == desc->get_conditions().at(i).type_) {
        ret = OB_ERR_OTHERS_MUST_LAST;
        LOG_WARN("OTHERS handler must be last among the exception handlers of a block",
                 K(ret));
      }
    }
  }
  OZ (handlers_.push_back(handler));
  return ret;
}

const ObPLBlockNS *ObPLStmt::get_namespace() const
{
  return NULL == parent_ ? NULL : &parent_->get_namespace();
}

const ObPLSymbolTable *ObPLStmt::get_symbol_table() const
{
  return PL_BLOCK == type_ ? static_cast<const ObPLStmtBlock*>(this)->get_symbol_table() : NULL == get_block() ? NULL : get_block()->get_symbol_table();
}

const ObPLLabelTable *ObPLStmt::get_label_table() const
{
  return PL_BLOCK == type_ ? static_cast<const ObPLStmtBlock*>(this)->get_label_table() : NULL == get_block() ? NULL : get_block()->get_label_table();
}

const ObPLConditionTable *ObPLStmt::get_condition_table() const
{
  return PL_BLOCK == type_ ? static_cast<const ObPLStmtBlock*>(this)->get_condition_table() : NULL == get_block() ? NULL : get_block()->get_condition_table();
}

const ObPLCursorTable *ObPLStmt::get_cursor_table() const
{
  return PL_BLOCK == type_ ? static_cast<const ObPLStmtBlock*>(this)->get_cursor_table() : NULL == get_block() ? NULL : get_block()->get_cursor_table();
}

const common::ObIArray<ObRawExpr*> *ObPLStmt::get_exprs() const
{
  return PL_BLOCK == type_ ? static_cast<const ObPLStmtBlock*>(this)->get_exprs() : NULL == get_block() ? NULL : get_block()->get_exprs();
}

const ObPLCondition *ObPLStmt::get_conditions() const
{
  return NULL == get_condition_table() ? NULL : get_condition_table()->get_conditions();
}

const ObIArray<ObPLCursor *>* ObPLStmt::get_cursors() const
{
  return NULL == get_cursor_table() ? NULL : &(get_cursor_table()->get_cursors());
}

const ObString *ObPLStmt::get_label() const
{
  return NULL == get_label_table() ? NULL : get_label_table()->get_label(label_);
}

int ObPLStmt::set_label_idx(int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_label_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null label table", K(ret));
  } else {
    ObPLLabelTable *pl_label = const_cast<ObPLLabelTable *>(get_label_table());
    if (0 <= idx && idx < pl_label->get_count()) {
      const ObPLStmt *ls = pl_label->get_next_stmt(idx);
      if (OB_ISNULL(ls)) {
        pl_label->set_next_stmt(idx, this);
      }
      label_ = idx;
    }
  }

  return ret;
}

ObPLCompileUnitAST::~ObPLCompileUnitAST()
{
  if (NULL != body_) {
    body_->~ObPLStmtBlock();
  }
}

void ObPLCompileUnitAST::process_default_compile_flag()
{
  if (compile_flag_.has_flag()) {
    common::ObIArray<ObPLRoutineInfo *> &routine_infos = routine_table_.get_routine_infos();
    for (int64_t i = 0; i < routine_infos.count(); ++i) {
      ObPLRoutineInfo *routine_info = routine_infos.at(i);
      if (OB_NOT_NULL(routine_info) && !routine_info->get_compile_flag().has_flag()) {
        routine_info->set_compile_flag(compile_flag_);
      }
    }
  }
  return ;
}

int ObPLCompileUnitAST::extract_assoc_index(
  sql::ObRawExpr &expr, ObIArray<sql::ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  if (expr.is_obj_access_expr()) {
    OZ (add_var_to_array_no_dup(exprs, &expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.get_param_count(); ++i) {
    if (OB_ISNULL(expr.get_param_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param expr is null", K(ret), K(i), K(expr.get_param_expr(i)));
    } else if (OB_FAIL(SMART_CALL(extract_assoc_index(*expr.get_param_expr(i), exprs)))) {
      LOG_WARN("failed to extract assoc index", K(ret), K(i), KPC(expr.get_param_expr(i)));
    }
  }
  return ret;
}

int ObPLCompileUnitAST::check_simple_calc_expr(ObRawExpr *&expr, bool &is_simple)
{

#define CHECK_BINARY_INTEGER(expr, result) \
    do { \
      if (OB_SUCC(ret)) { \
        if (T_QUESTIONMARK == expr->get_expr_type()) { \
          if (expr->get_result_type().is_integer_type()) { \
            int64_t idx = static_cast<ObConstRawExpr*>(expr)->get_value().get_unknown(); \
            if (OB_ISNULL(get_symbol_table().get_symbol(idx))) { \
              ret = OB_ERR_UNEXPECTED; \
            } else if (get_symbol_table().get_symbol(idx)->get_type().is_pl_integer_type() \
                && (PL_PLS_INTEGER == get_symbol_table().get_symbol(idx)->get_type().get_pl_integer_type() \
                    || PL_BINARY_INTEGER == get_symbol_table().get_symbol(idx)->get_type().get_pl_integer_type())) { \
              result = true; \
            } else { /*do nothing*/ } \
          } \
        } else if (expr->is_const_raw_expr()) { \
          ObObj &const_value = static_cast<ObConstRawExpr*>(expr)->get_value(); \
          if (const_value.is_integer_type() && const_value.get_int() >= INT32_MIN && const_value.get_int() <= INT32_MAX) { \
            const_value.set_type(ObInt32Type); \
            static_cast<ObConstRawExpr*>(expr)->set_expr_obj_meta(const_value.get_meta());\
            expr->set_data_type(ObInt32Type); \
            result = true; \
          } else if (const_value.is_number()) { \
            int64_t int_value = 0; \
            if (const_value.get_number().is_valid_int64(int_value) && int_value >= INT32_MIN && int_value <= INT32_MAX) { \
              const_value.set_int32(static_cast<int32_t>(int_value)); \
              static_cast<ObConstRawExpr*>(expr)->set_expr_obj_meta(const_value.get_meta());\
              expr->set_data_type(ObInt32Type); \
              result = true; \
            } \
          } else { /*do nothing*/ } \
        } else { /*do nothing*/ } \
      } \
    } while (0)

  /*
   * 这里仅优化两种情况：
   * 1、pls_integer compare pls_integer
   * 2、pls_integer +/- pls_integer，并向pls_integer赋值
   * 第1种情况，表达式是a Op p，result type是BOOL
   * 第2种情况，表达式是T_FUN_PL_INTEGER_CHECKER(T_FUN_COLUMN_CONV(T_FUN_PL_INTEGER_CHECKER(a op b)))，
   * 或者T_FUN_PL_INTEGER_CHECKER(T_FUN_COLUMN_CONV(a op b))的形式，result type是Int。
   * 第2种情况必须是这个格式是因为，pls_integer +/- pls_integer的result type是Number，向pls_integer赋值必须加T_FUN_COLUMN_CONV，
   * 如果没有T_FUN_COLUMN_CONV，说明是在向Number类型赋值，或者是没有期待类型，这种情况不好区分，所以统一不优化
   * */
  int ret = OB_SUCCESS;
  is_simple = false;
  ObRawExpr *op_expr = NULL;
  bool simple_expr_form = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(expr), K(ret));
  } else if (IS_COMMON_COMPARISON_OP(expr->get_expr_type())) {
    op_expr = expr;
    simple_expr_form = true;
  } else {
    ObRawExpr *cur_expr = expr;
    while (NULL != cur_expr
        && T_OP_ADD != cur_expr->get_expr_type()
        && T_OP_MINUS != cur_expr->get_expr_type()) {
      if (T_FUN_PL_INTEGER_CHECKER == cur_expr->get_expr_type()) {
        cur_expr = cur_expr->get_param_expr(0);
      } else if (T_FUN_COLUMN_CONV == cur_expr->get_expr_type() && cur_expr->get_result_type().is_int32()) {
        cur_expr = cur_expr->get_param_expr(4);
        simple_expr_form = true;
      } else {
        simple_expr_form = false;
        break;
      }
    }
    op_expr = simple_expr_form ? cur_expr : NULL;
  }

  if (OB_SUCC(ret) && NULL != op_expr && simple_expr_form) {
    if (IS_COMMON_COMPARISON_OP(op_expr->get_expr_type())
        || T_OP_ADD == op_expr->get_expr_type()
        || T_OP_MINUS == op_expr->get_expr_type()) {
      ObRawExpr *left = op_expr->get_param_expr(0);
      ObRawExpr *right = op_expr->get_param_expr(1);
      bool left_binary_integer = false;
      bool right_binary_integer = false;
      if (OB_ISNULL(left) || OB_ISNULL(right)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child expr is NULL", K(left), K(right), K(ret));
      } else {

        CHECK_BINARY_INTEGER(left, left_binary_integer);

        CHECK_BINARY_INTEGER(right, right_binary_integer);
      }

      if (OB_SUCC(ret)) {
        is_simple = left_binary_integer && right_binary_integer;
        if (is_simple) {
          if (T_OP_ADD == op_expr->get_expr_type() || T_OP_MINUS == op_expr->get_expr_type()) {
            op_expr->set_data_type(ObInt32Type);
          }
          expr = is_simple ? op_expr : expr;
        }
      }
    }
  }
  return ret;
}

int ObPLCompileUnitAST::add_expr(sql::ObRawExpr* expr, bool is_simple_integer)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    bool is_simple_calc = false;
    if (!is_simple_integer && OB_FAIL(check_simple_calc_expr(expr, is_simple_calc))) {
      LOG_WARN("failed to check simple calc expr", K(expr), K(ret));
    } else if (is_simple_calc || is_simple_integer) {
      if (OB_FAIL(add_simple_calc(exprs_.count()))) {
        LOG_WARN("failed to check simple calc expr", K(expr), K(ret));
      }
    } else { /*do nothing*/ }
  }
  if (OB_SUCC(ret) && OB_FAIL(exprs_.push_back(expr))) {
    LOG_WARN("push back error", K(expr), K(ret));
  }
  return ret;
}

int ObPLCompileUnitAST::set_exprs(common::ObIArray<sql::ObRawExpr*> &exprs)
{
  exprs_.reset();
  return add_exprs(exprs);
}

int ObPLCompileUnitAST::add_exprs(common::ObIArray<sql::ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(add_expr(exprs.at(i)))) {
      LOG_WARN("failed toadd expr", K(i), K(ret));
    }
  }
  return ret;
}

int ObPLCompileUnitAST::add_sql_exprs(common::ObIArray<sql::ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  OZ (add_exprs(exprs));
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    CK (OB_NOT_NULL(exprs.at(i)));
    OZ (extract_assoc_index(*exprs.at(i), obj_access_exprs_));
  }
  return ret;
}

int ObPLCompileUnitAST::add_dependency_objects(
                              const ObIArray<ObSchemaObjVersion> &dependency_objects)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < dependency_objects.count() ; ++i) {
    OZ (add_dependency_object(dependency_objects.at(i)));
  }
  return ret;
}

int ObPLCompileUnitAST::add_dependency_object(const share::schema::ObSchemaObjVersion &obj_version)
{
  return add_dependency_object_impl(get_dependency_table(), obj_version);
}

int ObPLCompileUnitAST::add_dependency_object_impl(const ObPLDependencyTable &dep_tbl,
                                                  const ObSchemaObjVersion &obj_version)
{
  return add_dependency_object_impl(const_cast<ObPLDependencyTable &>(dep_tbl), obj_version);
}
int ObPLCompileUnitAST::add_dependency_object_impl(ObPLDependencyTable &dep_tbl,
                                                  const ObSchemaObjVersion &obj_version)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  for (ObPLDependencyTable::iterator it = dep_tbl.begin();
                                 it < dep_tbl.end(); it++) {
    if (*it == obj_version) {
      exists = true;
      break;
    }
  }
  if (!exists) {
    OZ (dep_tbl.push_back(obj_version));
  }
  return ret;
}

int ObPLCompileUnitAST::generate_symbol_debuginfo()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < symbol_table_.get_count(); ++i) {
    OZ (symbol_debuginfo_table_.add(ObPLVarDebugInfo()));
  }
  if (OB_NOT_NULL(body_)) {
    OZ (body_->generate_symbol_debuginfo(symbol_debuginfo_table_));
  }
  return ret;
}

int ObPLStmtBlock::generate_symbol_debuginfo(
  ObPLSymbolDebugInfoTable &symbol_debuginfo_table) const
{
  int ret = OB_SUCCESS;
  int start = 0;
  int end = 0;
  // 当前Block的Line范围
  for (int64_t i = 0; OB_SUCC(ret) && i < stmts_.count(); ++i) {
    ObPLStmt *stmt = stmts_.at(i);
    CK (OB_NOT_NULL(stmt));
    if (OB_SUCC(ret) && !(0 == stmt->get_line() && 0 == stmt->get_col())) {
      start = start > stmt->get_line() ? stmt->get_line() : start;
      end = end < stmt->get_line() ? stmt->get_line() : end;
    }
  }
  // 设置当前Block内符号的范围
  for (int64_t i = 0; OB_SUCC(ret) && i < ns_.get_symbols().count(); ++i) {
    int64_t idx = ns_.get_symbols().at(i);
    const ObPLSymbolTable *symbol_table = ns_.get_symbol_table();
    const ObPLVar *var = NULL;
    CK (OB_NOT_NULL(symbol_table));
    CK (OB_NOT_NULL(var = symbol_table->get_symbol(idx)));
    OZ (symbol_debuginfo_table.add(
      idx, var->get_name(), var->get_pl_data_type().get_type(), start, end));
  }
  // 递归设置Block内部的Block
  for (int64_t i = 0; OB_SUCC(ret) && i < stmts_.count(); ++i) {
    ObPLStmt *stmt = stmts_.at(i);
    if (OB_ISNULL(stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current stmt is null!", K(ret), K(stmt));
    } else if (PL_IF == stmt->get_type()) {
      ObPLIfStmt *if_stmt = static_cast<ObPLIfStmt *>(stmt);
      CK (OB_NOT_NULL(if_stmt));
      CK (OB_NOT_NULL(if_stmt->get_then()));
      OZ (if_stmt->get_then()->generate_symbol_debuginfo(symbol_debuginfo_table));
      if (OB_SUCC(ret) && OB_NOT_NULL(if_stmt->get_else())) {
        OZ (if_stmt->get_else()->generate_symbol_debuginfo(symbol_debuginfo_table));
      }
    } else if (PL_FOR_LOOP == stmt->get_type()
              || PL_CURSOR_FOR_LOOP == stmt->get_type()
              || PL_LOOP == stmt->get_type()
              || PL_WHILE == stmt->get_type()
              || PL_REPEAT == stmt->get_type()) {
      ObPLLoop *loop_stmt = static_cast<ObPLLoop *>(stmt);
      CK (OB_NOT_NULL(loop_stmt));
      CK (OB_NOT_NULL(loop_stmt->get_body()));
      if (OB_SUCC(ret) && PL_CURSOR_FOR_LOOP == stmt->get_type()) {
        OZ (loop_stmt->get_body()
          ->get_block()->generate_symbol_debuginfo(symbol_debuginfo_table));
      }
      OZ (loop_stmt->get_body()->generate_symbol_debuginfo(symbol_debuginfo_table));
    } else if (PL_HANDLER == stmt->get_type()) {
      ObPLDeclareHandlerStmt *handler_stmt = static_cast<ObPLDeclareHandlerStmt *>(stmt);
      for (int64_t i = 0; OB_SUCC(ret) && i < handler_stmt->get_handlers().count(); ++i) {
        const ObPLDeclareHandlerStmt::DeclareHandler &handler = handler_stmt->get_handler(i);
        CK (OB_NOT_NULL(handler.get_desc()));
        CK (OB_NOT_NULL(handler.get_desc()->get_body()));
        OZ (handler.get_desc()->get_body()->generate_symbol_debuginfo(symbol_debuginfo_table));
      }
    } else if (PL_BLOCK == stmt->get_type()) {
      ObPLStmtBlock *block = static_cast<ObPLStmtBlock*>(stmt);
      CK (OB_NOT_NULL(block));
      OZ (block->generate_symbol_debuginfo(symbol_debuginfo_table));
    }
  }
  return ret;
}

int ObPLFunctionAST::add_argument(const common::ObString &name,
                                  const ObPLDataType &type,
                                  const sql::ObRawExpr *expr,
                                  const common::ObIArray<common::ObString>* type_info,
                                  const bool read_only,
                                  const bool is_udt_self_param)
{
  int ret = OB_SUCCESS;
  ObPLDataType copy = type;
  // is_read_only设置 : oracle mode根据真实情况设置
  // mysql mode 如果是record type,说明是在trigger中使用,也根据实际情况设置,否则为false
  bool is_read_only = lib::is_oracle_mode() ? read_only 
                      : (PL_RECORD_TYPE == type.get_type() ? read_only : false);
  if (copy.is_obj_type()) {
    ObDataType *type = copy.get_data_type();
    if (OB_ISNULL(type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data type is null", K(type), K(copy));
    } else if (type->get_meta_type().is_bit()) {
      ObObjMeta &meta_type = const_cast<ObObjMeta&>(type->get_meta_type());
      meta_type.set_scale(type->get_precision());
    } else {
      ObObjMeta &meta_type = const_cast<ObObjMeta&>(type->get_meta_type());
      meta_type.set_scale(type->get_scale());
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(type_info) && OB_FAIL(copy.set_type_info(type_info))) {
      LOG_WARN("fail to set type info", K(ret));
    } else if (OB_NOT_NULL(expr)
               && OB_FAIL(get_exprs().push_back(const_cast<ObRawExpr*>(expr)))) {
      LOG_WARN("failed to set argument default expr", K(ret), KPC(expr));
    } else if (OB_FAIL(get_symbol_table().add_symbol(name,
                                                     copy,
                                                     OB_NOT_NULL(expr)?get_exprs().count()-1:-1,
                                                     is_read_only,
                                                     false, // not null
                                                     false))) { // default construct
      LOG_WARN("failed to add variable to sysbol table", K(ret));
    } else if(is_udt_self_param) {
      get_symbol_table().set_self_param_idx();
    } else if (type.is_cursor_type()) {
      ObString dummy_sql;
      ObArray<int64_t> dummy_params;
      sql::stmt::StmtType dummy_stmt_type = sql::stmt::T_NONE;
      bool dummy_for_update = false;
      bool dummy_hidden_rowid = false;
      common::ObArray<ObSchemaObjVersion> dummy_ref_objects;
      const ObPLDataType dummy_return_type;
      const ObArray<int64_t> dummy_formal_params;
      OZ (get_cursor_table().add_cursor(get_package_id(),
                                        get_subprogram_id(),
                                        get_symbol_table().get_count() -1,
                                        dummy_sql,
                                        dummy_params,
                                        dummy_sql,
                                        dummy_stmt_type,
                                        dummy_for_update,
                                        dummy_hidden_rowid,
                                        OB_INVALID_ID,
                                        dummy_ref_objects,
                                        NULL, /*ref cursor的row desc不确定*/
                                        dummy_return_type,
                                        dummy_formal_params,
                                        ObPLCursor::PASSED_IN,
                                        false));
    } else { /*do nothing*/ }
  }
  OX (set_arg_count(get_arg_count() + 1));
  return ret;
}

int ObPLFunctionAST::get_argument(int64_t idx, common::ObString &name, ObPLDataType &type) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= get_arg_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid index", K(idx), K(ret));
  } else {
    const ObPLVar *var = get_symbol_table().get_symbol(idx);
    if (OB_ISNULL(var)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pl var is NULL", K(idx), K(get_symbol_table().get_symbol(idx)), K(ret));
    } else {
      name = var->get_name();
      type = var->get_type();
    }
  }
  return ret;
}

int ObPLStmtFactory::allocate(ObPLStmtType type, const ObPLStmtBlock *block, ObPLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  stmt = NULL;
  switch (type) {
  case PL_BLOCK: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLStmtBlock)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLStmtBlock(allocator_);
    }
  }
    break;
  case PL_VAR: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLDeclareVarStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLDeclareVarStmt(allocator_);
    }
  }
    break;
  case PL_USER_SUBTYPE:
  case PL_USER_TYPE: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLDeclareUserTypeStmt)));
    if (stmt != NULL) {
      stmt = new(stmt) ObPLDeclareUserTypeStmt();
    }
    break;
  }
  case PL_ASSIGN: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLAssignStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLAssignStmt(allocator_);
    }
  }
    break;
  case PL_IF: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLIfStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLIfStmt();
    }
  }
    break;
  case PL_LEAVE: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLLeaveStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLLeaveStmt();
    }
    break;
  }
  case PL_ITERATE: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLIterateStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLIterateStmt();
    }
    break;
  }
  case PL_WHILE: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLWhileStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLWhileStmt();
    }
    break;
  }
  case PL_FOR_LOOP: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLForLoopStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLForLoopStmt();
    }
    break;
  }
  case PL_CURSOR_FOR_LOOP: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLCursorForLoopStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLCursorForLoopStmt(allocator_);
    }
    break;
  }
  case PL_FORALL: {
    stmt = static_cast<ObPLForAllStmt*>(allocator_.alloc(sizeof(ObPLForAllStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLForAllStmt();
    }
    break;
  }
  case PL_REPEAT: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLRepeatStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLRepeatStmt();
    }
    break;
  }
  case PL_LOOP: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLLoopStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLLoopStmt();
    }
    break;
  }
  case PL_RETURN: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLReturnStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLReturnStmt();
    }
  }
    break;
  case PL_SQL: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLSqlStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLSqlStmt(allocator_);
    }
  }
    break;
  case PL_EXECUTE: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLExecuteStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLExecuteStmt(allocator_);
    }
  }
    break;
  case PL_EXTEND: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLExtendStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLExtendStmt();
    }
  }
    break;
  case PL_DELETE: {
    stmt = static_cast<ObPLDeleteStmt*>(allocator_.alloc(sizeof(ObPLDeleteStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLDeleteStmt();
    }
  }
    break;
  case PL_COND: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLDeclareCondStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLDeclareCondStmt();
    }
  }
    break;
  case PL_HANDLER: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLDeclareHandlerStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLDeclareHandlerStmt(allocator_);
    }
  }
    break;
  case PL_SIGNAL: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLSignalStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLSignalStmt();
    }
  }
    break;
  case PL_CALL: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLCallStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLCallStmt(allocator_);
    }
  }
    break;
  case PL_CURSOR: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLDeclareCursorStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLDeclareCursorStmt();
    }
  }
    break;
  case PL_OPEN: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLOpenStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLOpenStmt(allocator_);
    }
  }
    break;
  case PL_OPEN_FOR: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLOpenForStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLOpenForStmt(allocator_);
    }
  }
    break;
  case PL_FETCH: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLFetchStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLFetchStmt(allocator_);
    }
  }
    break;
  case PL_CLOSE: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLCloseStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLCloseStmt();
    }
  }
    break;
  case PL_NULL: {
      stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLNullStmt)));
      if (NULL != stmt) {
        stmt = new(stmt)ObPLNullStmt();
      }
    }
    break;
  case PL_PIPE_ROW: {
    stmt = static_cast<ObPLPipeRowStmt *>(allocator_.alloc(sizeof(ObPLPipeRowStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLPipeRowStmt();
    }
  }
    break;
  case PL_ROUTINE_DECL: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLRoutineDeclStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLRoutineDeclStmt();
    }
  }
    break;
  case PL_ROUTINE_DEF: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLRoutineDefStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLRoutineDefStmt();
    }
  }
    break;
  case PL_RAISE_APPLICATION_ERROR: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLRaiseAppErrorStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLRaiseAppErrorStmt(allocator_);
    }
  }
    break;
  case PL_GOTO: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLGotoStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLGotoStmt(allocator_);
    }
  }
    break;
  case PL_TRIM: {
    stmt = static_cast<ObPLStmt *>(allocator_.alloc(sizeof(ObPLTrimStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLTrimStmt();
    }
  }
    break;
  case PL_INTERFACE: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLInterfaceStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLInterfaceStmt();
    }
  }
    break;
  case PL_DO: {
    stmt = static_cast<ObPLStmt*>(allocator_.alloc(sizeof(ObPLDoStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLDoStmt(allocator_);
    }
  }
    break;
  case PL_CASE: {
    stmt = static_cast<ObPLStmt *>(allocator_.alloc(sizeof(ObPLCaseStmt)));
    if (NULL != stmt) {
      stmt = new(stmt)ObPLCaseStmt(allocator_);
    }
  }
    break;
  default:{
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected stmt type", K(ret), K(type));
  }
    break;
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(stmt)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory failed", K(ret), K(stmt));
    } else {
      stmt->set_block(block);
    }
  }
  return ret;
}

int ObPLStmtBlock::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLDeclareVarStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLDeclareUserTypeStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLAssignStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLIfStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLLeaveStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLIterateStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLWhileStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLForLoopStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLCursorForLoopStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLForAllStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLRepeatStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLLoopStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLReturnStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLSqlStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLExecuteStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLExtendStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLDeleteStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLDeclareCondStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLDeclareHandlerStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLSignalStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLCallStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLDeclareCursorStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLOpenStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLOpenForStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLFetchStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLCloseStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLNullStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLPipeRowStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLRaiseAppErrorStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLInterfaceStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLRoutineDefStmt::accept(ObPLStmtVisitor &visitor) const
{
  UNUSED(visitor);
  return 0;
}
int ObPLRoutineDeclStmt::accept(ObPLStmtVisitor &visitor) const
{
  UNUSED(visitor);
  return 0;
}
int ObPLGotoStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLTrimStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLDoStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
int ObPLCaseStmt::accept(ObPLStmtVisitor &visitor) const
{
  return visitor.visit(*this);
}
}
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 从ObPLBlockNS里解析一个symbol，并给出在符号表里的下标，如果找不到返回值为OB_SUCCESS，但是下标返回OB_INVALID_INDEX；
 封装成C接口供Parser调用
 */
int lookup_pl_symbol(const void *pl_ns, const char *symbol, size_t len, int64_t *idx)
{
 int ret = OB_SUCCESS;
 if (OB_ISNULL(pl_ns) || OB_ISNULL(symbol) || len <= 0 || OB_ISNULL(idx)) {
   ret = OB_ERR_UNEXPECTED;
   LOG_WARN("argument input is invalid");
 } else {
   uint64_t parent_id = OB_INVALID_INDEX;
   int64_t var_index = OB_INVALID_INDEX;
   oceanbase::pl::ObPLExternalNS::ExternalType type = oceanbase::pl::ObPLExternalNS::INVALID_VAR;
   oceanbase::pl::ObPLDataType pl_data_type;
   ObString var_name(len, symbol);
   const oceanbase::pl::ObPLBlockNS *ns = static_cast<const oceanbase::pl::ObPLBlockNS *>(pl_ns);
   if (OB_FAIL(ns->resolve_symbol(var_name, type, pl_data_type, parent_id, var_index))) {
     LOG_WARN("failed to get var index", K(var_name), K(ret));
   } else if (oceanbase::pl::ObPLExternalNS::LOCAL_VAR == type
              && !pl_data_type.is_cursor_type()) { // mysql can not access explicit cursor in sql/expression
     *idx = var_index;
   } else { /*do nothing*/ }
 }
 return ret;
}

#ifdef __cplusplus
}
#endif
