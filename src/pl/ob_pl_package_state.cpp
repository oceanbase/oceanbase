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

#include "pl/ob_pl_package_state.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_package_manager.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/serialization.h"
#include "lib/string/ob_string.h"
#include "observer/mysql/obmp_utils.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "pl/ob_pl_resolver.h"
namespace oceanbase
{
using namespace common;
using namespace obmysql;
using namespace observer;
using namespace sql;
namespace pl
{
OB_SERIALIZE_MEMBER(ObPackageStateVersion, package_version_, package_body_version_);

ObPackageStateVersion::ObPackageStateVersion(const ObPackageStateVersion &other)
{
  *this = other;
}

ObPackageStateVersion &ObPackageStateVersion::operator =(const ObPackageStateVersion &other)
{
  if (this != &other) {
    package_version_ = other.package_version_;
    package_body_version_ = other.package_body_version_;
  }
  return *this;
}

bool ObPackageStateVersion::operator ==(const ObPackageStateVersion &other)
{
  bool b_ret = true;
  if (package_version_ != other.package_version_
      || package_body_version_ != other.package_body_version_) {
    b_ret = false;
  }
  return b_ret;
}

OB_SERIALIZE_MEMBER(ObPackageVarSetName, package_id_, state_version_, var_type_, var_idx_);

int ObPackageVarSetName::encode(common::ObIAllocator &alloc, common::ObString &var_name_str)
{
  // @pkg.$package_id$package_version$var_idx
  int ret = OB_SUCCESS;
  const char *key_prefix = "pkg.";
  uint64_t key_prefix_len = strlen(key_prefix);
  int64_t ser_buf_pos = 0;
  uint64_t ser_buf_len = get_serialize_size();
  char *ser_buf = static_cast<char *>(alloc.alloc(ser_buf_len));
  if (OB_ISNULL(ser_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    if (OB_FAIL(serialize(ser_buf, ser_buf_len, ser_buf_pos))) {
      LOG_WARN("package var name serialize failed", K(ret));
    } else {
      uint64_t key_buf_len = 2*ser_buf_len + key_prefix_len+1;
      char *key_buf = static_cast<char *>(alloc.alloc(key_buf_len));
      if (OB_ISNULL(key_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMCPY(key_buf, key_prefix, static_cast<ObString::obstr_size_t>(key_prefix_len));
        if (OB_FAIL(to_hex_cstr(ser_buf, ser_buf_len, key_buf+key_prefix_len, key_buf_len-key_prefix_len))) {
          LOG_WARN("hex encode failed", K(ret));
        } else if (key_buf_len != ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, key_buf, key_buf_len, key_buf, key_buf_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("package var to down case failed", K(ret));
        } else {
          var_name_str.assign_ptr(key_buf, static_cast<ObString::obstr_size_t>(key_buf_len-1));
        }
      }
    }
  }
  if (OB_SUCC(ret) && !OB_ISNULL(ser_buf)) {
    alloc.free(ser_buf);
  }
  return ret;
}

int ObPackageVarSetName::decode(common::ObIAllocator &alloc, const common::ObString &var_name_str)
{
  // $package_id$package_version$var_idx
  int ret = OB_SUCCESS;
  const char *key_prefix = "pkg.";
  uint64_t key_prefix_len = strlen(key_prefix);
  int32_t var_name_len = var_name_str.length();
  int32_t hex_decoed_buf_len = var_name_len / 2;
  int64_t deser_buf_pos = 0;
  ObString var_name_str_upcase;
  char* serialize_buf = NULL;
  char *hex_decoed_buf = static_cast<char *>(alloc.alloc(hex_decoed_buf_len));
  if (OB_ISNULL(hex_decoed_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, var_name_str, var_name_str_upcase))) {
    LOG_WARN("package var name string copy failed", K(ret));
  } else if (var_name_len != ObCharset::caseup(CS_TYPE_UTF8MB4_GENERAL_CI,
                                               var_name_str_upcase.ptr(),
                                               var_name_str_upcase.length(),
                                               var_name_str_upcase.ptr(),
                                               var_name_str_upcase.length())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("package var to upper case failed", K(ret));
  } else if (FALSE_IT(serialize_buf = (var_name_str_upcase.ptr() + key_prefix_len))) {
  } else if (OB_FAIL(hex_to_cstr(serialize_buf,
                                 var_name_len - key_prefix_len,
                                 hex_decoed_buf,
                                 hex_decoed_buf_len))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("hex decode failed", K(ret));
  } else if (OB_FAIL(deserialize(hex_decoed_buf, hex_decoed_buf_len, deser_buf_pos))) {
    LOG_WARN("package var name serialize failed", K(ret));
  } else {
    LOG_DEBUG("decode package var set name",
              K(package_id_), K(var_idx_), K(var_type_),
              K(state_version_.package_version_),
              K(state_version_.package_body_version_));
  }
  return ret;
}

int ObPLPackageState::add_package_var_val(const common::ObObj &value, ObPLType type)
{
  int ret = OB_SUCCESS;
  OZ (types_.push_back(type));
  if (OB_SUCC(ret) && OB_FAIL(vars_.push_back(value))) {
    types_.pop_back();
    LOG_WARN("failed to push back", K(ret), K(value), K(type));
  }
  return ret;
}

void ObPLPackageState::reset(ObSQLSessionInfo *session_info)
{

  package_id_ = common::OB_INVALID_ID;
  changed_vars_.reset();
  for (int64_t i = 0; i < types_.count(); ++i) {
    if (!vars_.at(i).is_ext()) {
    } else if (PL_RECORD_TYPE == types_.at(i)
               || PL_NESTED_TABLE_TYPE == types_.at(i)
               || PL_ASSOCIATIVE_ARRAY_TYPE == types_.at(i)
               || PL_VARRAY_TYPE == types_.at(i)
               || PL_OPAQUE_TYPE == types_.at(i)) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(ObUserDefinedType::destruct_obj(vars_.at(i), session_info))) {
        LOG_WARN("failed to destruct composte obj", K(ret));
      }
    } else if (PL_CURSOR_TYPE == types_.at(i)) {
      ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo *>(vars_.at(i).get_ext());
      if (OB_NOT_NULL(cursor)) {
        cursor->close(*session_info);
        cursor->~ObPLCursorInfo();
      }
    }
  }
  types_.reset();
  vars_.reset();
  inner_allocator_.reset();
  cursor_allocator_.reset();
}

int ObPLPackageState::set_package_var_val(const int64_t var_idx, const ObObj &value, bool deep_copy_complex)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx >= vars_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("invalid var index", K(var_idx), K(vars_.count()), K(ret));
  } else {
    // VAR的生命周期是SESSION级, 因此这里需要深拷贝下
    if (value.need_deep_copy()) {
      int64_t pos = 0;
      char *buf = static_cast<char*>(inner_allocator_.alloc(value.get_deep_copy_size()));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for pacakge var", K(ret), K(buf));
      }
      OZ (vars_.at(var_idx).deep_copy(value, buf, value.get_deep_copy_size(), pos));
    } else if (value.is_pl_extend()
               && value.get_meta().get_extend_type() != PL_CURSOR_TYPE
               && value.get_meta().get_extend_type() != PL_REF_CURSOR_TYPE
               && deep_copy_complex) {
      ObObj copy;
      OZ (ObUserDefinedType::deep_copy_obj(inner_allocator_, value, copy));
      OX (vars_.at(var_idx) = copy);
    } else if (value.is_null()
               && vars_.at(var_idx).is_pl_extend()
               && types_.at(var_idx) != PL_CURSOR_TYPE
               && types_.at(var_idx) != PL_REF_CURSOR_TYPE) {
      CK (vars_.at(var_idx).get_ext() != 0);
      OZ (ObUserDefinedType::destruct_obj(vars_.at(var_idx), NULL));
    } else {
      vars_.at(var_idx) = value;
    }
  }
  return ret;
}

int ObPLPackageState::update_changed_vars(const int64_t var_idx)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx >= vars_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("invalid var index", K(ret), K(var_idx), K(vars_.count()));
  }
  // NOTE: trigger package variables do not need to sync!
  if (!share::schema::ObTriggerInfo::is_trigger_package_id(package_id_)) {
    OZ (changed_vars_.add_member(var_idx));
  }
  return ret;
}

int ObPLPackageState::get_package_var_val(const int64_t var_idx, ObObj &value)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx >= vars_.count()) {
    ret = OB_ARRAY_OUT_OF_RANGE;
    LOG_WARN("invalid var index", K(var_idx), K(vars_.count()), K(ret));
  } else {
    OX (value = vars_.at(var_idx));
  }
  return ret;
}

int ObPLPackageState::make_pkg_var_kv_key(ObIAllocator &alloc, int64_t var_idx, PackageVarType var_type, ObString &key)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx > vars_.count() || INVALID == var_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(var_idx), K(var_type), K(ret));
  } else {
    ObPackageVarSetName key_name;
    key_name.package_id_ = package_id_;
    key_name.state_version_ = state_version_;
    key_name.var_type_ = var_type;
    key_name.var_idx_ = var_idx;
    if (OB_FAIL(key_name.encode(alloc, key))) {
      LOG_WARN("package var name encode failed", K(ret));
    }
  }
  return ret;
}

int ObPLPackageState::make_pkg_var_kv_value(ObPLExecCtx &ctx, ObObj &var_val, int64_t var_idx, ObObj &value)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *sql_session = ctx.exec_ctx_->get_my_session();
  if (OB_ISNULL(sql_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session is null.", K(ret));
  } else {
    pl::ObPLPackageGuard *package_guard = NULL;
    CK (OB_NOT_NULL(sql_session->get_pl_engine()));
    OZ (ctx.exec_ctx_->get_package_guard(package_guard));
    CK (OB_NOT_NULL(package_guard));

    if (OB_SUCC(ret)) {
      const ObPLVar *var = NULL;
      ObPLResolveCtx resolve_ctx(*ctx.allocator_,
                                 *sql_session,
                                 *ctx.exec_ctx_->get_sql_ctx()->schema_guard_,
                                 nullptr != ctx.exec_ctx_->get_package_guard() ? *ctx.exec_ctx_->get_package_guard()
                                                                                 : *package_guard,
                                 *ctx.exec_ctx_->get_sql_proxy(),
                                 false /*is_ps*/);
      OZ (sql_session->get_pl_engine()
          ->get_package_manager().get_package_var(resolve_ctx, package_id_, var_idx, var));
      CK (OB_NOT_NULL(var));
      if (OB_FAIL(ret)) {
      } else if (var->get_type().is_cursor_type()) {
        ObPLCursorInfo *cursor = reinterpret_cast<ObPLCursorInfo *>(var_val.get_ext());
        // package cursor sync, we only sync open status and close status.
        // when remote server got open status, set cursor is sync status,
        // and user can not use cursor when cursor is synced.
        // when remote server got close status, set cursor is normal status,
        // and user can use cursor normally.
        if (OB_ISNULL(cursor) || !cursor->isopen()) {
          OX (value.set_bool(false));
        } else {
          OX (value.set_bool(true));
        }
      } else if (var->get_type().is_opaque_type()) {
        value.set_null();
      } else {
        OZ (var->get_type().serialize(resolve_ctx, var_val, value));
      }
    }
  }
  return ret;
}

int ObPLPackageState::convert_info_to_string_kv(
  ObPLExecCtx &pl_ctx, int64_t var_idx, PackageVarType var_type, ObString &key, ObObj &value)
{
  int ret = OB_SUCCESS;
  if (var_idx < 0 || var_idx >= vars_.count() || INVALID == var_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(var_idx), K(var_type), K(ret));
  } else if (OB_FAIL(make_pkg_var_kv_key(*pl_ctx.allocator_, var_idx, var_type, key))) {
    LOG_WARN("make package var kv key failed", K(var_idx), K(var_type), K(ret));
  } else if (OB_FAIL(make_pkg_var_kv_value(pl_ctx, vars_.at(var_idx), var_idx, value))) {
    LOG_WARN("make package var kv value failed", K(var_idx), K(var_type), K(ret));
  } else {
    LOG_DEBUG("convert pacakge var info to string kv",
              K(package_id_), K(var_idx), K(key), K(value), K(var_type));
  }
  return ret;
}

int ObPLPackageState::convert_changed_info_to_string_kvs(ObPLExecCtx &pl_ctx, ObIArray<ObString> &key, ObIArray<ObObj> &value)
{
  int ret = OB_SUCCESS;
  ObString key_str;
  ObObj value_obj;
  const share::schema::ObPackageInfo *package_info = NULL;
  const uint64_t tenant_id = get_tenant_id_by_object_id(package_id_);
  CK (OB_NOT_NULL(pl_ctx.exec_ctx_));
  CK (OB_NOT_NULL(pl_ctx.exec_ctx_->get_sql_ctx()));
  CK (OB_NOT_NULL(pl_ctx.exec_ctx_->get_sql_ctx()->schema_guard_));
  OZ (pl_ctx.exec_ctx_->get_sql_ctx()->schema_guard_->get_package_info(tenant_id, package_id_, package_info));
  if (OB_NOT_NULL(package_info)) {
    for (int64_t i = 0; i < vars_.count() && OB_SUCCESS == ret; ++i) {
      if (changed_vars_.has_member(i)
          && vars_.at(i).get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
        key_str.reset();
        value_obj.reset();
        if (OB_FAIL(convert_info_to_string_kv(pl_ctx, i, VARIABLE, key_str, value_obj))) {
          LOG_WARN("fail to convert package variable to string kv", K(i), K(ret));
        } else if (OB_FAIL(key.push_back(key_str))) {
          LOG_WARN("fail to push key ", K(ret));
        } else if (OB_FAIL(value.push_back(value_obj))) {
          LOG_WARN("fail to push value ", K(ret));
        } else {
          LOG_DEBUG("convert changed info to strings kvs success!",
                     K(package_id_), K(i), K(key_str), K(value_obj));
        }
      }
    }
  }
  return ret;
}

int ObPLPackageState::remove_user_variables_for_package_state(ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int64_t var_count = vars_.count();
  ObArenaAllocator allocator(ObModIds::OB_PL_TEMP);
  ObString key;
  for (int64_t var_idx = 0; var_idx < var_count; var_idx++) {
    // ignore error code, reset all variables
    key.reset();
    if (OB_FAIL(make_pkg_var_kv_key(allocator, var_idx, VARIABLE, key))) {
      LOG_WARN("make package var name failed", K(ret), K(package_id_), K(var_idx));
    } else if (session.user_variable_exists(key)) {
      if (OB_FAIL(session.remove_user_variable(key))) {
        LOG_WARN("fail to remove user var", K(ret), K(key), K(package_id_), K(var_idx));
      } else if (OB_FAIL(session.remove_changed_user_var(key))) {
        LOG_WARN("fail to remove change user var", K(ret), K(key), K(package_id_), K(var_idx));
      }
    }
  }
  return ret;
}

int ObPLPackageState::check_package_state_valid(ObExecContext &exec_ctx, bool &valid)
{
  int ret = OB_SUCCESS;
  valid = false;
  ObSQLSessionInfo *sql_session = exec_ctx.get_my_session();
  if (OB_ISNULL(sql_session) || OB_ISNULL(sql_session->get_pl_engine())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql session is null.", K(ret));
  } else if (OB_ISNULL(exec_ctx.get_sql_ctx()) || OB_ISNULL(exec_ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx or schema guard is null.", K(ret));
  } else {
    const ObPackageInfo *package_spec_info = NULL;
    const ObPackageInfo *package_body_info = NULL;
    if (OB_FAIL(sql_session->get_pl_engine()->get_package_manager().get_package_schema_info(*exec_ctx.get_sql_ctx()->schema_guard_,
                                                                                            package_id_,
                                                                                            package_spec_info,
                                                                                            package_body_info))) {
      LOG_WARN("package not exist", K(ret), K(package_id_));
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(package_spec_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("package info is null", K(ret), K(package_id_));
    } else {
      ObPackageStateVersion package_version(common::OB_INVALID_VERSION, common::OB_INVALID_VERSION);
      package_version.package_version_ = package_spec_info->get_schema_version();
      if (OB_NOT_NULL(package_body_info)) {
        package_version.package_body_version_ = package_body_info->get_schema_version();
      }
      valid = check_version(package_version);
    }
  }
  return ret;
}

} // end namespace pl
} // end namespace oceanbase
