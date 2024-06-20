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

#define USING_LOG_PREFIX SHARE
#include "share/system_variable/ob_system_variable_factory.h"
#include "share/system_variable/ob_system_variable.h"
#include "share/system_variable/ob_system_variable_alias.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_time_zone_info_manager.h"
#include "lib/oblog/ob_log.h"
#include "lib/number/ob_number_v2.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "share/ob_version.h"
#include "lib/utility/utility.h"
#include "common/sql_mode/ob_sql_mode_utils.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/ob_sql_utils.h"
#include "share/object/ob_obj_cast.h"
#include "observer/ob_server_struct.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_service.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "ob_nls_system_variable.h"
#include "sql/engine/expr/ob_expr_plsql_variable.h"
#include "share/resource_manager/ob_resource_manager_proxy.h"
#include "sql/engine/expr/ob_expr_uuid.h"
#include "lib/locale/ob_locale_type.h"
#include "share/ob_compatibility_control.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/ob_pl_warning.h"
#endif


using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace transaction;
namespace share
{
char ObSpecialSysVarValues::version_comment_[ObSpecialSysVarValues::VERSION_COMMENT_MAX_LEN];
char ObSpecialSysVarValues::version_[ObSpecialSysVarValues::VERSION_MAX_LEN];
char ObSpecialSysVarValues::system_time_zone_str_[ObSpecialSysVarValues::SYSTEM_TIME_ZONE_MAX_LEN];
char ObSpecialSysVarValues::default_coll_int_str_[ObSpecialSysVarValues::COLL_INT_STR_MAX_LEN];
char ObSpecialSysVarValues::server_uuid_[ObSpecialSysVarValues::SERVER_UUID_MAX_LEN];

ObSpecialSysVarValues::ObSpecialSysVarValues()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // OB_SV_VERSION_COMMENT
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(ObSpecialSysVarValues::version_comment_,
                                     ObSpecialSysVarValues::VERSION_COMMENT_MAX_LEN,
                                     pos,
#ifdef OB_BUILD_CLOSE_MODULES
                                     "OceanBase %s (r%s) (Built %s %s)",
#else
                                     "OceanBase_CE %s (r%s) (Built %s %s)",

#endif
                                     PACKAGE_VERSION, build_version(),
                                     build_date(), build_time()))) {
    LOG_ERROR("fail to print version_comment to buff", K(ret));
  }

  // OB_SV_VERSION
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(ObSpecialSysVarValues::version_,
                                     ObSpecialSysVarValues::VERSION_MAX_LEN,
#ifdef OB_BUILD_CLOSE_MODULES
                                     pos, "5.7.25-OceanBase-v%s", PACKAGE_VERSION))) {
#else
                                     pos, "5.7.25-OceanBase_CE-v%s", PACKAGE_VERSION))) {

#endif
    LOG_ERROR("fail to print version to buff", K(ret));
  }

  // OB_SV_SYSTEM_TIME_ZONE
  if (OB_SUCC(ret)) {
    pos = 0;
    tzset(); // init tzname
    int64_t current_time_us = ObTimeUtility::current_time();
    struct tm tmp_tm;
    UNUSED(localtime_r(&current_time_us, &tmp_tm));
    bool is_neg = false;
    if (tmp_tm.tm_gmtoff < 0) {
      is_neg = true;
      tmp_tm.tm_gmtoff = 0 - tmp_tm.tm_gmtoff;
    }
    const int64_t tz_hour = tmp_tm.tm_gmtoff / 3600;
    const int64_t tz_minuts = (tmp_tm.tm_gmtoff % 3600) % 60;
    if (OB_FAIL(databuff_printf(ObSpecialSysVarValues::system_time_zone_str_,
                                ObSpecialSysVarValues::SYSTEM_TIME_ZONE_MAX_LEN,
                                pos,
                                "%s%02ld:%02ld",
                                (is_neg ? "-" : "+"),
                                tz_hour,
                                tz_minuts))) {
      LOG_ERROR("fail to print system_time_zone to buff", K(ret), K(is_neg), K(tz_hour), K(tz_minuts));
    }
  }

  // charset和collation相关
  int64_t default_coll_int = static_cast<int64_t>(ObCharset::get_default_collation(
          ObCharset::get_default_charset()));
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(databuff_printf(ObSpecialSysVarValues::default_coll_int_str_,
                                     ObSpecialSysVarValues::COLL_INT_STR_MAX_LEN,
                                     pos,
                                     "%ld",
                                     default_coll_int))) {
    LOG_ERROR("fail to print coll to buff", K(ret), K(default_coll_int));
  }

  //OB_SV_SERVER_UUID
  if (OB_SUCC(ret)) {
    MEMSET(server_uuid_, '\0', SERVER_UUID_MAX_LEN);
    if (OB_FAIL(ObExprUuid::gen_server_uuid(server_uuid_, SERVER_UUID_MAX_LEN - 1))) {
      LOG_WARN("failed to gen server uuid", K(ret));
    } else {/*do nothing*/}
  }
}

static ObSpecialSysVarValues ob_special_sys_var_values_init;

const char *ObBoolSysVar::BOOL_TYPE_NAMES[] = {"OFF", "ON", 0};

const char *ObSqlModeVar::SQL_MODE_NAMES[] = {
  "REAL_AS_FLOAT", "PIPES_AS_CONCAT", "ANSI_QUOTES", "IGNORE_SPACE", ",",
  "ONLY_FULL_GROUP_BY", "NO_UNSIGNED_SUBTRACTION", "NO_DIR_IN_CREATE",
  "POSTGRESQL", "ORACLE", "MSSQL", "DB2", "MAXDB", "NO_KEY_OPTIONS",
  "NO_TABLE_OPTIONS", "NO_FIELD_OPTIONS", "MYSQL323", "MYSQL40", "ANSI",
  "NO_AUTO_VALUE_ON_ZERO", "NO_BACKSLASH_ESCAPES", "STRICT_TRANS_TABLES",
  "STRICT_ALL_TABLES", "NO_ZERO_IN_DATE", "NO_ZERO_DATE",
  "ALLOW_INVALID_DATES", "ERROR_FOR_DIVISION_BY_ZERO", "TRADITIONAL",
  "NO_AUTO_CREATE_USER", "HIGH_NOT_PRECEDENCE", "NO_ENGINE_SUBSTITUTION",
  "PAD_CHAR_TO_FULL_LENGTH", "ERROR_ON_RESOLVE_CAST", "TIME_TRUNCATE_FRACTIONAL", 0};

const char * ObBasicSysVar::EMPTY_STRING = "";

int ObBasicSysVar::init(const ObObj &value,
                        const ObObj &min_val,
                        const ObObj &max_val,
                        ObObjType type,
                        int64_t flags)
{
  int ret = OB_SUCCESS;
  bool is_nullable = ((flags & ObSysVarFlag::NULLABLE) && (value.get_type() == ObNullType));
  if (OB_UNLIKELY(!(value.get_type() == type || is_nullable))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("value type is unexpected", K(ret), K(value.get_type()), K(type), K(value));
  } else if (!is_base_value_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("base value should be empty", K(ret), K(value), K(base_value_));
  } else {
    base_value_ = value;
    min_val_ = min_val;
    max_val_ = max_val;
    type_ = type;
    flags_ = flags;
  }
  return ret;
}

void ObBasicSysVar::reset()
{
  base_version_ = OB_INVALID_VERSION;
  clean_value();
  type_ = ObUnknownType;
  flags_ = ObSysVarFlag::NONE;
  on_check_and_convert_ = NULL;
  on_update_ = NULL;
  to_select_obj_ = NULL;
  to_show_str_ = NULL;
  get_meta_type_ = NULL;
}

void ObBasicSysVar::clean_value()
{
  clean_base_value();
  clean_inc_value();
  min_val_.reset();
  max_val_.reset();
}

void ObBasicSysVar::clean_base_value()
{
  base_version_ = OB_INVALID_VERSION;
  base_value_.set_nop_value();
}

void ObBasicSysVar::clean_inc_value()
{
  inc_value_.set_nop_value();
}

bool ObBasicSysVar::is_base_value_empty() const
{
  return base_value_.is_nop_value();
}

bool ObBasicSysVar::is_inc_value_empty() const
{
  return inc_value_.is_nop_value();
}

const ObString ObBasicSysVar::get_name() const
{
  return ObSysVarFactory::get_sys_var_name_by_id(get_type());
}

/*
int ObBasicSysVar::is_need_serialize(bool &need_serialize) const
{
  // hard code，不使用内部表__all_sys_variable保存的值
  int ret = OB_SUCCESS;
  int64_t sys_var_idx = -1;
  if (OB_FAIL(ObSysVarFactory::calc_sys_var_store_idx(get_type(), sys_var_idx))) {
    LOG_WARN("fail to calc sys var store idx", K(ret), K(sys_var_idx));
  } else if (OB_UNLIKELY(sys_var_idx < 0) ||
             OB_UNLIKELY(sys_var_idx >= ObSysVariables::get_all_sys_var_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid sys var idx", K(ret),
              K(sys_var_idx), K(ObSysVariables::get_all_sys_var_count()));
  } else {
    need_serialize = (0 != (ObSysVariables::get_flags(sys_var_idx) & ObSysVarFlag::NEED_SERIALIZE));
  }
  return ret;
}
*/

int ObBasicSysVar::log_err_wrong_value_for_var(int error_no, const ObObj &val) const
{
  int ret = OB_SUCCESS;
  SMART_VAR(char[OB_MAX_SQL_LENGTH], val_str_buf) {
    int64_t pos = 0;
    if (OB_UNLIKELY(OB_ERR_WRONG_VALUE_FOR_VAR != error_no)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error no must be OB_ERR_WRONG_VALUE_FOR_VAR",
               K(ret), K(error_no), LITERAL_K(OB_ERR_WRONG_VALUE_FOR_VAR), K(lbt()));
    } else if (OB_FAIL(val.print_plain_str_literal(val_str_buf, OB_MAX_SQL_LENGTH, pos))) {
      LOG_WARN("fail to print_plain_str_literal", K(ret), K(OB_MAX_SQL_LENGTH), K(pos), K(lbt()));
    } else {
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR,
          get_name().length(), get_name().ptr(), static_cast<int>(pos), val_str_buf);
    }
  }
  return ret;
}

int ObBasicSysVar::check_and_convert(ObExecContext &ctx,
                                     const ObSetVar &set_var,
                                     const ObObj &in_val, ObObj &out_val)
{
  int ret = OB_SUCCESS;
  ObObj cur_val = in_val;
  if (OB_FAIL(do_check_and_convert(ctx, set_var, cur_val, out_val))) {
    LOG_WARN("fail to do check", K(ret), K(in_val), K(cur_val));
  } else if (FALSE_IT(cur_val = out_val)) {
  } else if (NULL != on_check_and_convert_ &&
             (OB_FAIL(on_check_and_convert_(ctx, set_var, *this, cur_val, out_val)))) {
    LOG_WARN("fail to run on check", K(ret), K(in_val), K(cur_val));
  } else {}
  return ret;
}

int ObBasicSysVar::check_update_type(const ObSetVar &set_var, const ObObj &val)
{
  UNUSED(set_var);
  UNUSED(val);
  return OB_SUCCESS;
}

int ObBasicSysVar::do_check_and_convert(ObExecContext &ctx,
                                        const ObSetVar &set_var,
                                        const ObObj &in_val, ObObj &out_val)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else {
    if (is_oracle_mode() && (ObNumberType == in_val.get_type())) {
      number::ObNumber num = in_val.get_number();
      int64_t int_val = 0;
      if (num.is_valid_int64(int_val)) {
        out_val.set_int(int_val);
      } else {
        ret = OB_ERR_WRONG_TYPE_FOR_VAR;
        LOG_WARN("not valid int value for var on oracle mode", K(in_val));
      }
    } else {
      out_val = in_val;
    }
  }
  return ret;
}

int ObBasicSysVar::session_update(ObExecContext &ctx,
                                  const ObSetVar &set_var,
                                  const ObObj &val)
{
  int ret = OB_SUCCESS;
  ObString extra_var_name;
  ObString extra_val;
  ObCollationType extra_coll_type = CS_TYPE_INVALID;
  bool should_update_extra_var = false;
  ObObj extra_val_obj;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is NULL", K(ret));
  }
  // 这里暂时写得有点乱，等下个版本就charset和collation不存两份了，只存collation，直接干掉这些代码
  // 改变collation相关的系统变量的时候要同时改变对应的charset系统变量
  if (OB_FAIL(ret)) {
  } else if (set_var.var_name_ == OB_SV_COLLATION_SERVER ||
             set_var.var_name_ == OB_SV_COLLATION_DATABASE ||
             set_var.var_name_ == OB_SV_COLLATION_CONNECTION) {
    ObString coll_str;
    if (ObVarcharType == val.get_type()) {
      coll_str = val.get_varchar();
    } else if (ObIntType == val.get_type()) {
      int64_t coll_int64 = val.get_int();
      if (OB_UNLIKELY(false == ObCharset::is_valid_collation(coll_int64))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid collation", K(ret), K(coll_int64), K(val));
      } else {
        const char *coll_str_ptr = ObCharset::collation_name(
            static_cast<ObCollationType>(coll_int64));
        coll_str = ObString(coll_str_ptr);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid collation", K(ret), K(val));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_charset_var_and_val_by_collation(
                set_var.var_name_, coll_str, extra_var_name, extra_val, extra_coll_type))) {
      LOG_ERROR("fail to get charset variable and value by collation",
                K(ret), K(set_var.var_name_), K(val), K(coll_str));
    } else {
      should_update_extra_var = true;
      extra_val_obj.set_int(static_cast<int64_t>(extra_coll_type));
    }
  } else if (set_var.var_name_ == OB_SV_NLS_DATE_FORMAT
             || set_var.var_name_ == OB_SV_NLS_TIMESTAMP_FORMAT
             || set_var.var_name_ == OB_SV_NLS_TIMESTAMP_TZ_FORMAT) {
    if (OB_UNLIKELY(val.is_null_oracle())) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("date format not recognized", K(ret), K(set_var.var_name_), K(val));
    }
  }
  // 改变charset相关的系统变量的时候要同时改变对应的collation系统变量
  else if (set_var.var_name_ == OB_SV_CHARACTER_SET_SERVER ||
           set_var.var_name_ == OB_SV_CHARACTER_SET_DATABASE ||
           set_var.var_name_ == OB_SV_CHARACTER_SET_CONNECTION) {
    ObString cs_str;
    if (ObVarcharType == val.get_type()) {
      cs_str = val.get_varchar();
    } else if (ObIntType == val.get_type()) {
      int64_t coll_int64 = val.get_int();
      if (OB_UNLIKELY(false == ObCharset::is_valid_collation(coll_int64))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid charset", K(ret), K(coll_int64), K(val));
      } else {
        ObCharsetType cs_type = ObCharset::charset_type_by_coll(
            static_cast<ObCollationType>(coll_int64));
        if (OB_UNLIKELY(CHARSET_INVALID == cs_type)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("is valid collation, but has no valid charset", K(ret), K(coll_int64));
        } else {
          const char *cs_str_ptr = ObCharset::charset_name(cs_type);
          cs_str = ObString(cs_str_ptr);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid charset", K(ret), K(val));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_collation_var_and_val_by_charset(
                set_var.var_name_, cs_str, extra_var_name, extra_val, extra_coll_type))) {
      LOG_ERROR("fail to get collation variable and value by charset",
                K(ret), K(set_var.var_name_), K(val), K(cs_str));
    } else {
      should_update_extra_var = true;
      extra_val_obj.set_int(static_cast<int64_t>(extra_coll_type));
    }
  } else if (set_var.var_name_ == OB_SV__ENABLE_PARALLEL_QUERY) {
      should_update_extra_var = true;
    //
    // 实现 Oracle 兼容行为方式如下：有变量 enable 和 parallel
    //  alter session enable parallel query 时 enable = true, parallel = 1   => 走 manual table dop 规则
    //  alter session disable parallel query 时 enable = false, parallel = 1  => 走 no parallel 规则
    //  alter session force parallel query parallel 1 时 enable = false, parallel = 1  =>  走 no parallel 规则
    //  alter session force parallel query parallel 7 时 enable = true, parallel = 7   => 走 force parallel 规则
    extra_var_name = ObString::make_string(OB_SV__FORCE_PARALLEL_QUERY_DOP);
    extra_val_obj.set_uint64(1);
  } else if (set_var.var_name_ == OB_SV__ENABLE_PARALLEL_DML) {
    should_update_extra_var = true;
    extra_var_name = ObString::make_string(OB_SV__FORCE_PARALLEL_DML_DOP);
    extra_val_obj.set_uint64(1);
  } else if (set_var.var_name_ == OB_SV__FORCE_PARALLEL_QUERY_DOP) {
    should_update_extra_var = true;
    uint64_t parallel = 0;
    extra_var_name = ObString::make_string(OB_SV__ENABLE_PARALLEL_QUERY);
    if (OB_FAIL(val.get_uint64(parallel))) {
      LOG_WARN("unexpected parallel value type", K(val), K(ret));
    } else if (1 == parallel) {
      extra_val_obj.set_int(0);
    } else {
      extra_val_obj.set_int(1);
    }
  } else if (set_var.var_name_ == OB_SV__FORCE_PARALLEL_DDL_DOP) {
    should_update_extra_var = true;
    uint64_t parallel = 0;
    extra_var_name = ObString::make_string(OB_SV__ENABLE_PARALLEL_DDL);
    if (OB_FAIL(val.get_uint64(parallel))) {
      LOG_WARN("unexpected parallel value type", K(val), K(ret));
    } else if (1 == parallel) {
      extra_val_obj.set_int(0);
    } else {
      extra_val_obj.set_int(1);
    }
  } else if (set_var.var_name_ == OB_SV__ENABLE_PARALLEL_DDL) {
    should_update_extra_var = true;
    extra_var_name = ObString::make_string(OB_SV__FORCE_PARALLEL_DDL_DOP);
    extra_val_obj.set_uint64(1);
  }

  // 更新需要额外更新的系统变量
  //FIXME 暂不考虑原子性
  if (true == should_update_extra_var && OB_SUCC(ret)) {
    if (OB_FAIL(session->update_sys_variable_by_name(extra_var_name, extra_val_obj))) {
      LOG_ERROR("fail to set extra variable to session",
                K(ret), K(extra_var_name), K(extra_val_obj));
    } else {}
  } else {}

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session->update_sys_variable_by_name(set_var.var_name_, val))) {
    if (OB_LIKELY(OB_ERR_SYS_VARIABLE_UNKNOWN == ret)) {
      LOG_USER_ERROR(OB_ERR_SYS_VARIABLE_UNKNOWN, set_var.var_name_.length(), set_var.var_name_.ptr());
    } else {
      LOG_WARN("fail to set variable to session", K(ret), K(set_var.var_name_), K(val));
    }
  } else {}
  return ret;
}

int ObBasicSysVar::update(ObExecContext &ctx,
                          const ObSetVar &set_var,
                          const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (NULL != on_update_) {
    if (OB_FAIL(on_update_(ctx, set_var, *this, val))) {
      LOG_WARN("fail to on update", K(ret), K(set_var), K(val), K(*this));
    }
  }
  return ret;
}

ObObjType ObBasicSysVar::inner_get_meta_type() const
{
  return type_;
}

int ObBasicSysVar::inner_to_select_obj(ObIAllocator &allocator,
                                       const ObBasicSessionInfo &session,
                                       ObObj &select_obj) const
{
  UNUSED(allocator);
  UNUSED(session);
  int ret = OB_SUCCESS;
  select_obj = get_value();
  return ret;
}

int ObBasicSysVar::inner_to_show_str(ObIAllocator &allocator,
                                     const ObBasicSessionInfo &session,
                                     ObString &show_str) const
{
  int ret = OB_SUCCESS;
  const ObObj &value = get_value();
  const ObObj *res_obj = NULL;
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session);
  ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, ObCharset::get_system_collation());
  EXPR_CAST_OBJ_V2(ObVarcharType, value, res_obj);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(res_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys var casted obj ptr is NULL", K(ret), K(value), K(get_name()));
  } else if (OB_UNLIKELY(ObVarcharType != res_obj->get_type())) {
    LOG_WARN("sys var casted obj is not ObVarcharType",
             K(ret), K(value), K(*res_obj), K(get_name()));
  } else {
    show_str = res_obj->get_varchar();
  }
  return ret;
}

int64_t ObBasicSysVar::get_base_version() const
{
  return base_version_;
}

ObObjType ObBasicSysVar::get_meta_type() const
{
  ObObjType ret_type = ObMaxType;
  if (NULL == get_meta_type_) {
    ret_type = inner_get_meta_type();
  } else {
    ret_type = get_meta_type_();
  }
  return ret_type;
}

const ObObj &ObBasicSysVar::get_value() const
{
  return is_inc_value_empty() ? base_value_ : inc_value_;
}

const ObObj &ObBasicSysVar::get_base_value() const
{
  return base_value_;
}

const ObObj &ObBasicSysVar::get_inc_value() const
{
  return inc_value_;
}

const ObObj &ObBasicSysVar::get_min_val() const
{
  return min_val_;
}

const ObObj &ObBasicSysVar::get_max_val() const
{
  return max_val_;
}

void ObBasicSysVar::set_value(const ObObj &value)
{
  inc_value_ = value;
}

ObObjType ObBasicSysVar::get_data_type() const
{
  return type_;
}

void ObBasicSysVar::set_data_type(ObObjType type)
{
  type_ = type;
}

void ObBasicSysVar::set_flags(int64_t flags)
{
  flags_ = flags;
}

int ObBasicSysVar::to_select_obj(ObIAllocator &allocator,
                                 const ObBasicSessionInfo &session,
                                 ObObj &select_obj) const
{
  int ret = OB_SUCCESS;
  if (NULL == to_select_obj_) {
    if (OB_FAIL(inner_to_select_obj(allocator, session, select_obj))) {
      LOG_WARN("fail to call inner_to_select_obj", K(ret), K(*this));
    } else {}
  } else {
    if (OB_FAIL(to_select_obj_(allocator, session, *this, select_obj))) {
      LOG_WARN("fail to call to_select_obj_", K(ret), K(*this));
    } else {}
  }
  return ret;
}

int ObBasicSysVar::to_show_str(ObIAllocator &allocator,
                               const ObBasicSessionInfo &session,
                               ObString &show_str) const
{
  int ret = OB_SUCCESS;
  if (NULL == to_show_str_) {
    if (OB_FAIL(inner_to_show_str(allocator, session, show_str))) {
      LOG_WARN("fail to call inner_to_show_str", K(ret), K(*this));
    } else {}
  } else {
    if (OB_FAIL(to_show_str_(allocator, session, *this, show_str))) {
      LOG_WARN("fail to call to_show_str_", K(ret), K(*this));
    } else {}
  }
  return ret;
}



int ObBasicSysVar::get_charset_var_and_val_by_collation(const ObString &coll_var_name,
                                                        const ObString &coll_val,
                                                        ObString &cs_var_name,
                                                        ObString &cs_val,
                                                        ObCollationType &coll_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCharsetSysVarPair::get_charset_var_by_collation_var(
              coll_var_name, cs_var_name))) {
    LOG_ERROR("fail to get collation variable by charset variable",
              K(ret), K(coll_var_name));
  } else if (OB_FAIL(ObCharset::charset_name_by_coll(coll_val, cs_val))) {
    LOG_ERROR("fail to get charset type by collation", K(ret), K(coll_val));
  } else {
    coll_type = ObCharset::collation_type(coll_val);
  }
  return ret;
}

int ObBasicSysVar::get_collation_var_and_val_by_charset(const ObString &cs_var_name,
                                                        const ObString &cs_val,
                                                        ObString &coll_var_name,
                                                        ObString &coll_val,
                                                        ObCollationType &coll_type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCharsetSysVarPair::get_collation_var_by_charset_var(
              cs_var_name, coll_var_name))) {
    LOG_ERROR("fail to get charset variable by collation variable", K(ret), K(cs_var_name));
  } else {
    coll_type = CS_TYPE_INVALID;
    ObCharsetType cs_type = ObCharset::charset_type(cs_val);
    if (OB_UNLIKELY(CHARSET_INVALID == cs_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid charset type", K(ret), K(cs_val));
    } else if (OB_FAIL(ObCharset::get_default_collation(cs_type, coll_type))) {
      LOG_ERROR("fail to get default collation", K(ret), K(cs_type));
    } else if (OB_FAIL(ObCharset::collation_name(coll_type, coll_val))) {
      LOG_ERROR("fail to get collation name", K(ret), K(coll_type));
    } else {
      // empty
    }
  }
  return ret;
}

int ObBasicSysVar::check_and_convert_int_tc_value(const common::ObObj &value,
                                                  int64_t invalid_value,
                                                  int64_t &result_value) const
{
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  switch (value.get_type()) {
    case ObTinyIntType:
      int_value = value.get_tinyint();
      break;
    case ObSmallIntType:
      int_value = value.get_smallint();
      break;
    case ObMediumIntType:
      int_value = value.get_mediumint();
      break;
    case ObInt32Type:
      int_value = value.get_int32();
      break;
    case ObIntType:
      int_value = value.get_int();
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected type", K(ret), K(value.get_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (int_value < 0 || int_value >= invalid_value) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
  } else {
    result_value = int_value;
  }
  return ret;
}

int ObBasicSysVar::check_and_convert_uint_tc_value(const common::ObObj &value,
                                                   uint64_t invalid_value,
                                                   uint64_t &result_value) const
{
  int ret = OB_SUCCESS;
  uint64_t uint_value = 0;
  switch (value.get_type()) {
    case ObUTinyIntType:
      uint_value = value.get_utinyint();
      break;
    case ObUSmallIntType:
      uint_value = value.get_usmallint();
      break;
    case ObUMediumIntType:
      uint_value = value.get_umediumint();
      break;
    case ObUInt32Type:
      uint_value = value.get_uint32();
      break;
    case ObUInt64Type:
      uint_value = value.get_uint64();
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected type", K(ret), K(value.get_type()));
  }
  if (OB_FAIL(ret)) {
  } else if (uint_value >= invalid_value) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
  } else {
    result_value = uint_value;
  }
  return ret;
}

DEF_TO_STRING(ObBasicSysVar)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("sys_var_name", get_name(), "sys_var_id", get_type(),
       K_(base_value), K_(inc_value), K_(type), K_(flags), K_(is_enum_type));
  J_OBJ_END();
  return pos;
}

// base_value只是存储基线数据，目前看需要序列化的场景一定是操作增量数据。
//OB_SERIALIZE_MEMBER(ObBasicSysVar, inc_value_, type_, flags_);

OB_DEF_SERIALIZE(ObBasicSysVar)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, get_value(), type_, flags_, is_enum_type_);
  return ret;
}

OB_DEF_DESERIALIZE(ObBasicSysVar)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, inc_value_, type_, flags_, is_enum_type_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObBasicSysVar)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, get_value(), type_, flags_, is_enum_type_);
  return len;
}

void ObTypeLibSysVar::reset()
{
  ObBasicSysVar::reset();
  type_lib_.reset();
}

int ObTypeLibSysVar::check_update_type(const ObSetVar &set_var, const ObObj &val)
{
  int ret = OB_SUCCESS;
  // 为了跟mysql表现得一样，这里不判断flags_中是否带有ObSysVarFlag::NULLABLE，
  // 而在do_check_and_convert函数中判断in_val的type为ObNullType再报错
  if (true == set_var.is_set_default_ || ObNullType == val.get_type()) {
    // do nothing
  } else if (false == ob_is_integer_type(val.get_type())
             && false == ob_is_string_type(val.get_type())) {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    if (is_oracle_mode()) {
      if (ObNumberType == val.get_type()) {
        number::ObNumber num = val.get_number();
        if (num.is_valid_int()) {
          ret = OB_SUCCESS;
          LOG_DEBUG("number is valid int", K(val), K(num));
        }
      } else if (ob_is_decimal_int(val.get_type())) {
        int tmp_ret = ret;
        bool is_valid_int64 = false;
        int64_t res_v = 0;
        if (OB_FAIL(wide::check_range_valid_int64(val.get_decimal_int(), val.get_int_bytes(),
                                                  is_valid_int64, res_v))) {
          LOG_WARN("check valid int64 failed", K(ret));
        } else if (is_valid_int64) {
          ret = OB_SUCCESS;
          LOG_DEBUG("decimal int is valid int", K(val), K(res_v));
        }
      }
    }
    if (OB_SUCCESS != ret) {
      LOG_WARN("wrong type for var", K(ret), K(val));
    }
  } else {}
  return ret;
}

int ObTypeLibSysVar::inner_to_show_str(ObIAllocator &allocator,
                                       const ObBasicSessionInfo &session,
                                       ObString &show_str) const
{
  int ret = OB_SUCCESS;
  const ObObj &value = get_value();
  if (OB_UNLIKELY(false == ob_is_integer_type(value.get_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid type of type lib", K(*this), K(value.get_type()));
  } else {
    const ObObj *res_obj = NULL;
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(&session);
    ObCastCtx cast_ctx(&allocator, &dtc_params, CM_NONE, ObCharset::get_system_collation());
    EXPR_CAST_OBJ_V2(ObUInt64Type, value, res_obj);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(res_obj)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys var casted obj ptr is NULL", K(ret), K(value), K(get_name()));
    } else if (OB_UNLIKELY(ObUInt64Type != res_obj->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys var casted obj is not ObUInt64Type", K(ret),
               K(value), K(*res_obj), K(get_name()));
    } else {
      int64_t type_lib_idx = static_cast<int64_t>(res_obj->get_uint64()); // FIXME 这里不考虑溢出
      if (OB_UNLIKELY(type_lib_idx < 0) || OB_UNLIKELY(type_lib_idx >= type_lib_.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid type lib idx", K(ret), K(type_lib_idx), K(value), K(get_name()));
      } else {
        show_str = ObString(type_lib_.type_names_[type_lib_idx]);
      }
    }
  }
  return ret;
}

int ObTypeLibSysVar::find_type(const ObString &type, int64_t &type_index) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < type_lib_.count_; ++i) {
    if (0 == type.case_compare(type_lib_.type_names_[i])) {
      type_index = i;
      ret = OB_SUCCESS;
    } else {}
  }
  return ret;
}

int ObTypeLibSysVar::do_check_and_convert(ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const ObObj &in_val, ObObj &out_val)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (ObNullType == in_val.get_type()) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, set_var.var_name_.length(),
                   set_var.var_name_.ptr(), (int)strlen("NULL"), "NULL");
  } else if (true == ob_is_string_type(in_val.get_type())) {
    ObString str_val;
    int64_t type_idx = -1;
    if (ObCharType == in_val.get_type() || ObVarcharType == in_val.get_type()) {
      if (OB_FAIL(in_val.get_string(str_val))) {
        LOG_ERROR("fail to get char", K(ret), K(in_val));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected type", K(ret), K(in_val));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(find_type(str_val, type_idx))) {
      if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
        }
      } else {
        LOG_WARN("fail to find type", K(ret), K(str_val), K(in_val));
      }
    } else if (OB_UNLIKELY(type_idx < 0) || OB_UNLIKELY(type_idx >= type_lib_.count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("type idx is invalid", K(ret), K(type_idx), K(str_val), K(in_val));
    } else {
      out_val.set_int(type_idx);
    }
  } else if (true == ob_is_int_tc(in_val.get_type())) {
    int64_t int64_val = 0;
    if (OB_FAIL(check_and_convert_int_tc_value(in_val, type_lib_.count_, int64_val))) {
      if (OB_LIKELY(OB_ERR_WRONG_VALUE_FOR_VAR == ret)) {
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
        }
      } else {
        LOG_WARN("fail to check int tc value", K(ret), K(in_val));
      }
    } else {
      out_val.set_int(int64_val);
    }
  } else if (true == ob_is_uint_tc(in_val.get_type())) {
    uint64_t uint64_val = 0;
    if (OB_FAIL(check_and_convert_uint_tc_value(in_val, type_lib_.count_, uint64_val))) {
      if (OB_LIKELY(OB_ERR_WRONG_VALUE_FOR_VAR == ret)) {
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
        }
      } else {
        LOG_WARN("fail to check uint tc value", K(ret), K(in_val));
      }
    } else if (uint64_val > static_cast<uint64_t>(INT64_MAX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("value is lager than INT64_MAX", K(ret), K(uint64_val), K(in_val));
    } else {
      out_val.set_int(static_cast<int64_t>(uint64_val));
    }
  } else if (is_oracle_mode() && (ObNumberType == in_val.get_type())) {
    number::ObNumber num = in_val.get_number();
    int64_t int_val = 0;
    if (num.is_valid_int64(int_val)) {
      if (int_val < 0 || int_val >= type_lib_.count_) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
        }
      } else {
        out_val.set_int(int_val);
      }
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("not valid int value for var on oracle mode", K(in_val));
    }
  } else if (is_oracle_mode() && ob_is_decimal_int(in_val.get_type())) {
    int64_t res_v = 0;
    bool is_valid_int64 = false;
    if (OB_FAIL(wide::check_range_valid_int64(in_val.get_decimal_int(), in_val.get_int_bytes(),
                                              is_valid_int64, res_v))) {
      LOG_WARN("check int64 range failed", K(ret));
    } else if (is_valid_int64) {
      if (res_v < 0 || res_v >= type_lib_.count_) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
        }
      } else {
        out_val.set_int(res_v);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid type", K(ret), K(in_val.get_type()));
  }
  return ret;
}

int ObEnumSysVar::inner_to_select_obj(ObIAllocator &allocator,
                                      const ObBasicSessionInfo &session,
                                      ObObj &select_obj) const
{
  int ret = OB_SUCCESS;
  ObString show_str;
  if (OB_FAIL(inner_to_show_str(allocator, session, show_str))) {
    LOG_WARN("fail to convert to show str", K(ret), K(get_value()));
  } else {
    // 对于ObEnumSysVar类，inner_to_select_obj和inner_to_show_str显示的内容一样
    select_obj.set_varchar(show_str);
    select_obj.set_collation_type(ObCharset::get_system_collation());
  }
  return ret;
}

int ObCharsetSysVar::check_update_type(const ObSetVar &set_var, const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_
      || (0 != (flags_ & ObSysVarFlag::NULLABLE) && ObNullType == val.get_type())) {
    // do nothing
  } else if (false == ob_is_integer_type(val.get_type())
             && false == ob_is_string_type(val.get_type())) {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    if (is_oracle_mode() && ObNumberType == val.get_type()) {
      number::ObNumber num = val.get_number();
      if (num.is_valid_int()) {
        ret = OB_SUCCESS;
        LOG_DEBUG("number is valid int", K(val), K(num));
      }
    }
    if (OB_SUCCESS != ret) {
      LOG_WARN("wrong type for var", K(ret), K(val));
    }
  }
  return ret;
}

int ObCharsetSysVar::do_check_and_convert(ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const ObObj &in_val, ObObj &out_val)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret), K(*this));
  } else if (true == set_var.is_set_default_) {
    // do nothing
  } else if (ObNullType == in_val.get_type()) {
    if (0 != (flags_ & ObSysVarFlag::NULLABLE)) {
      // do nothing
    } else {
      ret = OB_ERR_WRONG_VALUE_FOR_VAR;
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, set_var.var_name_.length(),
                     set_var.var_name_.ptr(), (int)strlen("NULL"), "NULL");
    }
  } else if (true == ob_is_string_type(in_val.get_type())) {
    ObCastCtx cast_ctx(&ctx.get_allocator(), &dtc_params, CM_NONE, ObCharset::get_system_collation());
    ObObj buf_obj;
    const ObObj *res_obj_ptr = NULL;
    if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, in_val, buf_obj, res_obj_ptr))) {
      LOG_WARN("failed to cast object to ObVarcharType ", K(ret), K(in_val));
    } else if (OB_ISNULL(res_obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to cast obj, but res_obj_ptr is NULL", K(ret));
    } else {
      out_val = *res_obj_ptr;
    }
  } else if (true == ob_is_integer_type(in_val.get_type())) {
    ObCastCtx cast_ctx(&ctx.get_allocator(), &dtc_params, CM_NONE, ObCharset::get_system_collation());
    ObObj buf_obj;
    const ObObj *res_obj_ptr = NULL;
    if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, in_val, buf_obj, res_obj_ptr))) {
      LOG_WARN("failed to cast object to ObIntType ", K(ret), K(in_val));
    } else if (OB_ISNULL(res_obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to cast obj, but res_obj_ptr is NULL", K(ret));
    } else {
      out_val = *res_obj_ptr;
    }
  } else if (is_oracle_mode() && (ObNumberType == in_val.get_type())) {
    number::ObNumber num = in_val.get_number();
    int64_t int_val = 0;
    if (num.is_valid_int64(int_val)) {
      out_val.set_int(int_val);
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("not valid int value for var on oracle mode", K(in_val));
    }
  } else {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("invalid type ", K(ret), K(in_val));
  }
  return ret;
}

int ObVersionSysVar::do_check_and_convert(ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const ObObj &in_val, ObObj &out_val)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret), K(*this));
  } else if (true == set_var.is_set_default_) {
    ret = OB_ERR_NO_DEFAULT;
    LOG_USER_ERROR(OB_ERR_NO_DEFAULT, set_var.var_name_.length(), set_var.var_name_.ptr());
  } else if (ObNullType == in_val.get_type()) {
    if (0 != (flags_ & ObSysVarFlag::NULLABLE)) {
      // do nothing
    } else {
      ret = OB_ERR_WRONG_VALUE_FOR_VAR;
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, set_var.var_name_.length(),
                     set_var.var_name_.ptr(), (int)strlen("NULL"), "NULL");
    }
  } else if (true == ob_is_string_type(in_val.get_type())) {
    ObCastCtx cast_ctx(&ctx.get_allocator(), &dtc_params, CM_NONE, ObCharset::get_system_collation());
    ObObj buf_obj;
    const ObObj *res_obj_ptr = NULL;
    if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, in_val, buf_obj, res_obj_ptr))) {
      LOG_WARN("failed to cast object to ObVarcharType ", K(ret), K(in_val));
    } else if (OB_ISNULL(res_obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to cast obj, but res_obj_ptr is NULL", K(ret));
    } else {
      out_val = *res_obj_ptr;
    }
  } else if (true == ob_is_integer_type(in_val.get_type())) {
    ObCastCtx cast_ctx(&ctx.get_allocator(), &dtc_params, CM_NONE, ObCharset::get_system_collation());
    ObObj buf_obj;
    const ObObj *res_obj_ptr = NULL;
    if (OB_FAIL(ObObjCaster::to_type(ObUInt64Type, cast_ctx, in_val, buf_obj, res_obj_ptr))) {
      LOG_WARN("failed to cast object to ObUInt64Type ", K(ret), K(in_val));
    } else if (OB_ISNULL(res_obj_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("succ to cast obj, but res_obj_ptr is NULL", K(ret));
    } else {
      out_val = *res_obj_ptr;
    }
  } else if (is_oracle_mode() && (ObNumberType == in_val.get_type())) {
    number::ObNumber num = in_val.get_number();
    uint64_t uint_val = 0;
    if (num.is_valid_uint64(uint_val)) {
      out_val.set_uint64(uint_val);
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("not valid int value for var on oracle mode", K(in_val));
    }
  } else {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("invalid type ", K(ret), K(in_val));
  }
  return ret;
}

int ObTinyintSysVar::check_update_type(const ObSetVar &set_var, const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_
      || (0 != (flags_ & ObSysVarFlag::NULLABLE) && ObNullType == val.get_type())) {
    // do nothing
  } else if (ObTinyIntType != val.get_type()) {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    if (is_oracle_mode() && ObNumberType == val.get_type()) {
      number::ObNumber num = val.get_number();
      //do value range check in do_check_and_convert
      if (num.is_valid_int()) {
        ret = OB_SUCCESS;
        LOG_DEBUG("number is valid int", K(val), K(num));
      } else {
        LOG_WARN("number is not valid int for sys var on oracle mode", K(val), K(num));
      }
    }
    if (OB_SUCCESS != ret) {
      LOG_WARN("wrong type for var", K(ret), K(val));
    }
  }
  return ret;
}

int ObTinyintSysVar::do_check_and_convert(ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const ObObj &in_val,
                                          ObObj &out_val)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (is_oracle_mode() && (ObNumberType == in_val.get_type())) {
    number::ObNumber num = in_val.get_number();
    int64_t int_val = 0;
    if (num.is_valid_int64(int_val)) {
      ObObj tmp_val;
      tmp_val.set_int(int_val);
      if (OB_FAIL(check_and_convert_int_tc_value(tmp_val, (1LL << 8), int_val))) {
        if (OB_ERR_WRONG_VALUE_FOR_VAR == ret) {
          int log_ret = OB_SUCCESS;
          if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
            // log_ret仅用于打日志，不覆盖ret
            LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
          } else {}
        } else {
          LOG_WARN("fail to check uint tc value", K(ret), K(in_val));
        }
        LOG_WARN("value is not valid tinyint for sys var on oracle mode", K(in_val), K(out_val));
      } else {
        out_val.set_tinyint(static_cast<int8_t>(int_val));
      }
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("not valid int value for sys var on oracle mode", K(in_val));
    }
  } else {
    out_val = in_val;
  }
  return ret;
}

int ObIntSysVar::check_update_type(const ObSetVar &set_var, const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_
      || (0 != (flags_ & ObSysVarFlag::NULLABLE) && ObNullType == val.get_type())) {
    // do nothing
  } else if (false == ob_is_integer_type(val.get_type())
             && ObNumberType != val.get_type()
             && ObDecimalIntType != val.get_type()) {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("wrong type for var", K(ret), K(val));
  }
  return ret;
}

int ObIntSysVar::do_check_and_convert(ObExecContext &ctx,
                                      const ObSetVar &set_var,
                                      const ObObj &in_val,
                                      ObObj &out_val)
{
  int ret = OB_SUCCESS;
  bool is_converted = false;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (OB_FAIL(do_convert(ctx, in_val, out_val, is_converted))) {
    LOG_WARN("fail to convert variable", K(ret), K(set_var.var_name_));
  } else if (is_converted && OB_FAIL(ObSysVarUtils::log_bounds_error_or_warning(
              ctx, set_var, in_val))) {
    if (OB_UNLIKELY(OB_ERR_WRONG_VALUE_FOR_VAR != ret)) {
      LOG_WARN("fail to log bounds error or warnning", K(ret));
    } else {
      LOG_WARN("wrong value for sys var", K(ret));
    }
  }
  return ret;
}

#define CHECK_DECIMAL_INT_VALID(TYPE)                      \
  case sizeof(TYPE##_t): {                                 \
    const TYPE##_t &l = *(decint->TYPE##_v_);              \
    const TYPE##_t r = get_scale_factor<TYPE##_t>(scale);  \
    is_in_val_valid = ((l % r) == 0);                      \
    if (is_in_val_valid) {                                 \
      div_res_val.from(l / r);                             \
    }                                                      \
    break;                                                 \
  }

int ObIntSysVar::do_convert(ObExecContext &ctx,
                            const common::ObObj &in_val,
                            common::ObObj &out_val,
                            bool &is_converted)
{
  int ret = OB_SUCCESS;
  is_converted = false;
  ObExprCtx expr_ctx;
  ObObj tmp_obj;
  if (OB_UNLIKELY(ObIntType != type_ && ObUInt64Type != type_) ||
      OB_ISNULL(expr_ctx.phy_plan_ctx_ = ctx.get_physical_plan_ctx()) ||
      OB_ISNULL(expr_ctx.my_session_ = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("type_ is neigher ObIntType nor ObUInt64Type, "
              "or physical plan ctx in ObExecContext is not NULL, "
              "or session in ObExecContext is NULL", K(ret),
              K(type_), K(expr_ctx.phy_plan_ctx_), K(expr_ctx.my_session_));
  } else {
    expr_ctx.calc_buf_ = &(ctx.get_allocator());
    expr_ctx.exec_ctx_ = &ctx;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);

    // FIXME(yyy),因为当前ob在parser阶段将所有大于int32的数值均转换为decimal处理，与mysql不兼容;
    // 因此，在这里进行特殊处理
    if (OB_FAIL(ret)) {
    } else if (ObNumberType == in_val.get_type()) {
      common::number::ObNumber in_number;
      int64_t int_val = 0;
      uint64_t uint_val = 0;
      if (OB_FAIL(in_val.get_number(in_number))) {
        LOG_WARN("fail to get number", K(ret), K(in_val));
      } else if (in_number.is_valid_int64(int_val)) {
        tmp_obj.set_int(int_val);
      } else if (in_number.is_valid_uint64(uint_val)) {
        tmp_obj.set_uint64(uint_val);
      } else {
        ret = OB_ERR_WRONG_TYPE_FOR_VAR;
        LOG_WARN("wrong type for int variables", K(ret), K(in_val));
      }
    } else if (ObDecimalIntType == in_val.get_type()) {
      bool is_in_val_valid = false;
      const ObDecimalInt *decint = in_val.get_decimal_int();
      const int32_t int_bytes = in_val.get_int_bytes();
      const int16_t scale = in_val.get_scale();
      ObDecimalIntBuilder div_res_val;
      switch (int_bytes) {
        CHECK_DECIMAL_INT_VALID(int32)
        CHECK_DECIMAL_INT_VALID(int64)
        CHECK_DECIMAL_INT_VALID(int128)
        CHECK_DECIMAL_INT_VALID(int256)
        CHECK_DECIMAL_INT_VALID(int512)
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("int_bytes is unexpected", K(ret), K(int_bytes));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (is_in_val_valid) {
          int64_t tmp_int = 0;
          uint64_t tmp_uint = 0;
          if (OB_FAIL(wide::check_range_valid_int64(div_res_val.get_decimal_int(),
              div_res_val.get_int_bytes(), is_in_val_valid, tmp_int))) {
            LOG_WARN("check_range_valid_int64 failed", K(ret), K(div_res_val.get_int_bytes()));
          } else if (is_in_val_valid) {
            tmp_obj.set_int(tmp_int);
          } else if (OB_FAIL(wide::check_range_valid_uint64(div_res_val.get_decimal_int(),
              div_res_val.get_int_bytes(), is_in_val_valid, tmp_uint))) {
            LOG_WARN("check_range_valid_uint64 failed", K(ret), K(div_res_val.get_int_bytes()));
          } else if (is_in_val_valid) {
            tmp_obj.set_uint64(tmp_uint);
          } else {
            ret = OB_ERR_WRONG_TYPE_FOR_VAR;
            LOG_WARN("wrong type for int variables", K(ret), K(in_val), K(int_bytes), K(scale));
          }
        } else {
          ret = OB_ERR_WRONG_TYPE_FOR_VAR;
          LOG_WARN("wrong type for int variables", K(ret), K(in_val), K(int_bytes), K(scale));
        }
      }
    } else {
      tmp_obj = in_val;
    }

    // cast input_value to the variable's data_type
    if (OB_FAIL(ret)) {
    } else if ((ob_is_int_tc(type_) && ob_is_uint_tc(tmp_obj.get_type())) ||
               (ob_is_uint_tc(type_) && ob_is_int_tc(tmp_obj.get_type()))) {
      const ObObj *out_val_ptr = NULL;
      EXPR_CAST_OBJ_V2(type_, tmp_obj, out_val_ptr);
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(out_val_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("succ to cast obj, but out_val_ptr is NULL", K(ret), K(type_), K(tmp_obj));
      } else {
        out_val = *out_val_ptr;
        is_converted = out_val_ptr != &tmp_obj ? true : false;
        //判断是否进行了值的转换（只是类型的转换不应该设置is_converted）
        if (out_val_ptr != &tmp_obj) {
          if (ObIntType == type_) {
            is_converted = static_cast<uint64_t>(out_val_ptr->get_int()) != tmp_obj.get_uint64() ?
                true : false;
          } else {
            is_converted = out_val_ptr->get_uint64() != static_cast<uint64_t>(tmp_obj.get_int()) ?
                true : false;
          }
        } else {}
      }
    } else {
      out_val = tmp_obj;
    }

    // adjust the out_val to meet variable's valid range
    if (OB_SUCC(ret)) {
      if (!min_val_.is_null() && out_val < min_val_) {
        out_val = min_val_;
        is_converted = true;
      }
      if (!max_val_.is_null() && out_val > max_val_) {
        out_val = max_val_;
        is_converted = true;
      }
    }
  }
  return ret;
}
#undef CHECK_DECIMAL_INT_VALID

int ObStrictRangeIntSysVar::do_check_and_convert(ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObObj &in_val,
                                                 ObObj &out_val)
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObObj tmp_obj;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (OB_UNLIKELY(ObIntType != type_ && ObUInt64Type != type_) ||
             OB_ISNULL(expr_ctx.phy_plan_ctx_ = ctx.get_physical_plan_ctx()) ||
             OB_ISNULL(expr_ctx.my_session_ = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("type_ is neigher ObIntType nor ObUInt64Type, "
              "or physical plan ctx in ObExecContext is not NULL, "
              "or session in ObExecContext is NULL", K(ret),
              K(type_), K(expr_ctx.phy_plan_ctx_), K(expr_ctx.my_session_));
  } else {
    expr_ctx.calc_buf_ = &(ctx.get_allocator());
    expr_ctx.exec_ctx_ = &ctx;
    EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);

    // FIXME(yyy),因为当前ob在parser阶段将所有大于int32的数值均转换为decimal处理，与mysql不兼容;
    // 因此，在这里进行特殊处理
    if (OB_FAIL(ret)) {
    } else if (ObNumberType == in_val.get_type()) {
      common::number::ObNumber in_number;
      int64_t int_val = 0;
      uint64_t uint_val = 0;
      if (OB_FAIL(in_val.get_number(in_number))) {
        LOG_WARN("fail to get number", K(ret), K(in_val));
      } else if (in_number.is_valid_int64(int_val)) {
        tmp_obj.set_int(int_val);
      } else if (in_number.is_valid_uint64(uint_val)) {
        tmp_obj.set_uint64(uint_val);
      } else {
        ret = OB_ERR_WRONG_TYPE_FOR_VAR;
        LOG_WARN("wrong type for int variables", K(ret), K(in_val));
      }
    } else {
      tmp_obj = in_val;
    }

    // cast input_value to the variable's data_type
    if (OB_FAIL(ret)) {
    } else if ((ob_is_int_tc(type_) && ob_is_uint_tc(tmp_obj.get_type())) ||
               (ob_is_uint_tc(type_) && ob_is_int_tc(tmp_obj.get_type()))) {
      const ObObj *out_val_ptr = NULL;
      EXPR_CAST_OBJ_V2(type_, tmp_obj, out_val_ptr);
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(out_val_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("succ to cast obj, but out_val_ptr is NULL", K(ret), K(type_), K(tmp_obj));
      } else {
        out_val = *out_val_ptr;
      }
    } else {
      out_val = tmp_obj;
    }

    // check the out_val's range
    if (OB_SUCC(ret)) {
      if (!min_val_.is_null() && out_val < min_val_) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
          // log_ret仅用于打日志，不覆盖ret
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
        }
      } else if (!max_val_.is_null() && out_val > max_val_) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
          // log_ret仅用于打日志，不覆盖ret
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
        }
      }
    }
  }
  return ret;
}

int ObNumericSysVar::check_update_type(const ObSetVar &set_var, const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_
      || (0 != (flags_ & ObSysVarFlag::NULLABLE) && ObNullType == val.get_type())) {
    // do nothing
  } else if (false == ob_is_numeric_type(val.get_type())) {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("wrong type for var", K(ret), K(val));
  }
  return ret;
}

int ObNumericSysVar::do_check_and_convert(ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const ObObj &in_val, ObObj &out_val)
{
  int ret = OB_SUCCESS;
  bool is_converted = false;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (OB_FAIL(do_convert(ctx, in_val, out_val, is_converted))) {
    LOG_WARN("fail to convert variable", K(ret), K(set_var.var_name_));
  } else if (is_converted && OB_FAIL(ObSysVarUtils::log_bounds_error_or_warning(
              ctx, set_var, in_val))) {
    if (OB_UNLIKELY(OB_ERR_WRONG_VALUE_FOR_VAR != ret)) {
      LOG_WARN("fail to log bounds error or warnning", K(ret));
    } else {
      LOG_WARN("wrong value for sys var", K(ret));
    }
  }
  return ret;
}

int ObNumericSysVar::do_convert(ObExecContext &ctx,
                                const common::ObObj &in_val,
                                common::ObObj &out_val,
                                bool &is_converted)
{
  int ret = OB_SUCCESS;
  is_converted = false;
  ObExprCtx expr_ctx;
  expr_ctx.calc_buf_ = &(ctx.get_allocator());
  expr_ctx.my_session_ = ctx.get_my_session();
  expr_ctx.cast_mode_ = CM_NONE;
  expr_ctx.phy_plan_ctx_ = ctx.get_physical_plan_ctx();
  const ObObj *res_obj = NULL;
  EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
  EXPR_CAST_OBJ_V2(ObNumberType, in_val, res_obj);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(res_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("succ to cast obj, but res_obj is null", K(ret), K(in_val));
  } else {
    out_val = *res_obj;

    // adjust the out_val to meet variable's valid range
    if (!min_val_.is_null() && out_val < min_val_) {
      out_val = min_val_;
      is_converted = true;
    }
    if (!max_val_.is_null() && out_val > max_val_) {
      out_val = max_val_;
      is_converted = true;
    }
  }
  return ret;
}

int ObVarcharSysVar::check_update_type(const ObSetVar &set_var, const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_
      || (0 != (flags_ & ObSysVarFlag::NULLABLE) && ObNullType == val.get_type())) {
    // do nothing
  } else if (lib::is_oracle_mode() && ob_is_null(val.get_type())) {
    //'' will be regard as NULL in oracle mode, let it go
  } else if (false == ob_is_string_type(val.get_type())) {
    if (set_var.var_name_ == OB_SV_NLS_DATE_FORMAT
             || set_var.var_name_ == OB_SV_NLS_TIMESTAMP_FORMAT
             || set_var.var_name_ == OB_SV_NLS_TIMESTAMP_TZ_FORMAT) {
      ret = OB_INVALID_DATE_FORMAT;
      LOG_WARN("date format not recognized", K(ret), K(set_var.var_name_), K(val));
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("wrong type for var", K(ret), K(val));
    }
  }
  return ret;
}

int ObVarcharSysVar::do_check_and_convert(ObExecContext &ctx,
                                          const ObSetVar &set_var,
                                          const ObObj &in_val, ObObj &out_val)
{
  // TODO 像mysql那样做隐式转换
  UNUSED(ctx);
  if (true == set_var.is_set_default_) {
    // do nothing
  } else {
    out_val = in_val;
  }
  return OB_SUCCESS;
}

int ObTimeZoneSysVar::check_update_type(const ObSetVar &set_var, const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_
      || (0 != (flags_ & ObSysVarFlag::NULLABLE) && ObNullType == val.get_type())) {
    // do nothing
  } else if (false == ob_is_string_type(val.get_type())) {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("wrong type for var", K(ret), K(val));
  }
  return ret;
}

int ObTimeZoneSysVar::find_pos_time_zone(ObExecContext &ctx, const ObString &str_val, const bool is_oracle_compatible)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObCollationType coll_type = CS_TYPE_INVALID;;
  ObTimeZoneInfoPos tz_info;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(session), K(ret));
  } else if (OB_FAIL(session->get_collation_connection(coll_type))) {
    LOG_WARN("fail to get connection collation", K(coll_type), K(ret));
  } else {
    uint64_t tenant_id = session->get_effective_tenant_id();
    int32_t no_sp_len = static_cast<int32_t>(ObCharset::strlen_byte_no_sp(coll_type,
                                                                          str_val.ptr(),
                                                                          str_val.length()));
    ObString val_no_sp(no_sp_len, str_val.ptr());
    if (is_oracle_compatible) {
      val_no_sp = val_no_sp.trim();
    }
  	ObTZMapWrap tz_map_wrap;
    ObTimeZoneInfoManager *tz_info_mgr = NULL;
    if (OB_FAIL(OTTZ_MGR.get_tenant_timezone(tenant_id, tz_map_wrap, tz_info_mgr))) {
      LOG_WARN("get tenant timezone failed", K(ret));
    } else if (OB_ISNULL(tz_info_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tz info mgr is null", K(ret));
    } else if (OB_FAIL(tz_info_mgr->find_time_zone_info(val_no_sp, tz_info))) {
      LOG_WARN("fail to find time zone", K(str_val), K(val_no_sp), K(ret));
    }
  }
  return ret;
}

int ObTimeZoneSysVar::do_check_and_convert(ObExecContext &ctx,
                                           const ObSetVar &set_var,
                                           const ObObj &in_val, ObObj &out_val)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_
      || (0 != (flags_ & ObSysVarFlag::NULLABLE) && ObNullType == in_val.get_type())) {
    // do nothing
  } else {
    ObString str_val;
    int32_t offset = 0;

    if (ObCharType == in_val.get_type()) {
      if (OB_FAIL(in_val.get_string(str_val))) {
        LOG_ERROR("fail to get char", K(ret), K(in_val));
      } else {}
    } else if (ObVarcharType == in_val.get_type()) {
      if (OB_FAIL(in_val.get_varchar(str_val))) {
        LOG_ERROR("fail to get varchar", K(ret), K(in_val));
      } else {}
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected type", K(ret), K(in_val));
    }

    const bool is_oracle_compatible = (NULL != ctx.get_my_session() ? is_oracle_mode() : false);
    CHECK_COMPATIBILITY_MODE(ctx.get_my_session());
    int ret_more = OB_SUCCESS;
    if (OB_SUCC(ret) && OB_FAIL(ObTimeConverter::str_to_offset(str_val, offset,
                                                          ret_more, is_oracle_compatible, true))) {
      if (OB_ERR_UNKNOWN_TIME_ZONE != ret) {
        LOG_WARN("fail to conver time zone", K(ret), K(str_val), K(is_oracle_compatible));
      } else if (OB_FAIL(find_pos_time_zone(ctx, str_val, is_oracle_compatible))) {
        LOG_WARN("fail to convert time zone", K(ret), K(ret_more), K(str_val));
        if (OB_SUCCESS != ret_more && is_oracle_compatible) {
          ret = ret_more;
        }
      }
    }

    if (OB_SUCC(ret)) {
      out_val = in_val;
    } else if (OB_ERR_UNKNOWN_TIME_ZONE == ret) {
      LOG_USER_ERROR(OB_ERR_UNKNOWN_TIME_ZONE, str_val.length(), str_val.ptr());
    }
  }
  return ret;
}

int ObSqlModeVar::do_check_and_convert(ObExecContext &ctx,
                                       const ObSetVar &set_var,
                                       const ObObj &in_val, ObObj &out_val)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else {
    ObSQLMode sql_mode = 0;
    if (in_val.get_type() == ObNullType) {
      sql_mode = 0;
    } else if (true == ob_is_string_type(in_val.get_type())) {
      ObString str_val;
      if (ObCharType == in_val.get_type()) {
        if (OB_FAIL(in_val.get_string(str_val))) {
          LOG_ERROR("fail to get char", K(ret), K(in_val));
        }
      } else if (ObVarcharType == in_val.get_type()) {
        if (OB_FAIL(in_val.get_varchar(str_val))) {
          LOG_ERROR("fail to get varchar", K(ret), K(in_val));
        }
      } else {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
        }
      }
      ObString val_without_space = str_val.trim_space_only();
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(find_set(val_without_space))) {
        LOG_WARN("fail to find type", K(ret), K(val_without_space), K(in_val));
      } else if (OB_FAIL(ob_str_to_sql_mode(val_without_space, sql_mode))) {
        LOG_WARN("fail to convert str to sql mode", K(ret), K(val_without_space), K(in_val));
      }
    } else if (ob_is_int_tc(in_val.get_type())) {
      int64_t int64_val = 0;
      if (OB_FAIL(check_and_convert_int_tc_value(in_val, (1LL << 32), int64_val))) {
        if (OB_ERR_WRONG_VALUE_FOR_VAR == ret) {
          int log_ret = OB_SUCCESS;
          if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
            // log_ret仅用于打日志，不覆盖ret
            LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
          } else {}
        } else {
          LOG_WARN("fail to check int tc value", K(ret), K(in_val));
        }
      } else {
        sql_mode = static_cast<uint64_t>(int64_val);
      }
    } else if (ob_is_uint_tc(in_val.get_type())) {
      uint64_t uint64_val = 0;
      if (OB_FAIL(check_and_convert_uint_tc_value(in_val, (1LL << 32), uint64_val))) {
        if (OB_ERR_WRONG_VALUE_FOR_VAR == ret) {
          int log_ret = OB_SUCCESS;
          if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
            // log_ret仅用于打日志，不覆盖ret
            LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
          } else {}
        } else {
          LOG_WARN("fail to check uint tc value", K(ret), K(in_val));
        }
      } else {
        sql_mode = uint64_val;
      }
    } else if (is_oracle_mode() && (ObNumberType == in_val.get_type())) {
      number::ObNumber num = in_val.get_number();
      uint64_t int_val = 0;
      if (num.is_valid_uint64(int_val)) {
        ObObj tmp_val;
        tmp_val.set_uint64(int_val);
        if (OB_FAIL(check_and_convert_uint_tc_value(tmp_val, (1LL << 32), int_val))) {
          if (OB_ERR_WRONG_VALUE_FOR_VAR == ret) {
            int log_ret = OB_SUCCESS;
            if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, in_val))) {
              // log_ret仅用于打日志，不覆盖ret
              LOG_ERROR("fail to log error", K(ret), K(log_ret), K(in_val));
            } else {}
          } else {
            LOG_WARN("fail to check uint tc value", K(ret), K(in_val));
          }
        } else {
          sql_mode = int_val;
        }
      } else {
        ret = OB_ERR_WRONG_TYPE_FOR_VAR;
        LOG_WARN("not valid int value for var on oracle mode", K(in_val));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid type for ObTypeLibSysVar", K(ret), K(in_val));
    }
    // check if valid
    if (OB_SUCC(ret)) {
      if (!is_sql_mode_supported(sql_mode)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "Value for sql_mode");
      } else {
        out_val.set_uint64(sql_mode);
      }
    }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_timestamp_service(ObExecContext &ctx,
                                                              const ObSetVar &set_var,
                                                              const ObBasicSysVar &sys_var,
                                                              const ObObj &in_val,
                                                              ObObj &out_val)
{
  int ret = OB_SUCCESS;
  UNUSED(sys_var);
  ObSQLSessionInfo *session = ctx.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session or sql_proxy is NULL", K(ret), KP(session));
  } else {
    // 创建租户时，actual_tenant_id_的值为UINT64_MAX
    uint64_t tenant_id = set_var.actual_tenant_id_;
    // if (tenant_id == OB_SYS_TENANT_ID) {
    //   if (ObIntType != in_val.get_type()) {
    //     ret = OB_ERR_UNEXPECTED;
    //     LOG_WARN("invalid type", K(ret), K(in_val.get_type()));
    //   } else {
    //     int64_t ts_type_idx = in_val.get_int();
    //     系统租户下，系统变量ob_timestamp_service只能为LTS（0）
    //     if (0 != ts_type_idx) {
    //       ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    //       ObString in_str = ObString(ObSysVarObTimestampService::OB_TIMESTAMP_SERVICE_NAMES[ts_type_idx]);
    //       LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, set_var.var_name_.length(), set_var.var_name_.ptr(),
    //                      in_str.length(), in_str.ptr());
    //     }
    //   }
    // } else {
      out_val = in_val;
    // }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_max_allowed_packet(ObExecContext &ctx,
                                                               const ObSetVar &set_var,
                                                               const ObBasicSysVar &sys_var,
                                                               const ObObj &in_val,
                                                               ObObj &out_val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_session_readonly(ctx, set_var, sys_var, in_val, out_val))) {
    LOG_WARN("fail to check session readonly", K(ret), K(set_var), K(in_val));
  } else if (set_var.is_set_default_) {
    // do nothing
  } else {
    ObObj g_net_buffer_length_obj;
    int64_t g_net_buffer_length = 0;
    int64_t max_allowed_pkt = 0;
    ObIAllocator &allocator = ctx.get_allocator();
    ObSQLSessionInfo *session = ctx.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("session or sql_proxy is NULL", K(ret), KP(session));
    } else if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(
                session, allocator, ObString(OB_SV_NET_BUFFER_LENGTH),
                g_net_buffer_length_obj))) {
      LOG_WARN("fail to get global sys var: net_buffer_length",
               K(ret), K(set_var), K(sys_var), K(in_val));
    } else if (OB_FAIL(g_net_buffer_length_obj.get_int(g_net_buffer_length))) {
      LOG_WARN("fail to get net_buffer_length int64", K(ret), K(set_var), K(sys_var),
               K(in_val), K(g_net_buffer_length_obj));
    } else if (OB_FAIL(in_val.get_int(max_allowed_pkt))) {
      LOG_WARN("fail to get max_allowed_packet int64", K(ret), K(set_var), K(sys_var), K(in_val));
    } else if (max_allowed_pkt < g_net_buffer_length) {
      LOG_USER_WARN(OB_WARN_OPTION_BELOW_LIMIT, "max_allowed_packet", "net_buffer_length");
    }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_net_buffer_length(ObExecContext &ctx,
                                                              const ObSetVar &set_var,
                                                              const ObBasicSysVar &sys_var,
                                                              const ObObj &in_val,
                                                              ObObj &out_val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_session_readonly(ctx, set_var, sys_var, in_val, out_val))) {
    LOG_WARN("fail to check session readonly", K(ret), K(set_var), K(in_val));
  } else if (set_var.is_set_default_) {
    // do nothing
  } else {
    ObObj g_max_allowed_packet_obj;
    int64_t g_max_allowed_packet = 0;
    int64_t net_buffer_len = 0;
    ObIAllocator &allocator = ctx.get_allocator();
    ObSQLSessionInfo *session = ctx.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("session or sql_proxy is NULL", K(ret), KP(session));
    } else if (OB_FAIL(ObBasicSessionInfo::get_global_sys_variable(
                session, allocator, ObString(OB_SV_MAX_ALLOWED_PACKET),
                g_max_allowed_packet_obj))) {
      LOG_WARN("fail to get global sys var: max_allowed_packet",
               K(ret), K(set_var), K(sys_var), K(in_val));
    } else if (OB_FAIL(g_max_allowed_packet_obj.get_int(g_max_allowed_packet))) {
      LOG_WARN("fail to get max_allowed_packet int64", K(ret), K(set_var), K(sys_var),
               K(in_val), K(g_max_allowed_packet_obj));
    } else if (OB_FAIL(in_val.get_int(net_buffer_len))) {
      LOG_WARN("fail to get net_buffer_length int64", K(ret), K(set_var), K(sys_var), K(in_val));
    } else if (net_buffer_len > g_max_allowed_packet) {
      LOG_USER_WARN(OB_WARN_OPTION_BELOW_LIMIT, "max_allowed_packet", "net_buffer_length");
    }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_charset(ObExecContext &ctx,
                                                    const ObSetVar &set_var,
                                                    const ObBasicSysVar &sys_var,
                                                    const ObObj &in_val,
                                                    ObObj &out_val)
{
  UNUSED(sys_var);
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObString cs_name;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (true == in_val.is_null()) {
    // do nothing
  } else {
    ObString cs_name;
    if (ObVarcharType == in_val.get_type()) {
      cs_name = in_val.get_varchar();
      ObCharsetType cs_type = CHARSET_INVALID;
      ObCollationType coll_type = CS_TYPE_INVALID;
      if (CHARSET_INVALID == (cs_type = ObCharset::charset_type(cs_name))) {
        ret = OB_ERR_UNKNOWN_CHARSET;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, cs_name.length(), cs_name.ptr());
      } else if (OB_UNLIKELY(CS_TYPE_INVALID == (coll_type =
                                                 ObCharset::get_default_collation(cs_type)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("charset is valid, but it has no default collation", K(ret),
                  K(cs_name), K(cs_type));
      } else {
        out_val.set_int(static_cast<int64_t>(coll_type));
      }
    } else if (ObIntType == in_val.get_type()) {
      int64_t int64_val = in_val.get_int();
      if (false == ObCharset::is_valid_collation(int64_val)) {
        ret = OB_ERR_UNKNOWN_CHARSET;
        int p_ret = OB_SUCCESS;
        const static int64_t val_buf_len = 1024;
        char val_buf[val_buf_len];
        int64_t pos = 0;
        if (OB_SUCCESS != (p_ret = databuff_printf(val_buf, val_buf_len, pos, "%ld", int64_val))) {
          // p_ret不覆盖ret
          LOG_WARN("fail to databuff_printf", K(ret), K(p_ret), K(int64_val));
        } else {
          ObString cs_name_str(val_buf);
          LOG_USER_ERROR(OB_ERR_UNKNOWN_CHARSET, cs_name_str.length(), cs_name_str.ptr());
        }
      } else {
        out_val = in_val;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid type", K(ret), K(in_val));
    }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_charset_not_null(ObExecContext &ctx,
                                                             const ObSetVar &set_var,
                                                             const ObBasicSysVar &sys_var,
                                                             const ObObj &in_val,
                                                             ObObj &out_val)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  ObString cs_name;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (true == in_val.is_null()) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, sys_var.get_name().length(), sys_var.get_name().ptr(),
                   (int)strlen("NULL"), "NULL");
  } else if (OB_FAIL(check_and_convert_charset(ctx, set_var, sys_var, in_val, out_val))) {
    LOG_WARN("fail to check and convert charset", K(ret), K(set_var), K(sys_var), K(in_val));
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_collation_not_null(ObExecContext &ctx,
                                                               const ObSetVar &set_var,
                                                               const ObBasicSysVar &sys_var,
                                                               const ObObj &in_val,
                                                               ObObj &out_val)
{
  UNUSED(sys_var);
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (true == in_val.is_null()) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, sys_var.get_name().length(), sys_var.get_name().ptr(),
                   (int)strlen("NULL"), "NULL");
  } else {
    ObString coll_name;
    if (ObVarcharType == in_val.get_type()) {
      coll_name = in_val.get_varchar();
      ObCollationType coll_type = CS_TYPE_INVALID;
      if (CS_TYPE_INVALID == (coll_type = ObCharset::collation_type(coll_name))) {
        ret = OB_ERR_UNKNOWN_COLLATION;
        LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, coll_name.length(), coll_name.ptr());
      } else {
        out_val.set_int(static_cast<int64_t>(coll_type));
      }
    } else if (ObIntType == in_val.get_type()) {
      int64_t int64_val = in_val.get_int();
      if (false == ObCharset::is_valid_collation(int64_val)) {
        ret = OB_ERR_UNKNOWN_COLLATION;
        int p_ret = OB_SUCCESS;
        const static int64_t val_buf_len = 1024;
        char val_buf[val_buf_len];
        int64_t pos = 0;
        if (OB_SUCCESS != (p_ret = databuff_printf(val_buf, val_buf_len, pos, "%ld", int64_val))) {
          // p_ret不覆盖ret
          LOG_WARN("fail to databuff_printf", K(ret), K(p_ret), K(int64_val));
        } else {
          ObString coll_name_str(val_buf);
          LOG_USER_ERROR(OB_ERR_UNKNOWN_COLLATION, coll_name_str.length(), coll_name_str.ptr());
        }
      } else {
        out_val = in_val;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid type", K(ret), K(in_val));
    }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_timeout_too_large(ObExecContext &ctx,
                                                              const ObSetVar &set_var,
                                                              const ObBasicSysVar &sys_var,
                                                              const common::ObObj &in_val,
                                                              common::ObObj &out_val)
{
  UNUSED(ctx);
  UNUSED(set_var);
  UNUSED(sys_var);

  int ret = OB_SUCCESS;
  int64_t timeout_val = 0;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (OB_FAIL(in_val.get_int(timeout_val))) {
  } else if (timeout_val > OB_MAX_USER_SPECIFIED_TIMEOUT) {
    out_val.set_int(OB_MAX_USER_SPECIFIED_TIMEOUT);
    LOG_USER_WARN(OB_ERR_TIMEOUT_TRUNCATED);
  } else {
    out_val = in_val;
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_tx_isolation(ObExecContext &ctx,
                                                         const ObSetVar &set_var,
                                                         const ObBasicSysVar &sys_var,
                                                         const common::ObObj &in_val,
                                                         common::ObObj &out_val)
{
  UNUSED(sys_var);
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  int32_t isolation = ObTransIsolation::get_level(in_val.get_string());
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session info", K(ret));
  } else if (!can_set_trans_var(set_var.set_scope_, *session)) {
    ret = OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS;
    LOG_WARN("fail to check tx_isolation", K(ret),
             K(set_var.set_scope_), K(session->get_trans_flags()));
  } else if (ObTransIsolation::UNKNOWN == isolation) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR,
                   set_var.var_name_.length(), set_var.var_name_.ptr(),
                   in_val.get_string().length(), in_val.get_string().ptr());
    LOG_WARN("invalid tx_isolation value", K(ret));
  } else if (ObTransIsolation::READ_UNCOMMITTED == isolation) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "Isolation level READ-UNCOMMITTED");
    LOG_WARN("isolation level read-uncommitted not supported", K(ret), K(in_val));
  } else {
    if (OB_FAIL(ob_write_obj(ctx.get_allocator(), in_val, out_val))) {
      LOG_WARN("deep copy out_val obj failed", K(ret));
    }
    ObString tmp_out_val = out_val.get_string();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ob_simple_low_to_up(ctx.get_allocator(),
                                    in_val.get_string(),
                                    tmp_out_val))) {
      LOG_WARN("Isolation level change to upper string failed", K(ret));
    }
    out_val.set_varchar(tmp_out_val);
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_tx_read_only(ObExecContext &ctx,
                                                         const ObSetVar &set_var,
                                                         const ObBasicSysVar &sys_var,
                                                         const common::ObObj &in_val,
                                                         common::ObObj &out_val)
{
  UNUSED(sys_var);
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session info", K(ret));
  } else if (!can_set_trans_var(set_var.set_scope_, *session)) {
    ret = OB_ERR_CANT_CHANGE_TX_CHARACTERISTICS;
    LOG_WARN("fail to check tx_read only", K(ret),
             K(set_var.set_scope_), K(session->get_trans_flags()));

  } else {
    out_val = in_val;
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_update_resource_manager_plan(ObExecContext &ctx,
                                                             const ObSetVar &set_var,
                                                             const ObBasicSysVar &sys_var,
                                                             const common::ObObj &val,
                                                             common::ObObj &out_val)
{
  UNUSED(sys_var);
  int ret = OB_SUCCESS;
  ObString plan;
  if (set_var.set_scope_== ObSetVar::SET_SCOPE_GLOBAL) {
    if (val.is_null()) {
      // maybe NULL, do nothing
    } else if (OB_FAIL(val.get_string(plan))) {
      LOG_WARN("fail to get sql mode str", K(ret), K(val), K(sys_var));
    } else if (0 == plan.length()) {
      // do nothing.
    } else {
      // check if plan exists
      ObResourceManagerProxy proxy;
      ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
      if (OB_ISNULL(session)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get session info", K(ret));
      } else {
        uint64_t tenant_id = session->get_effective_tenant_id();
        bool exists = false;
        if (OB_FAIL(proxy.check_if_plan_exist(tenant_id, plan, exists))) {
          LOG_WARN("fail check plan exists", K(tenant_id), K(plan), K(val), K(ret));
        } else if (!exists) {
          ret = OB_ERR_RES_MGR_PLAN_NOT_EXIST;
          LOG_USER_ERROR(OB_ERR_RES_MGR_PLAN_NOT_EXIST);
          LOG_WARN("plan not exist", K(plan), K(ret));
        }
      }
    }
    LOG_INFO("update resource manager plan", K(val), K(ret), K(set_var), K(sys_var));
  }
  out_val = val;
  return ret;
}

int ObSysVarOnCheckFuncs::check_log_row_value_option_is_valid(sql::ObExecContext &ctx,
                                                                 const ObSetVar &set_var,
                                                                 const ObBasicSysVar &sys_var,
                                                                 const common::ObObj &in_val,
                                                                 common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  ObString val = in_val.get_string();
  if (!val.empty()) {
    if (val.case_compare(OB_LOG_ROW_VALUE_PARTIAL_LOB) == 0) {
      // because not adapat obcdc, currently partial_lob is disabled
      // out_val = in_val;
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("partial_lob is not support, please use _enable_dbms_lob_partial_update instead", K(ret), K(in_val));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "partial_lob");
    } else if (val.case_compare(OB_LOG_ROW_VALUE_PARTIAL_JSON) == 0
        || val.case_compare(OB_LOG_ROW_VALUE_PARTIAL_ALL) == 0) {
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret), K(val));
      } else if (! ((DATA_VERSION_4_2_2_0 <= tenant_data_version && tenant_data_version < DATA_VERSION_4_3_0_0) || tenant_data_version >= DATA_VERSION_4_3_1_0)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("json partial update not support in current version", K(ret), K(tenant_data_version));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "json partial update not support in current version");
      } else {
        out_val = in_val;
      }
    } else {
      ret = OB_ERR_PARAM_VALUE_INVALID;
      LOG_USER_ERROR(OB_ERR_PARAM_VALUE_INVALID);
    }
  } else {
    out_val = in_val;
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_default_lob_inrow_threshold(sql::ObExecContext &ctx,
                                                const ObSetVar &set_var,
                                                const ObBasicSysVar &sys_var,
                                                const common::ObObj &in_val,
                                                common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  int64_t inrow_threshold = 0;
  if (OB_FAIL(in_val.get_int(inrow_threshold))) {
    LOG_WARN("get_int fail", K(ret), K(in_val));
  } else if (inrow_threshold < OB_MIN_LOB_INROW_THRESHOLD || inrow_threshold > OB_MAX_LOB_INROW_THRESHOLD) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob inrow_threshold invalid", KR(ret), K(inrow_threshold));
    // error msg to user
    int tmp_ret = OB_SUCCESS;
    const int64_t ERROR_MSG_LENGTH = 256;
    char error_msg[ERROR_MSG_LENGTH] = "";
    int64_t pos = 0;
    if (OB_SUCCESS != (tmp_ret = databuff_printf(error_msg, ERROR_MSG_LENGTH,
        pos, "lob inrow threshold, should be [%ld, %ld]", OB_MIN_LOB_INROW_THRESHOLD, OB_MAX_LOB_INROW_THRESHOLD))) {
      LOG_WARN("print error msg fail", K(ret), K(tmp_ret), K(error_msg), K(pos));
    } else {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, error_msg);
    }
  } else {
    out_val = in_val;
  }
  return ret;
}

bool ObSysVarOnCheckFuncs::can_set_trans_var(ObSetVar::SetScopeType scope,
                                             ObBasicSessionInfo &session)
{
  bool ret = false;
  if (lib::is_oracle_mode()) {
    /*
     * in oracle, isolation = SERIALIZABLE:
     * 1. statement may get a snapshot and read it without any transaction,
     *    create new snapshot if there is no current snapshot, otherwise
     *    reuse current snapshot generated by the previous statement.
     * 2. statement need a transaction ONLY IF it needs lock row,
     *    create new transaction if there is no current transaction, otherwise
     *    reuse current transaction generated by the previous statement.
     * 3. 'SET TRANSACTION xxx' will ALWAYS release current snapshot (if exists),
     *    then create new snapshot, but it will cause error in any of these cases:
     *   a. the current snapshot is generated by previous 'SET TRANSACTION xxx', or
     *   b. the current snapshot is in transaction.
     * 4. 'ALTER SESSION SET isolation_level' will ALWAYS release current snapshot
     *    (if exists), then modify the 'isolation_level' value in session, but it
     *    will cause error in any of these cases:
     *   a. the current snapshot is generated by previous 'SET TRANSACTION xxx'.
     *
     * since OB 4.0, transaction's model very like to Oracle's and we can handle
     * these transaction characteristics modifiy stmt more simply.
     *
     * in previouse version OB:
     * the most problem is: every snapshot must base on a transaction, create
     * new snapshot means create new transaction too. so we need some extra flags to
     * record some informations:
     * 1. has_set_trans_var: whether the current transaction is generated by 'SET TRANSACTION xxx',
     *    for oracle rule 3-a and 4-a.
     * 2. has_hold_row_lock: whether the current transaction hold row lock,
     *    for oracle rule 3-b.
     * ps: SET_SCOPE_GLOBAL could not appear in oracle mode, so we need not care scope
     *     because SET_SCOPE_SESSION and SET_SCOPE_NEXT_TRANS have same behavior in this
     *     function.
     * see:
     *
     *
     */
    ret = !session.is_in_transaction() || session.is_txn_free_route_temp();
  } else if (lib::is_mysql_mode()) {
    /*
     * mysql mode is much simpler than oracle mode, only 'SET TRANSACTION xxx' is forbidden
     * when it appears in an explicit transaction, and it will not create new snapshot or
     * transaction in all cases.
     */
    ret = !session.is_in_transaction() || ObSetVar::SET_SCOPE_NEXT_TRANS != scope;
  } else {
    // nothing.
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_max_user_connections(ObExecContext &ctx,
                                                                 const ObSetVar &set_var,
                                                                 const ObBasicSysVar &sys_var,
                                                                 const ObObj &in_val,
                                                                 ObObj &out_val)
{
  return check_session_readonly(ctx, set_var, sys_var, in_val, out_val);
}

//#define MAX_MODE_STR_BUF_LEN  512
int ObSysVarOnCheckFuncs::check_and_convert_sql_mode(ObExecContext &ctx,
                                                     const ObSetVar &set_var,
                                                     const ObBasicSysVar &sys_var,
                                                     const common::ObObj &in_val,
                                                     common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(set_var);
  UNUSED(sys_var);
  UNUSED(in_val);
  UNUSED(out_val);
  //todo , according to mysql
  //check the sql_mode is to be discard;
  /*if (true == set_var.is_set_default_) {
  //nothing to do
  } else {
  char *buf = reinterpret_cast<char*>(ctx.get_allocator().alloc(MAX_MODE_STR_BUF_LEN));
  if (false == ob_is_string_type(out_val.get_type())) {
  ret = OB_INVALID_ARGUMENT;
  SQL_SESSION_LOG(WARN, "invalid argument, sql_mode out_val should be string type", K(out_val));
  } else if (NULL == buf) {
  ret = OB_ALLOCATE_MEMORY_FAILED;
  SQL_SESSION_LOG(ERROR, "failed to alloc memory", K(ret));
  } else {
  ObString sql_mode_str = out_val.get_string();
  char *end_ptr = buf;
  if (0 == sql_mode_str.case_compare(STR_ANSI)) {
  snprintf(buf, MAX_MODE_STR_BUF_LEN, "%s", STR_COMBINE_ANSI);
  end_ptr += strlen(STR_COMBINE_ANSI);
  ObString out_str;
  out_str.assign_ptr(buf, static_cast<int32_t>(end_ptr - buf));
  out_val.set_varchar(out_str);
  } else if (0 == sql_mode_str.case_compare(STR_TRADITIONAL)) {
  snprintf(buf, MAX_MODE_STR_BUF_LEN, "%s", STR_COMBINE_TRADITIONAL);
  end_ptr += strlen(STR_COMBINE_TRADITIONAL);
  ObString out_str;
  out_str.assign_ptr(buf, static_cast<int32_t>(end_ptr - buf));
  out_val.set_varchar(out_str);
  }
  }
  }*/
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_time_zone(ObExecContext &ctx,
                                                     const ObSetVar &set_var,
                                                     const ObBasicSysVar &sys_var,
                                                     const common::ObObj &in_val,
                                                     common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(set_var);
  UNUSED(sys_var);
  UNUSED(in_val);
  UNUSED(out_val);

  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_max_min_timestamp(ObExecContext &ctx,
                                                              const ObSetVar &set_var,
                                                              const ObBasicSysVar &sys_var,
                                                              const common::ObObj &in_val,
                                                              common::ObObj &out_val)
{
  UNUSED(sys_var);
  int ret = OB_SUCCESS;
  ObCastMode cast_mode = CM_NONE;
  if (true == set_var.is_set_default_) {
    //nothing to do
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(ctx.get_my_session(), cast_mode))) {
    LOG_WARN("failed to get cast_mode", K(ret));
  } else if (in_val.get_number().is_negative() || in_val.get_number() >= TIMESTAMP_MAX_VAL) {
    if (CM_WARN_ON_FAIL == cast_mode) {
      const char *value = in_val.get_number().format();
      LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, set_var.var_name_.length(),
                    set_var.var_name_.ptr(), static_cast<int32_t>(strlen(value)),
                    value);
      common::number::ObNumber num;
      num.set_zero();
      out_val.set_number(num);
    } else {
      ret = OB_ERR_WRONG_VALUE_FOR_VAR;
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, set_var.var_name_.length(), set_var.var_name_.ptr(),
                     (int)strlen(in_val.get_number().format()), in_val.get_number().format());
    }
  } else {
    out_val = in_val;
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_ob_org_cluster_id(ObExecContext &ctx,
                                                              const ObSetVar &set_var,
                                                              const ObBasicSysVar &sys_var,
                                                              const ObObj &in_val,
                                                              ObObj &out_val)
{
  UNUSED(sys_var);
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (true == set_var.is_set_default_) { // 禁止set ob_org_cluster_id = default
    ret = OB_ERR_NO_DEFAULT;
    LOG_USER_ERROR(OB_ERR_NO_DEFAULT, set_var.var_name_.length(), set_var.var_name_.ptr());
  } else if (true == in_val.is_null()) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, sys_var.get_name().length(), sys_var.get_name().ptr(),
                   (int)strlen("NULL"), "NULL");
  } else {
    out_val = in_val;
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_sql_throttle_queue_time(ObExecContext &ctx,
                                                                    const ObSetVar &set_var,
                                                                    const ObBasicSysVar &sys_var,
                                                                    const ObObj &in_val,
                                                                    ObObj &out_val)
{
  UNUSED(ctx);
  UNUSED(set_var);
  UNUSED(sys_var);

  int ret = OB_SUCCESS;
  number::ObNumber num;
  char buf[32] = {};
  int64_t pos = 0;
  double lower = .0;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (OB_FAIL(in_val.get_number(num))) {
  } else if (OB_FAIL(num.format(buf, sizeof (buf), pos, 6))) {
  } else if (pos >= sizeof (buf)) {
    ret = OB_SIZE_OVERFLOW;
  } else if (FALSE_IT(lower = atof(buf))) {
  } else if (lower < 0.001 && num != -1l) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, sys_var.get_name().length(), sys_var.get_name().ptr(),
                   static_cast<int>(strlen("NULL")), "NULL");
  } else {
    out_val = in_val;
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_nls_currency_too_long(sql::ObExecContext &ctx,
                                                                  const ObSetVar &set_var,
                                                                  const ObBasicSysVar &sys_var,
                                                                  const common::ObObj &in_val,
                                                                  common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_session_readonly(ctx, set_var, sys_var, in_val, out_val))) {
    LOG_WARN("fail to check session readonly", K(ret), K(set_var), K(in_val));
  } else if (set_var.is_set_default_) {
    // do nothing
  } else {
    const int32_t MAX_NLS_CURRENCY_LEN = 10;
    ObString in_nls_currency_str = in_val.get_string();
    if (in_nls_currency_str.length() <= MAX_NLS_CURRENCY_LEN) {
      out_val = in_val;
    } else {
      ObString out_nls_currency_str;
      ObIAllocator &allocator = ctx.get_allocator();
      if (OB_FAIL(ob_write_string(allocator, in_nls_currency_str, out_nls_currency_str))) {
        LOG_WARN("failed to write stirng", K(ret));
      } else {
        out_nls_currency_str.assign_ptr(out_nls_currency_str.ptr(), MAX_NLS_CURRENCY_LEN);
        out_val.set_varchar(out_nls_currency_str);
      }
    }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_nls_iso_currency_is_valid(sql::ObExecContext &ctx,
                                                                      const ObSetVar &set_var,
                                                                      const ObBasicSysVar &sys_var,
                                                                      const common::ObObj &in_val,
                                                                      common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_session_readonly(ctx, set_var, sys_var, in_val, out_val))) {
    LOG_WARN("fail to check session readonly", K(ret), K(set_var), K(in_val));
  } else if (set_var.is_set_default_) {
    // do nothing
  } else {
    ObString in_country_str = in_val.get_string();
    ObString out_country_str;
    ObIAllocator &allocator = ctx.get_allocator();
    if (OB_FAIL(ob_simple_low_to_up(allocator, in_country_str, out_country_str))) {
      LOG_WARN("failed to write stirng", K(ret));
    } else {
      if (!IsoCurrencyUtils::is_country_valid(out_country_str)) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        LOG_WARN("failed to get currency by country name", K(ret));
      } else {
        out_val.set_varchar(out_country_str);
      }
    }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_nls_length_semantics_is_valid(ObExecContext &ctx,
                                                                          const ObSetVar &set_var,
                                                                          const ObBasicSysVar &sys_var,
                                                                          const common::ObObj &in_val,
                                                                          ObObj &out_val)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_session_readonly(ctx, set_var, sys_var, in_val, out_val))) {
    LOG_WARN("fail to check session readonly", K(ret), K(set_var), K(in_val));
  } else if (set_var.is_set_default_) {
    // do nothing
  } else {
    ObString str_val;
    ObLengthSemantics nls_length_semantics = LS_INVALIED;
    OZ (in_val.get_string(str_val));
    OX (nls_length_semantics = get_length_semantics(str_val));
    OV (nls_length_semantics != LS_INVALIED,
        OB_ERR_CANNOT_ACCESS_NLS_DATA_FILES_OR_INVALID_ENVIRONMENT_SPECIFIED, nls_length_semantics);
    OX (out_val = in_val);
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_session_readonly(ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObBasicSysVar &sys_var,
                                                 const ObObj &in_val,
                                                 ObObj &out_val)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (ObSetVar::SET_SCOPE_SESSION == set_var.set_scope_ && true == sys_var.is_session_readonly()) {
    ret = OB_ERR_VARIABLE_IS_READONLY;
    LOG_USER_ERROR(OB_ERR_VARIABLE_IS_READONLY, (int)strlen("SESSION"), "SESSION",
                   sys_var.get_name().length(), sys_var.get_name().ptr(),
                   (int)strlen("GLOBAL"), "GLOBAL");
  } else {
    out_val = in_val;
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_plsql_warnings(sql::ObExecContext &ctx,
                                                 const ObSetVar &set_var,
                                                 const ObBasicSysVar &sys_var,
                                                 const common::ObObj &in_val,
                                                 common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
  UNUSEDx(ctx, set_var, sys_var, in_val, out_val);
#else
  UNUSEDx(ctx, set_var, sys_var);
  if (OB_FAIL(pl::PlCompilerWarningCategory::verify_warning_settings(in_val.get_string(), NULL))) {
    ret = OB_ERR_PARAM_VALUE_INVALID;
    LOG_USER_ERROR(OB_ERR_PARAM_VALUE_INVALID);
  } else {
    out_val = in_val;
  }
#endif
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_plsql_ccflags(sql::ObExecContext &ctx,
                                                          const ObSetVar &set_var,
                                                          const ObBasicSysVar &sys_var,
                                                          const common::ObObj &in_val,
                                                          common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  UNUSEDx(ctx, set_var, sys_var, out_val);
  OZ (ObExprPLSQLVariable::check_plsql_ccflags(in_val.get_string()));
  return ret;
}

int ObSysVarOnCheckFuncs::get_string(const ObObj &val, ObString &str)
{
  int ret = OB_SUCCESS;
  if (ObCharType == val.get_type()) {
    str = val.get_string();
  } else if (ObVarcharType == val.get_type()) {
    str = val.get_varchar();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid type", K(ret), K(val));
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_runtime_filter_type_is_valid(
    sql::ObExecContext &ctx,
    const ObSetVar &set_var,
    const ObBasicSysVar &sys_var,
    const common::ObObj &in_val,
    common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  ObString str_val;
  if (OB_FAIL(in_val.get_varchar(str_val))) {
    LOG_WARN("fail to get varchar", K(ret), K(in_val));
  } else {
    int64_t rf_type = ObConfigRuntimeFilterChecker::get_runtime_filter_type(str_val.ptr(),
        str_val.length());
    if (rf_type >= 0) {
      out_val = in_val;
    } else {
      ret = OB_ERR_WRONG_VALUE_FOR_VAR;
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, str_val.length(), str_val.ptr(), str_val.length(), str_val.ptr());
    }
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_locale_type_is_valid(
    sql::ObExecContext &ctx,
    const ObSetVar &set_var,
    const ObBasicSysVar &sys_var,
    const common::ObObj &in_val,
    common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  const ObString &locale_val = in_val.get_string();
  ObString valid_locale = in_val.get_string();
  if (true == set_var.is_set_default_) {
    //do nothing
  } else if (!is_valid_ob_locale(locale_val, valid_locale)) {            //check if the variable is valid
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, sys_var.get_name().length(), sys_var.get_name().ptr(),
                                               locale_val.length(), locale_val.ptr());
  } else {
    OX(out_val.set_string(in_val.get_type(), valid_locale));
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_compat_version(sql::ObExecContext &ctx,
                                                           const ObSetVar &set_var,
                                                           const ObBasicSysVar &sys_var,
                                                           const common::ObObj &in_val,
                                                           common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (true == set_var.is_set_default_) {
    // do nothing
  } else if (OB_FAIL(check_and_convert_version(ctx, sys_var, in_val,
                                               set_var.actual_tenant_id_, compat_version))) {
    LOG_WARN("failed to check and convert version", K(ret));
  } else {
    out_val.set_uint64(compat_version);
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_security_version(sql::ObExecContext &ctx,
                                                            const ObSetVar &set_var,
                                                            const ObBasicSysVar &sys_var,
                                                            const common::ObObj &in_val,
                                                            common::ObObj &out_val)
{
  int ret = OB_SUCCESS;
  uint64_t security_version = 0;
  uint64_t old_version = 0;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session info", K(ret));
  } else if (true == set_var.is_set_default_) {
    // do nothing
  } else if (OB_FAIL(check_and_convert_version(ctx, sys_var, in_val,
                                               set_var.actual_tenant_id_, security_version))) {
    LOG_WARN("failed to check and convert version", K(ret));
  } else if (OB_FAIL(session->get_security_version(old_version))) {
    LOG_WARN("failed to get security version", K(ret));
  } else if (OB_UNLIKELY(set_var.actual_tenant_id_ != OB_INVALID_ID &&
                         security_version < old_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "decrease security version");
  } else {
    out_val.set_uint64(security_version);
  }
  return ret;
}

int ObSysVarOnCheckFuncs::check_and_convert_version(sql::ObExecContext &ctx,
                                                    const ObBasicSysVar &sys_var,
                                                    const common::ObObj &in_val,
                                                    const uint64_t tenant_id,
                                                    uint64_t &version)
{
  int ret = OB_SUCCESS;
  version = 0;
  if (ObVarcharType == in_val.get_type()) {
    const ObString &val = in_val.get_string();
    if (OB_FAIL(ObCompatControl::get_compat_version(val, version))) {
      if (OB_INVALID_ARGUMENT == ret) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, sys_var.get_name().length(),
                                                    sys_var.get_name().ptr(),
                                                    val.length(), val.ptr());
      } else {
        LOG_WARN("failed to get compat version", K(ret), K(val));
      }
    }
  } else if (ObUInt64Type == in_val.get_type()) {
    version = in_val.get_uint64();
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret), K(in_val));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObCompatControl::check_compat_version(tenant_id, version))) {
    if (OB_INVALID_ARGUMENT == ret) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "target version");
    } else {
      LOG_WARN("failed to check version", K(ret), K(version));
    }
  }
  return ret;
}

int ObSysVarOnUpdateFuncs::update_tx_isolation(ObExecContext &ctx,
                                               const ObSetVar &set_var,
                                               const ObBasicSysVar &sys_var,
                                               const ObObj &val)
{
  UNUSED(sys_var);
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
  const ObString &var_name = set_var.var_name_;
  const ObString &var_val = val.get_string();
  ObTxIsolationLevel isolation = transaction::tx_isolation_from_str(var_val);
  bool for_next_trans = (set_var.set_scope_ == ObSetVar::SET_SCOPE_NEXT_TRANS);
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get session info", K(ret));
  } else if (ObTxIsolationLevel::INVALID == isolation) {
    ret = OB_ERR_WRONG_VALUE_FOR_VAR;
    LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR,
                   var_name.length(), var_name.ptr(), var_val.length(), var_val.ptr());
    LOG_WARN("isolation level is invalid", K(ret), K(var_val), K(var_name));
  } else if (ObTxIsolationLevel::RU == isolation) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "Isolation level READ-UNCOMMITTED");
    LOG_WARN("isolation level read-uncommitted not supported", K(ret), K(var_val), K(var_name));
  } else if (for_next_trans && FALSE_IT(session->set_tx_isolation(isolation))) {
    // nothing.
  } else if (lib::is_oracle_mode()) {
    if (for_next_trans) {
      /*
       * 'SET TRANSACTION xxx' will ALWAYS create new snapshot in oracle
       * see comments in can_set_trans_var() for more details.
       * ps: read only can't be 'ALTER SESSION SET' in oracle, so use default value false.
       */
      session->set_tx_isolation(isolation);
      // tx must be ilde, previouse check `can_set_trans_var` has check this
      if (OB_FAIL(start_trans_by_set_trans_char_(ctx))) {
        // TODO: fatal bug, need disconnect
        LOG_WARN("auto start trans fail when set txn charactor", K(ret),
                 KPC(session->get_tx_desc()), KPC(session));
      }
    } else {
      /*
       * 'ALTER SESSION SET isolation_level' just release snapshot since 4.0
       * previouse check in can_set_trans_var promise no active trans in current session
       */
      if (ObTxIsolationLevel::SERIAL == isolation ||
          ObTxIsolationLevel::RR == isolation) {
        // release snapshot, following stmt will acquire snapshot again
        if (!session->is_txn_free_route_temp() &&
            OB_NOT_NULL(session->get_tx_desc()) &&
            OB_FAIL(MTL(transaction::ObTransService*)
                    ->release_snapshot(*session->get_tx_desc()))) {
          TRANS_LOG(WARN, "try to release snapshot for current session fail",
                    K(ret), KPC(session->get_tx_desc()));
          // TODO: fatal bug, need disconnect
        }
      }
    }
  }
  return ret;
}



int ObSysVarOnUpdateFuncs::update_tx_read_only_no_scope(ObExecContext &ctx,
                                                        const ObSetVar &set_var,
                                                        const ObBasicSysVar &sys_var,
                                                        const ObObj &val)
{
  UNUSED(sys_var);
  int ret = OB_SUCCESS;
  if (set_var.set_scope_ == ObSetVar::SET_SCOPE_NEXT_TRANS) {
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
    const bool read_only = val.get_bool();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get session info", K(ret));
    } else if (FALSE_IT(session->set_tx_read_only(!read_only, read_only))) {
      // nothing.
    } else if (lib::is_oracle_mode()) {
      // READ ONLY will use SERIALIZABLE implicitly,
      // READ WRITE need use default value in session, so set UNKNOWN.
      //
      // if read only, set tx isolation level to serializable
      // otherwise, use the value in session
      if (read_only) {
        session->set_tx_isolation(ObTxIsolationLevel::SERIAL);
      }
      if (OB_FAIL(start_trans_by_set_trans_char_(ctx))) {
        // TODO: fatal bug, need disconnect
        LOG_WARN("auto start trans fail when set txn charactor", K(ret),
                 KPC(session->get_tx_desc()), KPC(session));
      }
    }
    LOG_DEBUG("update tx_read only, while scope=none", K(ret), K(val.get_bool()));
  }
  return ret;
}

int ObSysVarOnUpdateFuncs::start_trans_by_set_trans_char_(
    sql::ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  auto &session = *GET_MY_SESSION(ctx);
  auto isolation = session.get_tx_isolation();
  if (OB_FAIL(ObSqlTransControl::explicit_start_trans(ctx,
                                 session.get_tx_read_only()))) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (ObTxIsolationLevel::SERIAL == isolation ||
             ObTxIsolationLevel::RR == isolation) {
    int64_t query_timeout = 0;
    session.get_query_timeout(query_timeout);
    int64_t stmt_expire_ts = session.get_query_start_time() + query_timeout;
    transaction::ObTxReadSnapshot snapshot;
    if (OB_FAIL(MTL(transaction::ObTransService*)
                ->get_read_snapshot(*session.get_tx_desc(),
                                    isolation,
                                    stmt_expire_ts,
                                    snapshot))) {
      LOG_WARN("fail to get snapshot for serializable / repeatable read", K(ret),
               KPC(session.get_tx_desc()), K(isolation), K(stmt_expire_ts));
      // rollback tx because of prepare snapshot fail
      int save_ret = ret;
      if (OB_FAIL(ObSqlTransControl::end_trans(ctx, true, true, NULL))) {
        LOG_WARN("rollback tx fail", K(ret), KPC(session.get_tx_desc()));
      }
      // rollback tx fail, need report to user, because session is corrupt
      ret = COVER_SUCC(save_ret);
    } else {
      LOG_TRACE("succeed get snapshot for oracle set trans charactor stmt",
                KPC(session.get_tx_desc()), K(isolation), K(snapshot));
    }
  } else {
    LOG_TRACE("set trans charactor done for RC isolation",
              K(isolation), KPC(session.get_tx_desc()));
  }
  return ret;
}

int ObSysVarOnUpdateFuncs::update_sql_mode(ObExecContext &ctx,
                                           const ObSetVar &set_var,
                                           const ObBasicSysVar &sys_var,
                                           const common::ObObj &val)
{
  UNUSED(ctx);
  UNUSED(sys_var);
  UNUSED(val);
  int ret = OB_SUCCESS;
  if (set_var.set_scope_== ObSetVar::SET_SCOPE_GLOBAL) {
    //nothing to do
  } else {
    //处理MODE_NO_BACKSLASH_ESCAPES
    //是否将反斜杠作为转义符。保存在系统变量中，
    //暂时不支持。
  }
  return ret;
}


int ObSysVarOnUpdateFuncs::update_safe_weak_read_snapshot(ObExecContext &ctx,
    const ObSetVar &set_var,
    const ObBasicSysVar &sys_var,
    const common::ObObj &val)
{
  UNUSED(ctx);
  UNUSED(set_var);
  UNUSED(sys_var);
  UNUSED(val);
  return OB_SUCCESS;
}

int ObSysVarToObjFuncs::to_obj_charset(ObIAllocator &allocator,
                                       const ObBasicSessionInfo &session,
                                       const ObBasicSysVar &sys_var,
                                       ObObj &result_obj)
{
  int ret = OB_SUCCESS;
  ObString result_str;
  if (ObNullType == sys_var.get_value().get_type()) {
    result_obj.set_null();
  } else if (OB_FAIL(ObSysVarToStrFuncs::to_str_charset(allocator, session, sys_var, result_str))) {
    LOG_WARN("fail to convert to str charset", K(ret), K(sys_var));
  } else {
    result_obj.set_varchar(result_str);
    result_obj.set_collation_type(ObCharset::get_system_collation());
    result_obj.set_collation_level(sys_var.get_value().get_collation_level());
  }
  return ret;
}

int ObSysVarToObjFuncs::to_obj_collation(ObIAllocator &allocator,
                                         const ObBasicSessionInfo &session,
                                         const ObBasicSysVar &sys_var,
                                         ObObj &result_obj)
{
  int ret = OB_SUCCESS;
  ObString result_str;
  if (ObNullType == sys_var.get_value().get_type()) {
    result_obj.set_null();
  } else if (OB_FAIL(ObSysVarToStrFuncs::to_str_collation(
              allocator, session, sys_var, result_str))) {
    LOG_WARN("fail to convert to str collation", K(ret), K(sys_var));
  } else {
    result_obj.set_varchar(result_str);
    result_obj.set_collation_type(ObCharset::get_system_collation());
    result_obj.set_collation_level(sys_var.get_value().get_collation_level());
  }
  return ret;
}

int ObSysVarToObjFuncs::to_obj_sql_mode(ObIAllocator &allocator,
                                        const ObBasicSessionInfo &session,
                                        const ObBasicSysVar &sys_var,
                                        ObObj &result_obj)
{
  UNUSED(session);
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_sql_mode_to_str(sys_var.get_value(), result_obj, &allocator))) {
    LOG_WARN("fail to convert sql mode to str", K(ret), K(sys_var));
  } else {
    result_obj.set_collation_type(ObCharset::get_system_collation());
    result_obj.set_collation_level(sys_var.get_value().get_collation_level());
  }
  return ret;
}

int ObSysVarToObjFuncs::to_obj_version(ObIAllocator &allocator,
                                       const ObBasicSessionInfo &session,
                                       const ObBasicSysVar &sys_var,
                                       ObObj &result_obj)
{
  int ret = OB_SUCCESS;
  ObString result_str;
  if (OB_FAIL(ObSysVarToStrFuncs::to_str_version(allocator, session, sys_var, result_str))) {
    LOG_WARN("fail to convert to str version", K(ret), K(sys_var));
  } else {
    result_obj.set_varchar(result_str);
    result_obj.set_collation_type(ObCharset::get_system_collation());
    result_obj.set_collation_level(sys_var.get_value().get_collation_level());
  }
  return ret;
}

int ObSysVarToStrFuncs::to_str_charset(ObIAllocator &allocator,
                                       const ObBasicSessionInfo &session,
                                       const ObBasicSysVar &sys_var,
                                       ObString &result_str)
{
  UNUSED(allocator);
  UNUSED(session);
  int ret = OB_SUCCESS;
  int64_t coll_type_int64 = -1;
  if (ObNullType == sys_var.get_value().get_type()) {
    result_str = ObString("");
  } else if (OB_FAIL(sys_var.get_value().get_int(coll_type_int64))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid value", K(ret), K(sys_var), K(sys_var.get_value()));
  } else if (coll_type_int64 == 0) {
    result_str = ObString("");
  } else if (OB_UNLIKELY(false == ObCharset::is_valid_collation(coll_type_int64))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid collation", K(ret), K(coll_type_int64), K(sys_var));
  } else if (OB_FAIL(ObCharset::charset_name_by_coll(
              static_cast<ObCollationType>(coll_type_int64), result_str))) {
    LOG_WARN("fail to get charset name by collation type", K(ret), K(coll_type_int64));
  }
  return ret;
}

int ObSysVarToStrFuncs::to_str_collation(ObIAllocator &allocator,
                                         const ObBasicSessionInfo &session,
                                         const ObBasicSysVar &sys_var,
                                         ObString &result_str)
{
  UNUSED(allocator);
  UNUSED(session);
  int ret = OB_SUCCESS;
  int64_t coll_type_int64 = -1;
  if (ObNullType == sys_var.get_value().get_type()) {
    result_str = ObString("");
  } else if (OB_FAIL(sys_var.get_value().get_int(coll_type_int64))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid value", K(ret), K(sys_var), K(sys_var.get_value()));
  } else if (OB_UNLIKELY(false == ObCharset::is_valid_collation(coll_type_int64))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid collation", K(ret), K(coll_type_int64), K(sys_var));
  } else {
    const char *coll_name_str_ptr = ObCharset::collation_name(
        static_cast<ObCollationType>(coll_type_int64));
    result_str = ObString(coll_name_str_ptr);
  }
  return ret;
}

int ObSysVarToStrFuncs::to_str_sql_mode(ObIAllocator &allocator,
                                        const ObBasicSessionInfo &session,
                                        const ObBasicSysVar &sys_var,
                                        ObString &result_str)
{
  UNUSED(session);
  int ret = OB_SUCCESS;
  ObObj str_obj;
  if (OB_FAIL(ob_sql_mode_to_str(sys_var.get_value(), str_obj, &allocator))) {
    LOG_WARN("fail to convert sql mode to str", K(ret), K(sys_var));
  } else if (OB_FAIL(str_obj.get_varchar(result_str))) {
    LOG_WARN("fail to get sql mode str", K(ret), K(str_obj), K(sys_var));
  }
  return ret;
}

int ObSysVarToStrFuncs::to_str_version(ObIAllocator &allocator,
                                       const ObBasicSessionInfo &session,
                                       const ObBasicSysVar &sys_var,
                                       ObString &result_str)
{
  UNUSED(allocator);
  UNUSED(session);
  int ret = OB_SUCCESS;
  uint64_t version = 0;
  if (OB_FAIL(sys_var.get_value().get_uint64(version))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid value", K(ret), K(sys_var), K(sys_var.get_value()));
  } else if (OB_FAIL(ObCompatControl::get_version_str(version, result_str, allocator))) {
    LOG_WARN("fail to get version str", K(ret), K(version));
  }
  return ret;
}

int ObSysVarSessionSpecialUpdateFuncs::update_identity(ObExecContext &ctx,
                                                       const ObSetVar &set_var,
                                                       const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(set_var.var_name_ != OB_SV_IDENTITY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("var name must be OB_SV_IDENTITY", K(ret), K(set_var.var_name_),
              K(OB_SV_IDENTITY), K(val));
  } else if (true == set_var.is_set_default_) {
    ret = OB_ERR_NO_DEFAULT;
    LOG_USER_ERROR(OB_ERR_NO_DEFAULT, set_var.var_name_.length(), set_var.var_name_.ptr());
  } else {
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
    if (OB_UNLIKELY(ObSetVar::SET_SCOPE_GLOBAL == set_var.set_scope_) ||
        OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("identity or last_insert_id is session variable or session is NULL",
               K(ret), K(set_var.var_name_), K(session));
    } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_IDENTITY, val))) {
      LOG_WARN("fail to update identity", K(ret), K(set_var.var_name_),
               K(SYS_VAR_IDENTITY), K(val));
    }
    //同时update系统变量last_insert_id
    else if (OB_FAIL(session->update_sys_variable(SYS_VAR_LAST_INSERT_ID, val))) {
      LOG_WARN("succ to update identity, but fail to update last_insert_id",
               K(ret), K(set_var.var_name_), K(OB_SV_LAST_INSERT_ID), K(val));
    }
  }
  return ret;
}

int ObSysVarSessionSpecialUpdateFuncs::update_last_insert_id(ObExecContext &ctx,
                                                             const ObSetVar &set_var,
                                                             const ObObj &val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(set_var.var_name_ != OB_SV_LAST_INSERT_ID)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("var name must be OB_SV_LAST_INSERT_ID", K(ret), K(set_var.var_name_),
              K(OB_SV_LAST_INSERT_ID), K(val));
  } else if (true == set_var.is_set_default_) {
    ret = OB_ERR_NO_DEFAULT;
    LOG_USER_ERROR(OB_ERR_NO_DEFAULT, set_var.var_name_.length(), set_var.var_name_.ptr());
  } else {
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
    if (ObSetVar::SET_SCOPE_GLOBAL == set_var.set_scope_ ||
        OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("identity or last_insert_id is session variableor session is NULL",
               K(ret), K(set_var.var_name_), K(session));
    } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_LAST_INSERT_ID, val))) {
      LOG_WARN("fail to update last_insert_id", K(ret), K(set_var.var_name_),
               K(OB_SV_LAST_INSERT_ID), K(val));
    }
    //同时update系统变量identity
    else if (OB_FAIL(session->update_sys_variable(SYS_VAR_IDENTITY, val))) {
      LOG_WARN("succ to update last_insert_id, but fail to update identity",
               K(ret), K(set_var.var_name_), K(OB_SV_IDENTITY), K(val));
    }
  }
  return ret;
}

int ObSysVarSessionSpecialUpdateFuncs::update_tx_isolation(ObExecContext &ctx,
                                                           const ObSetVar &set_var,
                                                           const ObObj &val)
{
  int ret = OB_SUCCESS;
  if ((OB_UNLIKELY(set_var.var_name_ != OB_SV_TX_ISOLATION))
    && (OB_UNLIKELY(set_var.var_name_ != OB_SV_TRANSACTION_ISOLATION))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("var name must be OB_SV_TX_ISOLATION or OB_SV_TRANSACTION_ISOLATION",
              K(ret), K(set_var.var_name_), K(OB_SV_TX_ISOLATION),
              K(OB_SV_TRANSACTION_ISOLATION),K(val));
  } else if (true == set_var.is_set_default_) {
    ret = OB_ERR_NO_DEFAULT;
    LOG_USER_ERROR(OB_ERR_NO_DEFAULT, set_var.var_name_.length(), set_var.var_name_.ptr());
  } else {
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret), K(set_var.var_name_), K(session));
    } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_TX_ISOLATION, val))) {
      LOG_WARN("fail to update tx_isolation", K(ret), K(set_var.var_name_),
               K(SYS_VAR_TX_ISOLATION), K(val));
    } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_TRANSACTION_ISOLATION, val))) {
      LOG_WARN("succ to update tx_isolation, but fail to update last_insert_id",
               K(ret), K(set_var.var_name_), K(OB_SV_TRANSACTION_ISOLATION), K(val));
    }
  }
  return ret;
}

int ObSysVarSessionSpecialUpdateFuncs::update_tx_read_only(sql::ObExecContext &ctx,
                                                                  const ObSetVar &set_var,
                                                                  const common::ObObj &val)
{
  int ret = OB_SUCCESS;
  if ((OB_UNLIKELY(set_var.var_name_ != OB_SV_TX_READ_ONLY))
    && (OB_UNLIKELY(set_var.var_name_ != OB_SV_TRANSACTION_READ_ONLY))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("var name must be OB_SV_TX_READ_ONLY or OB_SV_TRANSACTION_READ_ONLY",
              K(ret), K(set_var.var_name_), K(OB_SV_TX_ISOLATION),
              K(OB_SV_TRANSACTION_ISOLATION),K(val));
  } else if (true == set_var.is_set_default_) {
    ret = OB_ERR_NO_DEFAULT;
    LOG_USER_ERROR(OB_ERR_NO_DEFAULT, set_var.var_name_.length(), set_var.var_name_.ptr());
  } else {
    ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret), K(set_var.var_name_), K(session));
    } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_TX_READ_ONLY, val))) {
      LOG_WARN("fail to update tx_read_only", K(ret), K(set_var.var_name_),
               K(SYS_VAR_TX_ISOLATION), K(val));
    } else if (OB_FAIL(session->update_sys_variable(SYS_VAR_TRANSACTION_READ_ONLY, val))) {
      LOG_WARN("succ to update tx_read_only, but fail to update transaction_read_only",
               K(ret), K(set_var.var_name_), K(OB_SV_TRANSACTION_ISOLATION), K(val));
    }
  }
  return ret;
}

/////////////////////////////////////////

ObCharsetSysVarPair ObCharsetSysVarPair::CHARSET_SYS_VAR_PAIRS[SYS_CHARSET_SYS_VAR_PAIR_COUNT] =
{
  ObCharsetSysVarPair(OB_SV_CHARACTER_SET_SERVER, OB_SV_COLLATION_SERVER),
  ObCharsetSysVarPair(OB_SV_CHARACTER_SET_DATABASE, OB_SV_COLLATION_DATABASE),
  ObCharsetSysVarPair(OB_SV_CHARACTER_SET_CONNECTION, OB_SV_COLLATION_CONNECTION)
};

int ObCharsetSysVarPair::get_charset_var_by_collation_var(const ObString &coll_var_name,
                                                          ObString &cs_var_name)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < SYS_CHARSET_SYS_VAR_PAIR_COUNT; ++i) {
    if (CHARSET_SYS_VAR_PAIRS[i].coll_var_name_ == coll_var_name) {
      cs_var_name = CHARSET_SYS_VAR_PAIRS[i].cs_var_name_;
      ret = OB_SUCCESS;
    } else {}
  }
  return ret;
}

int ObCharsetSysVarPair::get_collation_var_by_charset_var(const ObString &cs_var_name,
                                                          ObString &coll_var_name)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; OB_ENTRY_NOT_EXIST == ret && i < SYS_CHARSET_SYS_VAR_PAIR_COUNT; ++i) {
    if (CHARSET_SYS_VAR_PAIRS[i].cs_var_name_ == cs_var_name) {
      coll_var_name = CHARSET_SYS_VAR_PAIRS[i].coll_var_name_;
      ret = OB_SUCCESS;
    } else {}
  }
  return ret;
}

int ObPreProcessSysVars::change_initial_value()
{
  int ret = OB_SUCCESS;
  // OB_SV_VERSION_COMMENT
  if (OB_FAIL(ObSysVariables::set_value(OB_SV_VERSION_COMMENT,
                                               ObSpecialSysVarValues::version_comment_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_VERSION_COMMENT),
             K(ObSpecialSysVarValues::version_comment_));

  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_VERSION_COMMENT,
                                               ObSpecialSysVarValues::version_comment_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_VERSION_COMMENT),
             K(ObSpecialSysVarValues::version_comment_));
  // OB_SV_SYSTEM_TIME_ZONE
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_SYSTEM_TIME_ZONE,
                                               ObSpecialSysVarValues::system_time_zone_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_SYSTEM_TIME_ZONE),
             K(ObSpecialSysVarValues::system_time_zone_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_SYSTEM_TIME_ZONE,
                                               ObSpecialSysVarValues::system_time_zone_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_SYSTEM_TIME_ZONE),
             K(ObSpecialSysVarValues::system_time_zone_str_));
  // charset和collation相关
  // OB_SV_CHARACTER_SET_SERVER
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_CHARACTER_SET_SERVER,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(ret), K(OB_SV_CHARACTER_SET_SERVER),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_CHARACTER_SET_SERVER,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(ret), K(OB_SV_CHARACTER_SET_SERVER),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  // OB_SV_CHARACTER_SET_CONNECTION
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_CHARACTER_SET_CONNECTION,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_CHARACTER_SET_CONNECTION),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_CHARACTER_SET_CONNECTION,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_CHARACTER_SET_CONNECTION),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  // OB_SV_CHARACTER_SET_CLIENT
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_CHARACTER_SET_CLIENT,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_CHARACTER_SET_CLIENT),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_CHARACTER_SET_CLIENT,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_CHARACTER_SET_CLIENT),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  // OB_SV_CHARACTER_SET_RESULTS
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_CHARACTER_SET_RESULTS,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_CHARACTER_SET_RESULTS),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_CHARACTER_SET_RESULTS,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_CHARACTER_SET_RESULTS),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  // OB_SV_CHARACTER_SET_SYSTEM
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_CHARACTER_SET_SYSTEM,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_CHARACTER_SET_SYSTEM),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_CHARACTER_SET_SYSTEM,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_CHARACTER_SET_SYSTEM),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  // OB_SV_COLLATION_SERVER
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_COLLATION_SERVER,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_COLLATION_SERVER),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_COLLATION_SERVER,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_COLLATION_SERVER),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  // OB_SV_COLLATION_DATABASE
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_COLLATION_DATABASE,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_COLLATION_DATABASE),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_COLLATION_DATABASE,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_COLLATION_DATABASE),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  // OB_SV_COLLATION_CONNECTION
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_COLLATION_CONNECTION,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_COLLATION_CONNECTION),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_COLLATION_CONNECTION,
                                             ObSpecialSysVarValues::default_coll_int_str_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_COLLATION_CONNECTION),
             K(ObSpecialSysVarValues::default_coll_int_str_));
  // OB_SV_SERVER_UUID
  } else if (OB_FAIL(ObSysVariables::set_value(OB_SV_SERVER_UUID,
                                               ObSpecialSysVarValues::server_uuid_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_SERVER_UUID),
             K(ObSpecialSysVarValues::server_uuid_));
  } else if (OB_FAIL(ObSysVariables::set_base_value(OB_SV_SERVER_UUID,
                                               ObSpecialSysVarValues::server_uuid_))) {
    LOG_WARN("fail to change initial value", K(OB_SV_SERVER_UUID),
             K(ObSpecialSysVarValues::server_uuid_));
  } else {
     LOG_INFO("succ to change_initial_value",
             "version_comment", ObSpecialSysVarValues::version_comment_,
             "system_time_zone_str", ObSpecialSysVarValues::system_time_zone_str_,
             "default_coll_int_str", ObSpecialSysVarValues::default_coll_int_str_,
             "server_uuid", ObSpecialSysVarValues::server_uuid_);
  }
  return ret;
}
int ObPreProcessSysVars::init_sys_var()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPreProcessSysVars::change_initial_value())) {
    LOG_ERROR("fail to change initial value", K(ret));
  } else if (OB_FAIL(ObSysVariables::init_default_values())) {
    LOG_ERROR("fail to init default values", K(ret));
  }
  return ret;
}

int ObSetSysVar::find_set(const ObString &str)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (str.length() <= 0) {
    //nothing to do
  } else if (OB_UNLIKELY(str.length() >=  MAX_STR_BUF_LEN)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("system variable string is too long", K(ret), K(str));
  } else if (OB_ISNULL(buf = strndupa(str.ptr(), str.length()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc memory", K(ret));
  } else {
    char *value = NULL;
    char *saveptr = NULL;
    for (value = strtok_r(buf, ",", &saveptr);
         OB_SUCC(ret) && NULL != value;
         value = strtok_r(NULL, ",", &saveptr)) {
      int64_t index = -1;
      ObString part_str(value);
      if (OB_FAIL(ObTypeLibSysVar::find_type(part_str, index))) {
        ret = OB_ERR_WRONG_VALUE_FOR_VAR;
        ObObj err_obj;
        err_obj.set_varchar(part_str);
        int log_ret = OB_SUCCESS;
        if (OB_SUCCESS != (log_ret = log_err_wrong_value_for_var(ret, err_obj))) {
          // log_ret仅用于打日志，不覆盖ret
          LOG_ERROR("fail to log error", K(ret), K(log_ret), K(err_obj));
        }
      }
    }
  }
  return ret;
}

int ObSysVarUtils::log_bounds_error_or_warning(ObExecContext &ctx,
                                               const ObSetVar &set_var,
                                               const ObObj &in_val)
{
  int ret = OB_SUCCESS;
  // 为了兼容mysql，sql mode中含有STRICT_ALL_TABLES的时候报error，否则报warnning
  const int64_t VALUE_STR_LENGTH = 32;
  char val_str[VALUE_STR_LENGTH];
  int64_t pos = 0;
  if (OB_FAIL(in_val.print_plain_str_literal(val_str, VALUE_STR_LENGTH, pos))) {
    LOG_WARN("fail to print varchar literal", K(ret));
  } else {
    ObSQLSessionInfo *session = ctx.get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("session is NULL", K(ret));
    } else if (session->get_sql_mode() & SMO_STRICT_ALL_TABLES) {
      ret = OB_ERR_WRONG_VALUE_FOR_VAR;
      LOG_USER_ERROR(OB_ERR_WRONG_VALUE_FOR_VAR, set_var.var_name_.length(),
                     set_var.var_name_.ptr(), static_cast<int32_t>(strlen(val_str)), val_str);
    } else {
      LOG_USER_WARN(OB_ERR_TRUNCATED_WRONG_VALUE, set_var.var_name_.length(),
                    set_var.var_name_.ptr(), static_cast<int32_t>(strlen(val_str)), val_str);
    }
  }
  return ret;
}

}
}
