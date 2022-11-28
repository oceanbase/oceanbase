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

#include "lib/string/ob_string.h"
#include "pl/ob_pl_warning.h"
#include "share/ob_define.h"
#include "share/system_variable/ob_sys_var_class_type.h"
#include <cstdlib>

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share;

namespace pl
{
using w_status = PlCompilerWarningCategory::w_status;
using w_conf = PlCompilerWarningCategory::w_conf;

int PlCompilerWarningCategory::string_to_err_code(const ObString &err_str, int64_t &err_code)
{
  int ret = OB_SUCCESS;
  char *end_ptr = NULL;
  errno = 0;
  err_code = 0;
  char cstr[6] = "";

  MEMCPY(cstr, err_str.ptr(), MIN(err_str.length(), 5));

  // 将string转成int
  int64_t ret_int_val = strtoll(cstr, &end_ptr, 10);

  // 如果转换过程出了错误，或者字符串中的字符没有全部转换为数字
  // 说明原始的字符串有问题，不能转换为string
  if (errno != 0 || (NULL != end_ptr && err_str.length() != (end_ptr - cstr))) {
    LOG_WARN("strtoll convert string to int value fail", K(ret_int_val), K(err_str),
             K(end_ptr), KP(err_str.ptr()), KP(end_ptr), "error", strerror(errno));
    ret = OB_INVALID_DATA;
  } else {
    /*
    1.       The severe code is in the range of 05000 to 05999.
    2.       The informational code is in the range of 06000 to 06999.
    3.       The performance code is in the range of 07000 to 07249.
    */
    if (!is_valid_err_code(ret_int_val)) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("error code is not in specific range", K(err_code));
     } else {
       err_code = ret_int_val;
     }
  }
  return ret;
}

int PlCompilerWarningCategory::update_status(w_status &ws, w_conf &wc, const int64_t err_code)
{
  int ret = OB_SUCCESS;
  if (is_valid_err_code(err_code)) {
    OZ (err_code_map_.set_refactored(err_code, err_code_wrap(ws, err_code)));
  } else {
    if (PlCompilerWarningCategory::WARN_UNKNOWN == ws ||
        PlCompilerWarningCategory::CONF_UNKNOWN == wc) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (PlCompilerWarningCategory::CONF_ALL == wc) {
        severe_ = static_cast<uint64_t>(ws);
        performance_ = static_cast<uint64_t>(ws);
        informational_ = static_cast<uint64_t>(ws);
      } else if (PlCompilerWarningCategory::CONF_SEVERE== wc) {
        severe_ = static_cast<uint64_t>(ws);
      } else if (PlCompilerWarningCategory::CONF_PERF == wc) {
        performance_ = static_cast<uint64_t>(ws);
      } else if (PlCompilerWarningCategory::CONF_INFO == wc) {
        informational_ = static_cast<uint64_t>(ws);
      }
    }
  }
  return ret;
}

int PlCompilerWarningCategory::verify_warning_settings(const ObString &confs_str,
                                                       PlCompilerWarningCategory *category)
{
  int ret = OB_SUCCESS;
  if (!confs_str.empty()) {


    ObString confs_dup = confs_str;
    ObString item = confs_dup.split_on(',');
    while (OB_SUCC(ret) && !item.empty()) {
      DoParseWarning dpw(item);
      OZ (dpw(category));
      item = confs_dup.split_on(',');
    }
    if (!confs_dup.empty()) {
      DoParseWarning dpw(confs_dup);
      OZ (dpw(category));
    }
  }
  return ret;
}

int PlCompilerWarningCategory::add_pl_warning_impl(sql::ObExecContext &ctx, const ObString &conf)
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = ctx.get_sql_ctx();
  ObSQLSessionInfo *sess_info = NULL;
  CK (OB_NOT_NULL(sql_ctx));
  OX (sess_info = ctx.get_sql_ctx()->session_info_);
  CK (OB_NOT_NULL(sess_info));
  if (OB_SUCC(ret)) {
    OZ (verify_warning_settings(conf, NULL));
    ObSqlString confs;
    ObObj formal_val;
    OZ (sess_info->get_sys_variable(ObSysVarClassType::SYS_VAR_PLSQL_WARNINGS, formal_val));
    if (!formal_val.get_string().empty()) {
      OZ (confs.append(formal_val.get_string()));
      OZ (confs.append(","));
    }
    OZ (confs.append(conf));
    OZ (sess_info->update_sys_variable(ObSysVarClassType::SYS_VAR_PLSQL_WARNINGS, confs.string()));
  }
  return ret;
}

int PlCompilerWarningCategory::add_pl_warning_setting_cat(sql::ObExecContext &ctx,
                                                              sql::ParamStore &params,
                                                              common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK (3 == params.count());
  CK (params.at(0).is_string_type() &&
      params.at(1).is_string_type() &&
      params.at(2).is_string_type());
  if (OB_SUCC(ret)) {
    ObString catg = params.at(0).get_string();
    ObString val = params.at(1).get_string();
    ObSqlString item;
    OZ (item.append(val));
    OZ (item.append(":"));
    OZ (item.append(catg));
    OZ (add_pl_warning_impl(ctx, item.string()));
  }
  return ret;
}

int PlCompilerWarningCategory::add_pl_warning_setting_num(sql::ObExecContext &ctx,
                                                              sql::ParamStore &params,
                                                              common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  int64_t err_code = 0;
  ObString err_str;
  char buf[20];
  CK (3 == params.count());
  CK (params.at(0).is_int32() && params.at(1).is_string_type() && params.at(2).is_string_type());
  if (OB_SUCC(ret)) {
    err_code = static_cast<int64_t>(params.at(0).get_int32());
    if (is_valid_err_code(err_code)) {
      OZ (databuff_printf(buf, 20, "%ld", err_code));
    } else {
      ret = OB_ERR_PARAM_VALUE_INVALID;
      LOG_WARN("parameter cannot be modified because specified value is invalid", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObString val = params.at(1).get_string();
    ObSqlString item;
    OZ (item.append(val));
    OZ (item.append(":"));
    OZ (item.append(ObString(buf)));
    OZ (add_pl_warning_impl(ctx, item.string()));
  }
  return ret;
}

int PlCompilerWarningCategory::set_pl_warning_setting(sql::ObExecContext &ctx,
                                                      sql::ParamStore &params,
                                                      common::ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  CK (2 == params.count());
  CK (params.at(0).is_string_type() || params.at(1).is_string_type());
  ObSqlCtx *sql_ctx = ctx.get_sql_ctx();
  ObSQLSessionInfo *sess_info = NULL;
  CK (OB_NOT_NULL(sql_ctx));
  OX (sess_info = ctx.get_sql_ctx()->session_info_);
  CK (OB_NOT_NULL(sess_info));
  OZ (verify_warning_settings(params.at(0).get_string(), NULL));
  OZ (sess_info->update_sys_variable(ObSysVarClassType::SYS_VAR_PLSQL_WARNINGS,
                                     params.at(0).get_string()));
  return ret;
}

int PlCompilerWarningCategory::get_category(sql::ObExecContext &ctx,
                                            sql::ParamStore &params,
                                            common::ObObj &result)
{
  int ret = OB_SUCCESS;
  CK (1 == params.count());
  CK (params.at(0).is_integer_type());
  ObString cat;
  if (OB_SUCC(ret)) {
    int64_t err_code = params.at(0).get_int();
    if (5000 <= err_code && 5999 >= err_code) {
      OZ (ob_write_string(ctx.get_allocator(), ObString("SEVERE"), cat));
    } else if (6000 <= err_code && 6999 >= err_code) {
      OZ (ob_write_string(ctx.get_allocator(), ObString("INFORMATIONAL"), cat));
    } else if (7000 <= err_code && 7249 >= err_code) {
      OZ (ob_write_string(ctx.get_allocator(), ObString("PERFORMANCE"), cat));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("illegal error code", K(ret), K(err_code));
    }
  }
  ObCharsetType default_charset = ObCharset::get_default_charset();
  ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
  OX (result.set_varchar(cat));
  OX (result.set_collation_type(default_collation));
  return ret;
}

int PlCompilerWarningCategory::get_warning_setting_cat(sql::ObExecContext &ctx,
                                                       sql::ParamStore &params,
                                                       common::ObObj &result)
{
  int ret = OB_SUCCESS;
  ObObj conf_str;
  ObSqlCtx *sql_ctx = ctx.get_sql_ctx();
  ObSQLSessionInfo *sess_info = NULL;
  CK (1 == params.count());
  CK (OB_NOT_NULL(sql_ctx));
  OX (sess_info = ctx.get_sql_ctx()->session_info_);
  CK (OB_NOT_NULL(sess_info));
  OZ (sess_info->get_sys_variable(ObSysVarClassType::SYS_VAR_PLSQL_WARNINGS, conf_str));
  PlCompilerWarningCategory pwc;
  OZ (pwc.init());
  OZ (verify_warning_settings(conf_str.get_string(), &pwc));
  ObString res;
  ObString category;
  OX (category = params.at(0).get_string());
  #define DEF_STAT_FUNC(catg, upper_catg) \
  do { \
    if (0 == category.case_compare(#catg)) { \
      if (pwc.is_enable_##catg()) { \
        OZ (ob_write_string(ctx.get_allocator(), ObString("ENABLE:" #upper_catg), res)); \
      } else if (pwc.is_disable_##catg()) { \
        OZ (ob_write_string(ctx.get_allocator(), ObString("DISABLE:" #upper_catg), res)); \
      } else if (pwc.is_error_##catg()) { \
        OZ (ob_write_string(ctx.get_allocator(), ObString("ERROR:" #upper_catg), res)); \
      } else { \
      } \
    } \
  } while(0)

  if (OB_SUCC(ret)) {
    DEF_STAT_FUNC(all, ALL);
    DEF_STAT_FUNC(severe, SEVERE);
    DEF_STAT_FUNC(performance, PERFORMANCE);
    DEF_STAT_FUNC(informational, INFORMATIONAL);
  }
  OZ (pwc.reset());
  if (OB_SUCC(ret)) {
    ObCharsetType default_charset = ObCharset::get_default_charset();
    ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
    result.set_varchar(res);
    result.set_collation_type(default_collation);
  }
  return ret;
}

int PlCompilerWarningCategory::get_warning_setting_num(sql::ObExecContext &ctx,
                                                       sql::ParamStore &params,
                                                       common::ObObj &result)
{
  int ret = OB_SUCCESS;
  int64_t err_code = 0;
  char buf[20];
  CK (1 == params.count());
  CK (params.at(0).is_int32());
  if (OB_SUCC(ret)) {
    err_code = static_cast<int64_t>(params.at(0).get_int32());
    if (is_valid_err_code(err_code)) {
      OZ (databuff_printf(buf, 20, "%ld", err_code));
    } else {
      ret = OB_ERR_PARAM_VALUE_INVALID;
      LOG_WARN("parameter cannot be modified because specified value is invalid", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObObj conf_str;
    ObSqlCtx *sql_ctx = ctx.get_sql_ctx();
    ObSQLSessionInfo *sess_info = NULL;
    CK (1 == params.count());
    CK (OB_NOT_NULL(sql_ctx));
    OX (sess_info = ctx.get_sql_ctx()->session_info_);
    CK (OB_NOT_NULL(sess_info));
    OZ (sess_info->get_sys_variable(ObSysVarClassType::SYS_VAR_PLSQL_WARNINGS, conf_str));
    PlCompilerWarningCategory pwc;
    OZ (pwc.init());
    OZ (verify_warning_settings(conf_str.get_string(), &pwc));
    ObSqlString sql_res;
    ObString res;
    const err_code_wrap *ecw = NULL;
    w_status ws = WARN_UNKNOWN;
    // 首先找一下map里面是否有定义
    if (OB_NOT_NULL(ecw = pwc.err_code_map_.get(err_code))) {
      ws = const_cast<err_code_wrap *>(ecw)->status;
    } else {
      ws = pwc.get_err_code_cat(err_code);
    }

    if (WARN_UNKNOWN == ws) {
      ret = OB_ERR_UNEXPECTED;
    } else if (WARN_ENABLE == ws) {
      sql_res.append(ObString("ENABLE:"));
      sql_res.append(ObString(buf));
      OZ (ob_write_string(ctx.get_allocator(), sql_res.string(), res));
    } else if (WARN_DISABLE == ws) {
      sql_res.append(ObString("DISABLE:"));
      sql_res.append(ObString(buf));
      OZ (ob_write_string(ctx.get_allocator(), sql_res.string(), res));
    } else if (WARN_ERROR == ws) {
      sql_res.append(ObString("ERROR:"));
      sql_res.append(ObString(buf));
      OZ (ob_write_string(ctx.get_allocator(), sql_res.string(), res));
    }
    OZ (pwc.reset());
    OX (result.set_varchar(res));
    ObCharsetType default_charset = ObCharset::get_default_charset();
    ObCollationType default_collation = ObCharset::get_default_collation(default_charset);
    OX (result.set_collation_type(default_collation));
  }
  return ret;
}

int PlCompilerWarningCategory::get_warning_setting_string(sql::ObExecContext &ctx,
                                                          sql::ParamStore &params,
                                                          common::ObObj &result)
{
  UNUSED(params);
  int ret = OB_SUCCESS;
  ObObj conf_str;
  ObSqlCtx *sql_ctx = ctx.get_sql_ctx();
  ObSQLSessionInfo *sess_info = NULL;
  CK (OB_NOT_NULL(sql_ctx));
  OX (sess_info = ctx.get_sql_ctx()->session_info_);
  CK (OB_NOT_NULL(sess_info));
  OZ (sess_info->get_sys_variable(ObSysVarClassType::SYS_VAR_PLSQL_WARNINGS, conf_str));
  OX (result.set_varchar(conf_str.get_string()));
  OX (result.set_collation_type(conf_str.get_collation_type()));
  return ret;
}

int PlCompilerWarningCategory::init()
{
  int ret = OB_SUCCESS;
  OZ (err_code_map_.create(16, ObModIds::OB_SQL_HASH_SET));
  return ret;
}

int PlCompilerWarningCategory::reset()
{
  return err_code_map_.clear();
}

w_status PlCompilerWarningCategory::get_err_code_cat(int64_t err_code)
{
   return (5000 <= err_code && 5999 >= err_code) ?
          static_cast<w_status>(severe_) : (6000 <= err_code && 6999 >= err_code) ?
          static_cast<w_status>(informational_) : (7000 <= err_code && 7249 >= err_code) ?
          static_cast<w_status>(performance_) : WARN_UNKNOWN;
}

int PlCompilerWarningCategory::DoParseWarning::operator()(PlCompilerWarningCategory *category)
{
  int ret = OB_SUCCESS;
  ObString catg;
  catg = warning_str_.split_on(':');
  if (catg.empty()) {
    ret = OB_ERR_PARAM_VALUE_INVALID;
    LOG_WARN("parameter cannot be modified because specified value is invalid", K(ret));
  } else {
    ObString catg_trim = catg.trim();
    ObString item_trim = warning_str_.trim();
    WarningParser sp_cat(catg_trim);
    WarningParser sp_item(item_trim);
    if (!sp_cat.is_category()) {
      ret = OB_ERR_PARAM_VALUE_INVALID;
      LOG_WARN("parameter cannot be modified because specified value is invalid", K(ret));
    } else if (!sp_item.is_config() && !sp_item.is_err_code()) {
      ret = OB_ERR_PARAM_VALUE_INVALID;
      LOG_WARN("parameter cannot be modified because specified value is invalid", K(ret));
    } else {
      if (OB_NOT_NULL(category)) {
        w_status ws = sp_cat.get_category();
        w_conf wc = sp_item.get_conf();
        int64_t err_code = 0;
        if (sp_item.is_err_code()) {
          OZ (sp_item.get_err_code(err_code));
        }
        OZ (category->update_status(ws, wc, err_code));
      }
    }
  }
  return ret;
}

int PlCompilerWarningCategory::WarningParser::get_err_code(int64_t &err_code)
{
  int ret = OB_SUCCESS;
  OZ (PlCompilerWarningCategory::string_to_err_code(str_, err_code));
  return ret;
}

bool PlCompilerWarningCategory::WarningParser::is_err_code() {
  int ret = OB_SUCCESS;
  int64_t err_code = 0;
  OZ (PlCompilerWarningCategory::string_to_err_code(str_, err_code));
  return OB_SUCC(ret);
}

} // namespace pl
} // oceanbase