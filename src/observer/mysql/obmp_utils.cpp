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

#define USING_LOG_PREFIX SERVER
#include "obmp_utils.h"
#include "obmp_base.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase {
namespace observer {
using namespace common;
using namespace share;
using namespace obmysql;
using namespace sql;

int ObMPUtils::add_changed_session_info(OMPKOK& ok_pkt, sql::ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  if (session.is_session_info_changed()) {
    ok_pkt.set_state_changed(true);
  }
  if (session.is_database_changed()) {
    ObString db_name = session.get_database_name();
    ok_pkt.set_changed_schema(db_name);
  }

  ObIAllocator& allocator = session.get_allocator();
  if (session.is_sys_var_changed()) {
    const ObIArray<sql::ObBasicSessionInfo::ChangedVar>& sys_var = session.get_changed_sys_var();
    LOG_DEBUG("sys var changed", K(session.get_tenant_name()), K(sys_var.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_var.count(); ++i) {
      sql::ObBasicSessionInfo::ChangedVar change_var = sys_var.at(i);
      ObObj new_val;
      bool changed = true;
      if (OB_FAIL(session.is_sys_var_actully_changed(change_var.id_, change_var.old_val_, new_val, changed))) {
        LOG_WARN("failed to check actully changed", K(ret), K(change_var), K(changed));
      } else if (changed) {
        ObStringKV str_kv;
        if (OB_FAIL(ObSysVarFactory::get_sys_var_name_by_id(change_var.id_, str_kv.key_))) {
          LOG_WARN("failed to get sys variable name", K(ret), K(change_var));
        } else if (OB_FAIL(get_plain_str_literal(allocator, new_val, str_kv.value_))) {
          LOG_WARN("failed to get sys vairable new value string", K(ret), K(new_val));
        } else if (OB_FAIL(ok_pkt.add_system_var(str_kv))) {
          LOG_WARN("failed to add system variable", K(str_kv), K(ret));
        } else {
          LOG_DEBUG("success add system var to ok pack", K(str_kv), K(change_var), K(new_val));
        }
      }
    }
  }

  if (session.is_user_var_changed()) {
    const ObIArray<ObString>& user_var = session.get_changed_user_var();
    ObSessionValMap& user_map = session.get_user_var_val_map();
    for (int64_t i = 0; i < user_var.count() && OB_SUCCESS == ret; ++i) {
      ObString name = user_var.at(i);
      ObSessionVariable sess_var;
      if (name.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid variable name", K(name), K(ret));
      }
      if (OB_FAIL(user_map.get_refactored(name, sess_var))) {
        LOG_WARN("unknown user variable", K(name), K(ret));
      } else {
        ObStringKV str_kv;
        str_kv.key_ = name;
        if (OB_FAIL(get_user_sql_literal(allocator, sess_var.value_, str_kv.value_, TZ_INFO(&session)))) {
          LOG_WARN("fail to get user sql literal", K(sess_var.value_), K(ret));
        } else if (OB_FAIL(ok_pkt.add_user_var(str_kv))) {
          LOG_WARN("fail to add user var", K(str_kv), K(ret));
        } else {
          LOG_DEBUG("succ to add user var", K(str_kv), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_client_feedback(ok_pkt, session))) {
      LOG_WARN("fail to add client feedback", K(ret));
    }
  }
  return ret;
}

// add OB_CLIENT_FEEDBACK, treat as user var
int ObMPUtils::add_client_feedback(OMPKOK& ok_pkt, sql::ObSQLSessionInfo& session)
{
  INIT_SUCC(ret);
  const ObFeedbackManager& fb_manager = session.get_feedback_manager();
  if (!fb_manager.is_empty()) {
    ObIAllocator& allocator = session.get_allocator();
    const int64_t SER_BUF_LEN = 1024;
    const int64_t RETRY_COUNT = 4;
    const int64_t MULTIPLIER = 8;

    int64_t tmp_len = 0;
    char* tmp_buff = NULL;
    int64_t pos = 0;
    bool need_retry = true;
    // assume: seri buffer never > 512KB
    for (int64_t i = 0; need_retry && (i < RETRY_COUNT) && OB_SUCC(ret); i++) {
      tmp_len = ((0 == i) ? (SER_BUF_LEN) : (SER_BUF_LEN * i * MULTIPLIER));
      tmp_buff = (char*)allocator.alloc(tmp_len);
      pos = 0;
      if (OB_ISNULL(tmp_buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(tmp_len), K(ret));
      } else if (OB_FAIL(fb_manager.serialize(tmp_buff, tmp_len, pos))) {
        if (OB_SIZE_OVERFLOW == ret || OB_BUF_NOT_ENOUGH == ret) {
          // buf not enough, retry
          ret = OB_SUCCESS;
        }
      } else if (OB_UNLIKELY(pos <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pos", K(pos), K(tmp_len), K(ret));
      } else {
        need_retry = false;  // break;
      }
    }
    if (OB_SUCC(ret)) {
      ObStringKV str_kv;
      str_kv.key_.assign_ptr(OB_CLIENT_FEEDBACK, static_cast<int32_t>(strlen(OB_CLIENT_FEEDBACK)));
      str_kv.value_.assign(tmp_buff, static_cast<int32_t>(pos));
      if (OB_FAIL(ok_pkt.add_user_var(str_kv))) {
        LOG_WARN("fail to add user var", K(str_kv), K(ret));
      }
    }
  }
  return ret;
}

int ObMPUtils::add_client_reroute_info(
    OMPKOK& okp, sql::ObSQLSessionInfo& session, share::ObFeedbackRerouteInfo& reroute_info)
{
  LOG_DEBUG("adding client reroute info", K(reroute_info), K(okp));
  int ret = OB_SUCCESS;
  ObIAllocator& allocator = session.get_allocator();
  char* tmp_buff = NULL;
  int64_t pos = 0;
  const int64_t SER_BUF_LEN = 1024;
  if (OB_ISNULL(tmp_buff = (char*)allocator.alloc(SER_BUF_LEN))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(reroute_info.serialize(tmp_buff, SER_BUF_LEN, pos))) {
    LOG_WARN("failed to serialize reroute info", K(ret));
  } else {
    ObStringKV str_kv;
    str_kv.key_.assign_ptr(OB_CLIENT_REROUTE_INFO, static_cast<int32_t>(strlen(OB_CLIENT_REROUTE_INFO)));
    str_kv.value_.assign(tmp_buff, static_cast<int32_t>(pos));
    if (OB_FAIL(okp.add_user_var(str_kv))) {
      LOG_WARN("failed to add user var", K(ret), K(str_kv));
    }
  }

  return ret;
}

int ObMPUtils::add_nls_format(OMPKOK& okp, sql::ObSQLSessionInfo& session, const bool only_changed /*false*/)
{
  int ret = OB_SUCCESS;
  if (only_changed) {
    ObIAllocator& allocator = session.get_allocator();
    if (session.is_sys_var_changed()) {
      const ObIArray<sql::ObBasicSessionInfo::ChangedVar>& sys_var = session.get_changed_sys_var();
      LOG_DEBUG("sys var changed", K(session.get_tenant_name()), K(sys_var.count()));
      int64_t max_add_count = ObNLSFormatEnum::NLS_MAX;
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_var.count() && max_add_count > 0; ++i) {
        const sql::ObBasicSessionInfo::ChangedVar change_var = sys_var.at(i);
        ObObj new_val;
        bool changed = true;
        ObNLSFormatEnum nls_enum = ObNLSFormatEnum::NLS_MAX;
        if (change_var.id_ == SYS_VAR_NLS_DATE_FORMAT) {
          nls_enum = ObNLSFormatEnum::NLS_DATE;
        } else if (change_var.id_ == SYS_VAR_NLS_TIMESTAMP_FORMAT) {
          nls_enum = ObNLSFormatEnum::NLS_TIMESTAMP;
        } else if (change_var.id_ == SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT) {
          nls_enum = ObNLSFormatEnum::NLS_TIMESTAMP_TZ;
        }
        if (nls_enum != ObNLSFormatEnum::NLS_MAX) {
          --max_add_count;
          if (OB_FAIL(session.is_sys_var_actully_changed(change_var.id_, change_var.old_val_, new_val, changed))) {
            LOG_WARN("failed to check actully changed", K(ret), K(change_var), K(changed));
          } else if (changed) {
            ObStringKV str_kv;
            str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(change_var.id_);  // shadow copy
            str_kv.value_ = session.get_local_nls_formats()[nls_enum];

            if (OB_FAIL(okp.add_system_var(str_kv))) {
              LOG_WARN("failed to add system variable", K(str_kv), K(ret));
            } else {
              // AS ob pkt encoding is different from mysql, we should not set_state_changed true
              okp.set_state_changed(false);
              LOG_DEBUG("success add system var to ok pack", K(str_kv), K(change_var), K(new_val), K(okp));
            }
          }
        }
      }
    }
  } else {
    // AS ob pkt encoding is different from mysql, we should not set_state_changed true
    okp.set_state_changed(false);

    ObStringKV nls_date_str_kv;
    nls_date_str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(SYS_VAR_NLS_DATE_FORMAT);  // shadow copy
    nls_date_str_kv.value_ = session.get_local_nls_date_format();

    ObStringKV nls_timestamp_str_kv;
    nls_timestamp_str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(SYS_VAR_NLS_TIMESTAMP_FORMAT);  // shadow copy
    nls_timestamp_str_kv.value_ = session.get_local_nls_timestamp_format();

    ObStringKV nls_timestamp_tz_str_kv;
    nls_timestamp_tz_str_kv.key_ =
        ObSysVarFactory::get_sys_var_name_by_id(SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT);  // shadow copy
    nls_timestamp_tz_str_kv.value_ = session.get_local_nls_timestamp_tz_format();

    if (OB_FAIL(okp.add_system_var(nls_date_str_kv))) {
      LOG_WARN("fail to add system var", K(nls_date_str_kv), K(ret));
    } else if (OB_FAIL(okp.add_system_var(nls_timestamp_str_kv))) {
      LOG_WARN("fail to add system var", K(nls_timestamp_str_kv), K(ret));
    } else if (OB_FAIL(okp.add_system_var(nls_timestamp_tz_str_kv))) {
      LOG_WARN("fail to add system var", K(nls_timestamp_tz_str_kv), K(ret));
    } else {
      LOG_DEBUG("succ to add system var", K(okp), K(ret));
    }
  }
  return ret;
}

int ObMPUtils::add_session_info_on_connect(OMPKOK& okp, sql::ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  // treat it as state changed
  okp.set_state_changed(true);

  // add database name
  if (session.is_database_changed()) {
    ObString db_name = session.get_database_name();
    okp.set_changed_schema(db_name);
  }

  // update_global_vars_version_ to global_vars_last_modified_time
  ObObj value;
  value.set_int(session.get_global_vars_version());
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = session.update_sys_variable(share::SYS_VAR_OB_PROXY_GLOBAL_VARIABLES_VERSION, value))) {
    LOG_WARN("failed to update global variables version, we will go on anyway", K(session), K(tmp_ret));
  }

  // add all sys variables
  ObIAllocator& allocator = session.get_allocator();
  for (int64_t i = 0; OB_SUCC(ret) && i < session.get_sys_var_count(); ++i) {
    const ObBasicSysVar* sys_var = NULL;
    if (NULL == (sys_var = session.get_sys_var(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("sys var is NULL", K(i), "total", session.get_sys_var_count(), K(ret));
    } else {
      ObStringKV str_kv;
      str_kv.key_ = ObSysVarFactory::get_sys_var_name_by_id(sys_var->get_type());  // shadow copy
      if (OB_FAIL(get_plain_str_literal(allocator, sys_var->get_value(), str_kv.value_))) {
        LOG_WARN("fail to get sql literal", K(i), K(ret));
      } else if (OB_FAIL(okp.add_system_var(str_kv))) {
        LOG_WARN("fail to add system var", K(i), K(str_kv), K(ret));
      }
    }
  }
  return ret;
}

int ObMPUtils::get_plain_str_literal(ObIAllocator& allocator, const ObObj& obj, ObString& value_str)
{
  int ret = OB_SUCCESS;
  char* data = NULL;
  int64_t pos = 0;
  const bool is_plain = true;
  int64_t plain_str_print_length = 0;
  if (obj.is_null()) {
    // if obj is null value , return ""; mysql have the same behavior
    pos = 0;
  } else if (OB_FAIL(get_literal_print_length(obj, is_plain, plain_str_print_length))) {
    LOG_WARN("fail to get buffer length", K(ret), K(obj), K(plain_str_print_length));
  } else if (OB_UNLIKELY(plain_str_print_length <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid buffer length", K(ret), K(obj), K(plain_str_print_length));
  } else if (NULL == (data = static_cast<char*>(allocator.alloc(plain_str_print_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem", K(plain_str_print_length), K(ret));
  } else {
    ret = obj.print_plain_str_literal(data, plain_str_print_length, pos);
  }
  if (OB_SUCC(ret)) {
    value_str.assign_ptr(data, static_cast<uint32_t>(pos));
  }
  return ret;
}

int ObMPUtils::get_user_sql_literal(
    ObIAllocator& allocator, const ObObj& obj, ObString& value_str, const ObTimeZoneInfo* tz_info)
{
  int ret = OB_SUCCESS;
  char* data = NULL;
  int64_t pos = 0;
  const bool is_plain = false;
  int64_t user_sql_print_length = 0;
  if (OB_FAIL(get_literal_print_length(obj, is_plain, user_sql_print_length))) {
    LOG_WARN("fail to get buffer length", K(ret), K(obj), K(user_sql_print_length));
  } else if (OB_UNLIKELY(user_sql_print_length <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid buffer length", K(ret), K(obj), K(user_sql_print_length));
  } else if (NULL == (data = static_cast<char*>(allocator.alloc(user_sql_print_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc mem", K(user_sql_print_length), K(ret));
  } else if (OB_FAIL(obj.print_sql_literal(data, user_sql_print_length, pos, tz_info))) {
    LOG_WARN("fail to print sql  literal", K(ret), K(pos), K(user_sql_print_length), K(obj));
  } else {
    value_str.assign_ptr(data, static_cast<uint32_t>(pos));
  }
  return ret;
}

int ObMPUtils::get_literal_print_length(const ObObj& obj, bool is_plain, int64_t& len)
{
  int ret = OB_SUCCESS;
  len = 0;
  int32_t len_of_string = 0;
  const ObLobLocator* locator = nullptr;
  if (!obj.is_string_or_lob_locator_type()) {
    len = OB_MAX_SYS_VAR_NON_STRING_VAL_LENGTH;
  } else if (OB_UNLIKELY((len_of_string = obj.get_string_len()) < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("string length invalid", K(obj), K(len_of_string));
  } else if (obj.is_char() || obj.is_varchar() || obj.is_text() || ob_is_nstring_type(obj.get_type())) {
    // if is_plain is false, 'j' will be print as "j\0" (with Quotation Marks here)
    // otherwise. as j\0 (withOUT Quotation Marks here)
    ObHexEscapeSqlStr sql_str(obj.get_string());
    len = len_of_string + (is_plain ? 1 : (3 + sql_str.get_extra_length()));
    if (CHARSET_UTF8MB4 != ObCharset::charset_type_by_coll(obj.get_collation_type())) {
      len += len_of_string;
    }
  } else if (obj.is_binary() || obj.is_varbinary() || obj.is_hex_string() || obj.is_blob()) {
    // if is_plain is false, 'j' will be print as "X'6a'\0" (With Quotation Marks Here)
    // otherwise. as X'6a'\0 (Without Quotation Marks Here)
    len = 2 * len_of_string + (is_plain ? 4 : 6);
  } else if (!obj.is_lob_locator()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("obj type unexpected", K(obj), K(is_plain));
  } else if (OB_ISNULL(locator = obj.get_lob_locator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null lob locator", K(ret), K(obj));
  } else if (obj.is_blob_locator()) {
    if (OB_UNLIKELY(!locator->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected lob locator", K(ret), K(obj), KPC(locator));
    } else {
      len = locator->get_total_size() * 2 + (is_plain ? 4 : 6);
    }
  } else if (obj.is_clob_locator()) {
    ObString payload;
    if (OB_UNLIKELY(!locator->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected lob locator", K(ret), K(obj), K(locator));
    } else if (OB_FAIL(locator->get_payload(payload))) {
      LOG_WARN("Failed to get lob payload from locator", K(obj), KPC(locator));
    } else {
      ObHexEscapeSqlStr sql_str(payload);
      len = locator->get_total_size() + (is_plain ? 1 : (3 + sql_str.get_extra_length()));
      if (CHARSET_UTF8MB4 != ObCharset::charset_type_by_coll(obj.get_collation_type())) {
        len += locator->get_data_length();
      }
    }
  }
  return ret;
}

}  // end of namespace observer
}  // end of namespace oceanbase
