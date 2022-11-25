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

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "rpc/obmysql/packet/ompk_change_user.h"
#include "lib/charset/ob_charset.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

OMPKChangeUser::OMPKChangeUser() : cmd_(COM_CHANGE_USER),
    character_set_(CS_TYPE_UTF8MB4_BIN),
    mysql_cap_(),
    username_(),
    auth_response_(),
    auth_plugin_name_(),
    database_(),
    connect_attrs_(),
    sys_vars_(),
    user_vars_()
{
}

// see com_change_user packet
// for proxy, add session vars as connect attrs
int64_t OMPKChangeUser::get_serialize_size() const
{
  int64_t len = 0;
  len = 1; // cmd
  len += username_.length() + 1;

  if (!!mysql_cap_.cap_flags_.OB_CLIENT_SECURE_CONNECTION) {
    len += 1;
    len += auth_response_.length();
  } else {
    len += auth_response_.length() + 1;
  }

  len += database_.length() + 1;
  len += 2; // character set

  if (!!mysql_cap_.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
    len += auth_plugin_name_.length() + 1;
  }

  if (!!mysql_cap_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
    len += ObMySQLUtil::get_number_store_len(get_connect_attrs_len());
    len += get_connect_attrs_len();  
  }
  return len;
}

int OMPKChangeUser::serialize(char *buffer, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_UNLIKELY(length - pos < static_cast<int64_t>(get_serialize_size()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow",  K(length), K(pos), "need_size", get_serialize_size(), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, cmd_, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, username_, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    } else {
      if (mysql_cap_.cap_flags_.OB_CLIENT_SECURE_CONNECTION) {
        if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length,
                    static_cast<uint8_t>(auth_response_.length()), pos))) {
          LOG_WARN("fail to store auth response length", K(ret));
        } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buffer, length,
                           auth_response_.ptr(), auth_response_.length(), pos))) {
          LOG_WARN("fail to store auth response", K_(auth_response), K(ret));
        }
      } else {
        if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, auth_response_, pos))) {
          LOG_WARN("fail to store auth response", K_(auth_response), K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, database_, pos))) {
      LOG_WARN("fail to store database", K_(database), K(ret));
    }

    if (OB_SUCC(ret) && OB_FAIL(ObMySQLUtil::store_int2(buffer, length, character_set_, pos))) {
      LOG_WARN("fail to store charset", K_(character_set), K(ret));
    }

    if (OB_SUCC(ret)) {
      if (mysql_cap_.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
        if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, auth_plugin_name_, pos))) {
          LOG_WARN("fail to store auth_plugin_name", K_(auth_plugin_name), K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (mysql_cap_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
        uint64_t all_attrs_len = get_connect_attrs_len();
        if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, all_attrs_len, pos))) {
          LOG_WARN("fail to store all_attrs_len", K(all_attrs_len), K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i <  connect_attrs_.count(); ++i) {
            ret = ObMySQLPacket::store_string_kv(buffer, length, connect_attrs_.at(i), pos);
          }  // end store normal connect attrs

          if (OB_SUCC(ret)) {
            // store session vars
            if (sys_vars_.empty() && user_vars_.empty()) {
              // do nothing
            } else if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, ObString::make_string(OB_MYSQL_PROXY_SESSION_VARS), pos))) {
              LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
            } else if (OB_FAIL(serialize_session_vars(buffer, length, pos))) {
              LOG_WARN("fail to store session vars", K(ret), KP(buffer), K(length), K(pos));
            }
          }  // end store session vars
        }
      }
    }
  }
  return ret;
}

uint64_t OMPKChangeUser::get_session_vars_len() const
{
  uint64_t session_vars_len = 0;
  for (int64_t i = 0; i< sys_vars_.count(); ++i) {
    session_vars_len += ObMySQLPacket::get_kv_encode_len(sys_vars_.at(i));
  }
  if (!user_vars_.empty()) {
    session_vars_len += ObMySQLPacket::get_kv_encode_len(ObMySQLPacket::get_separator_kv());
    for (int64_t i = 0; i< user_vars_.count(); ++i) {
      session_vars_len += ObMySQLPacket::get_kv_encode_len(user_vars_.at(i));
    }
  }
  return session_vars_len;
}

uint64_t OMPKChangeUser::get_connect_attrs_len() const
{
  uint64_t all_attr_len = 0;
  ObStringKV string_kv;
  if (!!mysql_cap_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
    for (int64_t i = 0; i < connect_attrs_.count(); i++) {
      all_attr_len += ObMySQLPacket::get_kv_encode_len(connect_attrs_.at(i));
    }
    // store session vars as connect attrs
    if (!sys_vars_.empty() || !user_vars_.empty()) {
      int64_t len = STRLEN(OB_MYSQL_PROXY_SESSION_VARS);
      all_attr_len += ObMySQLUtil::get_number_store_len(len);
      all_attr_len += len;
      all_attr_len += ObMySQLUtil::get_number_store_len(get_session_vars_len());
      all_attr_len += get_session_vars_len();
    }
  }
  return all_attr_len;
}

int OMPKChangeUser::serialize_session_vars(char *buffer,
                                           const int64_t length,
                                           int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, get_session_vars_len(), pos))) {
    LOG_WARN("fail to store session vars len", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i< sys_vars_.count(); ++i) {
      ret = ObMySQLPacket::store_string_kv(buffer, length, sys_vars_.at(i), pos);
    }
    if (OB_SUCC(ret)) {
      if (!user_vars_.empty()) {
        ret = ObMySQLPacket::store_string_kv(buffer, length, ObMySQLPacket::get_separator_kv(), pos);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < user_vars_.count(); ++i) {
        ret = ObMySQLPacket::store_string_kv(buffer, length, user_vars_.at(i), pos);
      }
    }
  }
  return ret;
}
