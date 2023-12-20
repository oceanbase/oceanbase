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

#include "rpc/obmysql/packet/ompk_handshake_response.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

void OMPKHandshakeResponse::reset()
{
  capability_.capability_ = 0;
  max_packet_size_ = 0;
  character_set_ = 0;
  username_.reset();
  auth_response_.reset();
  database_.reset();
  auth_plugin_name_.reset();
  connect_attrs_.reset();
}

int OMPKHandshakeResponse::decode()
{
  int ret = OB_SUCCESS;
  const char *pos = cdata_;
  const int64_t len = hdr_.len_;
  const char *end = pos + len;

  //OB_ASSERT(NULL != cdata_);
  if (OB_ISNULL(cdata_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("null input", K(ret), KP(cdata_));
  } else if (OB_UNLIKELY(len < HANDSHAKE_RESPONSE_MIN_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error handshake response packet", K(len), K(ret));
  } else {
    capability_.capability_ = uint2korr(pos);
    if (!capability_.cap_flags_.OB_CLIENT_PROTOCOL_41) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ob only support mysql client protocol 4.1", K(ret));
    } else {
      ObMySQLUtil::get_uint4(pos, capability_.capability_);
      // When the driver establishes a connection, it decides whether to open the CLIENT_MULTI_RESULTS
      // capability according to the capability returned by the sever. Both mysql 5.6 and 8.0 versions
      // are opened by default, and this behavior is compatible here by default.
      capability_.cap_flags_.OB_CLIENT_MULTI_STATEMENTS = 1;
      ObMySQLUtil::get_uint4(pos, max_packet_size_); //16MB
      ObMySQLUtil::get_uint1(pos, character_set_);
      pos += HANDSHAKE_RESPONSE_RESERVED_SIZE;//23 bytes reserved
    }
  }

  // get username
  if (OB_SUCC(ret) && pos < end) {
    username_ = ObString::make_string(pos);
    pos += strlen(pos) + 1;
  }

  // get auth response
  if (OB_SUCC(ret) && pos < end) {
    if (capability_.cap_flags_.OB_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
      // lenenc-int  length of auth-response
      // string[n]   auth-response
      uint64_t auth_response_len = 0;
      ret = ObMySQLUtil::get_length(pos, auth_response_len);
      if (OB_SUCC(ret)) {
        auth_response_.assign_ptr(pos, static_cast<uint32_t>(auth_response_len));
        pos += auth_response_len;
      } else {
        LOG_WARN("fail to get len encode number", K(ret));
      }
    } else if (capability_.cap_flags_.OB_CLIENT_SECURE_CONNECTION) {
      // 1           length of auth-response
      // string[n]   auth-response
      uint8_t auth_response_len = 0;
      ObMySQLUtil::get_uint1(pos, auth_response_len);
      auth_response_.assign_ptr(pos, auth_response_len);
      pos += auth_response_len;
    } else {
      //string[NUL]    auth-response
      auth_response_ = ObString::make_string(pos);
      pos += strlen(pos) + 1;
    }
  }

  // get database name
  if (OB_SUCC(ret) && pos < end) {
    if (capability_.cap_flags_.OB_CLIENT_CONNECT_WITH_DB) {
      database_ = ObString::make_string(pos);
      pos += strlen(pos) + 1;
    }
  }

  // get auth plugin name
  if (OB_SUCC(ret) && pos < end) {
    if (capability_.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
      auth_plugin_name_ = ObString::make_string(pos);
      pos += strlen(pos) + 1;
    }
  }

  // get client connect attrbutes
  if (OB_SUCC(ret) && pos < end) {
    if (capability_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
      uint64_t all_attrs_len = 0;
      ret = ObMySQLUtil::get_length(pos, all_attrs_len);
      //OB_ASSERT(OB_SUCC(ret));
      if (OB_SUCC(ret)) {
        ObStringKV str_kv;
        while (all_attrs_len > 0 && OB_SUCC(ret) && pos < end) {
          // get key
          uint64_t key_inc_len = 0;
          uint64_t key_len = 0;
          ret = ObMySQLUtil::get_length(pos, key_len, key_inc_len);
          //OB_ASSERT(OB_SUCC(ret) && all_attrs_len > key_inc_len);
          if (OB_SUCC(ret) && all_attrs_len > key_inc_len && pos < end) {
            all_attrs_len -= key_inc_len;
            str_kv.key_.assign_ptr(pos, static_cast<int32_t>(key_len));
            //OB_ASSERT(all_attrs_len > key_len);
            if (all_attrs_len > key_len) {
              all_attrs_len -= key_len;
              if (end - pos > key_len) {
                  pos += key_len;
                  // get value
                  uint64_t value_inc_len = 0;
                  uint64_t value_len = 0;
                  ret = ObMySQLUtil::get_length(pos, value_len, value_inc_len);
                  //OB_ASSERT(OB_SUCC(ret) && all_attrs_len > value_inc_len);
                  if (OB_SUCC(ret) && all_attrs_len >= value_inc_len && pos <= end) {
                    all_attrs_len -= value_inc_len;
                    str_kv.value_.assign_ptr(pos, static_cast<int32_t>(value_len));
                    //OB_ASSERT(all_attrs_len >= value_len);
                    if (all_attrs_len >= value_len) {
                      all_attrs_len -= value_len;
                      if (end - pos >= value_len) {
                          pos += value_len;
                          if (OB_FAIL(connect_attrs_.push_back(str_kv))) {
                            LOG_WARN("fail to push back str_kv", K(str_kv), K(ret));
                          }
                      } else {
                        // skip error
                      }
                    } else {
                      // skip error
                    }
                  } else {
                    // skip error
                  }
              } else {
                // skip error
              }
            } else {
              // skip error
            }
          } else {
            // skip error
          }
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("get len fail", K(ret), KP(pos), K(all_attrs_len));
      }
    }
  }

  // MySQL doesn't care whether there's bytes remain, we do so.  JDBC
  // won't set OB_CLIENT_CONNECT_WITH_DB but leaves a '\0' in the db
  // field when database name isn't specified. It can confuse us if we
  // check whether there's bytes remain in packet. So we ignore it
  // the same as what MySQL does.

  return ret;
}

int64_t OMPKHandshakeResponse::get_serialize_size() const
{
  int64_t len = HANDSHAKE_RESPONSE_MIN_SIZE;
  len += username_.length() + 1; // username
  if ((capability_.cap_flags_.OB_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
    len += ObMySQLUtil::get_number_store_len(auth_response_.length());
    len += auth_response_.length();
  } else if (capability_.cap_flags_.OB_CLIENT_SECURE_CONNECTION ) {
    len += 1;
    len += auth_response_.length();
  } else {
    len += auth_response_.length() + 1;
  }
  if (capability_.cap_flags_.OB_CLIENT_CONNECT_WITH_DB) {
    len += database_.length() + 1;
  }
  if (capability_.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
    len += auth_plugin_name_.length() + 1;
  }
  if (capability_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
    uint64_t all_attr_len = get_connect_attrs_len();
    all_attr_len += ObMySQLUtil::get_number_store_len(all_attr_len);
    len += all_attr_len;
  }
  return len;
}

int OMPKHandshakeResponse::serialize(char *buffer, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_UNLIKELY(length - pos < static_cast<int64_t>(get_serialize_size()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow",  K(length), K(pos), "need_size", get_serialize_size(), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int4(buffer, length, capability_ .capability_, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int4(buffer, length, max_packet_size_, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, character_set_, pos))) {
      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
    } else {
      char reserved[HANDSHAKE_RESPONSE_RESERVED_SIZE];
      memset(reserved, 0, sizeof (reserved));
      if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buffer, length, reserved, sizeof (reserved), pos))) {
        LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, username_, pos))) {
          LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
        }
      }
      if (capability_.cap_flags_.OB_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) {
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, auth_response_, pos))) {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          }
        }
      } else if (capability_.cap_flags_.OB_CLIENT_SECURE_CONNECTION ) {
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length,
              static_cast<uint8_t>(auth_response_.length()), pos))) {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buffer, length,
              auth_response_.ptr(), auth_response_.length(), pos))) {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          }
        }
      } else {
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, auth_response_, pos))) {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          }
        }
      }
      if (capability_.cap_flags_.OB_CLIENT_CONNECT_WITH_DB) {
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, database_, pos))) {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          }
        }
      }
      if (capability_.cap_flags_.OB_CLIENT_PLUGIN_AUTH) {
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, length, auth_plugin_name_, pos)))  {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          }
        }
      }
      if (capability_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
        uint64_t all_attr_len = get_connect_attrs_len();
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, all_attr_len, pos))) {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          }
        }

        ObStringKV string_kv;
        for (int64_t i = 0; OB_SUCC(ret) && i <  connect_attrs_.count(); ++i) {
          string_kv = connect_attrs_.at(i);
          if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, string_kv.key_, pos))) {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          } else if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, string_kv.value_, pos))) {
            LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
          }
        }
      }
    }
  }
  return ret;

}

int OMPKHandshakeResponse::add_connect_attr(const ObStringKV &string_kv)
{
  int ret = OB_SUCCESS;
  if (string_kv.key_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(string_kv), K(ret));
  } else if (OB_FAIL(connect_attrs_.push_back(string_kv))) {
    LOG_WARN("fail to push back string kv", K(string_kv), K(ret));
  }
  return ret;
}

uint64_t OMPKHandshakeResponse::get_connect_attrs_len() const
{
  uint64_t all_attr_len = 0;
  ObStringKV string_kv;
  if (!!capability_.cap_flags_.OB_CLIENT_CONNECT_ATTRS) {
    for (int64_t i = 0; i < connect_attrs_.count(); i++) {
      string_kv = connect_attrs_.at(i);
      all_attr_len += ObMySQLUtil::get_number_store_len(string_kv.key_.length());
      all_attr_len += string_kv.key_.length();
      all_attr_len += ObMySQLUtil::get_number_store_len(string_kv.value_.length());
      all_attr_len += string_kv.value_.length();
    }
  }
  return all_attr_len;
}

bool OMPKHandshakeResponse::is_obproxy_client_mode() const
{
  bool obproxy_mod = false;
  ObStringKV mod_kv;
  mod_kv.key_.assign_ptr(OB_MYSQL_CLIENT_MODE, static_cast<int32_t>(strlen(OB_MYSQL_CLIENT_MODE)));
  mod_kv.value_.assign_ptr(OB_MYSQL_CLIENT_OBPROXY_MODE_NAME,
      static_cast<int32_t>(strlen(OB_MYSQL_CLIENT_OBPROXY_MODE_NAME)));

  ObStringKV kv;
  for (int64_t i = 0; !obproxy_mod && i < connect_attrs_.count(); ++i) {
    kv = connect_attrs_.at(i);
    if (mod_kv.key_ == kv.key_ && mod_kv.value_ == kv.value_) {
      obproxy_mod = true;
      //break;
    }
  }
  return obproxy_mod;
}

bool OMPKHandshakeResponse::is_java_client_mode() const
{
  bool java_client_mod = false;
  ObStringKV mod_kv;
  mod_kv.key_.assign_ptr(OB_MYSQL_CLIENT_MODE, static_cast<int32_t>(strlen(OB_MYSQL_CLIENT_MODE)));
  mod_kv.value_.assign_ptr(OB_MYSQL_JAVA_CLIENT_MODE_NAME,
      static_cast<int32_t>(strlen(OB_MYSQL_JAVA_CLIENT_MODE_NAME)));

  ObStringKV kv;
  for (int64_t i = 0; !java_client_mod && i < connect_attrs_.count(); ++i) {
    kv = connect_attrs_.at(i);
    if (mod_kv.key_ == kv.key_ && mod_kv.value_ == kv.value_) {
      java_client_mod = true;
      //break;
    }
  }
  return java_client_mod;
}

bool OMPKHandshakeResponse::is_ob_client_jdbc() const
{
  bool is_jdbc = false;
  ObStringKV mod_kv;
  mod_kv.key_.assign_ptr(OB_MYSQL_CLIENT_NAME, static_cast<int32_t>(strlen(OB_MYSQL_CLIENT_NAME)));
  mod_kv.value_.assign_ptr(OB_MYSQL_JDBC_CLIENT_NAME,
      static_cast<int32_t>(strlen(OB_MYSQL_JDBC_CLIENT_NAME)));

  ObStringKV kv;
  for (int64_t i = 0; !is_jdbc && i < connect_attrs_.count(); ++i) {
    kv = connect_attrs_.at(i);
    if (mod_kv.key_ == kv.key_ && mod_kv.value_ == kv.value_) {
      is_jdbc = true;
      //break;
    }
  }
  return is_jdbc;
}

bool OMPKHandshakeResponse::is_ob_client_oci() const
{
  bool is_oci = false;
  ObStringKV mod_kv;
  mod_kv.key_.assign_ptr(OB_MYSQL_CLIENT_NAME, static_cast<int32_t>(strlen(OB_MYSQL_CLIENT_NAME)));
  mod_kv.value_.assign_ptr(OB_MYSQL_OCI_CLIENT_NAME,
      static_cast<int32_t>(strlen(OB_MYSQL_OCI_CLIENT_NAME)));

  ObStringKV kv;
  for (int64_t i = 0; !is_oci && i < connect_attrs_.count(); ++i) {
    kv = connect_attrs_.at(i);
    if (mod_kv.key_ == kv.key_ && mod_kv.value_ == kv.value_) {
      is_oci = true;
      //break;
    }
  }
  return is_oci;
}

int64_t OMPKHandshakeResponse::get_sql_request_level() const
{
  int64_t sql_req_level = 0; // share::OBCG_DEFAULT
  ObString sql_req_level_key;
  ObString sql_req_l0;
  ObString sql_req_l1;
  ObString sql_req_l2;
  ObString sql_req_l3;
  sql_req_level_key.assign_ptr(OB_SQL_REQUEST_LEVEL, static_cast<int32_t>(strlen(OB_SQL_REQUEST_LEVEL)));
  sql_req_l0.assign_ptr(OB_SQL_REQUEST_LEVEL0, static_cast<int32_t>(strlen(OB_SQL_REQUEST_LEVEL0)));
  sql_req_l1.assign_ptr(OB_SQL_REQUEST_LEVEL1, static_cast<int32_t>(strlen(OB_SQL_REQUEST_LEVEL1)));
  sql_req_l2.assign_ptr(OB_SQL_REQUEST_LEVEL2, static_cast<int32_t>(strlen(OB_SQL_REQUEST_LEVEL2)));
  sql_req_l3.assign_ptr(OB_SQL_REQUEST_LEVEL3, static_cast<int32_t>(strlen(OB_SQL_REQUEST_LEVEL3)));
  ObStringKV kv;
  for (int64_t i = 0; i < connect_attrs_.count(); ++i) {
    kv = connect_attrs_.at(i);
    if (sql_req_level_key == kv.key_) {
      if (sql_req_l1 == kv.value_) {
        sql_req_level = 1;
      } else if (sql_req_l2 == kv.value_) {
        sql_req_level = 2;
      } else if (sql_req_l3 == kv.value_) {
        sql_req_level = 3;
      }
      break;
    }
  }
  return sql_req_level;
}

bool OMPKHandshakeResponse::is_oci_client_mode() const
{
  bool oci_client_mod = false;
  ObStringKV mod_kv;
  mod_kv.key_.assign_ptr(OB_MYSQL_CLIENT_MODE, static_cast<int32_t>(strlen(OB_MYSQL_CLIENT_MODE)));
  mod_kv.value_.assign_ptr(OB_MYSQL_OCI_CLIENT_MODE_NAME,
      static_cast<int32_t>(strlen(OB_MYSQL_OCI_CLIENT_MODE_NAME)));

  ObStringKV kv;
  for (int64_t i = 0; !oci_client_mod && i < connect_attrs_.count(); ++i) {
    kv = connect_attrs_.at(i);
    if (mod_kv.key_ == kv.key_ && mod_kv.value_ == kv.value_) {
      oci_client_mod = true;
      //break;
    }
  }
  return oci_client_mod;
}

bool OMPKHandshakeResponse::is_jdbc_client_mode() const
{
  bool jdbc_client_mod = false;
  ObStringKV mod_kv;
  mod_kv.key_.assign_ptr(OB_MYSQL_CLIENT_MODE, static_cast<int32_t>(strlen(OB_MYSQL_CLIENT_MODE)));
  mod_kv.value_.assign_ptr(OB_MYSQL_JDBC_CLIENT_MODE_NAME,
      static_cast<int32_t>(strlen(OB_MYSQL_JDBC_CLIENT_MODE_NAME)));

  ObStringKV kv;
  for (int64_t i = 0; !jdbc_client_mod && i < connect_attrs_.count(); ++i) {
    kv = connect_attrs_.at(i);
    if (mod_kv.key_ == kv.key_ && mod_kv.value_ == kv.value_) {
      jdbc_client_mod = true;
      //break;
    }
  }
  return jdbc_client_mod;
}

