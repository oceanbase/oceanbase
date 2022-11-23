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

#include "rpc/obmysql/packet/ompk_handshake.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

//FIXME::here we use hard code to avoid security flaw from AliYun. In fact, ob do not use any mysql code
const char *OMPKHandshake::SERVER_VERSION_STR = "5.7.25";
const char *OMPKHandshake::AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD = "mysql_native_password";
const char *OMPKHandshake::AUTH_PLUGIN_MYSQL_OLD_PASSWORD = "mysql_old_password";
const char *OMPKHandshake::AUTH_PLUGIN_MYSQL_CLEAR_PASSWORD = "mysql_clear_password";
const char *OMPKHandshake::AUTH_PLUGIN_AUTHENTICATION_WINDOWS_CLIENT = "authentication_windows_client";

OMPKHandshake::OMPKHandshake()
    : auth_plugin_data2_(),
      terminated_(0)
{
  protocol_version_ = 10; // Protocol::HandshakeV10
  server_version_ = ObString::make_string(SERVER_VERSION_STR);
  thread_id_ = 1;
  memset(scramble_buff_, 'a', 8);
  filler_ = 0;
  //0xF7DF, a combination of multiple flags, in which the flag supporting the 4.1 protocol is set to 1,
  server_capabilities_lower_.capability_ = 0;
  server_capabilities_lower_.capability_flag_.OB_SERVER_LONG_PASSWORD = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_FOUND_ROWS = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_LONG_FLAG = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_CONNECT_WITH_DB = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_NO_SCHEMA = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_CAN_USE_COMPRESS = 0;
  server_capabilities_lower_.capability_flag_.OB_SERVER_ODBC = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_LOCAL_FILES = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_IGNORE_SPACE = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_PROTOCOL_41 = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_INTERACTIVE = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_SSL = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_IGNORE_SIGPIPE = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_TRANSACTIONS = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_RESERVED = 1;
  server_capabilities_lower_.capability_flag_.OB_SERVER_SECURE_CONNECTION = 1;

  server_capabilities_upper_.capability_ = 0;
  server_capabilities_upper_.capability_flag_.OB_SERVER_SESSION_VARIABLE_TRACK = 1;
  server_capabilities_upper_.capability_flag_.OB_SERVER_PLUGIN_AUTH = 1;
  server_capabilities_upper_.capability_flag_.OB_SERVER_CONNECT_ATTRS = 1;
  server_capabilities_upper_.capability_flag_.OB_SERVER_USE_LOB_LOCATOR = 1;
  server_capabilities_upper_.capability_flag_.OB_SERVER_RETURN_HIDDEN_ROWID = 1;
  server_capabilities_upper_.capability_flag_.OB_SERVER_PS_MULTIPLE_RESULTS = 1;

  if (server_capabilities_upper_.capability_flag_.OB_SERVER_PLUGIN_AUTH != 0) {
    auth_plugin_name_ = AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD;
    auth_plugin_data_len_ = DEFAULT_AUTH_PLUGIN_DATA_LEN;
  } else {
    auth_plugin_name_  = NULL;
    auth_plugin_data_len_ = 0;
  }

  server_language_ = 46;  // utf8mb4_bin
  server_status_ = 0;     // no this value in mysql protocol document
  memset(reserved_, 0, sizeof(reserved_));
  memset(auth_plugin_data2_, 'b', sizeof(auth_plugin_data2_) - 1);
  auth_plugin_data2_[sizeof(auth_plugin_data2_) - 1] = '\0';
}

OMPKHandshake::~OMPKHandshake()
{
}

//seq of handshake is 0
int OMPKHandshake::serialize(char *buffer, int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (NULL == buffer || 0 >= len || pos < 0 || len - pos < get_serialize_size()) {
    LOG_WARN("invalid argument", KP(buffer), K(len), K(pos), "need_size", get_serialize_size());
    ret = OB_INVALID_ARGUMENT;
  } else {
    // buffer is definitely enough
    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, len, protocol_version_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(len), K(protocol_version_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, len, server_version_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(len), K(server_version_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int4(buffer, len, thread_id_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(len), K(thread_id_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buffer, len, scramble_buff_, SCRAMBLE_SIZE, pos))) {
      LOG_WARN("store fail", KP(buffer), K(len), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, len, filler_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(filler_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(buffer, len, server_capabilities_lower_.capability_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(server_capabilities_lower_.capability_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, len, server_language_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(server_language_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(buffer, len, server_status_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(server_status_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int2(buffer, len, server_capabilities_upper_.capability_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(server_capabilities_upper_.capability_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(buffer, len, auth_plugin_data_len_, pos))) {
      LOG_WARN("store fail", KP(buffer), K(auth_plugin_data_len_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buffer, len, reserved_, sizeof (reserved_), pos))) {
      LOG_WARN("store fail", KP(buffer), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_str_vnzt(buffer, len, auth_plugin_data2_, sizeof (auth_plugin_data2_), pos))) {
      LOG_WARN("store fail", KP(buffer), K(pos));
    }

    if (OB_SUCC(ret)) {
      if (server_capabilities_upper_.capability_flag_.OB_SERVER_PLUGIN_AUTH) {
        // auth-plugin name, NULL terminated
        if (OB_FAIL(ObMySQLUtil::store_obstr_zt(buffer, len, auth_plugin_name_, pos))) {
          LOG_WARN("store fail", KP(buffer), K(pos));
        }
      }
    }
  }

  return ret;
}

int OMPKHandshake::set_server_version(ObString &version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(version.ptr()) || version.length() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid args", KP(version.ptr()), K(version.length()));
  } else {
    //OB_ASSERT(version.ptr() && version.length() >= 0);
    server_version_ = version;
  }
  return ret;
}

int64_t OMPKHandshake::get_serialize_size() const
{
  int64_t len =
      1                           // protocol version
      + server_version_.length() + 1 // server_version [NULL terminated]
      + 4                         // thread id
      + 8                         // auth plugin data part 1
      + 1                         // filler
      + 2                         // lower capability flags
      + 1                         // character set
      + 2                         // status flags
      + 2                         // upper capability flags
      + 1                         // auth plugin data len
      + 10                        // zero reserved
      + 13                        // auth plugin data part 2
      ;

  if (!!server_capabilities_upper_.capability_flag_.OB_SERVER_PLUGIN_AUTH) {
    len += strlen(auth_plugin_name_);
    len += 1;  // auth plugin name [NULL terminated]
  } else {
    len += 0;
  }

  return len;
}

int OMPKHandshake::decode()
{
  int ret = OB_SUCCESS;
  const char *buf = cdata_;
  const char *pos = cdata_;
  const int64_t len = hdr_.len_;
  const char *end = buf + len;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("null input", KP(buf), K(len), K(ret));
  } else {
    ObMySQLUtil::get_uint1(pos, protocol_version_);

    int64_t sv_len = strlen(pos);
    server_version_.assign_ptr(SERVER_VERSION_STR,
        static_cast<ObString::obstr_size_t>(strlen(SERVER_VERSION_STR)));
    pos += sv_len + 1;

    ObMySQLUtil::get_uint4(pos, thread_id_);

    MEMCPY(scramble_buff_, pos, 8);
    pos += 8;

    ObMySQLUtil::get_uint1(pos, filler_);

    uint16_t server_capabilities_lower = 0;
    ObMySQLUtil::get_uint2(pos, server_capabilities_lower);
    server_capabilities_lower_.capability_ = server_capabilities_lower;

    ObMySQLUtil::get_uint1(pos, server_language_);

    ObMySQLUtil::get_uint2(pos, server_status_);

    uint16_t server_capabilities_upper = 0;
    ObMySQLUtil::get_uint2(pos, server_capabilities_upper);
    server_capabilities_upper_.capability_ =  server_capabilities_upper;

    ObMySQLUtil::get_uint1(pos, auth_plugin_data_len_);

    MEMSET(reserved_, 0, 10);
    pos += 10;

    if (server_capabilities_lower_.capability_flag_.OB_SERVER_SECURE_CONNECTION) {
      int64_t tmp_len = std::max(13, auth_plugin_data_len_ - 8);
      MEMCPY(auth_plugin_data2_, pos, tmp_len);
      pos += tmp_len;
    }

    if (server_capabilities_upper_.capability_flag_.OB_SERVER_PLUGIN_AUTH) {
      int64_t auth_plugin_name_len = strlen(pos);  // NULL ternamite
      ObString name(auth_plugin_name_len, pos);
      auth_plugin_name_ = get_handshake_inner_pulgin_name(name);
      pos += (auth_plugin_name_len + 1);
    } else {
      // If client or server do not support pluggable authentication (CLIENT_PLUGIN_AUTH
      // capability flag is not set) then the authentication method used is inferred from
      // client and server capabilities as follows:
      // 1. The method used is Old Password Authentication if CLIENT_PROTOCOL_41 or
      //      CLIENT_SECURE_CONNECTION are not set.
      // 2. The method used is Secure Password Authentication if both CLIENT_PROTOCOL_41
      //    and CLIENT_SECURE_CONNECTION are set but CLIENT_PLUGIN_AUTH is not set.
      if (server_capabilities_lower_.capability_flag_.OB_SERVER_SECURE_CONNECTION
          && server_capabilities_lower_.capability_flag_.OB_SERVER_PROTOCOL_41) {
        auth_plugin_name_ = AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD;
      } else {
        auth_plugin_name_ = AUTH_PLUGIN_MYSQL_OLD_PASSWORD;
      }
    }


    if (OB_SUCC(ret)) {
      if (pos > end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pos must less than end", KP(pos), KP(end), K(ret));
      }
    }
  }

  return ret;
}

const char *OMPKHandshake::get_handshake_inner_pulgin_name(const ObString outer_string) const
{
  const char *name = NULL;
  if (!!outer_string.case_compare(AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD)) {
    name = AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD;
  } else if (!!outer_string.case_compare(AUTH_PLUGIN_MYSQL_OLD_PASSWORD)) {
    name = AUTH_PLUGIN_MYSQL_OLD_PASSWORD;
  } else if (!!outer_string.case_compare(AUTH_PLUGIN_MYSQL_CLEAR_PASSWORD)) {
    name = AUTH_PLUGIN_MYSQL_CLEAR_PASSWORD;
  } else if (!!outer_string.case_compare(AUTH_PLUGIN_AUTHENTICATION_WINDOWS_CLIENT)) {
    name = AUTH_PLUGIN_AUTHENTICATION_WINDOWS_CLIENT;
  } else {
    return NULL;
  }

  return name;
}

int OMPKHandshake::get_scramble(char *buffer, const int64_t len, int64_t &copy_len)
{
  int ret = OB_SUCCESS;
  int64_t part1_len = sizeof(scramble_buff_);
  int64_t part2_len = (sizeof (auth_plugin_data2_) - 1);
  if (OB_ISNULL(buffer) || OB_UNLIKELY(len < (part1_len + part2_len))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", KP(buffer), K(len), K(part1_len), K(part2_len), K(ret));
  } else {
    int64_t pos = 0;
    MEMCPY(buffer, scramble_buff_, part1_len);
    pos += part1_len;

    MEMCPY(buffer + pos, auth_plugin_data2_, part2_len);
    pos += part2_len;

    copy_len = pos;
  }

  return ret;
}

int OMPKHandshake::set_scramble(char *buffer, const int64_t len)
{
  int ret = OB_SUCCESS;
  int64_t part1_len = sizeof(scramble_buff_);
  int64_t part2_len = (sizeof (auth_plugin_data2_) - 1);
  if (OB_ISNULL(buffer) || OB_UNLIKELY(len < (part1_len + part2_len))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", KP(buffer), K(len), K(part1_len), K(part2_len), K(ret));
  } else {
    int64_t pos = 0;
    MEMCPY(scramble_buff_, buffer, part1_len);
    pos += part1_len;

    MEMCPY(auth_plugin_data2_, buffer + pos, part2_len);
    pos += part2_len;
  }
  return ret;
}
