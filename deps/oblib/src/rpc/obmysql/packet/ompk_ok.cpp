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

#include "rpc/obmysql/packet/ompk_ok.h"

#include "lib/utility/ob_macro_utils.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

OMPKOK::OMPKOK()
    : field_count_(0x00),
      affected_rows_(0),
      last_insert_id_(0),
      server_status_(0x22),
      warnings_(0),
      message_(),
      changed_schema_(),
      state_changed_(false),
      system_vars_(),
      user_vars_(),
      capability_(),
      is_schema_changed_(false),
      use_standard_serialize_(false)
{
}

int OMPKOK::set_message(const ObString &message)
{
  int ret = OB_SUCCESS;
  if (!message.empty()) {
    message_ = message;
  }
  return ret;
}

void OMPKOK::set_state_changed(const bool state_changed)
{
  state_changed_ = state_changed;
  // If the CLIENT_SESSION_TRACK capability is not enabled
  // the Server should not set the SERVER_SESSION_STATE_CHANGED Flag
  if (capability_.cap_flags_.OB_CLIENT_SESSION_TRACK) {
    server_status_.status_flags_.OB_SERVER_SESSION_STATE_CHANGED = 1;
  }
}

void OMPKOK::set_changed_schema(const common::ObString &schema)
{
  changed_schema_ = schema;
  is_schema_changed_ = true;
}

void OMPKOK::set_use_standard_serialize(const bool value)
{
  use_standard_serialize_ = value;
}

int OMPKOK::decode()
{
  int ret = OB_SUCCESS;
  const char *buf = cdata_;
  const char *pos = cdata_;
  const int64_t len = hdr_.len_;
  const char *end = buf + len;
  //OB_ASSERT(NULL != cdata_);
  if (NULL != cdata_) {
    ObMySQLUtil::get_uint1(pos, field_count_);
    if (OB_FAIL(ObMySQLUtil::get_length(pos, affected_rows_))) {
      LOG_WARN("get len fail", KP(pos));
    } else if (OB_FAIL(ObMySQLUtil::get_length(pos, last_insert_id_))) {
      LOG_WARN("get len fail", KP(pos));
    }
    if (OB_SUCC(ret) && pos < end) {
      if (capability_.cap_flags_.OB_CLIENT_PROTOCOL_41) {
        ObMySQLUtil::get_uint2(pos, server_status_.flags_);
        ObMySQLUtil::get_uint2(pos, warnings_);
      } else {
        ObMySQLUtil::get_uint2(pos, server_status_.flags_);
      }
    }
    if (OB_SUCC(ret) && pos < end) {
      if (capability_.cap_flags_.OB_CLIENT_SESSION_TRACK) {
        uint64_t info_len = 0;
        if (OB_FAIL(ObMySQLUtil::get_length(pos, info_len))) {
          LOG_WARN("fail to get len encode number", K(ret));
        } else {
          message_.assign_ptr(pos, static_cast<uint32_t>(info_len));
          pos += info_len;
        }
        if (server_status_.status_flags_.OB_SERVER_SESSION_STATE_CHANGED) {
          if (OB_FAIL(decode_session_state_info(pos))) {
            LOG_WARN("fail to decode session state info", K(ret));
          }
        }
      } else {
        message_.assign_ptr(pos, static_cast<uint32_t>(end - pos));
        pos = end;
      }
    }
    if (pos != end) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("pos not equals end", K(ret), KP(pos), KP(end)); //OB_ASSERT(pos == end);
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("null input", K(ret), KP(cdata_));
  }
  return ret;
}

int OMPKOK::decode_session_state_info(const char *&pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalie input value", KP(pos), K(ret));
  } else {
    uint64_t session_info_len = 0;
    if (OB_FAIL(ObMySQLUtil::get_length(pos, session_info_len))) {
      LOG_WARN("fail to get len encode number", K(ret));
    } else if (session_info_len > 0) {
      const char *end = pos + session_info_len;
      ObStringKV tmp_kv;
      while (pos < end && OB_SUCC(ret)) {
        uint8_t type = 0;
        ObMySQLUtil::get_uint1(pos, type);
        uint64_t filed_len = 0;
        ret = ObMySQLUtil::get_length(pos, filed_len);
        if (OB_SUCC(ret)) {
          if (SESSION_TRACK_SYSTEM_VARIABLES == type) {
            const char *sys_var_end = pos + filed_len;
            bool found_separator = false;
            while (pos < sys_var_end && OB_SUCC(ret)) {
              uint64_t name_len = 0;
              ret = ObMySQLUtil::get_length(pos, name_len);
              if (OB_SUCC(ret)) {
                tmp_kv.key_.assign_ptr(pos, static_cast<uint32_t>(name_len));
                pos += name_len;

                uint64_t value_len = 0;
                ret = ObMySQLUtil::get_length(pos, value_len);
                tmp_kv.value_.assign_ptr(pos, static_cast<uint32_t>(value_len));
                pos += value_len;

                if (OB_SUCC(ret)) {
                  if (tmp_kv.key_ == get_separator_kv().key_
                      && tmp_kv.value_ == get_separator_kv().value_) {
                    found_separator = true;
                    //continue;
                  } else {
                    if (found_separator) {
                      if (OB_FAIL(user_vars_.push_back(tmp_kv))) {
                        LOG_WARN("fail to push back user_vars", K(tmp_kv), K(ret));
                      }
                    } else {
                      if (OB_FAIL(system_vars_.push_back(tmp_kv))) {
                        LOG_WARN("fail to push back system_vars", K(tmp_kv), K(ret));
                      }
                    }
                  }
                }
              }
            }
          } else if (SESSION_TRACK_SCHEMA == type) {
            uint64_t schema_len = 0;
            ret = ObMySQLUtil::get_length(pos, schema_len);
            if (schema_len > 0) {
              changed_schema_.assign_ptr(pos, static_cast<uint32_t>(schema_len));
              pos += schema_len;
            }
            is_schema_changed_ = true;
          } else if (SESSION_TRACK_STATE_CHANGE == type) {
            uint64_t state_len = 0;
            ret = ObMySQLUtil::get_length(pos, state_len);
            if (OB_SUCC(ret)) {
              if (1 == state_len) {
                if ('1' == *pos) {
                  state_changed_ = true;
                }
                pos += 1;
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_ERROR("state_len not equal 1", K(ret), K(state_len));//OB_ASSERT(1 == state_len);
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unrecognized type", K(type), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int64_t OMPKOK::get_serialize_size() const
{
  int64_t len = 0;
  len += 1; // header
  len += ObMySQLUtil::get_number_store_len(affected_rows_);
  len += ObMySQLUtil::get_number_store_len(last_insert_id_);
  if (!!capability_.cap_flags_.OB_CLIENT_PROTOCOL_41) {
    len += 2; // status flag
    len += 2; // warnings
  } else if (!!capability_.cap_flags_.OB_CLIENT_TRANSACTIONS) {
    len += 2; // status flag
  }
  if (!!capability_.cap_flags_.OB_CLIENT_SESSION_TRACK) {
    if (use_standard_serialize_) {
      if (!message_.empty() || !!server_status_.status_flags_.OB_SERVER_SESSION_STATE_CHANGED) {
        len += ObMySQLUtil::get_number_store_len(message_.length());
        len += message_.length();
      }
    } else {
      int32_t tmp_length = message_.length();
      if (!message_.empty()) {
        tmp_length++;
      }
      len += ObMySQLUtil::get_number_store_len(tmp_length);
      len += tmp_length;
    }
    if (!!server_status_.status_flags_.OB_SERVER_SESSION_STATE_CHANGED) {
      const int64_t tmp_len = get_state_info_len();
      len += ObMySQLUtil::get_number_store_len(tmp_len);
      len += tmp_len;
    }
  } else {
    if (!message_.empty()) {
      if (use_standard_serialize_) {
        len += ObMySQLUtil::get_number_store_len(message_.length());
        len += message_.length();
      } else {
        len += (message_.length() + 1);
      }
    }
  }
  return len;
}

int OMPKOK::serialize(char *buffer, const int64_t length, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t orig_pos = pos;
  if (OB_ISNULL(buffer) || OB_UNLIKELY(length - pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos), K(ret));
  } else if (OB_UNLIKELY(length - pos < static_cast<int64_t>(get_serialize_size()))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow",  K(length), K(pos), "need_size", get_serialize_size(), K(ret));
  } else if (0 == capability_.capability_) {
    ret = OB_ITEM_NOT_SETTED;
    LOG_WARN("capability is not set", K_(capability_.capability), K(ret));
  } else {
    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, field_count_, pos))) {
      LOG_WARN("store int fail", KP(buffer), K(length), K(field_count_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, affected_rows_, pos))) {
      LOG_WARN("store int fail", KP(buffer), K(length), K(affected_rows_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, last_insert_id_, pos))) {
      LOG_WARN("store int fail", KP(buffer), K(length), K(last_insert_id_), K(pos));
    }
    if (OB_SUCC(ret)) {
      if (capability_.cap_flags_.OB_CLIENT_PROTOCOL_41) {
        if (OB_FAIL(ObMySQLUtil::store_int2(buffer, length, server_status_.flags_, pos))) {
          LOG_WARN("store int fail", KP(buffer), K(length), K(server_status_.flags_), K(pos));
        } else if (OB_FAIL(ObMySQLUtil::store_int2(buffer, length, warnings_, pos))) {
          LOG_WARN("store int fail", KP(buffer), K(length), K(warnings_), K(pos));
        }
      } else {
        ret = ObMySQLUtil::store_int2(buffer, length, server_status_.flags_, pos);
      }
    }
    if (OB_SUCC(ret)) {
      if (capability_.cap_flags_.OB_CLIENT_SESSION_TRACK) {
        if (use_standard_serialize_) {
          if (!message_.empty() || server_status_.status_flags_.OB_SERVER_SESSION_STATE_CHANGED) {
            if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, message_, pos))) {
              LOG_WARN("store str failed", KP(buffer), K(length), K(message_), K(pos));
            }
          }
        } else {
          if (OB_FAIL(ObMySQLUtil::store_obstr_with_pre_space(buffer, length, message_, pos))) {
            LOG_WARN("store str failed", KP(buffer), K(length), K(message_), K(pos));
          }
        }
        if (OB_SUCC(ret)) {
          if (server_status_.status_flags_.OB_SERVER_SESSION_STATE_CHANGED) {
            uint64_t all_len = get_state_info_len();
            ret = ObMySQLUtil::store_length(buffer, length, all_len, pos);
            if (OB_SUCC(ret)) {
              if (system_vars_.count() > 0 || user_vars_.count() > 0) {
                if (use_standard_serialize_) {
                  ObStringKV string_kv;
                  for (int64_t i = 0; OB_SUCC(ret) && i < system_vars_.count(); ++i) {
                    if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, SESSION_TRACK_SYSTEM_VARIABLES, pos))) {
                      LOG_WARN("store int fail", KP(buffer), K(length), K(pos), K(ret));
                    } else if (FALSE_IT(string_kv = system_vars_.at(i))) {
                    } else if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, get_kv_encode_len(string_kv), pos))) {
                      LOG_WARN("store_length fail", K(length), K(pos), K(string_kv), K(ret));
                    } else if (OB_FAIL(serialize_string_kv(buffer, length, pos, string_kv))) {
                      LOG_WARN("store_length fail", K(length), K(pos), K(string_kv), K(ret));
                    }
                  }
                } else {
                  ret = ObMySQLUtil::store_int1(buffer, length, SESSION_TRACK_SYSTEM_VARIABLES, pos);
                  if (OB_SUCC(ret)) {
                    uint64_t sys_var_len = get_track_system_vars_len();
                    ret = ObMySQLUtil::store_length(buffer, length, sys_var_len, pos);
                    ObStringKV string_kv;
                    for (int64_t i = 0; OB_SUCC(ret) && i < system_vars_.count(); ++i) {
                      string_kv = system_vars_.at(i);
                      ret = serialize_string_kv(buffer, length, pos, string_kv);
                    }
                    if (OB_SUCC(ret)) {
                      if (user_vars_.count() > 0) {
                        ret = serialize_string_kv(buffer, length, pos, get_separator_kv());
                      }
                      for (int64_t i = 0; OB_SUCC(ret) && i < user_vars_.count(); ++i) {
                        string_kv = user_vars_.at(i);
                        ret = serialize_string_kv(buffer, length, pos, string_kv);
                      }
                    }
                  }
                }
              }
              if (OB_SUCC(ret)) {
                if (is_schema_changed_) {
                  if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, SESSION_TRACK_SCHEMA, pos))) {
                    LOG_WARN("store int fail", KP(buffer), K(length), K(pos), K(ret));
                  } else {
                    uint64_t schema_len = 0;
                    schema_len += ObMySQLUtil::get_number_store_len(changed_schema_.length());
                    schema_len += changed_schema_.length();
                    if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, schema_len, pos))) {
                      LOG_WARN("store length fail", KP(buffer), K(length), K(schema_len), K(pos), K(ret));
                    } else if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, changed_schema_, pos))) {
                      LOG_WARN("store obstr fail", KP(buffer), K(length), K(changed_schema_), K(pos), K(ret));
                    }
                  }
                }
              }
              if (OB_SUCC(ret)) {
                if (state_changed_) {
                  if (OB_FAIL(ObMySQLUtil::store_int1(buffer, length, SESSION_TRACK_STATE_CHANGE, pos))) {
                    LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
                  } else {
                    ObString state_changed_str = ObString::make_string("1");
                    if (OB_FAIL(ObMySQLUtil::store_length(buffer, length, 2, pos))) {
                      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
                    } else if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, state_changed_str, pos))) {
                      LOG_WARN("store fail", K(ret), KP(buffer), K(length), K(pos));
                    }
                  }
                }
              }
            }
          }
        }
      } else {
        if (!message_.empty()) {
          if (use_standard_serialize_) {
            ret = ObMySQLUtil::store_obstr(buffer, length, message_, pos);
          } else {
            ret = ObMySQLUtil::store_obstr_nzt_with_pre_space(buffer, length, message_, pos);
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(pos - orig_pos != get_serialize_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("serialize", K(ret),"result_len", pos - orig_pos, "calc_len", get_serialize_size());
    } else {
      ObString tmp_str(pos - orig_pos, buffer + orig_pos);
      LOG_DEBUG("serialize", K(ret),"result_len", pos - orig_pos, "calc_len", get_serialize_size(), K(message_), K(tmp_str), KPC(this));
    }
  }
  return ret;
}

int OMPKOK::add_system_var(const ObStringKV &system_var)
{
  int ret = OB_SUCCESS;
  if (system_var.key_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(system_var), K(ret));
  } else if (OB_FAIL(system_vars_.push_back(system_var))) {
    LOG_WARN("fail to push back system variable", K(system_var), K(ret));
  }
  return ret;
}

int OMPKOK::add_user_var(const ObStringKV &user_var)
{
  int ret = OB_SUCCESS;
  if (user_var.key_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input value", K(user_var), K(ret));
  } else if (OB_FAIL(user_vars_.push_back(user_var))) {
    LOG_WARN("fail to push back user variable", K(user_var), K(ret));
  }
  return ret;
}

uint64_t OMPKOK::get_state_info_len() const
{
  uint64_t all_info_len = 0;
  if (system_vars_.count() > 0 || user_vars_.count() > 0) {
    if (use_standard_serialize_) {
      all_info_len += get_standard_track_system_vars_len();
    } else {
      all_info_len += 1; // type
      uint64_t sys_vars_len = get_track_system_vars_len();
      all_info_len += ObMySQLUtil::get_number_store_len(sys_vars_len);
      all_info_len += sys_vars_len;
    }
  }

  if (is_schema_changed_) {
    all_info_len += 1; // type
    uint64_t schema_len = 0;
    schema_len += ObMySQLUtil::get_number_store_len(changed_schema_.length());
    schema_len += changed_schema_.length();
    all_info_len += ObMySQLUtil::get_number_store_len(schema_len);
    all_info_len += schema_len;
  }

  if (state_changed_) {
    all_info_len += 1;  // type
    uint64_t state_changed_len = 0;
    state_changed_len += ObMySQLUtil::get_number_store_len(1);
    state_changed_len += 1;
    all_info_len += ObMySQLUtil::get_number_store_len(state_changed_len);
    all_info_len += state_changed_len;
  }

  return all_info_len;
}

uint64_t OMPKOK::get_track_system_vars_len() const
{
  uint64_t system_vars_len = 0;
  ObStringKV string_kv;
  if (!system_vars_.empty()) {
    for (int64_t i = 0; i < system_vars_.count(); ++i) {
      string_kv = system_vars_.at(i);
      system_vars_len += get_kv_encode_len(string_kv);
    }
  }
  if (!user_vars_.empty()) {
    system_vars_len += get_kv_encode_len(get_separator_kv());
    for (int64_t i = 0; i < user_vars_.count(); ++i) {
      string_kv = user_vars_.at(i);
      system_vars_len += get_kv_encode_len(string_kv);
    }
  }
  return system_vars_len;
}

uint64_t OMPKOK::get_standard_track_system_vars_len() const
{
  uint64_t system_vars_len = 0;
  ObStringKV string_kv;
  if (!system_vars_.empty()) {
    for (int64_t i = 0; i < system_vars_.count(); ++i) {
      string_kv = system_vars_.at(i);
      system_vars_len += 1; // type

      //total len
      const uint64_t kv_encode_len = get_kv_encode_len(string_kv);
      system_vars_len += ObMySQLUtil::get_number_store_len(kv_encode_len);

      //kv
      system_vars_len += kv_encode_len;
    }
  }
  return system_vars_len;
}

uint64_t OMPKOK::get_kv_encode_len(const ObStringKV &string_kv)
{
  uint64_t len = 0;
  len += ObMySQLUtil::get_number_store_len(string_kv.key_.length());
  len += string_kv.key_.length();
  len += ObMySQLUtil::get_number_store_len(string_kv.value_.length());
  len += string_kv.value_.length();
  return len;
}

int OMPKOK::serialize_string_kv(char *buffer, const int64_t length,
                                int64_t &pos, const ObStringKV &string_kv) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buffer || length <=0 || pos < 0)) {
    LOG_WARN("invalid argument", KP(buffer), K(length), K(pos));
  } else {
    if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, string_kv.key_, pos))) {
      LOG_WARN("store obstr fail", KP(buffer), K(length), K(string_kv.key_), K(pos));
    } else if (OB_FAIL(ObMySQLUtil::store_obstr(buffer, length, string_kv.value_, pos))) {
      LOG_WARN("store obstr fail", KP(buffer), K(length), K(string_kv.key_), K(pos));
    }
  }
  return ret;
}

ObStringKV OMPKOK::get_separator_kv()
{
  static ObStringKV separator_kv;
  separator_kv.key_ = ObString::make_string("__NULL");
  separator_kv.value_ = ObString::make_string("__NULL");
  return separator_kv;
}

int64_t OMPKOK::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV("header", hdr_,
       K_(field_count),
       K_(affected_rows),
       K_(last_insert_id),
       K_(server_status_.flags),
       K_(warnings),
       K_(message),
       K_(changed_schema),
       K_(state_changed),
       K_(system_vars),
       K_(user_vars),
       K_(capability_.capability),
       K_(use_standard_serialize));
  J_OBJ_END();
  return pos;
}
