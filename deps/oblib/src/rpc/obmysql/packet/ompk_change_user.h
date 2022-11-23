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

#ifndef OCEANBASE_OBMYSQL_OMPK_CHANGE_USER_H_
#define OCEANBASE_OBMYSQL_OMPK_CHANGE_USER_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
using common::ObString;
namespace obmysql
{

class OMPKChangeUser
    : public ObMySQLRawPacket
{
public:
  OMPKChangeUser();

  virtual ~OMPKChangeUser() {}

  /**
   * Serialize all data not include packet header to buffer
   * @param buffer  buffer
   * @param len     buffer length
   * @param pos     buffer pos
   */
  virtual int serialize(char *buffer, int64_t len, int64_t &pos) const;

  inline void set_mysql_capability(const ObMySQLCapabilityFlags &mysql_cap) { mysql_cap_ = mysql_cap; }
  inline void set_username(const ObString &username) { username_ = username; }
  inline void set_auth_response(const ObString &auth_response) { auth_response_ = auth_response; }
  inline void set_database(const ObString &database) { database_ = database; }
  inline void set_character_set(const uint8_t charset) { character_set_ = charset; }
  inline void set_auth_plugin_name(const ObString &auth_plugin_name) { auth_plugin_name_ = auth_plugin_name; }
  inline const ObString& get_username() { return username_; }
  inline const common::ObIArray<ObStringKV> &get_system_vars() const { return sys_vars_; }
  inline const common::ObIArray<ObStringKV> &get_user_vars() const { return user_vars_; }
  inline common::ObIArray<ObStringKV> &get_system_vars() { return sys_vars_; }
  inline common::ObIArray<ObStringKV> &get_user_vars() { return user_vars_; }
  inline const common::ObIArray<ObStringKV> &get_connect_attrs() const { return connect_attrs_; }
  inline void set_capability_flag(const ObMySQLCapabilityFlags mysql_cap) { mysql_cap_ = mysql_cap; }
  virtual int64_t get_serialize_size() const;

private:
  uint64_t get_session_vars_len() const;
  uint64_t get_connect_attrs_len() const;
  int serialize_session_vars(char *buffer,
                             const int64_t length,
                             int64_t &pos) const;

private:
  uint8_t cmd_;
  uint8_t character_set_;
  ObMySQLCapabilityFlags mysql_cap_;
  ObString username_;
  ObString auth_response_;
  ObString auth_plugin_name_;
  ObString database_;
  common::ObSEArray<ObStringKV, 8> connect_attrs_;
  common::ObSEArray<ObStringKV, 128> sys_vars_;
  common::ObSEArray<ObStringKV, 16> user_vars_;
  DISALLOW_COPY_AND_ASSIGN(OMPKChangeUser);
}; // end of class

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OMPK_CHANGE_USER_H_ */
