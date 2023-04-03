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

#ifndef _OMPK_HANDSHAKE_H_
#define _OMPK_HANDSHAKE_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_packet.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKHandshake
    : public ObMySQLPacket
{
public:
  static const int32_t SCRAMBLE_SIZE = 8;
  static const int64_t SCRAMBLE_TOTAL_SIZE = 20;

public:
  OMPKHandshake();

  //TODO use ob server info to init handshake packet
  //OMPKHandshake(ObServerInfo& info);
  virtual ~OMPKHandshake();

  // shadow copy
  int set_server_version(common::ObString &version);

  inline void set_server_status(uint16_t status) { server_status_ = status; }

  inline void set_server_language(uint8_t language) { server_language_ = language; }

  virtual int64_t get_serialize_size() const;
  /**
   * Serialize all data not include packet header to buffer
   * @param buffer  buffer
   * @param len     buffer length
   * @param pos     buffer pos
   */
  virtual int serialize(char *buffer, int64_t len, int64_t &pos) const;

  // decode handshake packet
  virtual int decode();
  int get_scramble(char *buffer, const int64_t len, int64_t &copy_len);
  int set_scramble(char *buffer, const int64_t len);

  uint32_t get_thread_id() const { return thread_id_; }
  void set_thread_id(uint32_t thread_id) { thread_id_ = thread_id; }
  void set_ssl_cap(const bool use_ssl)
  {
    server_capabilities_lower_.capability_flag_.OB_SERVER_SSL = (use_ssl ? 1 : 0);
  }

  struct CapabilitiesFlagLower
  {
    uint16_t OB_SERVER_LONG_PASSWORD:1;
    uint16_t OB_SERVER_FOUND_ROWS:1;
    uint16_t OB_SERVER_LONG_FLAG:1;
    uint16_t OB_SERVER_CONNECT_WITH_DB:1;
    uint16_t OB_SERVER_NO_SCHEMA:1;
    uint16_t OB_SERVER_CAN_USE_COMPRESS:1;
    uint16_t OB_SERVER_ODBC:1;
    uint16_t OB_SERVER_LOCAL_FILES:1;
    uint16_t OB_SERVER_IGNORE_SPACE:1;
    uint16_t OB_SERVER_PROTOCOL_41:1;
    uint16_t OB_SERVER_INTERACTIVE:1;
    uint16_t OB_SERVER_SSL:1;
    uint16_t OB_SERVER_IGNORE_SIGPIPE:1;
    uint16_t OB_SERVER_TRANSACTIONS:1;
    uint16_t OB_SERVER_RESERVED:1;
    uint16_t OB_SERVER_SECURE_CONNECTION:1;
  };
  union ServerCapabilitiesLower
  {
    CapabilitiesFlagLower capability_flag_;
    uint16_t capability_;
  };

  struct CapabilitiesFlagUpper
  {
    uint16_t OB_SERVER_MULTIPLE_STATEMENTS:1;
    uint16_t OB_SERVER_MULTIPLE_RESULTS:1;
    uint16_t OB_SERVER_PS_MULTIPLE_RESULTS:1;
    uint16_t OB_SERVER_PLUGIN_AUTH:1;
    uint16_t OB_SERVER_CONNECT_ATTRS:1;
    uint16_t OB_SERVER_PLUGIN_AUTH_LENENC_CLIENT_DATA:1;
    uint16_t OB_SERVER_CAN_HANDLE_EXPIRED_PASSWORDS:1;
    uint16_t OB_SERVER_SESSION_VARIABLE_TRACK:1;
    uint16_t OB_SERVER_DEPRECATE_EOF:1;
    uint16_t OB_SERVER_RESERVED:2;
    uint16_t OB_SERVER_SUPPORT_ORACLE_MODE:1;
    uint16_t OB_SERVER_RETURN_HIDDEN_ROWID:1;
    uint16_t OB_SERVER_USE_LOB_LOCATOR:1;
    uint32_t OB_SERVER_SSL_VERIFY_SERVER_CERT:1;
    uint32_t OB_SERVER_REMEMBER_OPTIONS:1;
  };
  union ServerCapabilitiesUpper
  {
    CapabilitiesFlagUpper capability_flag_;
    uint16_t capability_;
  };
private:
  const char *get_handshake_inner_pulgin_name(const common::ObString outer_string) const;

private:
  DISALLOW_COPY_AND_ASSIGN(OMPKHandshake);
  static const char *SERVER_VERSION_STR;

  const static char *AUTH_PLUGIN_MYSQL_NATIVE_PASSWORD; // Secure Password Authentication
  const static char *AUTH_PLUGIN_MYSQL_OLD_PASSWORD;    // Old Password Authentication
  const static char *AUTH_PLUGIN_MYSQL_CLEAR_PASSWORD;  // Clear Text Authentication
  const static char *AUTH_PLUGIN_AUTHENTICATION_WINDOWS_CLIENT; // Windows Native Authentication

  const static uint8_t DEFAULT_AUTH_PLUGIN_DATA_LEN = 8 + 13;

  ObMySQLPacketHeader header_;
  uint8_t protocol_version_;
  common::ObString server_version_;// human-readable server version
  uint32_t thread_id_;// connection_id
  char scramble_buff_[8];// auth_plugin_data_part_1 : first 8 bytes of the auth-plugin data
  uint8_t filler_;                  /* always 0x00 */
  ServerCapabilitiesLower server_capabilities_lower_;  /* set value to use 4.1protocol */
  ServerCapabilitiesUpper server_capabilities_upper_;  /* set value to use 4.1protocol */
  uint8_t server_language_;
  uint16_t server_status_;
  uint8_t auth_plugin_data_len_;
  char reserved_[10];
  char auth_plugin_data2_[13];
  const char *auth_plugin_name_;
  char terminated_;
}; // end of class OmpkHandake

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OMPK_HANDSHAKE_H_ */
