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

#ifndef _OMPK_OK_H_
#define _OMPK_OK_H_

#include "lib/string/ob_string.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKOK : public ObMySQLPacket
{
public:
  OMPKOK();
  virtual ~OMPKOK() {}

  // decode ok packet
  virtual int decode();
  // serialize all data into thread buffer not include packet header
  // Attention!! before called serialize or get_serialize_size, must set capability
  virtual int serialize(char *buffer, const int64_t length, int64_t &pos) const;
  virtual int64_t get_serialize_size() const;

  inline void set_affected_rows(const uint64_t row) { affected_rows_ = row; }
  inline void set_last_insert_id(const uint64_t id) { last_insert_id_ = id; }
  inline void set_server_status(const ObServerStatusFlags status) { server_status_ = status; }
  inline void set_warnings(const uint16_t warnings) { warnings_ = warnings; }
  // shadow copy
  int set_message(const common::ObString &message);
  void set_changed_schema(const common::ObString &schema);
  void set_state_changed(const bool state_changed);
  void set_use_standard_serialize(const bool value);
  int add_system_var(const ObStringKV &system_var);
  int add_user_var(const ObStringKV &user_var);
  inline void set_capability(const ObMySQLCapabilityFlags &cap) { capability_ = cap; }
  inline void set_track_session_cap(const bool flag)
  {
    capability_.cap_flags_.OB_CLIENT_SESSION_TRACK = (flag ? 1 : 0);
  }

  inline uint8_t get_field_count() const { return field_count_; }
  inline uint64_t get_affected_rows() const { return affected_rows_; }
  inline uint64_t get_last_insert_id() const { return last_insert_id_; }
  inline ObServerStatusFlags get_server_status() const { return server_status_; }
  inline uint16_t get_warnings() const { return warnings_; }
  inline const common::ObString &get_message() const { return message_; }
  inline const common::ObString &get_changed_schema() const { return changed_schema_; };
  inline bool is_state_changed() const { return state_changed_; }
  inline const common::ObIArray<ObStringKV> &get_system_vars() const { return system_vars_; }
  inline const common::ObIArray<ObStringKV> &get_user_vars() const { return user_vars_; }
  inline ObMySQLCapabilityFlags get_capability() const  { return capability_; }
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_OKP; }

  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int decode_session_state_info(const char *&pos);
  uint64_t get_state_info_len() const;
  uint64_t get_track_system_vars_len() const;
  uint64_t get_standard_track_system_vars_len() const;
  int serialize_string_kv(char *buffer,
                          const int64_t length,
                          int64_t &pos,
                          const ObStringKV &string_kv) const;
  static uint64_t get_kv_encode_len(const ObStringKV &string_kv);
  static ObStringKV get_separator_kv();

private:
  const static int64_t SESSION_TRACK_SYSTEM_VARIABLES = 0x00;
  const static int64_t SESSION_TRACK_SCHEMA = 0x01;
  const static int64_t SESSION_TRACK_STATE_CHANGE = 0x02;
  DISALLOW_COPY_AND_ASSIGN(OMPKOK);

  uint8_t field_count_;         // always 0x00
  uint64_t affected_rows_;
  uint64_t last_insert_id_;
  ObServerStatusFlags server_status_;
  uint16_t warnings_;
  common::ObString message_;
  common::ObString changed_schema_;
  bool state_changed_;
  common::ObSEArray<ObStringKV, 16> system_vars_;
  common::ObSEArray<ObStringKV, 8> user_vars_;
  ObMySQLCapabilityFlags capability_;

  // use to track database changed;
  // changed_schema_ maybe empty, but we need transfer it to obproxy,
  // such as drop database;
  bool is_schema_changed_;

  //current serialize is not compat with mysql, proxy is also uncompat.
  //we cannot fix it for compatibility.
  //when obclient connect observer directly, we should set this true.
  //it will affect  OB_SERVER_SESSION_STATE_CHANGED encoding
  bool use_standard_serialize_;
};

} // end namespace obmysql
} // end namespace oceanbase
#endif /* _OMPK_OK_H_ */
