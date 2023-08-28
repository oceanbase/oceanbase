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

#ifndef OCEANBASE_OBSERVER_MYSQL_OBSM_STRUCT_H_
#define OCEANBASE_OBSERVER_MYSQL_OBSM_STRUCT_H_

#include <stdint.h>
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "rpc/ob_packet.h"
#include "lib/lock/ob_latch.h"
#include "rpc/obmysql/ob_packet_record.h"
#include "rpc/obmysql/ob_2_0_protocol_struct.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace omt
{
class ObTenant;
}

namespace observer
{

struct ObSMConnection
{
public:
  static const uint32_t INITIAL_SESSID = 0;
  static const int64_t SCRAMBLE_BUF_SIZE = 20;

  ObSMConnection()
  {
    cap_flags_.capability_ = 0;
    is_proxy_ = false;
    is_java_client_ = false;
    is_oci_client_ = false;
    is_jdbc_client_ = false;
    is_sess_alloc_ = false;
    is_sess_free_ = false;
    has_inc_active_num_ = false;
    is_need_clear_sessid_ = true;
    is_tenant_locked_ = false;
    handle_ = &handle1_;
    tmp_handle_ = &handle2_;
    connection_phase_ = rpc::ConnectionPhaseEnum::CPE_CONNECTED;
    sessid_ = INITIAL_SESSID;
    proxy_sessid_ = 0;
    sess_create_time_ = 0;
    resource_group_id_ = 0;
    tenant_id_ = 0;
    proxy_cap_flags_.capability_ = 0,
    tenant_ = NULL;
    MEMSET(tenant_name_buf_, 0, sizeof(tenant_name_buf_));
    MEMSET(user_name_buf_, 0, sizeof(user_name_buf_));
    vid_ = OB_INVALID_ID;
    MEMSET(vip_buf_, 0, sizeof(vip_buf_));
    vport_ = 0;
    connect_in_bytes_ = 0;
    ret_ = common::OB_SUCCESS;
    scramble_buf_[SCRAMBLE_BUF_SIZE] = '\0';
    proxy_version_ = 0;
    group_id_ = 0;
    client_cs_type_ = 0;
    sql_req_level_ = 0;
    pkt_rec_wrapper_.init();
    client_type_ = common::OB_CLIENT_INVALID_TYPE;
    client_version_ = 0;
  }

  obmysql::ObCompressType get_compress_type() {
    obmysql::ObCompressType type_ret = obmysql::ObCompressType::NO_COMPRESS;
    //unauthed connection, treat it do not use compress
    if (is_in_authed_phase() && 1 == cap_flags_.cap_flags_.OB_CLIENT_COMPRESS) {
      if (is_proxy_) {
        if (1 == proxy_cap_flags_.cap_flags_.OB_CAP_CHECKSUM) {
          type_ret = obmysql::ObCompressType::PROXY_CHECKSUM;
        } else {
          type_ret = obmysql::ObCompressType::PROXY_COMPRESS;
        }
      } else if (is_java_client_) {
        if (1 == proxy_cap_flags_.cap_flags_.OB_CAP_CHECKSUM) {
          type_ret = obmysql::ObCompressType::DEFAULT_CHECKSUM;
        } else {
          type_ret = obmysql::ObCompressType::DEFAULT_COMPRESS;
        }
      } else if (is_oci_client_) {
        if (1 == proxy_cap_flags_.cap_flags_.OB_CAP_CHECKSUM) {
          type_ret = obmysql::ObCompressType::DEFAULT_CHECKSUM;
        } else {
          type_ret = obmysql::ObCompressType::DEFAULT_COMPRESS;
        }
      } else if (is_jdbc_client_) {
        if (1 == proxy_cap_flags_.cap_flags_.OB_CAP_CHECKSUM) {
          type_ret = obmysql::ObCompressType::DEFAULT_CHECKSUM;
        } else {
          type_ret = obmysql::ObCompressType::DEFAULT_COMPRESS;
        }
      } else {
        type_ret = obmysql::ObCompressType::DEFAULT_COMPRESS;
      }
    }
    return type_ret;
  }

  bool need_send_extra_ok_packet() const {
    return (is_proxy_ || (is_java_client_ && proxy_cap_flags_.is_extra_ok_packet_for_ocj_support()));
  }

  bool is_normal_client() const {
    return (!is_proxy_ && !is_java_client_ && !is_oci_client_ && !is_jdbc_client_);
  }

  bool is_driver_client() const {
    return is_oci_client_ || is_jdbc_client_;
  }

  bool is_support_proxy_reroute() const {
    return (1 == proxy_cap_flags_.cap_flags_.OB_CAP_OB_PROTOCOL_V2
            && 1 == proxy_cap_flags_.cap_flags_.OB_CAP_PROXY_REROUTE);
  }

  bool is_support_sessinfo_sync() const {
    return (1 == proxy_cap_flags_.cap_flags_.OB_CAP_OB_PROTOCOL_V2
            && 1 == proxy_cap_flags_.cap_flags_.OB_CAP_PROXY_SESSIOIN_SYNC);
  }

  common::ObCSProtocolType get_cs_protocol_type() const
  {
    common::ObCSProtocolType type = common::OB_INVALID_CS_TYPE;
    if (proxy_cap_flags_.is_ob_protocol_v2_support()) {
      type = common::OB_2_0_CS_TYPE;
    } else if (1 == cap_flags_.cap_flags_.OB_CLIENT_COMPRESS) {
      type = common::OB_MYSQL_COMPRESS_CS_TYPE;
    } else {
      type = common::OB_MYSQL_CS_TYPE;
    }
    return type;
  }

  inline bool is_in_connected_phase() { return rpc::ConnectionPhaseEnum::CPE_CONNECTED == connection_phase_; }
  inline bool is_in_ssl_connect_phase() { return rpc::ConnectionPhaseEnum::CPE_SSL_CONNECT == connection_phase_; }
  inline bool is_in_authed_phase() { return rpc::ConnectionPhaseEnum::CPE_AUTHED == connection_phase_; }
  inline void set_ssl_connect_phase() { connection_phase_ = rpc::ConnectionPhaseEnum::CPE_SSL_CONNECT; }
  inline void set_auth_phase() { connection_phase_ = rpc::ConnectionPhaseEnum::CPE_AUTHED; }
  inline void set_connect_phase() { connection_phase_ = rpc::ConnectionPhaseEnum::CPE_CONNECTED; }
public:
  obmysql::ObMySQLCapabilityFlags cap_flags_;
  bool is_proxy_;
  bool is_java_client_;
  bool is_oci_client_;
  bool is_jdbc_client_;
  bool is_sess_alloc_;
  bool is_sess_free_;
  bool has_inc_active_num_;
  bool is_need_clear_sessid_;
  bool is_tenant_locked_;

  // for enable_latch_diagnose
  common::ObLDHandle handle1_;
  common::ObLDHandle handle2_;
  common::ObLDHandle *handle_;
  common::ObLDHandle *tmp_handle_;

  rpc::ConnectionPhaseEnum connection_phase_;
  uint32_t sessid_;
  uint32_t version_;
  uint64_t proxy_sessid_;
  int64_t sess_create_time_; //proxy连接模式下, 记录client->proxy的sess连接时间
  uint64_t tenant_id_;
  uint64_t resource_group_id_;
  obmysql::ObProxyCapabilityFlags proxy_cap_flags_;
  //在ObSMHandler::on_connect阶段会发生某些错误，这些错误信息需要返回给客户端；
  //而在on_connect中无法给客户端返回准确错误信息，因此记录到这里，在ObMPConnect::Process中处理
  int ret_;
  omt::ObTenant *tenant_;
  char tenant_name_buf_[OB_MAX_TENANT_NAME_LENGTH + 1];
  char user_name_buf_[OB_MAX_USER_NAME_LENGTH + 1];
  int64_t vid_;
  char vip_buf_[MAX_IP_ADDR_LENGTH];
  int32_t vport_;
  int64_t connect_in_bytes_;
  obmysql::ObMysqlPktContext mysql_pkt_context_;
  obmysql::ObCompressedPktContext compressed_pkt_context_;
  obmysql::ObProto20PktContext proto20_pkt_context_;
  char scramble_buf_[SCRAMBLE_BUF_SIZE + 1];
  uint64_t proxy_version_;
  int32_t group_id_;
  int32_t client_cs_type_;
  int64_t sql_req_level_;
  obmysql::ObPacketRecordWrapper pkt_rec_wrapper_;
  ObClientType client_type_;
  uint64_t client_version_;
};
} // end of namespace observer
} // end of namespace oceanbase

#endif // OCEANBASE_OBSERVER_MYSQL_OBSM_STRUCT_H_
