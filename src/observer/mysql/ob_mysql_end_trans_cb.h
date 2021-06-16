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

#ifndef SRC_OBSERVER_MYSQL_OB_MYSQL_END_TRANS_CB_CPP_
#define SRC_OBSERVER_MYSQL_OB_MYSQL_END_TRANS_CB_CPP_
#include "sql/ob_i_end_trans_callback.h"
#include "sql/ob_end_trans_cb_packet_param.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/objectpool/ob_tc_factory.h"
#include "lib/allocator/ob_mod_define.h"
#include "storage/transaction/ob_trans_result.h"
#include "rpc/obmysql/ob_2_0_protocol_utils.h"

namespace oceanbase {
namespace sql {
class ObSQLSessionInfo;
}
namespace obmysql {
class ObMySQLPacket;
}
namespace rpc {
class ObRequest;
}
namespace observer {
class ObOKPParam;
class ObMySQLResultSet;
struct ObSMConnection;
class ObSqlEndTransCb {
public:
  ObSqlEndTransCb();
  virtual ~ObSqlEndTransCb();

  void callback(int cb_param);
  void callback(int cb_param, const transaction::ObTransID& trans_id);
  int init(rpc::ObRequest* req, sql::ObSQLSessionInfo* sess_info, uint8_t packet_seq, bool conn_status);
  int set_packet_param(const sql::ObEndTransCbPacketParam& pkt_param);
  void set_seq(uint8_t value);
  void set_conn_valid(bool value);
  void destroy();
  void reset();
  void set_need_disconnect(bool need_disconnect)
  {
    need_disconnect_ = need_disconnect;
  }

private:
  int revert_session(sql::ObSQLSessionInfo* sess_info);
  ObSMConnection* get_conn() const;
  int get_conn_id(uint32_t& sessid) const;

protected:
  int encode_ok_packet(rpc::ObRequest* req, easy_buf_t* buf, observer::ObOKPParam& ok_param,
      sql::ObSQLSessionInfo& sess_info, uint8_t packet_seq);
  int encode_error_packet(rpc::ObRequest* req, easy_buf_t* ez_buf, sql::ObSQLSessionInfo& sess_info, uint8_t packet_seq,
      int err, const char* errmsg, bool is_partition_hit);
  int get_conn(rpc::ObRequest* req, ObSMConnection*& conn) const;

  inline int encode_packet(obmysql::ObMySQLPacket& pkt, const uint8_t packet_seq, const bool is_last);

protected:
  char* data_;
  rpc::ObRequest* mysql_request_;
  sql::ObSQLSessionInfo* sess_info_;
  sql::ObEndTransCbPacketParam pkt_param_;
  int16_t warning_count_;
  uint8_t seq_;
  obmysql::ObCompressionContext comp_context_;
  obmysql::ObProto20Context proto20_context_;
  easy_buf_t* ez_buf_;
  bool conn_valid_;
  bool need_disconnect_;
  uint32_t sessid_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlEndTransCb);
};
}  // namespace observer
}  // end of namespace oceanbase

#endif /* SRC_OBSERVER_MYSQL_OB_MYSQL_END_TRANS_CB_CPP_ */
