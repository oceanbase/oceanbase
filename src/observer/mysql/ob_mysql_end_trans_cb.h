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
#include "storage/tx/ob_trans_result.h"
#include "rpc/obmysql/ob_2_0_protocol_utils.h"
#include "observer/mysql/obmp_packet_sender.h"


namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace obmysql
{
class ObMySQLPacket;
}
namespace rpc
{
class ObRequest;
}
namespace observer
{
struct ObOKPParam;
class ObMySQLResultSet;
struct ObSMConnection;
class ObSqlEndTransCb
{
public:
  ObSqlEndTransCb();
  virtual ~ObSqlEndTransCb();

  void callback(int cb_param);
  void callback(int cb_param, const transaction::ObTransID &trans_id);
  int init(ObMPPacketSender& packet_sender, 
           sql::ObSQLSessionInfo *sess_info, 
           int32_t stmt_id = 0,
           uint64_t params_num = 0);
  int set_packet_param(const sql::ObEndTransCbPacketParam &pkt_param);
  void destroy();
  void reset();
  void set_need_disconnect(bool need_disconnect) { need_disconnect_ = need_disconnect; }
  void set_stmt_id(int32_t id) { stmt_id_ = id; }
  void set_param_num(uint64_t num) { params_num_ = num; }
  sql::ObSQLSessionInfo* get_sess_info_ptr() { return sess_info_; }

protected:
  ObMPPacketSender packet_sender_;
  sql::ObSQLSessionInfo *sess_info_;
  sql::ObEndTransCbPacketParam pkt_param_;
  bool need_disconnect_;
  int32_t stmt_id_;
  uint64_t params_num_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlEndTransCb);
};
} //end of namespace obmysql
} //end of namespace oceanbase



#endif /* SRC_OBSERVER_MYSQL_OB_MYSQL_END_TRANS_CB_CPP_ */
