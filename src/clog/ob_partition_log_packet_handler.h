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

#ifndef OCEANBASE_CLOG_OB_PARTITION_LOG_PACKET_HANDLER_
#define OCEANBASE_CLOG_OB_PARTITION_LOG_PACKET_HANDLER_

#include "ob_i_partition_log_packet_handler.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace clog {
class ObIPartitionLogService;
class ObLogReqContext;
class ObICLogMgr;
class ObILogEngine;

class ObIPartitionLogService;
class ObPartitionLogPacketHandler : public ObIPartitionLogPacketHandler {
public:
  typedef ObIPartitionLogService LogService;
  typedef ObLogReqContext Context;
  ObPartitionLogPacketHandler() : partition_service_(NULL), clog_mgr_(NULL)
  {}
  virtual ~ObPartitionLogPacketHandler()
  {}
  int init(storage::ObPartitionService* partition_service, ObICLogMgr* clog_mgr);
  int handle_request(Context& req);

protected:
  int handle_single_request(Context& req);
  int handle_batch_request(Context& req);
  static int receive_log(LogService* log_service, ObILogEngine* log_engine, Context& ctx, ReceiveLogType type);
  static int receive_renew_ms_log(LogService* log_service, Context& ctx, ReceiveLogType type);
  static int fake_receive_log(LogService* log_service, Context& ctx);
  static int ack_log(LogService* log_service, Context& ctx);
  static int standby_ack_log(LogService* log_service, Context& ctx);
  static int fake_ack_log(LogService* log_service, Context& ctx);
  static int ack_renew_ms_log(LogService* log_service, Context& ctx);
  static int fetch_log(LogService* log_service, Context& ctx);
  static int prepare_req(LogService* log_service, Context& ctx);
  static int prepare_resp(LogService* log_service, Context& ctx);
  static int handle_standby_prepare_req(LogService* log_service, Context& ctx);
  static int handle_standby_prepare_resp(LogService* log_service, Context& ctx);
  static int handle_query_sync_start_id_req(LogService* log_service, Context& ctx);
  static int handle_sync_start_id_resp(LogService* log_service, Context& ctx);
  int receive_confirmed_info(LogService* log_service, Context& ctx);
  int receive_renew_ms_log_confirmed_info(LogService* log_service, Context& ctx);
  static int process_keepalive_msg(LogService* log_service, Context& ctx);
  static int process_restore_takeover_msg(LogService* log_service, Context& ctx);
  static int process_sync_log_archive_progress_msg(LogService* log_service, Context& ctx);
  static int notify_log_missing(LogService* log_service, Context& ctx);
  static int fetch_register_server_req(LogService* log_service, Context& ctx);
  static int fetch_register_server_resp_v2(LogService* log_service, Context& ctx);
  static int process_reject_msg(LogService* log_service, Context& ctx);
  static int process_restore_alive_msg(LogService* log_service, Context& ctx);
  static int process_restore_alive_req(LogService* log_service, Context& ctx);
  static int process_restore_alive_resp(LogService* log_service, Context& ctx);
  static int process_restore_log_finish_msg(LogService* log_service, Context& ctx);
  static int process_reregister_msg(LogService* log_service, Context& ctx);
  static int update_broadcast_info(LogService* log_service, Context& ctx);
  static int replace_sick_child(LogService* log_service, Context& ctx);
  static int process_leader_max_log_msg(LogService* log_service, Context& ctx);
  static int process_check_rebuild_req(LogService* log_service, Context& ctx);

private:
  storage::ObPartitionService* partition_service_;
  ObICLogMgr* clog_mgr_;
};
};      // end namespace clog
};      // end namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_PARTITION_LOG_PACKET_HANDLER_
