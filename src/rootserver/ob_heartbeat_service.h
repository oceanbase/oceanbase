/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROOTSERVER_OB_HEARTBEAT_SERVICE_H
#define OCEANBASE_ROOTSERVER_OB_HEARTBEAT_SERVICE_H

#include "lib/container/ob_array.h" // ObArray
#include "lib/net/ob_addr.h" // ObZone
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_zone.h" // ObAddr
#include "share/ob_server_table_operator.h" // ObServerTableOperator
#include "share/ob_heartbeat_struct.h"
#include "rootserver/ob_primary_ls_service.h" // ObTenantThreadHelper
#include "rootserver/ob_rs_async_rpc_proxy.h" // ObSendHeartbeatProxy
#include "share/ob_heartbeat_handler.h" // ObServerHealthStatus
namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
};
namespace rootserver
{
class ObHeartbeatService : public ObTenantThreadHelper,
                           public logservice::ObICheckpointSubHandler,
                           public logservice::ObIReplaySubHandler
{
public:
  typedef common::ObArray<share::ObHBRequest> ObHBRequestArray;
  typedef common::ObArray<share::ObHBResponse> ObHBResponseArray;
  typedef common::hash::ObHashMap<common::ObAddr, share::ObServerHBInfo> ObServerHBInfoMap;
  typedef common::ObArray<share::ObServerInfoInTable> ObServerInfoInTableArray;
  static const int64_t THREAD_COUNT = 2;
  ObHeartbeatService();
  virtual ~ObHeartbeatService();
  int init();
  void destroy();
  virtual void do_work() override;
  virtual int switch_to_leader() override;
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(share::SCN &rec_scn) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(buffer, nbytes, lsn, scn);
    return ret;
  }
  static bool is_service_enabled() {return is_service_enabled_; }
  DEFINE_MTL_FUNC(ObHeartbeatService)
private:
  static const int64_t HB_IDLE_TIME_US = 2 * 1000 * 1000L; // 2s
  static const int64_t HB_FAILED_IDLE_TIME_US = 0.5 * 1000 * 1000L; // 0.5s
  // (based on the whitelist) generate & send hb_requests and receive hb_responses
  int check_is_service_enabled_();
  int64_t get_epoch_id_() const { return ATOMIC_LOAD(&epoch_id_); }
  void set_epoch_id_(int64_t epoch_id) { ATOMIC_SET(&epoch_id_, epoch_id); }
  int check_upgrade_compat_();
  int send_heartbeat_();
  int prepare_hb_requests_(ObHBRequestArray &hb_requests, int64_t &whitelist_epoch_id);
  // generate the whitelist and process received hb_responses
  int set_hb_responses_(const int64_t whitelist_epoch_id, ObSendHeartbeatProxy *proxy);
  int get_and_reset_hb_responses_(ObHBResponseArray &hb_responses, int64_t &hb_responses_epoch_id);
  int manage_heartbeat_();
  // read __all_server table and generate whitelist
  int prepare_whitelist_();
  int check_or_update_service_epoch_(const int64_t epoch_id);
  int process_hb_responses_();
  int check_server_(
      const ObHBResponseArray &hb_responses,
      const share::ObServerInfoInTable &server_info_in_table,
      const common::ObArray<common::ObZone> &zone_list,
      const int64_t now,
      const int64_t hb_responses_epoch_id);
  int init_server_hb_info_(
      const int64_t now,
      const share::ObServerInfoInTable &server_info_in_table,
      share::ObServerHBInfo &server_hb_info);
  int check_server_without_hb_response_(
      const int64_t now,
      const share::ObServerInfoInTable &server_info_in_table,
      const int64_t hb_responses_epoch_id,
      share::ObServerHBInfo &server_hb_info);
  int update_table_for_online_to_offline_server_(
    const share::ObServerInfoInTable &server_info_in_table,
    const int64_t now,
    const int64_t hb_responses_epoch_id);
  int check_server_with_hb_response_(
    const share::ObHBResponse &hb_response,
    const share::ObServerInfoInTable &server_info_in_table,
    const common::ObArray<common::ObZone> &zone_list,
    const int64_t now,
    const int64_t hb_responses_epoch_id,
    share::ObServerHBInfo &server_hb_info);
  int check_if_hb_response_can_be_processed_(
      const share::ObHBResponse &hb_response,
      const share::ObServerInfoInTable &server_info_in_table,
      const common::ObArray<common::ObZone> &zone_list) const;
  // a common func. for all servers to update server_hb_info
  // if a server has hb_response, server_hb_info.server_health_status_ will be updated at
  // check_server_with_hb_response()
  int update_server_hb_info_(
      const int64_t now,
      const bool hb_response_exists,
      share::ObServerHBInfo &server_hb_info);
  int check_and_execute_start_or_stop_server_(
    const share::ObHBResponse &hb_response,
    const share::ObServerHBInfo &server_hb_info,
    const share::ObServerInfoInTable &server_info_in_table);
  int clear_deleted_servers_in_all_servers_hb_info_();

  int update_table_for_server_with_hb_response_(
    const share::ObHBResponse &hb_response,
    const share::ObServerInfoInTable &server_info_in_table,
    const int64_t hb_responses_epoch_id);
  template <typename T>
      bool has_server_exist_in_array_(
          const ObIArray<T> &array,
          const common::ObAddr &server,
          int64_t &idx);
  int end_trans_and_refresh_server_(
      const ObAddr &server,
      const bool commit,
      common::ObMySQLTransaction &trans);
  bool is_inited_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;
  int64_t epoch_id_; // the leader epoch, only be updated when the ls becomes leader
  int64_t whitelist_epoch_id_; // the leader epoch when we read prepare whitelist
  int64_t hb_responses_epoch_id_; // It indicates that current hb_responses are based on which epoch of the whitelist
  common::SpinRWLock hb_responses_rwlock_; // when we read/write hb_responses_, need_process_hb_responses_
                                           // and hb_responses_epoch_id_, we should use this lock
  common::SpinRWLock all_servers_info_in_table_rwlock_; // when we read/write all_servers_info_in_table_
                                                        // and whitelist_epoch_id_, we should use this lock
  ObServerHBInfoMap all_servers_hb_info_; // only used in manage_heartbeat()
  ObServerInfoInTableArray all_servers_info_in_table_;  // whitelist, send_heartbeat() read it and manage_heartbeat() write it
  common::ObArray<common::ObZone> inactive_zone_list_;
  ObHBResponseArray hb_responses_; // send_heartbeat() write it and manage_heartbeat() read it
  bool need_process_hb_responses_; // true if send rpc, and will be reset if responses are processed
  static bool is_service_enabled_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObHeartbeatService);
}; // end class ObHeartbeatService
} // end namespace rootserver
} // end namespace oceanbase
#endif
