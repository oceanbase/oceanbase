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

#ifndef OCEANBASE_TRANSACTION_OB_TENANT_WEAK_READ_CLUSTER_SERVICE_H_
#define OCEANBASE_TRANSACTION_OB_TENANT_WEAK_READ_CLUSTER_SERVICE_H_

#include "lib/lock/ob_spin_rwlock.h"          // SpinRWLock
#include "lib/net/ob_addr.h"                  // ObAddr
#include "lib/mysqlclient/ob_mysql_proxy.h"   // ObMySQLProxy

#include "ob_tenant_weak_read_cluster_version_mgr.h"  // ObTenantWeakReadClusterVersionMgr
#include "common/ob_member_list.h"                              //ObMemberList
#include "ob_i_weak_read_service.h"                   // WRS_VERSION_GAP_FOR_PERSISTENCE

namespace oceanbase
{
namespace common { class ObSqlString; }

namespace transaction
{

/// Tenant Cluster Weak Read Version generation service
class ObTenantWeakReadClusterService
{
  // print skipped server info interval
  static const int64_t PRINT_CLUSTER_SERVER_INFO_INTERVAL = 10 * 1000 * 1000L;
  static const int64_t MAX_ERROR_THRESHOLD_FOR_CHANGE_LEADER = 5 * 1000;
  static const int64_t ERROR_STATISTIC_INTERVAL_FOR_CHANGE_LEADER = 10 * 1000 * 1000L;
  static const int64_t LEADER_ALIVE_THRESHOLD_FOR_CHANGE_LEADER = 10 * 1000 * 1000L;
  static const int64_t PROCESS_CLUSTER_HEARTBEAT_RPC_RDLOCK_TIMEOUT = 1 * 1000L;
  static const int64_t LAST_ERROR_TSTAMP_INTERVAL_FOR_CHANGE_LEADER = 2 * 1000 * 1000L;

  // Time threshold to force update version after start service
  // Backgroud: After starting the service, we require that all servers are registered successfully
  // before counting the version number to ensure that all servers are counted
  static const int64_t FORCE_UPDATE_VERSION_TIME_AFTER_START_SERVICE = 2 * 1000 * 1000L;
public:
  ObTenantWeakReadClusterService();
  virtual ~ObTenantWeakReadClusterService();
public:
  TO_STRING_KV(K_(current_version), K_(min_version), K_(max_version));
  const ObTabletID &get_cluster_service_tablet_id() const { return cluster_service_tablet_id_; }

  int init(const uint64_t tenant_id,
      common::ObMySQLProxy &mysql_proxy);
  void destroy();

  /// START WRS SERVICE
  ///
  /// @retval OB_SUCCESS   start service success 
  /// @retval OB_NOT_MASTER self is not wrs leader
  /// @retval OB_NEED_RETRY need retry
  /// @retval OTHER CODE    fail
  int start_service();

  /// STOP WRS SERVICE
  void stop_service();

  /// stop service if previous service leader epoch equals target_leader_epoch
  ///
  /// @param target_leader_epoch  target leader epoch
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OTHER CODE          fail
  int stop_service_if_leader_info_match(const int64_t target_leader_epoch);

  /// get wrs version
  ///
  /// @retval OB_SUCCESS            success
  /// @retval OB_NOT_IN_SERVICE     self server not in wrs service
  /// @retval OB_NOT_MASTER         self is in service, but not wrs leader, should stop service
  /// @retval OB_NEED_RETRY         wrs not ready，need retry
  /// @retval OTHER CODE            fail
  int get_cluster_version(share::SCN &version) const;
  int get_cluster_version(share::SCN &version, share::SCN &min_version, share::SCN &max_version) const;

  /// update wrs version
  ///
  /// @retval OB_SUCCESS            success
  /// @retval OB_NOT_IN_SERVICE     self server not in wrs service
  /// @retval OB_NOT_MASTER         self is in service, but not wrs leader, should stop service
  /// @retval OB_NEED_RETRY         need retry
  /// @retval OTHER CODE            fail
  int update_cluster_version(int64_t &affected_rows);

  bool need_print_skipped_server();

  /// self check
  /// 1. revoke if not wrs leader or leader switch and in service
  /// 2. takeover if not in service and is wrs leader
  void self_check();

  bool is_in_service() const { return ATOMIC_LOAD(&in_service_); }
  bool is_service_master() const;

  // get cluster server count
  int64_t get_cluster_registered_server_count() const;
  int64_t get_cluster_skipped_server_count() const;

  /// get current service info
  ///
  /// @param in_service   return parameter, if in wrs service
  /// @param leader_epoch return paramter, current service leader epoch if in wrs service
  void get_serve_info(bool &in_service, int64_t &leader_epoch) const;

  /// update server level weak read version
  int update_server_version(const common::ObAddr &addr,
      const share::SCN version,
      const int64_t valid_part_count,
      const int64_t total_part_count,
      const int64_t generate_timestamp);

private:
  int check_leader_info_(int64_t &leader_epoch) const;
  void update_valid_server_count_();
  int query_cluster_version_range_(share::SCN &cur_min_version, share::SCN &cur_max_version,
      bool &record_exist);
  int persist_version_if_need_(const share::SCN last_min_version,
      const share::SCN last_max_version,
      const share::SCN new_min_version,
      const share::SCN new_max_version,
      const bool record_exist,
      int64_t &affected_rows);
  int build_update_version_sql_(const share::SCN last_min_version,
      const share::SCN last_max_version,
      const share::SCN new_min_version,
      const share::SCN new_max_version,
      const bool record_exist,
      common::ObSqlString &sql);
  bool check_can_update_version_();
  share::SCN generate_max_version_(const share::SCN min_version) const
  {
    return share::SCN::plus(min_version, WRS_VERSION_GAP_FOR_PERSISTENCE * 1000);
  }
  void stop_service_impl_();
  int compute_cluster_version_(int64_t &skipped_servers, bool need_print, share::SCN &new_version) const;
  int get_cluster_version_(share::SCN &version, share::SCN &min_version, share::SCN &max_version) const;
  bool need_force_change_leader_();
  int force_change_leader_() const;
  int verify_candidate_server_(const common::ObAddr &server) const;
  int get_candidate_server_(const common::ObAddr &self,
      const common::ObMemberList &member_list,
      common::ObAddr &candidate) const;
  void reset_change_leader_info_();
private:
  typedef common::SpinRWLock      RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;

  typedef ObTenantWeakReadClusterVersionMgr ClusterVersionMgr;
private:
  bool                          inited_;
  ObTabletID                    cluster_service_tablet_id_;
  common::ObMySQLProxy          *mysql_proxy_;

  // if in wrs service
  bool                          in_service_;
  // if can update version
  // NOTE: version can be updated after all server registered
  bool                          can_update_version_;

  // start service timestamp
  int64_t                       start_service_tstamp_;

  // current WRS leader epoch
  int64_t                       leader_epoch_;

  // skipped server count cached in local
  int64_t                       skipped_server_count_;

  // the last print skipped server timestamp
  int64_t                       last_print_skipped_server_tstamp_;

  // error occur times in one epoch
  int64_t                       error_count_for_change_leader_;
  int64_t                       last_error_tstamp_for_change_leader_;

  int64_t                       all_valid_server_count_;

  // current version cached in local
  share::SCN                     current_version_ CACHE_ALIGNED;
  share::SCN                     min_version_ CACHE_ALIGNED;
  share::SCN                     max_version_;

  ClusterVersionMgr             cluster_version_mgr_;

  mutable RWLock                rwlock_;
};

}
}

#endif
