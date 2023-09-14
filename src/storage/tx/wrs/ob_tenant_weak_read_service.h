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

#ifndef OCEANBASE_TRANSACTION_OB_TENANT_WEAK_READ_SERVICE_H_
#define OCEANBASE_TRANSACTION_OB_TENANT_WEAK_READ_SERVICE_H_

#include "share/ob_thread_pool.h"                     // ObThreadPool
#include "lib/net/ob_addr.h"                          // ObAddr
#include "lib/thread/thread_mgr_interface.h"          // TGRunnable
#include "common/ob_queue_thread.h"                   // ObCond

#include "ob_tenant_weak_read_server_version_mgr.h"   // ObTenantWeakReadServerVersionMgr
#include "ob_tenant_weak_read_cluster_service.h"      // ObTenantWeakReadClusterService
#include "ob_tenant_weak_read_stat.h"                 // ObTenantWeakReadStat
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace common { class ObMySQLProxy; }

namespace transaction
{

class ObIWrsRpc;
class ObTenantWeakReadService : public lib::TGRunnable
{
  static const int64_t PRINT_CLUSTER_MASTER_INFO_INTERVAL = 10 * 1000L * 1000L;
  /// location cache get cluster service master cost too much time, print WARN log
  static const int64_t LOCATION_CACHE_GET_WARN_THRESHOLD = 50 * 1000L;
  /// work thread interval
  static const int64_t THREAD_WAIT_INTERVAL = 10 * 1000L;
  /// min CLUSTER HEARTBEAT interval
  static const int64_t MIN_CLUSTER_HEARTBEAT_INTERVAL = 50 * 1000L;
  /// max CLUSTER HEARTBEAT interval
  static const int64_t MAX_CLUSTER_HEARTBEAT_INTERVAL = 1 * 1000 * 1000L;
  static const int64_t SELF_CHECK_INTERVAL = 500 * 1000L;
  static const int64_t GENERATE_CLUSTER_VERSION_INTERVAL = 100 * 1000L;
  // rlock wait time when get cluster version
  static const int64_t GET_CLUSTER_VERSION_RDLOCK_TIMEOUT = 100;
  static const int64_t THREAD_RUN_INTERVAL = 10 * 1000L;
  // force refresh location cache interval
  static const int64_t REFRESH_LOCATION_CACHE_INTERVAL = 500 * 1000L;
public:
  ObTenantWeakReadService();
  virtual ~ObTenantWeakReadService();
public:
  // TENANT WORK THREAD
  void run1();
public:
  int init(const uint64_t tenant_id,
      common::ObMySQLProxy &mysql_proxy,
      ObIWrsRpc &wrs_rpc,
      const common::ObAddr &self);
  int start();
  void stop();
  void wait();
  void destroy();

  /// get SERVER level weak read version
  share::SCN get_server_version() const;

  /// get CLUSTER level weak read version
  ///
  /// @retval OB_SUCCESS                            success
  /// @retval OB_TRANS_WEAK_READ_VERSION_NOT_READY  fail
  int get_cluster_version(share::SCN &version);

  // update SERVER level weak read version based on partition readable snapshot version in partition iteration
  //
  // identify different epoch with epoch_tstamp, reset if epoch changes
  int update_server_version_with_part_info(const int64_t epoch_tstamp,
      const bool need_skip,
      const bool is_user_part,
      const share::SCN version);

  // generate new SERVER level weak read version after scan all partitions
  int generate_server_version(const int64_t epoch_tstamp,
      const bool need_print_status);

  /// process cluster heartbeat RPC callback
  void process_cluster_heartbeat_rpc_cb(const obrpc::ObRpcResultCode &rcode,
      const common::ObAddr &dst);

  /// process Cluster level RPC
  int process_cluster_heartbeat_rpc(const common::ObAddr &svr,
      const share::SCN version,
      const int64_t valid_part_count,
      const int64_t total_part_count,
      const int64_t generate_timestamp);

  /// process get cluster version RPC
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_NEED_RETRY       need retry
  /// @retval OB_NOT_IN_SERVICE   self not in service
  /// @retval OB_NOT_MASTER       self is not MASTER
  /// @retval OTHER CODE          fail
  int process_get_cluster_version_rpc(share::SCN &version);

  // get weak read info stat
  void get_weak_read_stat(ObTenantWeakReadStat &wrs_stat) const;
  int check_can_start_service(const SCN &current_gts,
                              bool &can_start_service,
                              SCN &min_version,
                              share::ObLSID &ls_id);
  bool check_can_skip_ls(ObLS *ls);
public:
  // tenant level variables init and destroy function
  static int mtl_init(ObTenantWeakReadService* &twrs);

public:
  TO_STRING_KV(K_(inited), K_(tenant_id), K_(self), K_(svr_version_mgr), K_(cluster_service));

private:
  /// internal get cluster version function
  ///
  /// @retval OB_SUCCESS          success
  /// @retval OB_NEED_RETRY       need retry
  /// @retval OB_NOT_IN_SERVICE   self not in service
  /// @retval OB_NOT_MASTER       self is not MASTER
  /// @retval OTHER CODE          fail
  int get_cluster_version_internal_(share::SCN &version, const bool only_request_local);
  int get_cluster_version_by_rpc_(share::SCN &version);
  int get_cluster_service_master_(common::ObAddr &cluster_service_master);
  void refresh_cluster_service_master_();
  /// do Cluster Heartbeat request，report self's server version
  void do_cluster_heartbeat_();
  int post_cluster_heartbeat_rpc_(const share::SCN version,
      const int64_t valid_part_count,
      const int64_t total_part_count,
      const int64_t generate_timestamp);
  bool need_cluster_heartbeat_(const int64_t cur_tstamp);
  bool need_generate_cluster_version_(const int64_t cur_tstamp);
  bool need_self_check_(const int64_t cur_tstamp);
  bool need_force_self_check_(int ret, int64_t affected_rows, bool &need_stop_service);
  void generate_cluster_version_();
  void print_stat_();
  void cluster_service_self_check_();
  void do_thread_task_(const int64_t begin_tstamp, int64_t &last_print_stat_ts);
  void set_force_self_check_(bool need_stop_service);
  void set_cluster_service_master_(const common::ObAddr &addr);
  void generate_tenant_weak_read_timestamp_(bool need_print);
  int update_server_version_epoch_tstamp_(const int64_t cur_time);
  int scan_all_ls_(storage::ObLSService *ls_svr);
  int handle_ls_(storage::ObLS &ls);

private:
  struct ModuleInfo
  {
    ModuleInfo(ObTenantWeakReadService &twrs, const char *mod);
    ~ModuleInfo();
    const char *str() { return buf_; }
    char buf_[128];
  };
  typedef common::ObTimeGuard TimeGuard;

private:
  bool                              inited_;
  uint64_t                          tenant_id_;
  ObIWrsRpc                         *wrs_rpc_;
  common::ObAddr                    self_;

  ObTenantWeakReadServerVersionMgr  svr_version_mgr_;     // local server version MGR
  ObTenantWeakReadClusterService    cluster_service_;     // Cluster Service

  common::ObCond                    thread_cond_;

  // last locate_cache refresh timestamp
  int64_t                           last_refresh_locaction_cache_tstamp_;

  // cluster heartbeat variables
  int64_t                           last_post_cluster_heartbeat_tstamp_;
  int64_t                           post_cluster_heartbeat_count_;
  int64_t                           last_succ_cluster_heartbeat_tstamp_;
  int64_t                           succ_cluster_heartbeat_count_;
  int64_t                           cluster_heartbeat_interval_;
  common::ObAddr                    cluster_service_master_;

  // last self check timestamp
  int64_t                           last_self_check_tstamp_;

  // last cluster version generation timestamp
  int64_t                           last_generate_cluster_version_tstamp_;
  // cluster version the last get_version request get
  share::SCN                         local_cluster_version_;

  // need force self check or not
  bool                              force_self_check_;
  int tg_id_;

  // epoch of current server level weak read version
  // update with every partition scan, increase only
  int64_t  server_version_epoch_tstamp_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantWeakReadService);
};

} // transaction
} // oceanbase

#endif
