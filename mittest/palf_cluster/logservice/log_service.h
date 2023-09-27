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

#ifndef OCEANBASE_PALF_CLUSTER_OB_LOG_SERVICE_
#define OCEANBASE_PALF_CLUSTER_OB_LOG_SERVICE_

#include "common/ob_role.h"
#include "lib/ob_define.h"
#include "share/ob_tenant_info_proxy.h"                // ObTenantRole
#include "logservice/applyservice/ob_log_apply_service.h"
#include "mittest/palf_cluster/rpc/palf_cluster_rpc_req.h"
#include "mittest/palf_cluster/rpc/palf_cluster_rpc_proxy.h"
#include "mittest/palf_cluster/logservice/ob_log_client.h"
#include "logservice/palf/log_block_pool_interface.h"             // ILogBlockPool
#include "logservice/palf/log_define.h"
#include "logservice/ob_log_monitor.h"
#include "logservice/ob_log_handler.h"
#include "logservice/ob_log_service.h"
#include "role_coordinator.h"
#include "ls_adapter.h"
// #include "replayservice/ob_log_replay_service.h"

namespace oceanbase
{
namespace commom
{
class ObAddr;
class ObILogAllocator;
}

namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}

namespace share
{
class ObLSID;
class SCN;
}

namespace palf
{
class PalfHandleGuard;
class PalfRoleChangeCb;
class PalfDiskOptions;
class PalfEnv;
}

namespace palfcluster
{

class LogService
{
public:
  LogService();
  virtual ~LogService();
  static int mtl_init(LogService* &logservice);
  static void mtl_destroy(LogService* &logservice);
  int start();
  void stop();
  void wait();
  void destroy();
public:
  static palf::AccessMode get_palf_access_mode(const share::ObTenantRole &tenant_role);
  int init(const palf::PalfOptions &options,
           const char *base_dir,
           const common::ObAddr &self,
           common::ObILogAllocator *alloc_mgr,
           rpc::frame::ObReqTransport *transport,
           palf::ILogBlockPool *log_block_pool);
  //--日志流相关接口--
  //新建日志流接口，该接口会创建日志流对应的目录，新建一个以PalfBaeInfo为日志基点的日志流。
  //其中包括生成并初始化对应的ObReplayStatus结构
  // @param [in] id，日志流标识符
  // @param [in] replica_type，日志流的副本类型
  // @param [in] tenant_role, 租户角色, 以此决定Palf使用模式(APPEND/RAW_WRITE)
  // @param [in] palf_base_info, 日志同步基点信息
  // @param [out] log_handler，新建日志流以logservice::ObLogHandler形式返回，保证上层使用日志流时的生命周期
  int create_ls(const share::ObLSID &id,
                const common::ObReplicaType &replica_type,
                const share::ObTenantRole &tenant_role,
                const palf::PalfBaseInfo &palf_base_info,
                const bool allow_log_sync,
                logservice::ObLogHandler &log_handler);

  //删除日志流接口:外层调用create_ls()之后，后续流程失败，需要调用remove_ls()
  int remove_ls(const share::ObLSID &id,
                logservice::ObLogHandler &log_handler);

  int check_palf_exist(const share::ObLSID &id, bool &exist) const;
  //宕机重启恢复日志流接口，包括生成并初始化对应的ObReplayStatus结构
  // @param [in] id，日志流标识符
  // @param [out] log_handler，新建日志流以logservice::ObLogHandler形式返回，保证上层使用日志流时的生命周期
  int add_ls(const share::ObLSID &id,
             logservice::ObLogHandler &log_handler);

  int open_palf(const share::ObLSID &id,
                palf::PalfHandleGuard &palf_handle);

  // get role of current palf replica.
  // NB: distinguish the difference from get_role of log_handler
  // In general, get the replica role to do migration/blance/report, use this interface,
  // to write log, use get_role of log_handler
  int get_palf_role(const share::ObLSID &id,
                    common::ObRole &role,
                    int64_t &proposal_id);

  // @brief get palf disk usage
  // @param [out] used_size_byte
  // @param [out] total_size_byte, if in shrinking status, total_size_byte is the value after shrinking.
  // NB: total_size_byte may be smaller than used_size_byte.
  int get_palf_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte);

  // @brief get palf disk usage
  // @param [out] used_size_byte
  // @param [out] total_size_byte, if in shrinking status, total_size_byte is the value before shrinking.
  int get_palf_stable_disk_usage(int64_t &used_size_byte, int64_t &total_size_byte);
  // why we need update 'log_disk_size_' and 'log_disk_util_threshold' separately.
  //
  // 'log_disk_size' is a member of unit config.
  // 'log_disk_util_threshold' and 'log_disk_util_limit_threshold' are members of tenant parameters.
  // If we just only provide 'update_disk_options', the correctness of PalfDiskOptions can not be guaranteed.
  // for example, original PalfDiskOptions is that
  // {
  //   log_disk_size = 100G,
  //   log_disk_util_limit_threshold = 95,
  //   log_disk_util_threshold = 80
  // }
  //
  // 1. thread1 update 'log_disk_size' with 50G, and it will used 'update_disk_options' with PalfDiskOptions
  // {
  //   log_disk_size = 50G,
  //   log_disk_util_limit_threshold = 95,
  //   log_disk_util_threshold = 80
  // }
  // 2. thread2 updaet 'log_disk_util_limit_threshold' with 85, and it will used 'update_disk_options' with PalfDiskOptions
  // {
  //   log_disk_size = 100G,
  //   log_disk_util_limit_threshold = 85,
  //   log_disk_util_threshold = 80
  // }.
  int update_palf_options_except_disk_usage_limit_size();
  int update_log_disk_usage_limit_size(const int64_t log_disk_usage_limit_size);
  int get_palf_options(palf::PalfOptions &options);
  int iterate_palf(const ObFunction<int(const palf::PalfHandle&)> &func);

  int get_io_start_time(int64_t &last_working_time);
  int check_disk_space_enough(bool &is_disk_enough);

  palf::PalfEnv *get_palf_env() { return palf_env_; }
  // TODO by yunlong: temp solution, will by removed after Reporter be added in MTL
  // ObLogReplayService *get_log_replay_service()  { return &replay_service_; }
  obrpc::PalfClusterRpcProxy *get_rpc_proxy() { return &rpc_proxy_; }
  ObAddr &get_self() { return self_; }

  int create_palf_replica(const int64_t palf_id,
                          const common::ObMemberList &member_list,
                          const int64_t replica_num,
                          const int64_t leader_idx);

  int create_log_clients(const int64_t thread_num,
                        const int64_t log_size,
                        const int64_t palf_group_num,
                        std::vector<common::ObAddr> leader_list);
public:
  palfcluster::LogClientMap *get_log_client_map() { return &log_client_map_; }
  static const int64_t THREAD_NUM = 2000;
  palfcluster::LogRemoteClient clients_[THREAD_NUM];
private:
  int create_ls_(const share::ObLSID &id,
                 const common::ObReplicaType &replica_type,
                 const share::ObTenantRole &tenant_role,
                 const palf::PalfBaseInfo &palf_base_info,
                 const bool allow_log_sync,
                 logservice::ObLogHandler &log_handler);
private:
  bool is_inited_;
  bool is_running_;

  common::ObAddr self_;
  palf::PalfEnv *palf_env_;

  logservice::ObLogApplyService apply_service_;
  logservice::ObLogReplayService replay_service_;
  palfcluster::RoleCoordinator role_change_service_;
  logservice::ObLogMonitor monitor_;
  obrpc::PalfClusterRpcProxy rpc_proxy_;
  ObSpinLock update_palf_opts_lock_;
  palfcluster::LogClientMap log_client_map_;
  palfcluster::MockLSAdapter ls_adapter_;
  logservice::ObLocationAdapter location_adapter_;
  obrpc::ObLogServiceRpcProxy log_service_rpc_proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogService);
};
} // end namespace palfcluster
} // end namespace oceanbase
#endif // OCEANBASE_PALF_CLUSTER_OB_LOG_SERVICE_
