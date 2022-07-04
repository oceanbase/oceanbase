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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_SERVICE_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_SERVICE_H_

#include "lib/thread_local/thread_buffer.h"
#include "lib/net/ob_addr.h"
#include "lib/thread/ob_work_queue.h"
#include "ob_log_archive_scheduler.h"

#include "share/backup/ob_backup_info_mgr.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_inner_config_root_addr.h"
#include "share/ob_root_addr_agent.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_remote_sql_proxy.h"
#include "share/ob_unit_replica_counter.h"
#include "share/partition_table/ob_inmemory_partition_table.h"

#include "sql/ob_index_sstable_builder.h"

#include "rpc/ob_packet.h"
#include "observer/ob_restore_ctx.h"
#include "rootserver/restore/ob_restore_scheduler.h"
#include "rootserver/restore/ob_restore_info.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_all_server_task.h"
#include "rootserver/ob_all_server_checker.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_root_major_freeze_v2.h"
#include "rootserver/ob_root_minor_freeze.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_daily_merge_scheduler.h"
#include "rootserver/ob_partition_table_util.h"
#include "rootserver/ob_leader_coordinator.h"
#include "rootserver/ob_major_freeze_launcher.h"
#include "rootserver/ob_freeze_info_updater.h"
#include "rootserver/ob_global_max_decided_trans_version_mgr.h"
#include "rootserver/ob_vtable_location_getter.h"
#include "rootserver/ob_root_balancer.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "rootserver/ob_rebalance_task.h"
#include "rootserver/ob_system_admin_util.h"
#include "rootserver/ob_rebalance_task_executor.h"
#include "rootserver/ob_root_inspection.h"
#include "rootserver/ob_lost_replica_checker.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_empty_server_checker.h"
#include "rootserver/ob_rs_thread_checker.h"
#include "rootserver/ob_freeze_info_manager.h"
#include "rootserver/ob_inner_table_monitor.h"
#include "rootserver/ob_global_index_builder.h"
#include "rootserver/ob_partition_spliter.h"
#include "rootserver/ob_rs_gts_manager.h"
#include "rootserver/ob_rs_gts_task_mgr.h"
#include "rootserver/ob_rs_gts_monitor.h"
#include "rootserver/ob_upgrade_storage_format_version_executor.h"
#include "rootserver/ob_schema_split_executor.h"
#include "rootserver/ob_upgrade_executor.h"
#include "rootserver/ob_upgrade_storage_format_version_executor.h"
#include "rootserver/ob_create_inner_schema_executor.h"
#include "rootserver/ob_schema_revise_executor.h"
#include "rootserver/ob_root_backup.h"
#include "rootserver/backup/ob_root_validate.h"
#include "rootserver/ob_update_rs_list_task.h"
#include "rootserver/ob_schema_history_recycler.h"
#include "rootserver/ob_backup_data_clean.h"
#include "rootserver/backup/ob_backup_auto_delete_expired_backup.h"
#include "rootserver/ob_single_partition_balance.h"
#include "rootserver/backup/ob_backup_lease_service.h"
#include "rootserver/ob_restore_point_service.h"
#include "rootserver/backup/ob_backup_archive_log_scheduler.h"
#include "rootserver/backup/ob_backup_backupset.h"
#include "rootserver/ob_ttl_scheduler.h"

namespace oceanbase {

namespace common {
class ObPacket;
class ObServerConfig;
class ObConfigManager;
class ObMySQLProxy;
class ObRequestTZInfoResult;
class ObRequestTZInfoArg;
class ObPartitionKey;
class ObString;
}  // namespace common

namespace share {

namespace status {
enum ObRootServiceStatus;
}
class ObAutoincrementService;
namespace schema {
class ObMultiVersionSchemaService;
class ObTenantSchema;
class ObDatabaseSchema;
class ObTablegroupSchema;
class ObTableSchema;
class ObSchemaGetterGuard;
}  // namespace schema
class ObPartitionTableOperator;
class ObPartitionInfo;
class ObPartitionReplica;
class ObIPartitionLocationCache;
class ObRsMgr;
class ObRemotePartitionTableOperator;
}  // namespace share

namespace obrpc {
class ObSrvRpcProxy;
class ObCommonRpcProxy;
struct ObGetSwitchoverStatusRes;
}  // namespace obrpc
namespace storage {
class ObMajorFreeze;
}

namespace rootserver {
class ObSinglePartBalance;
class ObRsStatus {
public:
  ObRsStatus() : rs_status_(share::status::INIT)
  {}
  virtual ~ObRsStatus()
  {}
  int set_rs_status(const share::status::ObRootServiceStatus status);
  share::status::ObRootServiceStatus get_rs_status() const;
  bool need_do_restart() const;
  bool can_start_service() const;
  bool is_start() const;
  bool is_stopping() const;
  bool is_full_service() const;
  bool in_service() const;
  bool is_need_stop() const;
  int revoke_rs();
  int try_set_stopping();

private:
  common::SpinRWLock lock_;
  share::status::ObRootServiceStatus rs_status_;
};
// Root Service Entry Class
class ObRootService {
public:
  friend class TestRootServiceCreateTable_check_rs_capacity_Test;
  friend class ObStandbyIneffectSchemaTask;
  friend class ObTenantWrsTask;

  class ObStartStopServerTask : public share::ObAsyncTask {
  public:
    ObStartStopServerTask(ObRootService& root_service, const common::ObAddr& server, const bool start)
        : root_service_(root_service), server_(server), start_(start)
    {}
    virtual ~ObStartStopServerTask()
    {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

  private:
    ObRootService& root_service_;
    const common::ObAddr server_;
    const bool start_;
  };

  class ObOfflineServerTask : public share::ObAsyncTask {
  public:
    ObOfflineServerTask(ObRootService& root_service, const common::ObAddr& server)
        : root_service_(root_service), server_(server)
    {
      set_retry_times(0); /*not repeat*/
    }
    virtual ~ObOfflineServerTask()
    {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

  private:
    ObRootService& root_service_;
    const common::ObAddr server_;
  };

  class ObMergeErrorTask : public share::ObAsyncTask {
  public:
    explicit ObMergeErrorTask(ObRootService& root_service) : root_service_(root_service)
    {}
    virtual ~ObMergeErrorTask()
    {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

  private:
    ObRootService& root_service_;
  };

  class ObStatisticPrimaryZoneCountTask : public share::ObAsyncTask {
  public:
    explicit ObStatisticPrimaryZoneCountTask(ObRootService& root_service) : root_service_(root_service)
    {}
    virtual ~ObStatisticPrimaryZoneCountTask()
    {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

  private:
    ObRootService& root_service_;
  };

  class ObCreateHaGtsUtilTask : public share::ObAsyncTask {
  public:
    explicit ObCreateHaGtsUtilTask(ObRootService& root_service) : root_service_(root_service)
    {}
    virtual ~ObCreateHaGtsUtilTask()
    {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

  private:
    ObRootService& root_service_;
  };

  class ObRefreshServerTask : public common::ObAsyncTimerTask {
  public:
    const static int64_t REFRESH_SERVER_INTERVAL = 1L * 1000 * 1000;  // 1 second
    explicit ObRefreshServerTask(ObRootService& root_service);
    virtual ~ObRefreshServerTask(){};
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObRefreshServerTask);
  };

  class ObStatusChangeCallback : public ObIStatusChangeCallback {
  public:
    explicit ObStatusChangeCallback(ObRootService& root_service);
    virtual ~ObStatusChangeCallback();

    virtual int wakeup_balancer() override;
    virtual int wakeup_daily_merger() override;
    virtual int on_server_status_change(const common::ObAddr& server) override;
    virtual int on_start_server(const common::ObAddr& server) override;
    virtual int on_stop_server(const common::ObAddr& server) override;
    virtual int on_offline_server(const common::ObAddr& server) override;

  private:
    ObRootService& root_service_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObStatusChangeCallback);
  };

  class ObServerChangeCallback : public ObIServerChangeCallback {
  public:
    explicit ObServerChangeCallback(ObRootService& root_service) : root_service_(root_service)
    {}
    virtual ~ObServerChangeCallback()
    {}

    virtual int on_server_change() override;

  private:
    ObRootService& root_service_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObServerChangeCallback);
  };

  class ObRestartTask : public common::ObAsyncTimerTask {
  public:
    explicit ObRestartTask(ObRootService& root_service);
    virtual ~ObRestartTask();

    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& root_service_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObRestartTask);
  };

  class ObReportCoreTableReplicaTask : public common::ObAsyncTimerTask {
  public:
    explicit ObReportCoreTableReplicaTask(ObRootService& root_service);
    virtual ~ObReportCoreTableReplicaTask()
    {}

  public:
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObReportCoreTableReplicaTask);
  };

  class ObReloadUnitManagerTask : public common::ObAsyncTimerTask {
  public:
    explicit ObReloadUnitManagerTask(ObRootService& root_service, ObUnitManager& unit_manager);
    virtual ~ObReloadUnitManagerTask()
    {}

  public:
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& root_service_;
    ObUnitManager& unit_manager_;
    DISALLOW_COPY_AND_ASSIGN(ObReloadUnitManagerTask);
  };

  class ObLoadIndexBuildTask : public common::ObAsyncTimerTask {
  public:
    ObLoadIndexBuildTask(ObRootService& root_service, ObGlobalIndexBuilder& global_index_builder);
    virtual ~ObLoadIndexBuildTask() = default;
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& root_service_;
    ObGlobalIndexBuilder& global_index_builder_;
  };

  class ObUpdateAllServerConfigTask : public common::ObAsyncTimerTask {
  public:
    explicit ObUpdateAllServerConfigTask(ObRootService& root_service);
    virtual ~ObUpdateAllServerConfigTask()
    {}

  public:
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObUpdateAllServerConfigTask);
  };

  class ObSelfCheckTask : public common::ObAsyncTimerTask {
  public:
    explicit ObSelfCheckTask(ObRootService& root_service);
    virtual ~ObSelfCheckTask(){};
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    void print_error_log();

  private:
    ObRootService& root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObSelfCheckTask);
  };

  class ObIndexSSTableBuildTask : public share::ObAsyncTask {
  public:
    ObIndexSSTableBuildTask(const sql::ObIndexSSTableBuilder::BuildIndexJob& job,
        sql::ObIndexSSTableBuilder::ReplicaPicker& replica_picker, ObGlobalIndexBuilder& global_index_builder,
        common::ObMySQLProxy& sql_proxy, common::ObOracleSqlProxy& oracle_sql_proxy, const int64_t abs_timeout_us)
        : job_(job),
          replica_picker_(replica_picker),
          global_index_builder_(global_index_builder),
          sql_proxy_(sql_proxy),
          oracle_sql_proxy_(oracle_sql_proxy),
          abs_timeout_us_(abs_timeout_us)
    {
      set_retry_times(0);
    }

    virtual int process() override;

    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }

    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

    sql::ObIndexSSTableBuilder::BuildIndexJob job_;
    sql::ObIndexSSTableBuilder::ReplicaPicker& replica_picker_;
    ObGlobalIndexBuilder& global_index_builder_;
    common::ObMySQLProxy& sql_proxy_;
    common::ObOracleSqlProxy& oracle_sql_proxy_;
    int64_t abs_timeout_us_;

    DISALLOW_COPY_AND_ASSIGN(ObIndexSSTableBuildTask);
  };

  class RsListChangeCb : public share::ObIRsListChangeCb {
  public:
    explicit RsListChangeCb(ObRootService& rs) : rs_(rs)
    {}
    virtual ~RsListChangeCb()
    {}
    // interface of ObIRsListChangeCb
    virtual int submit_update_rslist_task(const bool force_update = false) override
    {
      return rs_.submit_update_rslist_task(force_update);
    }
    virtual int submit_report_replica() override
    {
      return rs_.report_replica();
    }
    virtual int submit_report_replica(const common::ObPartitionKey& key) override
    {
      return rs_.report_single_replica(key);
    }

  private:
    ObRootService& rs_;
    DISALLOW_COPY_AND_ASSIGN(RsListChangeCb);
  };

  class MergeErrorCallback : public share::ObIMergeErrorCb {
  public:
    MergeErrorCallback(ObRootService& rs) : rs_(rs)
    {}
    virtual ~MergeErrorCallback()
    {}
    virtual int submit_merge_error_task() override
    {
      return rs_.submit_merge_error_task();
    }

  private:
    // data members
    ObRootService& rs_;
    DISALLOW_COPY_AND_ASSIGN(MergeErrorCallback);
  };

  class ObInnerTableMonitorTask : public common::ObAsyncTimerTask {
  public:
    const static int64_t PURGE_INTERVAL = 3600L * 1000L * 1000L;  // 1h
    ObInnerTableMonitorTask(ObRootService& rs);
    virtual ~ObInnerTableMonitorTask()
    {}

    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& rs_;
    DISALLOW_COPY_AND_ASSIGN(ObInnerTableMonitorTask);
  };
  class ObSwitchOverToFollowerTask : public share::ObAsyncTask {
  public:
    const static int64_t RETRY_INTERVAL = 1 * 1000L * 1000L;  // 1s
    explicit ObSwitchOverToFollowerTask(ObRootService& rs);
    virtual ~ObSwitchOverToFollowerTask()
    {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const
    {
      return sizeof(*this);
    }
    virtual share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

  private:
    ObRootService& rs_;
    DISALLOW_COPY_AND_ASSIGN(ObSwitchOverToFollowerTask);
  };

  class ObPrimaryClusterInspectionTask : public share::ObAsyncTask {
  public:
    const static int64_t RETRY_INTERVAL = 100 * 1000L * 1000L;  // 100s
    explicit ObPrimaryClusterInspectionTask(ObRootService& rs);
    virtual ~ObPrimaryClusterInspectionTask()
    {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const
    {
      return sizeof(*this);
    }
    virtual share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

  private:
    ObRootService& rs_;
    DISALLOW_COPY_AND_ASSIGN(ObPrimaryClusterInspectionTask);
  };

  class ObGenNextSchemaVersionTask : public common::ObAsyncTimerTask {
  public:
    const static int64_t RETRY_INTERVAL = 2 * 1000L * 1000L;  // 2s
    ObGenNextSchemaVersionTask(ObRootService& rs);
    virtual ~ObGenNextSchemaVersionTask()
    {}

    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& rs_;
    DISALLOW_COPY_AND_ASSIGN(ObGenNextSchemaVersionTask);
  };

  class ObLostReplicaCheckerTask : public share::ObAsyncTask {
  public:
    const static int64_t RETRY_INTERVAL = 3 * 1000L * 1000L;  // 10s
    ObLostReplicaCheckerTask(ObRootService& rs) : rs_(rs)
    {}
    virtual ~ObLostReplicaCheckerTask()
    {}

    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& rs_;
    DISALLOW_COPY_AND_ASSIGN(ObLostReplicaCheckerTask);
  };

  class ObMinorFreezeTask : public share::ObAsyncTask {
  public:
    explicit ObMinorFreezeTask(const obrpc::ObRootMinorFreezeArg& arg) : arg_(arg)
    {}
    virtual ~ObMinorFreezeTask()
    {}
    virtual int process();
    virtual int64_t get_deep_copy_size() const;
    share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;

  private:
    obrpc::ObRootMinorFreezeArg arg_;
  };

  class ObUpdateClusterInfoTask : public common::ObAsyncTimerTask {
  public:
    const static int64_t REFRESH_SERVER_INTERVAL = 100L * 1000 * 1000;  // 100 second
    explicit ObUpdateClusterInfoTask(ObRootService& root_service);
    virtual ~ObUpdateClusterInfoTask(){};
    // interface of AsyncTask
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObUpdateClusterInfoTask);
  };

  class ObReloadUnitReplicaCounterTask : public common::ObAsyncTimerTask {
  public:
    const static int64_t REFRESH_UNIT_INTERVAL = 60 * 10L * 1000 * 1000;  // 1 second
    explicit ObReloadUnitReplicaCounterTask(ObRootService& root_service);
    virtual ~ObReloadUnitReplicaCounterTask(){};
    virtual int process() override;
    virtual int64_t get_deep_copy_size() const override
    {
      return sizeof(*this);
    }
    virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

  private:
    ObRootService& root_service_;
    DISALLOW_COPY_AND_ASSIGN(ObReloadUnitReplicaCounterTask);
  };

public:
  ObRootService();
  virtual ~ObRootService();
  void reset_fail_count();
  void update_fail_count(int ret);
  int fake_init(common::ObServerConfig& config, common::ObConfigManager& config_mgr, obrpc::ObSrvRpcProxy& rpc_proxy,
      obrpc::ObCommonRpcProxy& common_proxy, common::ObAddr& self, common::ObMySQLProxy& sql_proxy,
      share::ObRsMgr& rs_mgr, share::schema::ObMultiVersionSchemaService* schema_mgr_,
      share::ObPartitionTableOperator& pt_operator_, share::ObRemotePartitionTableOperator& remote_pt_operator_,
      share::ObIPartitionLocationCache& location_cache);

  int init(common::ObServerConfig& config, common::ObConfigManager& config_mgr, obrpc::ObSrvRpcProxy& rpc_proxy,
      obrpc::ObCommonRpcProxy& common_proxy, common::ObAddr& self, common::ObMySQLProxy& sql_proxy,

      share::ObRemoteSqlProxy& remote_sql_proxy, observer::ObRestoreCtx& restore_ctx, share::ObRsMgr& rs_mgr,
      share::schema::ObMultiVersionSchemaService* schema_mgr_, share::ObPartitionTableOperator& pt_operator_,
      share::ObRemotePartitionTableOperator& remote_pt_operator_, share::ObIPartitionLocationCache& location_cache);
  inline bool is_inited() const
  {
    return inited_;
  }
  void destroy();

  // add virtual make the following functions mockable
  virtual int start_service();
  int revoke_rs();
  bool is_need_stop() const;
  virtual int stop_service();
  virtual int stop();
  virtual void wait();
  virtual bool in_service() const;
  bool need_do_restart() const;
  int set_rs_status(const share::status::ObRootServiceStatus status);
  virtual bool is_full_service() const;
  virtual bool is_major_freeze_done() const
  {
    return is_full_service();
  }
  virtual bool is_ddl_allowed() const
  {
    return is_full_service();
  }
  bool can_start_service() const;
  bool is_stopping() const;
  bool is_start() const;
  share::status::ObRootServiceStatus get_status() const;
  bool in_debug() const
  {
    return debug_;
  }
  void set_debug()
  {
    debug_ = true;
  }
  int reload_config();
  virtual bool check_config(const ObConfigItem& item, const char*& err_info);
  // misc get functions
  share::ObPartitionTableOperator& get_pt_operator()
  {
    return *pt_operator_;
  }
  share::ObRemotePartitionTableOperator& get_remote_pt_operator()
  {
    return *remote_pt_operator_;
  }
  share::schema::ObMultiVersionSchemaService& get_schema_service()
  {
    return *schema_service_;
  }
  ObServerManager& get_server_mgr()
  {
    return server_manager_;
  }
  ObZoneManager& get_zone_mgr()
  {
    return zone_manager_;
  }
  ObUnitManager& get_unit_mgr()
  {
    return unit_manager_;
  }
  ObRebalanceTaskMgr& get_rebalance_task_mgr()
  {
    return rebalance_task_mgr_;
  }
  ObRootBalancer& get_root_balancer()
  {
    return root_balancer_;
  }
  ObDailyMergeScheduler& get_daily_merge_scheduler()
  {
    return daily_merge_scheduler_;
  }
  ObRestoreScheduler& get_restore_scheduler()
  {
    return restore_scheduler_;
  }
  ObRootInspection& get_root_inspection()
  {
    return root_inspection_;
  }
  ObLeaderCoordinator& get_leader_coordinator()
  {
    return leader_coordinator_;
  }
  common::ObMySQLProxy& get_sql_proxy()
  {
    return sql_proxy_;
  }
  share::ObRemoteSqlProxy& get_remote_sql_proxy()
  {
    return *remote_sql_proxy_;
  }
  obrpc::ObCommonRpcProxy& get_common_rpc_proxy()
  {
    return common_proxy_;
  }
  obrpc::ObSrvRpcProxy& get_rpc_proxy()
  {
    return rpc_proxy_;
  }
  common::ObWorkQueue& get_task_queue()
  {
    return task_queue_;
  }
  common::ObWorkQueue& get_inspect_task_queue()
  {
    return inspect_task_queue_;
  }
  common::ObServerConfig* get_server_config()
  {
    return config_;
  }
  ObGlobalIndexBuilder& get_global_index_builder()
  {
    return global_index_builder_;
  }
  int64_t get_core_meta_table_version()
  {
    return core_meta_table_version_;
  }
  ObSchemaHistoryRecycler& get_schema_history_recycler()
  {
    return schema_history_recycler_;
  }
  int admin_clear_balance_task(const obrpc::ObAdminClearBalanceTaskArg& arg);
  int check_has_enough_member(const common::ObIArray<common::ObAddr>& server_list, bool& has_enough_member);
  int check_has_need_offline_replica(
      obrpc::ObTenantSchemaVersions& tenant_schema_versions, bool& has_need_offline_replica);
  int check_server_have_enough_resource_for_delete_server(const ObIArray<ObAddr>& servers, const ObZone& zone);

  // not implemented rpc, helper function for rs rpc processor define.
  int not_implement();

  int execute_bootstrap(const obrpc::ObBootstrapArg& arg);
  int set_disable_ps_config();
  int set_max_trx_size_config();
  // one phase commit
  int set_1pc_config();
  int set_balance_strategy_config();
  int check_config_result(const char* name, const char* value);
  int set_enable_oracle_priv_check();

  int check_ddl_allowed();
  int create_sys_index_table();

  int renew_lease(const share::ObLeaseRequest& lease_request, share::ObLeaseResponse& lease_response);
  int get_root_partition(share::ObPartitionInfo& partition_info);
  int report_root_partition(const share::ObPartitionReplica& replica);
  int remove_root_partition(const common::ObAddr& server);
  int rebuild_root_partition(const common::ObAddr& server);
  int clear_rebuild_root_partition(const common::ObAddr& server);
  int fetch_location(const obrpc::UInt64& table_id, common::ObSArray<share::ObPartitionLocation>& locations);
  int merge_error(const obrpc::ObMergeErrorArg& arg);
  int handle_merge_error();
  int merge_finish(const obrpc::ObMergeFinishArg& arg);

  int try_block_server(int rc, const common::ObAddr& server);

  int statistic_primary_zone_entity_count();

  int upgrade_cluster_create_ha_gts_util();

  // balance over
  template <typename TASK, typename T>
  int do_receive_balance_over(TASK& task, const T& arg);

  int receive_balance_over(const obrpc::ObMigrateReplicaRes& res)
  {
    ObMigrateReplicaTask task;
    return do_receive_balance_over(task, res);
  }
  int receive_balance_over(const obrpc::ObChangeReplicaRes& res)
  {
    ObTypeTransformTask task;
    return do_receive_balance_over(task, res);
  }
  int receive_balance_over(const obrpc::ObAddReplicaRes& res)
  {
    ObAddReplicaTask task;
    return do_receive_balance_over(task, res);
  }
  int receive_balance_over(const obrpc::ObRebuildReplicaRes& res)
  {
    ObRebuildReplicaTask task;
    return do_receive_balance_over(task, res);
  }
  int receive_balance_over(const obrpc::ObRestoreReplicaRes& res)
  {
    ObRestoreTask task;
    return do_receive_balance_over(task, res);
  }
  int receive_balance_over(const obrpc::ObPhyRestoreReplicaRes& res)
  {
    ObPhysicalRestoreTask task;
    return do_receive_balance_over(task, res);
  }
  int receive_balance_over(const obrpc::ObValidateRes& res)
  {
    ObValidateTask task;
    root_validate_.wakeup();
    return do_receive_balance_over(task, res);
  }
  // batch balance over
  template <typename TASK, typename T>
  int do_receive_batch_balance_over(TASK& task, const T& arg);

  int receive_batch_balance_over(const obrpc::ObMigrateReplicaBatchRes& res)
  {
    ObMigrateReplicaTask task;
    return do_receive_batch_balance_over(task, res);
  }
  int receive_batch_balance_over(const obrpc::ObChangeReplicaBatchRes& res)
  {
    ObTypeTransformTask task;
    return do_receive_batch_balance_over(task, res);
  }
  int receive_batch_balance_over(const obrpc::ObAddReplicaBatchRes& res)
  {
    ObAddReplicaTask task;
    return do_receive_batch_balance_over(task, res);
  }
  int receive_batch_balance_over(const obrpc::ObRebuildReplicaBatchRes& res)
  {
    ObRebuildReplicaTask task;
    return do_receive_batch_balance_over(task, res);
  }
  int receive_batch_balance_over(const obrpc::ObStandbyCutDataBatchTaskRes& res)
  {
    UNUSED(res);
    return OB_NOT_SUPPORTED;
  }
  int receive_batch_balance_over(const obrpc::ObCopySSTableBatchRes& res)
  {
    ObCopySSTableTask task;
    return do_receive_batch_balance_over(task, res);
  }
  int receive_batch_balance_over(const obrpc::ObBackupBatchRes& res)
  {
    ObBackupTask task;
    bool has_failed_task = false;
    for (int64_t i = 0; i < res.res_array_.count() && !has_failed_task; ++i) {
      const obrpc::ObBackupRes& backup_res = res.res_array_.at(i);
      if (OB_SUCCESS != backup_res.result_) {
        has_failed_task = true;
      }
    }
    if (!has_failed_task) {
      root_backup_.wakeup();
    }
    return do_receive_batch_balance_over(task, res);
  }
  int receive_batch_balance_over(const obrpc::ObValidateBatchRes& res)
  {
    ObValidateTask task;
    root_validate_.wakeup();
    return do_receive_batch_balance_over(task, res);
  }
  int receive_batch_balance_over(const obrpc::ObBackupBackupsetBatchRes& res)
  {
    ObBackupBackupsetTask task;
    backup_backupset_.wakeup();
    return do_receive_batch_balance_over(task, res);
  }

  int observer_copy_local_index_sstable(const obrpc::ObServerCopyLocalIndexSSTableArg& arg);

  int schedule_sql_bkgd_task(const sql::ObSchedBKGDDistTask& bkgd_task);
  int sql_bkgd_task_execute_over(const sql::ObBKGDTaskCompleteArg& arg);

  // int add_replica_over(const obrpc::ObAddReplicaRes &arg);
  // int migrate_replica_over(const obrpc::ObMigrateReplicaRes &arg);
  int admin_rebuild_replica(const obrpc::ObAdminRebuildReplicaArg& arg);
  int broadcast_ds_action(const obrpc::ObDebugSyncActionArg& arg);
  int sync_pt_finish(const obrpc::ObSyncPartitionTableFinishArg& arg);
  int sync_pg_pt_finish(const obrpc::ObSyncPGPartitionMTFinishArg& arg);
  int check_dangling_replica_finish(const obrpc::ObCheckDanglingReplicaFinishArg& arg);
  int fetch_alive_server(const obrpc::ObFetchAliveServerArg& arg, obrpc::ObFetchAliveServerResult& result);
  int get_tenant_schema_versions(
      const obrpc::ObGetSchemaArg& arg, obrpc::ObTenantSchemaVersions& tenant_schema_versions);

  // ddl related
  int create_resource_unit(const obrpc::ObCreateResourceUnitArg& arg);
  int alter_resource_unit(const obrpc::ObAlterResourceUnitArg& arg);
  int drop_resource_unit(const obrpc::ObDropResourceUnitArg& arg);
  int create_resource_pool(const obrpc::ObCreateResourcePoolArg& arg);
  int alter_resource_pool(const obrpc::ObAlterResourcePoolArg& arg);
  int drop_resource_pool(const obrpc::ObDropResourcePoolArg& arg);
  int split_resource_pool(const obrpc::ObSplitResourcePoolArg& arg);
  int merge_resource_pool(const obrpc::ObMergeResourcePoolArg& arg);
  int create_tenant(const obrpc::ObCreateTenantArg& arg, obrpc::UInt64& tenant_id);
  int create_tenant_end(const obrpc::ObCreateTenantEndArg& arg);
  int commit_alter_tenant_locality(const rootserver::ObCommitAlterTenantLocalityArg& arg);
  int commit_alter_tablegroup_locality(const rootserver::ObCommitAlterTablegroupLocalityArg& arg);
  int commit_alter_table_locality(const rootserver::ObCommitAlterTableLocalityArg& arg);
  int drop_tenant(const obrpc::ObDropTenantArg& arg);
  int flashback_tenant(const obrpc::ObFlashBackTenantArg& arg);
  int purge_tenant(const obrpc::ObPurgeTenantArg& arg);
  int modify_tenant(const obrpc::ObModifyTenantArg& arg);
  int lock_tenant(const obrpc::ObLockTenantArg& arg);
  int modify_system_variable(const obrpc::ObModifySysVarArg& arg);
  int add_system_variable(const obrpc::ObAddSysVarArg& arg);
  int create_database(const obrpc::ObCreateDatabaseArg& arg, obrpc::UInt64& db_id);
  int create_tablegroup(const obrpc::ObCreateTablegroupArg& arg, obrpc::UInt64& tg_id);
  int get_frozen_status(const obrpc::Int64& arg, storage::ObFrozenStatus& frozen_status);
  int create_table(const obrpc::ObCreateTableArg& arg, obrpc::ObCreateTableRes& res);
  int alter_database(const obrpc::ObAlterDatabaseArg& arg);
  int alter_table(const obrpc::ObAlterTableArg& arg, obrpc::ObAlterTableRes& res);
  int alter_tablegroup(const obrpc::ObAlterTablegroupArg& arg);
  int rename_table(const obrpc::ObRenameTableArg& arg);
  int truncate_table(const obrpc::ObTruncateTableArg& arg);
  int create_index(const obrpc::ObCreateIndexArg& arg, obrpc::ObAlterTableRes& res);
  int drop_table(const obrpc::ObDropTableArg& arg);
  int drop_database(const obrpc::ObDropDatabaseArg& arg, obrpc::UInt64& affected_row);
  int drop_tablegroup(const obrpc::ObDropTablegroupArg& arg);
  int drop_index(const obrpc::ObDropIndexArg& arg);
  int rebuild_index(const obrpc::ObRebuildIndexArg& arg, obrpc::ObAlterTableRes& res);
  int submit_build_index_task(const obrpc::ObSubmitBuildIndexTaskArg &arg);
  // the interface only for switchover: execute skip check enable_ddl
  int force_drop_index(const obrpc::ObDropIndexArg& arg);
  int flashback_index(const obrpc::ObFlashBackIndexArg& arg);
  int purge_index(const obrpc::ObPurgeIndexArg& arg);
  int create_table_like(const obrpc::ObCreateTableLikeArg& arg);
  int refresh_config();
  int get_frozen_version(obrpc::Int64& frozen_version);
  int root_major_freeze(const obrpc::ObRootMajorFreezeArg& arg);
  int root_minor_freeze(const obrpc::ObRootMinorFreezeArg& arg);
  int update_index_status(const obrpc::ObUpdateIndexStatusArg& arg);
  int purge_table(const obrpc::ObPurgeTableArg& arg);
  int flashback_table_from_recyclebin(const obrpc::ObFlashBackTableFromRecyclebinArg& arg);
  int purge_database(const obrpc::ObPurgeDatabaseArg& arg);
  int flashback_database(const obrpc::ObFlashBackDatabaseArg& arg);
  int check_tenant_in_alter_locality(const uint64_t tenant_id, bool& in_alter_locality);
  int root_split_partition(const obrpc::ObRootSplitPartitionArg& arg);

  int create_restore_point(const obrpc::ObCreateRestorePointArg& arg);
  int drop_restore_point(const obrpc::ObDropRestorePointArg& arg);
  // for inner table monitor, purge in fixed time
  int purge_expire_recycle_objects(const obrpc::ObPurgeRecycleBinArg& arg, obrpc::Int64& affected_rows);
  int check_unique_index_response(const obrpc::ObCheckUniqueIndexResponseArg& arg);
  int calc_column_checksum_repsonse(const obrpc::ObCalcColumnChecksumResponseArg& arg);
  int optimize_table(const obrpc::ObOptimizeTableArg& arg);

  //----Functions for managing privileges----
  int create_user(obrpc::ObCreateUserArg& arg, common::ObSArray<int64_t>& failed_index);
  int drop_user(const obrpc::ObDropUserArg& arg, common::ObSArray<int64_t>& failed_index);
  int rename_user(const obrpc::ObRenameUserArg& arg, common::ObSArray<int64_t>& failed_index);
  int set_passwd(const obrpc::ObSetPasswdArg& arg);
  int grant(const obrpc::ObGrantArg& arg);
  int revoke_user(const obrpc::ObRevokeUserArg& arg);
  int lock_user(const obrpc::ObLockUserArg& arg, common::ObSArray<int64_t>& failed_index);
  int revoke_database(const obrpc::ObRevokeDBArg& arg);
  int revoke_table(const obrpc::ObRevokeTableArg& arg);
  int revoke_syspriv(const obrpc::ObRevokeSysPrivArg& arg);
  int alter_user_profile(const obrpc::ObAlterUserProfileArg& arg);
  //----End of functions for managing privileges----

  //----Functions for managing outlines----
  int create_outline(const obrpc::ObCreateOutlineArg& arg);
  int alter_outline(const obrpc::ObAlterOutlineArg& arg);
  int drop_outline(const obrpc::ObDropOutlineArg& arg);
  //----End of functions for managing outlines----

  //----Functions for managing UDF----
  int create_user_defined_function(const obrpc::ObCreateUserDefinedFunctionArg& arg);
  int drop_user_defined_function(const obrpc::ObDropUserDefinedFunctionArg& arg);
  //----End of functions for managing UDF----
  //----Functions for managing dblinks----
  int create_dblink(const obrpc::ObCreateDbLinkArg& arg);
  int drop_dblink(const obrpc::ObDropDbLinkArg& arg);
  //----End of functions for managing dblinks----

  //----Functions for managing synonyms----
  int create_synonym(const obrpc::ObCreateSynonymArg& arg);
  int drop_synonym(const obrpc::ObDropSynonymArg& arg);
  //----End of functions for managing synonyms----

  //----Functions for managing sequence----
  // create alter drop actions all in one, avoid noodle-like code
  int do_sequence_ddl(const obrpc::ObSequenceDDLArg& arg);
  //----End of functions for managing sequence----

  //----Functions for managing profile----
  int do_profile_ddl(const obrpc::ObProfileDDLArg& arg);
  //----End of functions for managing sequence----

  // server related
  int add_server(const obrpc::ObAdminServerArg& arg);
  int delete_server(const obrpc::ObAdminServerArg& arg);
  int cancel_delete_server(const obrpc::ObAdminServerArg& arg);
  int start_server(const obrpc::ObAdminServerArg& arg);
  int stop_server(const obrpc::ObAdminServerArg& arg);

  // zone related
  int add_zone(const obrpc::ObAdminZoneArg& arg);
  int delete_zone(const obrpc::ObAdminZoneArg& arg);
  int start_zone(const obrpc::ObAdminZoneArg& arg);
  int stop_zone(const obrpc::ObAdminZoneArg& arg);
  int alter_zone(const obrpc::ObAdminZoneArg& arg);

  // gts replica stop server/zone related
  int check_gts_replica_enough_when_stop_server(
      const obrpc::ObCheckGtsReplicaStopServer& server_need_stopped, obrpc::Bool& can_stop);
  int check_gts_replica_enough_when_stop_zone(
      const obrpc::ObCheckGtsReplicaStopZone& zone_need_stopped, obrpc::Bool& can_stop);

  // system admin command (alter system ...)
  int admin_switch_replica_role(const obrpc::ObAdminSwitchReplicaRoleArg& arg);
  int admin_switch_rs_role(const obrpc::ObAdminSwitchRSRoleArg& arg);
  int admin_drop_replica(const obrpc::ObAdminDropReplicaArg& arg);
  int admin_change_replica(const obrpc::ObAdminChangeReplicaArg& arg);
  int admin_migrate_replica(const obrpc::ObAdminMigrateReplicaArg& arg);
  int admin_report_replica(const obrpc::ObAdminReportReplicaArg& arg);
  int admin_recycle_replica(const obrpc::ObAdminRecycleReplicaArg& arg);
  int admin_merge(const obrpc::ObAdminMergeArg& arg);
  int admin_clear_roottable(const obrpc::ObAdminClearRoottableArg& arg);
  int admin_refresh_schema(const obrpc::ObAdminRefreshSchemaArg& arg);
  int admin_set_config(obrpc::ObAdminSetConfigArg& arg);
  int admin_clear_location_cache(const obrpc::ObAdminClearLocationCacheArg& arg);
  int admin_reload_gts();
  int admin_refresh_memory_stat(const obrpc::ObAdminRefreshMemStatArg& arg);
  int admin_reload_unit();
  int admin_reload_server();
  int admin_reload_zone();
  int admin_clear_merge_error();
  int admin_migrate_unit(const obrpc::ObAdminMigrateUnitArg& arg);
  int admin_upgrade_virtual_schema();
  int run_job(const obrpc::ObRunJobArg& arg);
  int run_upgrade_job(const obrpc::ObUpgradeJobArg& arg);
  int admin_flush_cache(const obrpc::ObAdminFlushCacheArg& arg);
  int admin_upgrade_cmd(const obrpc::Bool& arg);
  int admin_rolling_upgrade_cmd(const obrpc::ObAdminRollingUpgradeArg& arg);
  int admin_set_tracepoint(const obrpc::ObAdminSetTPArg& arg);
  int restore_tenant(const obrpc::ObRestoreTenantArg& arg);
  /* physical restore */
  int physical_restore_tenant(const obrpc::ObPhysicalRestoreTenantArg& arg);
  int rebuild_index_in_restore(const obrpc::ObRebuildIndexInRestoreArg& arg);
  /*-----------------*/
  int admin_load_baseline(const obrpc::ObAdminLoadBaselineArg& arg);
  int refresh_time_zone_info(const obrpc::ObRefreshTimezoneArg& arg);
  int request_time_zone_info(const common::ObRequestTZInfoArg& arg, common::ObRequestTZInfoResult& result);
  // async tasks and callbacks
  // @see ObStatusChangeCallback
  int submit_update_all_server_task(const common::ObAddr& server);
  int submit_start_server_task(const common::ObAddr& server);
  int submit_stop_server_task(const common::ObAddr& server);
  int submit_offline_server_task(const common::ObAddr& server);
  int submit_report_core_table_replica_task();
  int submit_reload_unit_manager_task();
  int report_replica();
  int report_single_replica(const common::ObPartitionKey& key);
  // @see RsListChangeCb
  int submit_update_rslist_task(const bool force_update = false);
  // @see ObIMergeErrorCb
  int submit_merge_error_task();
  int submit_schema_split_task();
  int submit_schema_split_task_v2();
  int submit_upgrade_task(const int64_t version);
  int submit_upgrade_storage_format_version_task();
  int submit_create_inner_schema_task();
  int submit_schema_revise_task();
  int submit_statistic_primary_zone_count();
  int submit_create_ha_gts_util();
  int submit_async_minor_freeze_task(const obrpc::ObRootMinorFreezeArg& arg);
  // @see ObLostReplicaCheckerTask
  int submit_lost_replica_checker_task();
  int submit_update_all_server_config_task();

  int submit_index_sstable_build_task(const sql::ObIndexSSTableBuilder::BuildIndexJob& job,
      sql::ObIndexSSTableBuilder::ReplicaPicker& replica_picker, const int64_t abs_timeout_us);
  // may modify arg before taking effect
  int set_config_pre_hook(obrpc::ObAdminSetConfigArg& arg);
  // arg is readonly after take effect
  int set_config_post_hook(const obrpc::ObAdminSetConfigArg& arg);
  int wakeup_auto_delete(const obrpc::ObAdminSetConfigItem *item);

  // @see ObReplicaControlCleanTask
  int submit_replica_control_clean_task();

  // @see ObDelSvrWorkingDirCleanTask
  int submit_del_svr_working_dir_clean_task();

  // @see ObBlkRecycleScheduleTask
  int submit_blk_recycle_schedule_task();

  //@see ObBlacklistProcess
  int submit_black_list_task(const int64_t count, const common::ObIArray<uint64_t>& table_ids,
      const common::ObIArray<int64_t>& partition_ids, const common::ObIArray<common::ObAddr>& servers,
      const share::ObPartitionReplica::FailList& fail_msgs);

  // @see ObRestartTask
  int after_restart();
  int do_after_full_service();
  int schedule_restart_timer_task(const int64_t delay);
  int schedule_self_check_task();
  int schedule_temporary_offline_timer_task();
  // @see ObCheckServerTask
  int schedule_check_server_timer_task();
  // @see ObRefreshServerTask
  int schedule_refresh_server_timer_task(const int64_t delay);
  // @see ObInnerTableMonitorTask
  int schedule_inner_table_monitor_task();
  int schedule_recyclebin_task(int64_t delay);
  int schedule_force_drop_schema_task(int64_t delay);
  // @see ObInspector
  int schedule_inspector_task();
  int schedule_update_rs_list_task();
  // update statistic cache
  int update_stat_cache(const obrpc::ObUpdateStatCacheArg& arg);

  int schedule_load_building_index_task();
  // ob_admin command, must be called in ddl thread
  int force_create_sys_table(const obrpc::ObForceCreateSysTableArg& arg);
  int force_set_locality(const obrpc::ObForceSetLocalityArg& arg);

  int restore_partitions(const obrpc::ObRestorePartitionsArg& arg);
  int physical_restore_partitions(const obrpc::ObRestorePartitionsArg& arg);
  int logical_restore_partitions(const obrpc::ObRestorePartitionsArg& arg);

  int check_is_log_sync(bool& is_log_sync, const common::ObIArray<common::ObAddr>& stop_server);
  int generate_log_in_sync_sql(const common::ObIArray<common::ObAddr> &dest_server_array, common::ObSqlString &sql);
  int generate_stop_server_log_in_sync_dest_server_array(const common::ObIArray<common::ObAddr>& alive_server_array,
      const common::ObIArray<common::ObAddr>& excluded_server_array,
      common::ObIArray<common::ObAddr>& dest_server_array);
  int log_nop_operation(const obrpc::ObDDLNopOpreatorArg& arg);
  int broadcast_schema(const obrpc::ObBroadcastSchemaArg& arg);
  int check_other_rs_exist(bool& is_other_rs_exist);
  ObRsGtsManager& get_rs_gts_manager()
    {
    return rs_gts_manager_;
  }
  ObFreezeInfoManager& get_freeze_manager()
  {
    return freeze_info_manager_;
  }
  ObDDLService& get_ddl_service()
  {
    return ddl_service_;
  }

  int get_schema_split_version(int64_t& version_in_core_table, int64_t& version_in_ddl_table);
  int finish_schema_split(const obrpc::ObFinishSchemaSplitArg& arg);

  int check_merge_finish(const obrpc::ObCheckMergeFinishArg& arg);
  int get_recycle_schema_versions(
      const obrpc::ObGetRecycleSchemaVersionsArg& arg, obrpc::ObGetRecycleSchemaVersionsResult& result);
  // backup and restore
  int force_drop_schema(const obrpc::ObForceDropSchemaArg& arg);
  int handle_archive_log(const obrpc::ObArchiveLogArg& arg);
  int handle_backup_database(const obrpc::ObBackupDatabaseArg& arg);
  int handle_backup_manage(const obrpc::ObBackupManageArg& arg);
  int handle_validate_database(const obrpc::ObBackupManageArg& arg);
  int handle_validate_backupset(const obrpc::ObBackupManageArg& arg);
  int handle_cancel_validate(const obrpc::ObBackupManageArg& arg);
  int handle_backup_archive_log(const obrpc::ObBackupArchiveLogArg& arg);
  int handle_backup_backupset(const obrpc::ObBackupBackupsetArg& arg);
  int handle_backup_backuppiece(const obrpc::ObBackupBackupPieceArg& arg);
  int handle_backup_archive_log_batch_res(const obrpc::ObBackupArchiveLogBatchRes& arg);
  int update_table_schema_version(const obrpc::ObUpdateTableSchemaVersionArg& arg);
  int modify_schema_in_restore(const obrpc::ObRestoreModifySchemaArg& arg);
  int check_backup_scheduler_working(obrpc::Bool& is_working);
  int update_freeze_schema_versions(
      const int64_t frozen_version, obrpc::ObTenantSchemaVersions& tenant_schema_versions);
  int send_physical_restore_result(const obrpc::ObPhysicalRestoreResult& res);
  int execute_range_part_split(const obrpc::ObExecuteRangePartSplitArg& arg);
  int get_is_in_bootstrap(bool& is_bootstrap) const;
  int purge_recyclebin_objects(int64_t purge_each_time);
  /////////////////////////////////////////////////////
  // for standby
  int switch_cluster(const obrpc::ObAlterClusterInfoArg& arg);
  int alter_cluster_attr(const obrpc::ObAlterClusterInfoArg& arg);
  int alter_cluster_info(const obrpc::ObAlterClusterInfoArg& arg);
  int cluster_regist(const obrpc::ObRegistClusterArg& arg, obrpc::ObRegistClusterRes& res);
  int alter_standby(const obrpc::ObAdminClusterArg& arg);
  int get_cluster_info(const obrpc::ObGetClusterInfoArg& arg, share::ObClusterInfo& cluster_info);
  int get_switchover_status(obrpc::ObGetSwitchoverStatusRes& res);
  int check_cluster_valid_to_add(const obrpc::ObCheckAddStandbyArg& arg);
  int check_standby_can_access(const obrpc::ObCheckStandbyCanAccessArg& arg, obrpc::Bool& can_access);
  int get_schema_snapshot(const obrpc::ObSchemaSnapshotArg& arg, obrpc::ObSchemaSnapshotRes& res);
  int cluster_action_verify(const obrpc::ObClusterActionVerifyArg& arg);
  int gen_next_schema_version(const obrpc::ObDDLArg& arg);
  int get_cluster_stats(obrpc::ObClusterTenantStats& tenant_stats);
  int standby_upgrade_virtual_schema(const obrpc::ObDDLNopOpreatorArg& arg);
  int update_standby_cluster_info(const share::ObClusterAddr& arg);
  int cluster_heartbeat(const share::ObClusterAddr& arg, obrpc::ObStandbyHeartBeatRes& res);
  int standby_grant(const obrpc::ObStandbyGrantArg& arg);
  int finish_replay_schema(const obrpc::ObFinishReplayArg& arg);

  // table api
  int handle_user_ttl(const obrpc::ObTableTTLArg& arg);
  int ttl_response(const obrpc::ObTTLResponseArg& arg);

  ////////////////////////////////////////////////////
private:
  int check_parallel_ddl_conflict(share::schema::ObSchemaGetterGuard& schema_guard, const obrpc::ObDDLArg& arg);
  int check_can_start_as_primary();
  int fetch_root_partition_info();
  int generate_user(const ObClusterType& cluster_type, const char* usr_name, const char* user_passwd);
  int set_core_table_unit_id();
  // create system table in mysql backend for debugging mode.
  int init_debug_database();
  int do_restart();
  int refresh_server(const bool fast_recover, const bool need_retry);
  int refresh_schema(const bool fast_recover);
  int init_sequence_id();
  int load_server_manager();
  int start_timer_tasks();
  int stop_timer_tasks();
  int request_heartbeats();
  int self_check();
  int update_all_server_and_rslist();
  int check_zone_and_server(const ObIArray<ObAddr>& servers, bool& is_same_zone, bool& is_all_stopped);
  int update_rslist();
  int check_can_stop(
      const common::ObZone& zone, const common::ObIArray<common::ObAddr>& servers, const bool is_stop_zone);
  bool have_other_stop_task(const ObZone& zone);
  int check_is_log_sync_twice(bool& is_log_sync, const common::ObIArray<common::ObAddr>& stop_server);
  int init_sys_admin_ctx(ObSystemAdminCtx& ctx);
  int set_cluster_version();
  int get_root_partition_table(common::ObArray<common::ObAddr>& rs_list);
  bool is_replica_count_reach_rs_limit(int64_t replica_count)
  {
    return replica_count > OB_MAX_CLUSTER_REPLICA_COUNT;
  }
  int update_all_server_config();
  int get_readwrite_servers(
      const common::ObIArray<common::ObAddr>& input_servers, common::ObIArray<common::ObAddr>& readwrite_servers);
  int check_tenant_group_config_legality(
      common::ObIArray<ObTenantGroupParser::TenantNameGroup>& tenant_groups, bool& legal);
  int generate_table_schema_in_tenant_space(
      const obrpc::ObCreateTableArg& arg, share::schema::ObTableSchema& table_schema);
  int clear_bootstrap();
  int check_tenant_gts_config(
      const int64_t tenant_id, bool& tenant_gts_config_ok, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_database_config(
      const int64_t tenant_id, bool& db_config_ok, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_table_config(const int64_t tenant_id, bool& table_config_ok, bool& table_split_ok,
      share::schema::ObSchemaGetterGuard& schema_guard, const int64_t snapshot_schema_version);
  int check_tablegroup_config(const int64_t tenant_id, bool& tablegroup_config_ok, bool& tablegroup_split_ok,
      share::schema::ObSchemaGetterGuard& schema_guard, const int64_t snapshot_schema_version);
  int check_all_tenant_config(ObArray<ObString>& not_allow_reasons, share::schema::ObSchemaGetterGuard& schema_guard,
      const int64_t snapshot_schema_version, ObArray<uint64_t>& tenant_ids);
  bool continue_check(const int ret);
  int handle_backup_database_cancel(const obrpc::ObBackupManageArg& arg);
  int handle_backup_delete_obsolete_backup(const obrpc::ObBackupManageArg& arg);
  int handle_backup_delete_backup_set(const obrpc::ObBackupManageArg& arg);
  int refresh_unit_replica_counter();
  int reload_unit_replica_counter_task();
  inline static bool cmp_tenant_id(const uint64_t lhs, const uint64_t tenant_id)
  {
    return lhs < tenant_id;
  }
  int handle_backup_delete_backup_data(const obrpc::ObBackupManageArg &arg);
  int handle_cancel_delete_backup(const obrpc::ObBackupManageArg &arg);
  int handle_cancel_backup_backup(const obrpc::ObBackupManageArg &arg);
  int handle_cancel_all_backup_force(const obrpc::ObBackupManageArg &arg);
  int wait_refresh_config();

private:
  bool is_sys_tenant(const common::ObString& tenant_name);
  int table_is_split(
      uint64_t tenant_id, const ObString& database_name, const ObString& table_name, const bool is_index = false);
  int table_allow_ddl_operation(const obrpc::ObAlterTableArg& arg);
  int get_table_schema(uint64_t tenant_id, const common::ObString& database_name, const common::ObString& table_name,
      const bool is_index, const int64_t session_id, const share::schema::ObTableSchema*& table_schema);
  int batch_root_split_partition(const obrpc::ObRootSplitPartitionArg& arg);
  int update_baseline_schema_version();
  int finish_bootstrap();
  void construct_lease_expire_time(const share::ObLeaseRequest& lease_request, share::ObLeaseResponse& lease_response,
      const share::ObServerStatus& server_status);

private:
  int update_all_sys_tenant_schema_version();
  int finish_schema_split_v1(const obrpc::ObFinishSchemaSplitArg& arg);
  int finish_schema_split_v2(const obrpc::ObFinishSchemaSplitArg& arg);
  int check_has_restore_tenant(bool& has_restore_tenant);
  int build_range_part_split_arg(const common::ObPartitionKey& partition_key, const common::ObRowkey& rowkey,
      share::schema::AlterTableSchema& alter_table_schema);

private:
  static const int64_t OB_MAX_CLUSTER_REPLICA_COUNT = 10000000;
  static const int64_t OB_ROOT_SERVICE_START_FAIL_COUNT_UPPER_LIMIT = 10;
  bool inited_;
  volatile bool server_refreshed_;  // server manager reload and force request heartbeat
  // use mysql server backend for debug.
  bool debug_;

  common::ObAddr self_addr_;
  common::ObServerConfig* config_;
  common::ObConfigManager* config_mgr_;

  obrpc::ObSrvRpcProxy rpc_proxy_;
  obrpc::ObCommonRpcProxy common_proxy_;
  common::ObMySQLProxy sql_proxy_;
  common::ObOracleSqlProxy oracle_sql_proxy_;
  share::ObRemoteSqlProxy* remote_sql_proxy_;
  observer::ObRestoreCtx* restore_ctx_;
  share::ObRsMgr* rs_mgr_;
  share::schema::ObMultiVersionSchemaService* schema_service_;

  // server manager related
  ObStatusChangeCallback status_change_cb_;
  ObServerChangeCallback server_change_callback_;
  ObServerManager server_manager_;
  ObHeartbeatChecker hb_checker_;
  ObAllServerChecker server_checker_;
  RsListChangeCb rs_list_change_cb_;

  // major freeze
  ObFreezeInfoManager freeze_info_manager_;
  ObRootMajorFreezeV2 root_major_freeze_v2_;

  // minor freeze
  ObRootMinorFreeze root_minor_freeze_;

  // partition table related
  share::ObPartitionTableOperator* pt_operator_;
  share::ObRemotePartitionTableOperator* remote_pt_operator_;
  share::ObIPartitionLocationCache* location_cache_;

  ObZoneManager zone_manager_;

  // ddl related
  ObDDLService ddl_service_;
  ObUnitManager unit_manager_;
  ObLeaderCoordinator leader_coordinator_;

  ObPartitionTableUtil pt_util_;
  ObDailyMergeScheduler daily_merge_scheduler_;
  MergeErrorCallback merge_error_cb_;

  ObMajorFreezeLauncher major_freeze_launcher_;
  ObFreezeInfoUpdater freeze_info_updater_;

  ObGlobalMaxDecidedTransVersionMgr global_trans_version_mgr_;

  // partition balance
  ObRebalanceTaskExecutor rebalance_task_executor_;
  ObRebalanceTaskMgr rebalance_task_mgr_;
  ObRootBalancer root_balancer_;

  // empty server checker
  ObEmptyServerChecker empty_server_checker_;

  // lost replica checker
  ObLostReplicaChecker lost_replica_checker_;

  // thread checker
  ObRsThreadChecker thread_checker_;

  // virtual table related
  ObVTableLocationGetter vtable_location_getter_;

  // broadcast rs list related
  share::ObUnifiedAddrAgent addr_agent_;

  // upgrade inspection
  ObRootInspection root_inspection_;

  // schema split
  ObSchemaSplitExecutor schema_split_executor_;

  // upgrade post job
  ObUpgradeExecutor upgrade_executor_;

  // upgrade storage format version
  ObUpgradeStorageFormatVersionExecutor upgrade_storage_format_executor_;

  // create inner oracle role(for upgrade)
  ObCreateInnerSchemaExecutor create_inner_schema_executor_;

  // revise data in inner_table (for upgrade)
  ObSchemaReviseExecutor schema_revise_executor_;

  // avoid concurrent run of do_restart and bootstrap
  common::ObLatch bootstrap_lock_;

  common::SpinRWLock broadcast_rs_list_lock_;

  // Inner table mointor
  ObInnerTableMonitor inner_table_monitor_;
  ObPartitionChecksumChecker partition_checksum_checker_;

  // the single task queue for all async tasks and timer tasks
  common::ObWorkQueue task_queue_;
  common::ObWorkQueue inspect_task_queue_;

  // async timer tasks
  ObRestartTask restart_task_;                           // not repeat & no retry
  ObRefreshServerTask refresh_server_task_;              // not repeat & no retry
  ObCheckServerTask check_server_task_;                  // repeat & no retry
  ObSelfCheckTask self_check_task_;                      // repeat to succeed & no retry
  ObLoadIndexBuildTask load_building_index_task_;        // repeat to succeed & no retry
  share::ObEventTableClearTask event_table_clear_task_;  // repeat & no retry
  ObInnerTableMonitorTask inner_table_monitor_task_;     // repeat & no retry

  ObInspector inspector_task_;                    // repeat & no retry
  ObPurgeRecyclebinTask purge_recyclebin_task_;   // not repeat & no retry
  ObForceDropSchemaTask force_drop_schema_task_;  // not repeat & no retry
  // for set_config
  ObLatch set_config_lock_;

  ObGlobalIndexBuilder global_index_builder_;
  ObPartitionSpliter partition_spliter_;
  ObSnapshotInfoManager snapshot_manager_;
  int64_t core_meta_table_version_;
  ObRsGtsManager rs_gts_manager_;
  ObUpdateRsListTimerTask update_rs_list_timer_task_;
  ObRsGtsTaskMgr rs_gts_task_mgr_;
  ObRsGtsMonitor rs_gts_monitor_;
  int64_t baseline_schema_version_;
  mutable common::SpinRWLock upgrade_cluster_create_ha_gts_lock_;

  // backup
  ObRestoreScheduler restore_scheduler_;
  ObLogArchiveScheduler log_archive_scheduler_;
  ObRootBackup root_backup_;
  ObRootValidate root_validate_;
  int64_t start_service_time_;
  ObRsStatus rs_status_;

  int64_t fail_count_;
  ObSchemaHistoryRecycler schema_history_recycler_;
  ObBackupDataClean backup_data_clean_;
  ObBackupAutoDeleteExpiredData backup_auto_delete_;
  ObReloadUnitReplicaCounterTask reload_unit_replica_counter_task_;
  ObSinglePartBalance single_part_balance_;
  ObRestorePointService restore_point_service_;
  ObBackupArchiveLogScheduler backup_archive_log_;
  ObBackupBackupset backup_backupset_;
  ObBackupLeaseService backup_lease_service_;

  // tableapi
  ObTTLScheduler ttl_scheduler_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootService);
};

template <typename TASK, typename T>
int ObRootService::do_receive_balance_over(TASK& task, const T& arg)
{
  int ret = common::OB_SUCCESS;
  RS_LOG(INFO, "receive balance over", K(arg));
  common::ObArray<int> rc_array;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    RS_LOG(WARN, "not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid arg", K(arg), K(ret));
  } else if (OB_FAIL(task.build_by_task_result(arg))) {
    RS_LOG(WARN, "fail to init task", K(ret), K(arg));
  } else if (OB_FAIL(rc_array.push_back(static_cast<int>(arg.result_)))) {
    RS_LOG(WARN, "fail to push back", K(ret));
  } else if (OB_FAIL(try_block_server(static_cast<int>(arg.result_), arg.dst_.get_server()))) {
    RS_LOG(WARN, "try block failed", "rc", arg.result_, "server", arg.dst_.get_server(), K(ret));
  } else if (OB_FAIL(rebalance_task_mgr_.execute_over(task, rc_array))) {
    RS_LOG(WARN, "execute over failed", K(ret), K(arg), K(task));
  } else {
    ObRebalanceTaskType type = task.get_rebalance_task_type();
    if (ObRebalanceTaskType::COPY_SSTABLE == type || ObRebalanceTaskType::PHYSICAL_RESTORE_REPLICA == type) {
      restore_scheduler_.wakeup();
    }
    RS_LOG(INFO, "process balance rpc over", K(arg), K(task));
  }
  return ret;
}

template <typename TASK, typename T>
int ObRootService::do_receive_batch_balance_over(TASK& task, const T& arg)
{
  int ret = common::OB_SUCCESS;
  RS_LOG(INFO, "receive batch balance over", K(arg));
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    RS_LOG(WARN, "not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(task.build_by_task_result(arg))) {
    RS_LOG(WARN, "fail to init task", K(ret), K(arg));
  } else {
    common::ObArray<int> rc_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.res_array_.count(); ++i) {
      if (OB_FAIL(try_block_server(
              static_cast<int>(arg.res_array_.at(i).result_), arg.res_array_.at(i).dst_.get_server()))) {
        RS_LOG(WARN,
            "try block server failed",
            K(ret),
            "server",
            arg.res_array_.at(i).dst_.get_server(),
            "ret_code",
            arg.res_array_.at(i).result_);
      } else if (OB_FAIL(rc_array.push_back(static_cast<int>(arg.res_array_.at(i).result_)))) {
        RS_LOG(WARN, "fail to push back", K(ret));
      } else {
      }  // no more to do
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rebalance_task_mgr_.execute_over(task, rc_array))) {
        RS_LOG(WARN, "fail to execute over", K(ret), K(task));
      } else {
      }  // no more to do
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ROOT_SERVICE_H_
