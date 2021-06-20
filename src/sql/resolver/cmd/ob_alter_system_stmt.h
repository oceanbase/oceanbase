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

#ifndef OCEANBASE_RESOLVER_CMD_OB_ALTER_SYSTEM_STMT_
#define OCEANBASE_RESOLVER_CMD_OB_ALTER_SYSTEM_STMT_

#include "sql/resolver/cmd/ob_system_cmd_stmt.h"
#include "share/ob_rpc_struct.h"
#include "share/scheduler/ob_sys_task_stat.h"

namespace oceanbase {
namespace sql {
class ObFreezeStmt : public ObSystemCmdStmt {
public:
  ObFreezeStmt()
      : ObSystemCmdStmt(stmt::T_FREEZE),
        major_freeze_(false),
        opt_server_list_(),
        opt_tenant_ids_(),
        opt_partition_key_()
  {}
  ObFreezeStmt(common::ObIAllocator* name_pool)
      : ObSystemCmdStmt(name_pool, stmt::T_FREEZE),
        major_freeze_(false),
        opt_server_list_(),
        opt_tenant_ids_(),
        opt_partition_key_()
  {}
  virtual ~ObFreezeStmt()
  {}

  bool is_major_freeze() const
  {
    return major_freeze_;
  }
  void set_major_freeze(bool major_freeze)
  {
    major_freeze_ = major_freeze;
  }
  inline obrpc::ObServerList& get_ignore_server_list()
  {
    return opt_server_list_;
  }
  inline obrpc::ObServerList& get_server_list()
  {
    return opt_server_list_;
  }
  inline common::ObSArray<uint64_t>& get_tenant_ids()
  {
    return opt_tenant_ids_;
  }
  inline common::ObPartitionKey& get_partition_key()
  {
    return opt_partition_key_;
  }
  inline common::ObZone& get_zone()
  {
    return opt_zone_;
  }
  inline int push_server(const common::ObAddr& server)
  {
    return opt_server_list_.push_back(server);
  }

  TO_STRING_KV(
      N_STMT_TYPE, ((int)stmt_type_), K_(major_freeze), K(opt_server_list_), K(opt_tenant_ids_), K(opt_partition_key_));

private:
  bool major_freeze_;
  // for major_freeze, it is ignore server list
  // for minor_freeze, it is candidate server list
  obrpc::ObServerList opt_server_list_;
  // for minor_freeze only,
  common::ObSArray<uint64_t> opt_tenant_ids_;
  // for minor_freeze only
  common::ObPartitionKey opt_partition_key_;
  // for minor_freeze only
  common::ObZone opt_zone_;
};

class ObFlushCacheStmt : public ObSystemCmdStmt {
public:
  ObFlushCacheStmt() : ObSystemCmdStmt(stmt::T_FLUSH_CACHE), flush_cache_arg_(), is_global_(false)
  {}
  virtual ~ObFlushCacheStmt()
  {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(flush_cache_arg));

  obrpc::ObAdminFlushCacheArg flush_cache_arg_;
  bool is_global_;
};

class ObLoadBaselineStmt : public ObSystemCmdStmt {
public:
  ObLoadBaselineStmt() : ObSystemCmdStmt(stmt::T_LOAD_BASELINE), load_baseline_arg_()
  {}
  virtual ~ObLoadBaselineStmt()
  {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(load_baseline_arg));

  obrpc::ObAdminLoadBaselineArg load_baseline_arg_;
};

class ObFlushKVCacheStmt : public ObSystemCmdStmt {
public:
  ObFlushKVCacheStmt() : ObSystemCmdStmt(stmt::T_FLUSH_KVCACHE)
  {}
  virtual ~ObFlushKVCacheStmt()
  {}

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_name), K_(cache_name));
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> cache_name_;
};

class ObFlushIlogCacheStmt : public ObSystemCmdStmt {
public:
  ObFlushIlogCacheStmt() : ObSystemCmdStmt(stmt::T_FLUSH_ILOGCACHE), file_id_(0)
  {}
  virtual ~ObFlushIlogCacheStmt()
  {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(file_id));

  int32_t file_id_;
};

class ObFlushDagWarningsStmt : public ObSystemCmdStmt {
public:
  ObFlushDagWarningsStmt() : ObSystemCmdStmt(stmt::T_FLUSH_DAG_WARNINGS)
  {}
  virtual ~ObFlushDagWarningsStmt()
  {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_));
};

class ObAdminServerStmt : public ObSystemCmdStmt {
public:
  ObAdminServerStmt() : ObSystemCmdStmt(stmt::T_ADMIN_SERVER), op_(obrpc::ObAdminServerArg::ADD)
  {}

  ObAdminServerStmt(common::ObIAllocator* name_pool) : ObSystemCmdStmt(name_pool, stmt::T_ADMIN_SERVER)
  {}

  virtual ~ObAdminServerStmt()
  {}

  inline obrpc::ObServerList& get_server_list()
  {
    return server_list_;
  }
  inline const common::ObZone& get_zone() const
  {
    return zone_;
  }
  inline void set_zone(const common::ObZone& zone)
  {
    zone_ = zone;
  }
  inline obrpc::ObAdminServerArg::AdminServerOp get_op() const
  {
    return op_;
  }
  inline void set_op(const obrpc::ObAdminServerArg::AdminServerOp op)
  {
    op_ = op;
  }

private:
  obrpc::ObAdminServerArg::AdminServerOp op_;
  obrpc::ObServerList server_list_;
  common::ObZone zone_;
};

class ObAdminZoneStmt : public ObSystemCmdStmt {
public:
  ObAdminZoneStmt() : ObSystemCmdStmt(stmt::T_ADMIN_ZONE), arg_()
  {}

  ObAdminZoneStmt(common::ObIAllocator* name_pool) : ObSystemCmdStmt(name_pool, stmt::T_ADMIN_ZONE), arg_()
  {}

  virtual ~ObAdminZoneStmt()
  {}

  inline const common::ObZone& get_zone() const
  {
    return arg_.zone_;
  }
  inline void set_zone(const common::ObZone& zone)
  {
    arg_.zone_ = zone;
  }
  inline const common::ObRegion& get_region() const
  {
    return arg_.region_;
  }
  inline void set_region(const common::ObRegion& region)
  {
    arg_.region_ = region;
  }
  inline const common::ObIDC& get_idc() const
  {
    return arg_.idc_;
  }
  inline void set_idc(const common::ObIDC& idc)
  {
    arg_.idc_ = idc;
  }
  inline const common::ObZoneType& get_zone_type() const
  {
    return arg_.zone_type_;
  }
  inline void set_zone_type(const common::ObZoneType& zone_type)
  {
    arg_.zone_type_ = zone_type;
  }
  inline const common::ObString& get_sql_stmt_str() const
  {
    return arg_.sql_stmt_str_;
  }
  inline void set_sql_stmt_str(const common::ObString& sql_stmt_str)
  {
    arg_.sql_stmt_str_ = sql_stmt_str;
  }
  inline obrpc::ObAdminZoneArg::AdminZoneOp get_op() const
  {
    return arg_.op_;
  }
  inline void set_op(const obrpc::ObAdminZoneArg::AdminZoneOp op)
  {
    arg_.op_ = op;
    if (obrpc::ObAdminZoneArg::FORCE_STOP == op) {
      arg_.force_stop_ = true;
    }
  }
  inline int set_alter_region_option()
  {
    return arg_.alter_zone_options_.add_member(obrpc::ObAdminZoneArg::ALTER_ZONE_REGION);
  }
  inline bool has_alter_region_option()
  {
    return arg_.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_REGION);
  }
  inline int set_alter_idc_option()
  {
    return arg_.alter_zone_options_.add_member(obrpc::ObAdminZoneArg::ALTER_ZONE_IDC);
  }
  inline int has_alter_idc_option()
  {
    return arg_.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_IDC);
  }
  inline int set_alter_zone_type_option()
  {
    return arg_.alter_zone_options_.add_member(obrpc::ObAdminZoneArg::ALTER_ZONE_TYPE);
  }
  inline int has_alter_zone_type_option()
  {
    return arg_.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_TYPE);
  }
  const obrpc::ObAdminZoneArg& get_arg() const
  {
    return arg_;
  }

private:
  obrpc::ObAdminZoneArg arg_;
};

class ObSwitchReplicaRoleStmt : public ObSystemCmdStmt {
public:
  ObSwitchReplicaRoleStmt() : ObSystemCmdStmt(stmt::T_SWITCH_REPLICA_ROLE)
  {}
  virtual ~ObSwitchReplicaRoleStmt()
  {}

  obrpc::ObAdminSwitchReplicaRoleArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminSwitchReplicaRoleArg rpc_arg_;
};

class ObSwitchRSRoleStmt : public ObSystemCmdStmt {
public:
  ObSwitchRSRoleStmt() : ObSystemCmdStmt(stmt::T_SWITCH_RS_ROLE)
  {}
  virtual ~ObSwitchRSRoleStmt()
  {}

  obrpc::ObAdminSwitchRSRoleArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminSwitchRSRoleArg rpc_arg_;
};

class ObChangeReplicaStmt : public ObSystemCmdStmt {
public:
  ObChangeReplicaStmt() : ObSystemCmdStmt(stmt::T_CHANGE_REPLICA)
  {}
  virtual ~ObChangeReplicaStmt()
  {}

  obrpc::ObAdminChangeReplicaArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminChangeReplicaArg rpc_arg_;
};

class ObDropReplicaStmt : public ObSystemCmdStmt {
public:
  ObDropReplicaStmt() : ObSystemCmdStmt(stmt::T_DROP_REPLICA)
  {}
  virtual ~ObDropReplicaStmt()
  {}

  obrpc::ObAdminDropReplicaArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminDropReplicaArg rpc_arg_;
};

class ObMigrateReplicaStmt : public ObSystemCmdStmt {
public:
  ObMigrateReplicaStmt() : ObSystemCmdStmt(stmt::T_MIGRATE_REPLICA)
  {}
  virtual ~ObMigrateReplicaStmt()
  {}

  obrpc::ObAdminMigrateReplicaArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminMigrateReplicaArg rpc_arg_;
};

class ObReportReplicaStmt : public ObSystemCmdStmt {
public:
  ObReportReplicaStmt() : ObSystemCmdStmt(stmt::T_REPORT_REPLICA)
  {}
  virtual ~ObReportReplicaStmt()
  {}

  obrpc::ObAdminReportReplicaArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminReportReplicaArg rpc_arg_;
};

class ObRecycleReplicaStmt : public ObSystemCmdStmt {
public:
  ObRecycleReplicaStmt() : ObSystemCmdStmt(stmt::T_RECYCLE_REPLICA)
  {}
  virtual ~ObRecycleReplicaStmt()
  {}

  obrpc::ObAdminRecycleReplicaArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminRecycleReplicaArg rpc_arg_;
};

class ObAdminMergeStmt : public ObSystemCmdStmt {
public:
  ObAdminMergeStmt() : ObSystemCmdStmt(stmt::T_ADMIN_MERGE)
  {}
  virtual ~ObAdminMergeStmt()
  {}

  obrpc::ObAdminMergeArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminMergeArg rpc_arg_;
};

class ObClearRoottableStmt : public ObSystemCmdStmt {
public:
  ObClearRoottableStmt() : ObSystemCmdStmt(stmt::T_CLEAR_ROOT_TABLE)
  {}
  virtual ~ObClearRoottableStmt()
  {}

  obrpc::ObAdminClearRoottableArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminClearRoottableArg rpc_arg_;
};

class ObRefreshSchemaStmt : public ObSystemCmdStmt {
public:
  ObRefreshSchemaStmt() : ObSystemCmdStmt(stmt::T_REFRESH_SCHEMA)
  {}
  virtual ~ObRefreshSchemaStmt()
  {}

  obrpc::ObAdminRefreshSchemaArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminRefreshSchemaArg rpc_arg_;
};

class ObRefreshMemStatStmt : public ObSystemCmdStmt {
public:
  ObRefreshMemStatStmt() : ObSystemCmdStmt(stmt::T_REFRESH_MEMORY_STAT)
  {}
  virtual ~ObRefreshMemStatStmt()
  {}

  obrpc::ObAdminRefreshMemStatArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminRefreshMemStatArg rpc_arg_;
};

class ObSetConfigStmt : public ObSystemCmdStmt {
public:
  ObSetConfigStmt() : ObSystemCmdStmt(stmt::T_ALTER_SYSTEM_SET_PARAMETER)
  {}
  virtual ~ObSetConfigStmt()
  {}

  obrpc::ObAdminSetConfigArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminSetConfigArg rpc_arg_;
};

class ObSetTPStmt : public ObSystemCmdStmt {
public:
  ObSetTPStmt() : ObSystemCmdStmt(stmt::T_ALTER_SYSTEM_SETTP)
  {}
  virtual ~ObSetTPStmt()
  {}

  obrpc::ObAdminSetTPArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminSetTPArg rpc_arg_;
};

class ObMigrateUnitStmt : public ObSystemCmdStmt {
public:
  ObMigrateUnitStmt() : ObSystemCmdStmt(stmt::T_MIGRATE_UNIT)
  {}
  virtual ~ObMigrateUnitStmt()
  {}

  obrpc::ObAdminMigrateUnitArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));

private:
  obrpc::ObAdminMigrateUnitArg rpc_arg_;
};

class ObClearLocationCacheStmt : public ObSystemCmdStmt {
public:
  ObClearLocationCacheStmt() : ObSystemCmdStmt(stmt::T_CLEAR_LOCATION_CACHE)
  {}
  virtual ~ObClearLocationCacheStmt()
  {}

  obrpc::ObAdminClearLocationCacheArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

private:
  obrpc::ObAdminClearLocationCacheArg rpc_arg_;
};

class ObReloadGtsStmt : public ObSystemCmdStmt {
public:
  ObReloadGtsStmt() : ObSystemCmdStmt(stmt::T_RELOAD_GTS)
  {}
  virtual ~ObReloadGtsStmt()
  {}
};

class ObReloadUnitStmt : public ObSystemCmdStmt {
public:
  ObReloadUnitStmt() : ObSystemCmdStmt(stmt::T_RELOAD_UNIT)
  {}
  virtual ~ObReloadUnitStmt()
  {}
};

class ObReloadServerStmt : public ObSystemCmdStmt {
public:
  ObReloadServerStmt() : ObSystemCmdStmt(stmt::T_RELOAD_SERVER)
  {}
  virtual ~ObReloadServerStmt()
  {}
};

class ObReloadZoneStmt : public ObSystemCmdStmt {
public:
  ObReloadZoneStmt() : ObSystemCmdStmt(stmt::T_RELOAD_ZONE)
  {}
  virtual ~ObReloadZoneStmt()
  {}
};

class ObClearMergeErrorStmt : public ObSystemCmdStmt {
public:
  ObClearMergeErrorStmt() : ObSystemCmdStmt(stmt::T_CLEAR_MERGE_ERROR)
  {}
  virtual ~ObClearMergeErrorStmt()
  {}
};

class ObUpgradeVirtualSchemaStmt : public ObSystemCmdStmt {
public:
  ObUpgradeVirtualSchemaStmt() : ObSystemCmdStmt(stmt::T_UPGRADE_VIRTUAL_SCHEMA)
  {}
  virtual ~ObUpgradeVirtualSchemaStmt()
  {}
};

class ObAdminUpgradeCmdStmt : public ObSystemCmdStmt {
public:
  enum AdminUpgradeOp {
    BEGIN = 1,
    END = 2,
  };
  ObAdminUpgradeCmdStmt() : ObSystemCmdStmt(stmt::T_ADMIN_UPGRADE_CMD), op_(BEGIN)
  {}
  virtual ~ObAdminUpgradeCmdStmt()
  {}

  inline const AdminUpgradeOp& get_op() const
  {
    return op_;
  }
  inline void set_op(const AdminUpgradeOp op)
  {
    op_ = op;
  }

private:
  AdminUpgradeOp op_;
};

class ObAdminRollingUpgradeCmdStmt : public ObSystemCmdStmt {
public:
  enum AdminUpgradeOp {
    BEGIN = 1,
    END = 2,
  };
  ObAdminRollingUpgradeCmdStmt() : ObSystemCmdStmt(stmt::T_ADMIN_ROLLING_UPGRADE_CMD), op_(BEGIN)
  {}
  virtual ~ObAdminRollingUpgradeCmdStmt()
  {}

  inline const AdminUpgradeOp& get_op() const
  {
    return op_;
  }
  inline void set_op(const AdminUpgradeOp op)
  {
    op_ = op;
  }

private:
  AdminUpgradeOp op_;
};

class ObRestoreTenantStmt : public ObSystemCmdStmt {
public:
  ObRestoreTenantStmt() : ObSystemCmdStmt(stmt::T_RESTORE_TENANT), rpc_arg_()
  {}
  virtual ~ObRestoreTenantStmt()
  {}

  obrpc::ObRestoreTenantArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

private:
  obrpc::ObRestoreTenantArg rpc_arg_;
};

class ObPhysicalRestoreTenantStmt : public ObSystemCmdStmt {
public:
  ObPhysicalRestoreTenantStmt() : ObSystemCmdStmt(stmt::T_PHYSICAL_RESTORE_TENANT), rpc_arg_()
  {}
  virtual ~ObPhysicalRestoreTenantStmt()
  {}

  obrpc::ObPhysicalRestoreTenantArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

private:
  obrpc::ObPhysicalRestoreTenantArg rpc_arg_;
};

class ObRunJobStmt : public ObSystemCmdStmt {
public:
  ObRunJobStmt() : ObSystemCmdStmt(stmt::T_RUN_JOB)
  {}
  virtual ~ObRunJobStmt()
  {}

  obrpc::ObRunJobArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

private:
  obrpc::ObRunJobArg rpc_arg_;
};

class ObRunUpgradeJobStmt : public ObSystemCmdStmt {
public:
  ObRunUpgradeJobStmt() : ObSystemCmdStmt(stmt::T_ADMIN_RUN_UPGRADE_JOB)
  {}
  virtual ~ObRunUpgradeJobStmt()
  {}

  obrpc::ObUpgradeJobArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

private:
  obrpc::ObUpgradeJobArg rpc_arg_;
};

class ObStopUpgradeJobStmt : public ObSystemCmdStmt {
public:
  ObStopUpgradeJobStmt() : ObSystemCmdStmt(stmt::T_ADMIN_STOP_UPGRADE_JOB)
  {}
  virtual ~ObStopUpgradeJobStmt()
  {}

  obrpc::ObUpgradeJobArg& get_rpc_arg()
  {
    return rpc_arg_;
  }

private:
  obrpc::ObUpgradeJobArg rpc_arg_;
};

class ObRefreshTimeZoneInfoStmt : public ObSystemCmdStmt {
public:
  ObRefreshTimeZoneInfoStmt() : ObSystemCmdStmt(stmt::T_REFRESH_TIME_ZONE_INFO), tenant_id_(OB_INVALID_TENANT_ID)
  {}
  virtual ~ObRefreshTimeZoneInfoStmt()
  {}
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  uint64_t get_tenant_id()
  {
    return tenant_id_;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id));

  uint64_t tenant_id_;
};

class ObCancelTaskStmt : public ObSystemCmdStmt {
public:
  ObCancelTaskStmt() : ObSystemCmdStmt(stmt::T_CANCEL_TASK), task_type_(share::MAX_SYS_TASK_TYPE), task_id_()
  {}
  virtual ~ObCancelTaskStmt()
  {}
  const share::ObSysTaskType& get_task_type()
  {
    return task_type_;
  }
  const common::ObString& get_task_id()
  {
    return task_id_;
  }
  int set_param(const share::ObSysTaskType& task_type, const common::ObString& task_id)
  {
    int ret = common::OB_SUCCESS;

    if (task_type < 0 || task_type > share::MAX_SYS_TASK_TYPE || task_id.length() <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      task_type_ = task_type;
      task_id_ = task_id;
    }

    return ret;
  }

private:
  share::ObSysTaskType task_type_;
  common::ObString task_id_;
};

class ObSetDiskValidStmt : public ObSystemCmdStmt {
public:
  ObSetDiskValidStmt() : ObSystemCmdStmt(stmt::T_SET_DISK_VALID), server_()
  {}
  virtual ~ObSetDiskValidStmt()
  {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(server));

  common::ObAddr server_;
};

class ObAddDiskStmt : public ObSystemCmdStmt {
public:
  ObAddDiskStmt() : ObSystemCmdStmt(stmt::T_ALTER_DISKGROUP_ADD_DISK)
  {}
  virtual ~ObAddDiskStmt()
  {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(arg));

  obrpc::ObAdminAddDiskArg arg_;
};

class ObDropDiskStmt : public ObSystemCmdStmt {
public:
  ObDropDiskStmt() : ObSystemCmdStmt(stmt::T_ALTER_DISKGROUP_DROP_DISK)
  {}
  virtual ~ObDropDiskStmt()
  {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(arg));

  obrpc::ObAdminDropDiskArg arg_;
};

class ObEnableSqlThrottleStmt : public ObSystemCmdStmt {
public:
  ObEnableSqlThrottleStmt()
      : ObSystemCmdStmt(stmt::T_ENABLE_SQL_THROTTLE),
        priority_(99),
        rt_(-1.),
        io_(-1),
        network_(-1.),
        cpu_(-1.),
        logical_reads_(-1),
        queue_time_(-1.)
  {}
  void set_priority(int64_t priority)
  {
    priority_ = priority;
  }
  void set_rt(double rt)
  {
    rt_ = rt;
  }
  void set_io(int64_t io)
  {
    io_ = io;
  }
  void set_network(double network)
  {
    network_ = network;
  }
  void set_cpu(double cpu)
  {
    cpu_ = cpu;
  }
  void set_logical_reads(int64_t logical_reads)
  {
    logical_reads_ = logical_reads;
  }
  void set_queue_time(double queue_time)
  {
    queue_time_ = queue_time;
  }

  int64_t get_priority() const
  {
    return priority_;
  }
  double get_rt() const
  {
    return rt_;
  }
  int64_t get_io() const
  {
    return io_;
  }
  double get_network() const
  {
    return network_;
  }
  double get_cpu() const
  {
    return cpu_;
  }
  int64_t get_logical_reads() const
  {
    return logical_reads_;
  }
  double get_queue_time() const
  {
    return queue_time_;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(priority), K_(rt), K_(io), K_(network), K_(cpu), K_(logical_reads),
      K_(queue_time));

private:
  int64_t priority_;
  double rt_;
  int64_t io_;
  double network_;
  double cpu_;
  int64_t logical_reads_;
  double queue_time_;
};

class ObDisableSqlThrottleStmt : public ObSystemCmdStmt {
public:
  ObDisableSqlThrottleStmt() : ObSystemCmdStmt(stmt::T_DISABLE_SQL_THROTTLE)
  {}
};

class ObChangeTenantStmt : public ObSystemCmdStmt {
public:
  ObChangeTenantStmt() : ObSystemCmdStmt(stmt::T_CHANGE_TENANT), tenant_id_(OB_INVALID_TENANT_ID)
  {}
  virtual ~ObChangeTenantStmt()
  {}
  void set_tenant_id(uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  uint64_t get_tenant_id()
  {
    return tenant_id_;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id));

  uint64_t tenant_id_;
};

class ObArchiveLogStmt : public ObSystemCmdStmt {
public:
  ObArchiveLogStmt() : ObSystemCmdStmt(stmt::T_ARCHIVE_LOG), enable_(true)
  {}
  virtual ~ObArchiveLogStmt()
  {}
  bool is_enable() const
  {
    return enable_;
  }
  void set_is_enable(const bool enable)
  {
    enable_ = enable;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(enable));

private:
  bool enable_;
};

class ObBackupDatabaseStmt : public ObSystemCmdStmt {
public:
  ObBackupDatabaseStmt() : ObSystemCmdStmt(stmt::T_BACKUP_DATABASE), tenant_id_(OB_INVALID_ID), incremental_(false)
  {}
  virtual ~ObBackupDatabaseStmt()
  {}
  bool get_incremental() const
  {
    return incremental_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int set_param(const uint64_t tenant_id, const int64_t incremental)
  {
    int ret = common::OB_SUCCESS;

    if (tenant_id == OB_INVALID_ID || (0 != incremental && 1 != incremental)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", K(tenant_id), K(incremental));
    } else {
      incremental_ = incremental != 0;
      tenant_id_ = tenant_id;
    }

    return ret;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(incremental));

private:
  uint64_t tenant_id_;
  bool incremental_;
};

class ObBackupManageStmt : public ObSystemCmdStmt {
public:
  ObBackupManageStmt()
      : ObSystemCmdStmt(stmt::T_BACKUP_MANAGE),
        tenant_id_(OB_INVALID_ID),
        type_(obrpc::ObBackupManageArg::MAX_TYPE),
        value_(0)
  {}
  virtual ~ObBackupManageStmt()
  {}
  obrpc::ObBackupManageArg::Type get_type() const
  {
    return type_;
  }
  int64_t get_value() const
  {
    return value_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int set_param(const uint64_t tenant_id, const int64_t type, const int64_t value)
  {
    int ret = common::OB_SUCCESS;

    if (tenant_id == OB_INVALID_ID || type < 0 || type >= obrpc::ObBackupManageArg::MAX_TYPE || value < 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", K(tenant_id), K(type), K(value));
    } else {
      type_ = static_cast<obrpc::ObBackupManageArg::Type>(type);
      value_ = value;
      tenant_id_ = tenant_id;
    }

    return ret;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(type), K_(value));

private:
  uint64_t tenant_id_;
  obrpc::ObBackupManageArg::Type type_;
  int64_t value_;
};

class ObBackupSetEncryptionStmt : public ObSystemCmdStmt {
public:
  ObBackupSetEncryptionStmt();
  virtual ~ObBackupSetEncryptionStmt()
  {}
  share::ObBackupEncryptionMode::EncryptionMode get_mode() const
  {
    return mode_;
  }
  const ObString get_passwd() const
  {
    return encrypted_passwd_;
  }
  int set_param(const int64_t mode, const common::ObString& passwd);
  TO_STRING_KV(
      N_STMT_TYPE, ((int)stmt_type_), "mode", share::ObBackupEncryptionMode::to_str(mode_), K_(encrypted_passwd));

private:
  share::ObBackupEncryptionMode::EncryptionMode mode_;
  char passwd_buf_[OB_MAX_PASSWORD_LENGTH];
  ObString encrypted_passwd_;
};

class ObBackupSetDecryptionStmt : public ObSystemCmdStmt {
public:
  ObBackupSetDecryptionStmt();
  virtual ~ObBackupSetDecryptionStmt()
  {}
  ObString get_passwd_array() const
  {
    return passwd_array_;
  }
  int add_passwd(const ObString& passwd);
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(pos), K_(passwd_array));

private:
  char passwd_array_[OB_MAX_PASSWORD_ARRAY_LENGTH];
  int64_t pos_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_RESOLVER_CMD_OB_ALTER_SYSTEM_STMT_
