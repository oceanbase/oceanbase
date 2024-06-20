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
#include "share/backup/ob_backup_clean_struct.h"
#include "rootserver/ob_transfer_partition_command.h"

namespace oceanbase
{
namespace sql
{
enum FreezeAllUserOrMeta {
  FREEZE_ALL = 0x01,
  FREEZE_ALL_USER = 0x02,
  FREEZE_ALL_META = 0x04
};

class ObFreezeStmt : public ObSystemCmdStmt
{
public:
  ObFreezeStmt()
    : ObSystemCmdStmt(stmt::T_FREEZE),
      major_freeze_(false),
      freeze_all_flag_(0),
      opt_server_list_(),
      opt_tenant_ids_(),
      opt_tablet_id_(),
      opt_ls_id_(share::ObLSID::INVALID_LS_ID),
      rebuild_column_group_(false) {}
  ObFreezeStmt(common::ObIAllocator *name_pool)
    : ObSystemCmdStmt(name_pool, stmt::T_FREEZE),
      major_freeze_(false),
      freeze_all_flag_(0),
      opt_server_list_(),
      opt_tenant_ids_(),
      opt_tablet_id_(),
      opt_ls_id_(share::ObLSID::INVALID_LS_ID),
      rebuild_column_group_(false) {}
  virtual ~ObFreezeStmt() {}

  bool is_major_freeze() const { return major_freeze_; }
  void set_major_freeze(bool major_freeze) { major_freeze_ = major_freeze; }
  bool is_freeze_all() const { return 0 != (freeze_all_flag_ & FREEZE_ALL); }
  void set_freeze_all() { freeze_all_flag_ |= FREEZE_ALL; }
  bool is_freeze_all_user() const { return 0 != (freeze_all_flag_ & FREEZE_ALL_USER); }
  void set_freeze_all_user() { freeze_all_flag_ |= FREEZE_ALL_USER; }
  bool is_freeze_all_meta() const { return 0 != (freeze_all_flag_ & FREEZE_ALL_META); }
  void set_freeze_all_meta() { freeze_all_flag_ |= FREEZE_ALL_META; }
  bool is_rebuild_column_group() const { return rebuild_column_group_; }
  void set_rebuild_column_group(bool rebuild_column_group) { rebuild_column_group_ = rebuild_column_group; }
  inline obrpc::ObServerList &get_ignore_server_list() { return opt_server_list_; }
  inline obrpc::ObServerList &get_server_list() { return opt_server_list_; }
  inline common::ObSArray<uint64_t> &get_tenant_ids() { return opt_tenant_ids_; }
  inline common::ObZone &get_zone() { return opt_zone_; }
  inline common::ObTabletID &get_tablet_id() { return opt_tablet_id_; }
  inline int64_t &get_ls_id() { return opt_ls_id_; }
  inline int push_server(const common::ObAddr& server) {
    return opt_server_list_.push_back(server);
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(major_freeze), K(freeze_all_flag_),
               K(opt_server_list_), K(opt_tenant_ids_), K(opt_tablet_id_), K(opt_ls_id_));
private:
  bool major_freeze_;
  // for major_freeze, it is ignore server list
  // for minor_freeze, it is candidate server list
  int freeze_all_flag_;
  // for major_freeze only
  obrpc::ObServerList opt_server_list_;
  // for minor_freeze only,
  common::ObSArray<uint64_t> opt_tenant_ids_;
  // for minor_freeze only
  common::ObZone opt_zone_;

  // for minor_freeze only
  common::ObTabletID opt_tablet_id_;
  int64_t opt_ls_id_;
  // for major_freeze only
  bool rebuild_column_group_;
};

class ObFlushCacheStmt : public ObSystemCmdStmt
{
public:
  ObFlushCacheStmt() :
    ObSystemCmdStmt(stmt::T_FLUSH_CACHE),
    flush_cache_arg_(),
    is_global_(false)
  {}
  virtual ~ObFlushCacheStmt() {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(flush_cache_arg));

  obrpc::ObAdminFlushCacheArg flush_cache_arg_;
  bool is_global_;
};

class ObFlushKVCacheStmt : public ObSystemCmdStmt
{
public:
  ObFlushKVCacheStmt() : ObSystemCmdStmt(stmt::T_FLUSH_KVCACHE) {}
  virtual ~ObFlushKVCacheStmt() {}

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_name), K_(cache_name));
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> cache_name_;
};

class ObFlushIlogCacheStmt : public ObSystemCmdStmt
{
public:
  ObFlushIlogCacheStmt() : ObSystemCmdStmt(stmt::T_FLUSH_ILOGCACHE), file_id_(0) {}
  virtual ~ObFlushIlogCacheStmt() {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(file_id));

  int32_t file_id_;
};

class ObFlushDagWarningsStmt : public ObSystemCmdStmt
{
public:
  ObFlushDagWarningsStmt() : ObSystemCmdStmt(stmt::T_FLUSH_DAG_WARNINGS) {}
  virtual ~ObFlushDagWarningsStmt() {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_));
};

class ObAdminServerStmt : public ObSystemCmdStmt
{
public:
  ObAdminServerStmt()
      : ObSystemCmdStmt(stmt::T_ADMIN_SERVER), op_(obrpc::ObAdminServerArg::ADD)
  {
  }

  ObAdminServerStmt(common::ObIAllocator *name_pool)
      : ObSystemCmdStmt(name_pool, stmt::T_ADMIN_SERVER)
  {
  }

  virtual ~ObAdminServerStmt() {}

  inline obrpc::ObServerList &get_server_list() { return server_list_; }
  inline const common::ObZone &get_zone() const { return zone_; }
  inline void set_zone(const common::ObZone &zone) { zone_ = zone; }
  inline obrpc::ObAdminServerArg::AdminServerOp get_op() const { return op_; }
  inline void set_op(const obrpc::ObAdminServerArg::AdminServerOp op) { op_ = op; }
private:
  obrpc::ObAdminServerArg::AdminServerOp op_;
  obrpc::ObServerList server_list_;
  common::ObZone zone_;
};

class ObAdminZoneStmt : public ObSystemCmdStmt
{
public:
  ObAdminZoneStmt()
      : ObSystemCmdStmt(stmt::T_ADMIN_ZONE),
        arg_()
  {
  }

  ObAdminZoneStmt(common::ObIAllocator *name_pool)
      : ObSystemCmdStmt(name_pool, stmt::T_ADMIN_ZONE),
        arg_()
  {
  }

  virtual ~ObAdminZoneStmt() {}

  inline const common::ObZone &get_zone() const { return arg_.zone_; }
  inline void set_zone(const common::ObZone &zone) { arg_.zone_ = zone; }
  inline const common::ObRegion &get_region() const { return arg_.region_; }
  inline void set_region(const common::ObRegion &region) { arg_.region_ = region; }
  inline const common::ObIDC &get_idc() const { return arg_.idc_; }
  inline void set_idc(const common::ObIDC &idc) { arg_.idc_ = idc; }
  inline const common::ObZoneType &get_zone_type() const { return arg_.zone_type_; }
  inline void set_zone_type(const common::ObZoneType &zone_type) { arg_.zone_type_ = zone_type; }
  inline const common::ObString &get_sql_stmt_str() const { return arg_.sql_stmt_str_; }
  inline void set_sql_stmt_str(const common::ObString &sql_stmt_str) {arg_.sql_stmt_str_ = sql_stmt_str; }
  inline obrpc::ObAdminZoneArg::AdminZoneOp get_op() const { return arg_.op_; }
  inline void set_op(const obrpc::ObAdminZoneArg::AdminZoneOp op) {
    arg_.op_ = op;
    if (obrpc::ObAdminZoneArg::FORCE_STOP == op) {
      arg_.force_stop_ = true;
    }
  }
  inline int set_alter_region_option() {
    return arg_.alter_zone_options_.add_member(obrpc::ObAdminZoneArg::ALTER_ZONE_REGION);
  }
  inline bool has_alter_region_option() {
    return arg_.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_REGION);
  }
  inline int set_alter_idc_option() {
    return arg_.alter_zone_options_.add_member(obrpc::ObAdminZoneArg::ALTER_ZONE_IDC);
  }
  inline int has_alter_idc_option() {
    return arg_.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_IDC);
  }
  inline int set_alter_zone_type_option() {
    return arg_.alter_zone_options_.add_member(obrpc::ObAdminZoneArg::ALTER_ZONE_TYPE);
  }
  inline bool has_alter_zone_type_option() {
    return arg_.alter_zone_options_.has_member(obrpc::ObAdminZoneArg::ALTER_ZONE_TYPE);
  }
  const obrpc::ObAdminZoneArg &get_arg() const { return arg_; }
private:
  obrpc::ObAdminZoneArg arg_;
};

class ObSwitchReplicaRoleStmt : public ObSystemCmdStmt
{
public:
  ObSwitchReplicaRoleStmt() : ObSystemCmdStmt(stmt::T_SWITCH_REPLICA_ROLE) {}
  virtual ~ObSwitchReplicaRoleStmt() {}

  obrpc::ObAdminSwitchReplicaRoleArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminSwitchReplicaRoleArg rpc_arg_;
};

class ObSwitchRSRoleStmt : public ObSystemCmdStmt
{
public:
  ObSwitchRSRoleStmt() : ObSystemCmdStmt(stmt::T_SWITCH_RS_ROLE) {}
  virtual ~ObSwitchRSRoleStmt() {}

  obrpc::ObAdminSwitchRSRoleArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminSwitchRSRoleArg rpc_arg_;
};

class ObReportReplicaStmt : public ObSystemCmdStmt
{
public:
  ObReportReplicaStmt() : ObSystemCmdStmt(stmt::T_REPORT_REPLICA) {}
  virtual ~ObReportReplicaStmt() {}

  obrpc::ObAdminReportReplicaArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminReportReplicaArg rpc_arg_;
};

class ObRecycleReplicaStmt : public ObSystemCmdStmt
{
public:
  ObRecycleReplicaStmt() : ObSystemCmdStmt(stmt::T_RECYCLE_REPLICA) {}
  virtual ~ObRecycleReplicaStmt() {}

  obrpc::ObAdminRecycleReplicaArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminRecycleReplicaArg rpc_arg_;
};

class ObAdminMergeStmt: public ObSystemCmdStmt
{
public:
  ObAdminMergeStmt() : ObSystemCmdStmt(stmt::T_ADMIN_MERGE) {}
  virtual ~ObAdminMergeStmt() {}

  obrpc::ObAdminMergeArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminMergeArg rpc_arg_;
};

class ObAdminRecoveryStmt: public ObSystemCmdStmt
{
public:
  ObAdminRecoveryStmt() : ObSystemCmdStmt(stmt::T_ADMIN_RECOVERY) {}
  virtual ~ObAdminRecoveryStmt() {}

  obrpc::ObAdminRecoveryArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminRecoveryArg rpc_arg_;
};

class ObClearRoottableStmt : public ObSystemCmdStmt
{
public:
  ObClearRoottableStmt() : ObSystemCmdStmt(stmt::T_CLEAR_ROOT_TABLE) {}
  virtual ~ObClearRoottableStmt() {}

  obrpc::ObAdminClearRoottableArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminClearRoottableArg rpc_arg_;
};

class ObRefreshSchemaStmt : public ObSystemCmdStmt
{
public:
  ObRefreshSchemaStmt() : ObSystemCmdStmt(stmt::T_REFRESH_SCHEMA) {}
  virtual ~ObRefreshSchemaStmt() {}

  obrpc::ObAdminRefreshSchemaArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminRefreshSchemaArg rpc_arg_;
};

class ObRefreshMemStatStmt : public ObSystemCmdStmt
{
public:
  ObRefreshMemStatStmt() : ObSystemCmdStmt(stmt::T_REFRESH_MEMORY_STAT) {}
  virtual ~ObRefreshMemStatStmt() {}

  obrpc::ObAdminRefreshMemStatArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminRefreshMemStatArg rpc_arg_;
};

class ObWashMemFragmentationStmt : public ObSystemCmdStmt
{
public:
  ObWashMemFragmentationStmt() : ObSystemCmdStmt(stmt::T_WASH_MEMORY_FRAGMENTATION) {}
  virtual ~ObWashMemFragmentationStmt() {}

  obrpc::ObAdminWashMemFragmentationArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminWashMemFragmentationArg rpc_arg_;
};

class ObRefreshIOCalibraitonStmt : public ObSystemCmdStmt
{
public:
  ObRefreshIOCalibraitonStmt() : ObSystemCmdStmt(stmt::T_REFRESH_IO_CALIBRATION) {}
  virtual ~ObRefreshIOCalibraitonStmt() {}

  obrpc::ObAdminRefreshIOCalibrationArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminRefreshIOCalibrationArg rpc_arg_;
};

class ObSetConfigStmt : public ObSystemCmdStmt
{
public:
  ObSetConfigStmt() : ObSystemCmdStmt(stmt::T_ALTER_SYSTEM_SET_PARAMETER) {}
  virtual ~ObSetConfigStmt() {}

  obrpc::ObAdminSetConfigArg &get_rpc_arg() { return rpc_arg_; }
  
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminSetConfigArg rpc_arg_;
};

class ObSetTPStmt : public ObSystemCmdStmt
{
public:
  ObSetTPStmt() : ObSystemCmdStmt(stmt::T_ALTER_SYSTEM_SETTP) {}
  virtual ~ObSetTPStmt() {}

  obrpc::ObAdminSetTPArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminSetTPArg rpc_arg_;
};

class ObMigrateUnitStmt : public ObSystemCmdStmt
{
public:
  ObMigrateUnitStmt() : ObSystemCmdStmt(stmt::T_MIGRATE_UNIT) {}
  virtual ~ObMigrateUnitStmt() {}

  obrpc::ObAdminMigrateUnitArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminMigrateUnitArg rpc_arg_;
};

class ObAddArbitrationServiceStmt : public ObSystemCmdStmt
{
public:
  ObAddArbitrationServiceStmt() : ObSystemCmdStmt(stmt::T_ADD_ARBITRATION_SERVICE) {}
  virtual ~ObAddArbitrationServiceStmt() {}
#ifndef OB_BUILD_ARBITRATION
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_));
#else
  obrpc::ObAdminAddArbitrationServiceArg &get_rpc_arg() { return rpc_arg_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminAddArbitrationServiceArg rpc_arg_;
#endif
};

class ObRemoveArbitrationServiceStmt : public ObSystemCmdStmt
{
public:
  ObRemoveArbitrationServiceStmt() : ObSystemCmdStmt(stmt::T_REMOVE_ARBITRATION_SERVICE) {}
  virtual ~ObRemoveArbitrationServiceStmt() {}
#ifndef OB_BUILD_ARBITRATION
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_));
#else
  obrpc::ObAdminRemoveArbitrationServiceArg &get_rpc_arg() { return rpc_arg_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminRemoveArbitrationServiceArg rpc_arg_;
#endif
};

class ObReplaceArbitrationServiceStmt : public ObSystemCmdStmt
{
public:
  ObReplaceArbitrationServiceStmt() : ObSystemCmdStmt(stmt::T_REPLACE_ARBITRATION_SERVICE) {}
  virtual ~ObReplaceArbitrationServiceStmt() {}
#ifndef OB_BUILD_ARBITRATION
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_));
#else
  obrpc::ObAdminReplaceArbitrationServiceArg &get_rpc_arg() { return rpc_arg_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminReplaceArbitrationServiceArg rpc_arg_;
#endif
};

class ObClearLocationCacheStmt : public ObSystemCmdStmt
{
public:
  ObClearLocationCacheStmt() : ObSystemCmdStmt(stmt::T_CLEAR_LOCATION_CACHE) {}
  virtual ~ObClearLocationCacheStmt() {}

  obrpc::ObAdminClearLocationCacheArg &get_rpc_arg() { return rpc_arg_; }
private:
  obrpc::ObAdminClearLocationCacheArg rpc_arg_;
};

class ObReloadUnitStmt : public ObSystemCmdStmt
{
public:
  ObReloadUnitStmt() : ObSystemCmdStmt(stmt::T_RELOAD_UNIT) {}
  virtual ~ObReloadUnitStmt() {}
};

class ObReloadServerStmt : public ObSystemCmdStmt
{
public:
  ObReloadServerStmt() : ObSystemCmdStmt(stmt::T_RELOAD_SERVER) {}
  virtual ~ObReloadServerStmt() {}
};

class ObReloadZoneStmt : public ObSystemCmdStmt
{
public:
  ObReloadZoneStmt() : ObSystemCmdStmt(stmt::T_RELOAD_ZONE) {}
  virtual ~ObReloadZoneStmt() {}
};

class ObClearMergeErrorStmt : public ObSystemCmdStmt
{
public:
  ObClearMergeErrorStmt() : ObSystemCmdStmt(stmt::T_CLEAR_MERGE_ERROR) {}
  virtual ~ObClearMergeErrorStmt() {}

  obrpc::ObAdminMergeArg &get_rpc_arg() { return rpc_arg_; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminMergeArg rpc_arg_;
};

class ObUpgradeVirtualSchemaStmt : public ObSystemCmdStmt
{
public:
  ObUpgradeVirtualSchemaStmt() : ObSystemCmdStmt(stmt::T_UPGRADE_VIRTUAL_SCHEMA) {}
  virtual ~ObUpgradeVirtualSchemaStmt() {}
};

class ObAdminUpgradeCmdStmt : public ObSystemCmdStmt
{
public:
  enum AdminUpgradeOp
  {
    BEGIN = 1,
    END = 2,
  };
  ObAdminUpgradeCmdStmt() : ObSystemCmdStmt(stmt::T_ADMIN_UPGRADE_CMD), op_(BEGIN) {}
  virtual ~ObAdminUpgradeCmdStmt() {}

  inline const AdminUpgradeOp &get_op() const { return op_; }
  inline void set_op(const AdminUpgradeOp op) { op_ = op; }
private:
  AdminUpgradeOp op_;
};

class ObAdminRollingUpgradeCmdStmt : public ObSystemCmdStmt
{
public:
  enum AdminUpgradeOp
  {
    BEGIN = 1,
    END = 2,
  };
  ObAdminRollingUpgradeCmdStmt() : ObSystemCmdStmt(stmt::T_ADMIN_ROLLING_UPGRADE_CMD), op_(BEGIN) {}
  virtual ~ObAdminRollingUpgradeCmdStmt() {}

  inline const AdminUpgradeOp &get_op() const { return op_; }
  inline void set_op(const AdminUpgradeOp op) { op_ = op; }
private:
  AdminUpgradeOp op_;
};

class ObPhysicalRestoreTenantStmt : public ObSystemCmdStmt
{
public:
  ObPhysicalRestoreTenantStmt()
    : ObSystemCmdStmt(stmt::T_PHYSICAL_RESTORE_TENANT),
      rpc_arg_(),
      is_preview_(false) {}
  virtual ~ObPhysicalRestoreTenantStmt() {}

  obrpc::ObPhysicalRestoreTenantArg &get_rpc_arg() { return rpc_arg_; }
  void set_is_preview(const bool is_preview) { is_preview_ = is_preview; }
  bool get_is_preview() const { return is_preview_; }
private:
  obrpc::ObPhysicalRestoreTenantArg rpc_arg_;
  bool is_preview_;
};

class ObRunJobStmt : public ObSystemCmdStmt
{
public:
  ObRunJobStmt() : ObSystemCmdStmt(stmt::T_RUN_JOB) {}
  virtual ~ObRunJobStmt() {}

  obrpc::ObRunJobArg &get_rpc_arg() { return rpc_arg_; }
private:
  obrpc::ObRunJobArg rpc_arg_;
};

class ObRunUpgradeJobStmt : public ObSystemCmdStmt
{
public:
  ObRunUpgradeJobStmt() : ObSystemCmdStmt(stmt::T_ADMIN_RUN_UPGRADE_JOB) {}
  virtual ~ObRunUpgradeJobStmt() {}

  obrpc::ObUpgradeJobArg &get_rpc_arg() { return rpc_arg_; }
private:
  obrpc::ObUpgradeJobArg rpc_arg_;
};

class ObStopUpgradeJobStmt : public ObSystemCmdStmt
{
public:
  ObStopUpgradeJobStmt() : ObSystemCmdStmt(stmt::T_ADMIN_STOP_UPGRADE_JOB) {}
  virtual ~ObStopUpgradeJobStmt() {}

  obrpc::ObUpgradeJobArg &get_rpc_arg() { return rpc_arg_; }
private:
  obrpc::ObUpgradeJobArg rpc_arg_;
};

class ObRefreshTimeZoneInfoStmt : public ObSystemCmdStmt
{
public:
  ObRefreshTimeZoneInfoStmt() : ObSystemCmdStmt(stmt::T_REFRESH_TIME_ZONE_INFO),
                                tenant_id_(OB_INVALID_TENANT_ID)
  { }
  virtual ~ObRefreshTimeZoneInfoStmt() {}
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() { return tenant_id_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id));

  uint64_t tenant_id_;
};

class ObCancelTaskStmt : public ObSystemCmdStmt
{
public:
  ObCancelTaskStmt()
    : ObSystemCmdStmt(stmt::T_CANCEL_TASK),
      task_type_(share::MAX_SYS_TASK_TYPE),
      task_id_()
  {
  }
  virtual ~ObCancelTaskStmt() {}
  const share::ObSysTaskType &get_task_type() { return task_type_; }
  const common::ObString &get_task_id() { return task_id_; }
  int set_param(const share::ObSysTaskType &task_type, const common::ObString &task_id)
  {
    int ret = common::OB_SUCCESS;

    if (task_type < 0 || task_type> share::MAX_SYS_TASK_TYPE || task_id.length() <= 0) {
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

class ObSetDiskValidStmt : public ObSystemCmdStmt
{
public:
  ObSetDiskValidStmt():
    ObSystemCmdStmt(stmt::T_SET_DISK_VALID),
    server_()
  {}
  virtual ~ObSetDiskValidStmt() {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(server));

  common::ObAddr server_;
};

class ObAddDiskStmt : public ObSystemCmdStmt
{
public:
  ObAddDiskStmt():
    ObSystemCmdStmt(stmt::T_ALTER_DISKGROUP_ADD_DISK)
  {}
  virtual ~ObAddDiskStmt() {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(arg));

  obrpc::ObAdminAddDiskArg arg_;
};

class ObDropDiskStmt : public ObSystemCmdStmt
{
public:
  ObDropDiskStmt():
    ObSystemCmdStmt(stmt::T_ALTER_DISKGROUP_DROP_DISK)
  {}
  virtual ~ObDropDiskStmt() {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(arg));

  obrpc::ObAdminDropDiskArg arg_;
};

class ObEnableSqlThrottleStmt
    : public ObSystemCmdStmt
{
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
  void set_priority(int64_t priority) { priority_ = priority; }
  void set_rt(double rt) { rt_ = rt; }
  void set_io(int64_t io) { io_ = io; }
  void set_network(double network) { network_ = network; }
  void set_cpu(double cpu) { cpu_ = cpu; }
  void set_logical_reads(int64_t logical_reads) { logical_reads_ = logical_reads; }
  void set_queue_time(double queue_time) { queue_time_ = queue_time; }

  int64_t get_priority() const { return priority_; }
  double get_rt() const { return rt_; }
  int64_t get_io() const { return io_; }
  double get_network() const { return network_; }
  double get_cpu() const { return cpu_; }
  int64_t get_logical_reads() const { return logical_reads_; }
  double get_queue_time() const { return queue_time_; }

  TO_STRING_KV(
      N_STMT_TYPE, ((int)stmt_type_),
      K_(priority),
      K_(rt),
      K_(io),
      K_(network),
      K_(cpu),
      K_(logical_reads),
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

class ObDisableSqlThrottleStmt
  : public ObSystemCmdStmt
{
public:
  ObDisableSqlThrottleStmt()
    : ObSystemCmdStmt(stmt::T_DISABLE_SQL_THROTTLE)
    {}
};

class ObChangeTenantStmt : public ObSystemCmdStmt
{
public:
  ObChangeTenantStmt():
    ObSystemCmdStmt(stmt::T_CHANGE_TENANT),
    tenant_id_(OB_INVALID_TENANT_ID)
  {}
  virtual ~ObChangeTenantStmt() {}
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() { return tenant_id_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id));

  uint64_t tenant_id_;
};

class ObArchiveLogStmt : public ObSystemCmdStmt
{
public:
  ObArchiveLogStmt()
    : ObSystemCmdStmt(stmt::T_ARCHIVE_LOG),
      enable_(true),
      tenant_id_(OB_INVALID_TENANT_ID),
      archive_tenant_ids_()
  {
  }
  virtual ~ObArchiveLogStmt() {}
  bool is_enable() const { return enable_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const common::ObIArray<uint64_t> &get_archive_tenant_ids() const { return archive_tenant_ids_; }
  int set_param(
      const bool enable,
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &archive_tenant_ids)
  {
    int ret = common::OB_SUCCESS;
    if (OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", K(tenant_id)); 
    } else if (OB_FAIL(archive_tenant_ids_.assign(archive_tenant_ids))) {
      COMMON_LOG(WARN, "failed to assign archive tenant ids", K(ret), K(archive_tenant_ids));
    } else {
      enable_ = enable;
      tenant_id_ = tenant_id; 
    }
    return ret;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(enable), K_(tenant_id), K_(archive_tenant_ids));

private:
  bool enable_;
  uint64_t tenant_id_;
  common::ObArray<uint64_t> archive_tenant_ids_;
};

class ObBackupDatabaseStmt : public ObSystemCmdStmt
{
public:
  ObBackupDatabaseStmt()
    : ObSystemCmdStmt(stmt::T_BACKUP_DATABASE),
      tenant_id_(OB_INVALID_ID),
	    incremental_(false),
      compl_log_(false),
      backup_dest_(),
      backup_description_(),
      backup_tenant_ids_()
  {
  }
  virtual ~ObBackupDatabaseStmt() {}
  bool get_incremental() const { return incremental_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
	bool get_compl_log() const { return compl_log_; }
  const share::ObBackupPathString &get_backup_dest() const { return backup_dest_; }
  const share::ObBackupDescription &get_backup_description() const { return backup_description_; }
  const common::ObSArray<uint64_t> &get_backup_tenant_ids() const { return backup_tenant_ids_; }
  int set_param(const uint64_t tenant_id, const int64_t incremental, const int64_t compl_log, 
      const share::ObBackupPathString &backup_dest, const share::ObBackupDescription &backup_description,
      const ObIArray<uint64_t> &backup_tenant_ids)
  {
    int ret = common::OB_SUCCESS;

    if (tenant_id == OB_INVALID_ID
	      || (0 != incremental && 1 != incremental)
        || (0 != compl_log && 1 != compl_log)) {
      ret = OB_INVALID_ARGUMENT;
	    COMMON_LOG(WARN, "invalid args", K(tenant_id), K(incremental), K(compl_log));
    } else if (OB_FAIL(backup_dest_.assign(backup_dest))) {
      COMMON_LOG(WARN, "set backup dest failed", K(backup_dest));
    } else if (OB_FAIL(append(backup_tenant_ids_, backup_tenant_ids))) {
      COMMON_LOG(WARN, "append backup tenant ids failed", K(backup_tenant_ids));
    } else if (OB_FAIL(backup_description_.assign(backup_description))) {
      COMMON_LOG(WARN, "set backup description failed", K(backup_description));
    } else {
      incremental_ = incremental != 0;
      tenant_id_ = tenant_id;
      compl_log_ = compl_log!= 0;
    }
    return ret;
  }
	TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(incremental), K_(backup_tenant_ids), 
      K_(backup_description), K_(compl_log), K_(backup_dest));


private:
	uint64_t tenant_id_;
  bool incremental_;
  bool compl_log_;
  share::ObBackupPathString backup_dest_;
  share::ObBackupDescription backup_description_;
  common::ObSArray<uint64_t> backup_tenant_ids_;
};

class ObCancelRestoreStmt : public ObSystemCmdStmt
{
public:
  ObCancelRestoreStmt()
    : ObSystemCmdStmt(stmt::T_CANCEL_RESTORE),
      drop_tenant_arg_() {}
  virtual ~ObCancelRestoreStmt() {}
  obrpc::ObDropTenantArg &get_drop_tenant_arg() { return drop_tenant_arg_; }
	TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(drop_tenant_arg));
private:
  obrpc::ObDropTenantArg drop_tenant_arg_;
};

class ObBackupBackupsetStmt : public ObSystemCmdStmt
{
public:
  ObBackupBackupsetStmt()
    : ObSystemCmdStmt(stmt::T_BACKUP_BACKUPSET),
      tenant_id_(OB_INVALID_ID),
      backup_set_id_(-1),
      max_backup_times_(-1)
  {
  }
  virtual ~ObBackupBackupsetStmt() {}
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_backup_set_id() const { return backup_set_id_; }
  int64_t get_max_backup_times() const { return max_backup_times_; }
  const ObString get_backup_backup_dest() const { return backup_backup_dest_; }

  int set_param(
      const uint64_t tenant_id,
      int64_t backup_set_id,
      const int64_t max_backup_times,
      const common::ObString &backup_backup_dest)
  {
    int ret = common::OB_SUCCESS;
    if (OB_INVALID_ID == tenant_id || backup_set_id < 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", KR(ret), K(tenant_id),
          K(backup_set_id), K(backup_backup_dest));
    } else if (OB_FAIL(databuff_printf(backup_backup_dest_, sizeof(backup_backup_dest_),
        "%.*s", backup_backup_dest.length(), backup_backup_dest.ptr()))) {
      COMMON_LOG(WARN, "failed to databuff printf", KR(ret));
    } else {
      tenant_id_ = tenant_id;
      backup_set_id_ = backup_set_id;
      max_backup_times_ = max_backup_times;
    }
    return ret;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id),
      K_(backup_set_id), K_(max_backup_times), K_(backup_backup_dest));
private:
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t max_backup_times_;
  char backup_backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH];
};

class ObBackupArchiveLogStmt : public ObSystemCmdStmt
{
public:
  ObBackupArchiveLogStmt()
    : ObSystemCmdStmt(stmt::T_BACKUP_ARCHIVELOG),
      enable_(true)
  {
  }
  virtual ~ObBackupArchiveLogStmt() {}
  bool is_enable() const { return enable_; }
  void set_is_enable(const bool enable) { enable_ = enable; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(enable));

private:
  bool enable_;
};

class ObBackupBackupPieceStmt : public ObSystemCmdStmt
{
public:
  ObBackupBackupPieceStmt()
    : ObSystemCmdStmt(stmt::T_BACKUP_BACKUPPIECE),
      tenant_id_(OB_INVALID_ID),
      piece_id_(-1),
      max_backup_times_(-1),
      backup_all_(false),
      backup_backup_dest_(""),
      with_active_piece_(false)
  {
  }
  virtual ~ObBackupBackupPieceStmt() {}
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_piece_id() const { return piece_id_; }
  int64_t get_max_backup_times() const { return max_backup_times_; }
  bool is_backup_all() const { return backup_all_; }
  const ObString get_backup_backup_dest() const { return backup_backup_dest_; }
  bool with_active_piece() const { return with_active_piece_; }

  int set_param(const uint64_t tenant_id, const int64_t piece_id,
      const int64_t max_backup_times, const bool backup_all,
      const bool with_active_piece, const common::ObString &backup_backup_dest)
  {
    int ret = common::OB_SUCCESS;
    const int64_t MAX_BACKUP_TIMES = 2;
    if (OB_INVALID_ID == tenant_id || piece_id < 0
        || max_backup_times > MAX_BACKUP_TIMES) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(piece_id), K(max_backup_times));
    } else if (OB_FAIL(databuff_printf(backup_backup_dest_, sizeof(backup_backup_dest_),
        "%.*s", backup_backup_dest.length(), backup_backup_dest.ptr()))) {
      COMMON_LOG(WARN, "failed to databuff printf", KR(ret));
    } else {
      tenant_id_ = tenant_id;
      piece_id_ = piece_id;
      max_backup_times_ = max_backup_times;
      backup_all_ = backup_all;
      with_active_piece_ = with_active_piece;
    }
    return ret;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(piece_id),
      K_(max_backup_times), K_(backup_all), K_(backup_backup_dest));
private:
  uint64_t tenant_id_;
  int64_t piece_id_;
  int64_t max_backup_times_;
  bool backup_all_;
  char backup_backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH];
  bool with_active_piece_;
};

class ObBackupManageStmt : public ObSystemCmdStmt
{
public:
  ObBackupManageStmt():
      ObSystemCmdStmt(stmt::T_BACKUP_MANAGE),
      tenant_id_(OB_INVALID_ID),
      managed_tenant_ids_(),
      type_(obrpc::ObBackupManageArg::MAX_TYPE),
      value_(0),
      copy_id_(0)
  {
  }
  virtual ~ObBackupManageStmt() {}
  obrpc::ObBackupManageArg::Type get_type() const { return type_; }
  int64_t get_value() const { return value_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_copy_id() const { return copy_id_; }
  const common::ObSArray<uint64_t> &get_managed_tenant_ids() const { return managed_tenant_ids_; }
  int set_param(const uint64_t tenant_id, const int64_t type, const int64_t value, const int64_t copy_id,
      common::ObIArray<uint64_t> &managed_tenant_ids)
  {
    int ret = common::OB_SUCCESS;

    if (tenant_id == OB_INVALID_ID || type < 0 || type >= obrpc::ObBackupManageArg::MAX_TYPE
        || value < 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", K(tenant_id), K(type), K(value));
    } else if (OB_FAIL(append(managed_tenant_ids_, managed_tenant_ids))) {
      COMMON_LOG(WARN, "failed to append managed tenants", K(managed_tenant_ids));
    } else {
      type_ = static_cast<obrpc::ObBackupManageArg::Type>(type);
      value_ = value;
      tenant_id_ = tenant_id;
      copy_id_ = copy_id;
    }

    return ret;
  }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(managed_tenant_ids), K_(type), K_(value), K_(copy_id));


private:
  uint64_t tenant_id_;
  common::ObSArray<uint64_t> managed_tenant_ids_; 
  obrpc::ObBackupManageArg::Type type_;
  int64_t value_;
  int64_t copy_id_;
};

class ObBackupCleanStmt : public ObSystemCmdStmt
{
public:
  ObBackupCleanStmt():
      ObSystemCmdStmt(stmt::T_BACKUP_CLEAN),
      initiator_tenant_id_(OB_INVALID_TENANT_ID),
      type_(share::ObNewBackupCleanType::MAX),
      value_(0),
      copy_id_(0),
      description_(),
      clean_tenant_ids_()
  {
  }
  virtual ~ObBackupCleanStmt() {}
  share::ObNewBackupCleanType::TYPE get_type() const { return type_; }
  int64_t get_value() const { return value_; }
  uint64_t get_tenant_id() const { return initiator_tenant_id_; }
  int64_t get_copy_id() const { return copy_id_; }
  const share::ObBackupDescription &get_description() const { return description_; }
  const common::ObSArray<uint64_t> &get_clean_tenant_ids() const { return clean_tenant_ids_; }
  int set_param(
      const uint64_t tenant_id, 
      const int64_t type, 
      const int64_t value, 
      const int64_t copy_id, 
      const share::ObBackupDescription &description,
      const ObSArray<uint64_t> &clean_tenant_ids)
  {
    int ret = common::OB_SUCCESS;

    if (OB_INVALID_ID == tenant_id || type <= 0 || type >= share::ObNewBackupCleanType::MAX
        || value < 0) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", K(tenant_id), K(type), K(value));
    } else if (OB_FAIL(description_.assign(description))) {
      COMMON_LOG(WARN, "set description failed", K(description));
    } else if (OB_FAIL(append(clean_tenant_ids_, clean_tenant_ids))) {
      COMMON_LOG(WARN, "append clean tenant ids failed", K(clean_tenant_ids));
    } else {
      type_ = static_cast<share::ObNewBackupCleanType::TYPE>(type);
      value_ = value;
      initiator_tenant_id_ = tenant_id;
      copy_id_ = copy_id;
    }

    return ret;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(initiator_tenant_id), K_(type), K_(value), K_(copy_id), K_(description), K_(clean_tenant_ids));

private:
  uint64_t initiator_tenant_id_;
  share::ObNewBackupCleanType::TYPE type_;
  int64_t value_;
  int64_t copy_id_;
  share::ObBackupDescription description_; 
  common::ObSArray<uint64_t> clean_tenant_ids_; 
};

class ObDeletePolicyStmt : public ObSystemCmdStmt
{
public:
  ObDeletePolicyStmt():
      ObSystemCmdStmt(stmt::T_DELETE_POLICY),
      initiator_tenant_id_(OB_INVALID_TENANT_ID),
      type_(share::ObPolicyOperatorType::MAX),
      policy_name_(),
      recovery_window_(),
      redundancy_(0),
      backup_copies_(0),
      clean_tenant_ids_()
  {
  }
  virtual ~ObDeletePolicyStmt() {}
  share::ObPolicyOperatorType get_type() const { return type_; }
  uint64_t get_tenant_id() const { return initiator_tenant_id_; }
  const char *get_policy_name() const { return policy_name_; }
  const char *get_recovery_window() const { return recovery_window_; }
  int64_t get_redundancy() const { return redundancy_; }
  int64_t get_backup_copies() const { return backup_copies_; }

  const common::ObSArray<uint64_t> &get_clean_tenant_ids() const { return clean_tenant_ids_; }
  int set_param(
      const uint64_t tenant_id, 
      const int64_t type,
      const ObString &policy_name,
      const ObSArray<uint64_t> &clean_tenant_ids)
  {
    int ret = common::OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id) || type < 0 || type >= share::ObPolicyOperatorType::MAX) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", K(tenant_id), K(type));
    } else if (OB_FAIL(databuff_printf(policy_name_, sizeof(policy_name_), "%s", policy_name.ptr()))) {
      COMMON_LOG(WARN, "set policy name failed", K(policy_name));
    } else if (OB_FAIL(append(clean_tenant_ids_, clean_tenant_ids))) {
      COMMON_LOG(WARN, "append clean tenant ids failed", K(clean_tenant_ids));
    } else {
      type_ = static_cast<share::ObPolicyOperatorType>(type);
      initiator_tenant_id_ = tenant_id;
    }
    return ret;
  }
  int set_delete_policy(
      const ObString &recovery_window,
      const int64_t redundancy,
      const int64_t backup_copies)
  {
    int ret = common::OB_SUCCESS; 
    if (OB_FAIL(databuff_printf(recovery_window_, sizeof(recovery_window_), "%s", recovery_window.ptr()))) {
      COMMON_LOG(WARN, "set recovery window failed", K(recovery_window));
    } else {
      redundancy_ = redundancy; 
      backup_copies_ = backup_copies;
    }
    return ret;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(initiator_tenant_id), K_(type), K_(policy_name), 
      K_(recovery_window), K_(redundancy),  K_(backup_copies), K_(clean_tenant_ids));

private:
  uint64_t initiator_tenant_id_;
  share::ObPolicyOperatorType type_;
  char policy_name_[share::OB_BACKUP_DELETE_POLICY_NAME_LENGTH];
  char recovery_window_[share::OB_BACKUP_RECOVERY_WINDOW_LENGTH];
  int64_t redundancy_;
  int64_t backup_copies_;
  common::ObSArray<uint64_t> clean_tenant_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObDeletePolicyStmt);
};

class ObBackupKeyStmt : public ObSystemCmdStmt
{
public:
  ObBackupKeyStmt()
    : ObSystemCmdStmt(stmt::T_BACKUP_KEY),
      tenant_id_(OB_INVALID_TENANT_ID),
      backup_dest_()
  {
  }
  virtual ~ObBackupKeyStmt() {}

  uint64 get_tenant_id() const { return tenant_id_; }
  const share::ObBackupPathString &get_backup_dest() const { return backup_dest_; }
  const ObString &get_encrypt_key() const { return encrypt_key_; }
  int set_param(const uint64_t tenant_id,
                const share::ObBackupPathString &backup_dest,
                const ObString &encrypt_key)
  {
    int ret = common::OB_SUCCESS;
    if (OB_INVALID_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
	    COMMON_LOG(WARN, "invalid args", K(tenant_id));
    } else if (OB_FAIL(backup_dest_.assign(backup_dest))) {
      COMMON_LOG(WARN, "set backup dest failed", K(backup_dest));
    } else {
      tenant_id_ = tenant_id;
      encrypt_key_ = encrypt_key;
    }
    return ret;
  }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(backup_dest));

private:
  uint64_t tenant_id_;
  share::ObBackupPathString backup_dest_;
  ObString encrypt_key_;
};
class ObTableTTLStmt : public ObSystemCmdStmt {
public:
  ObTableTTLStmt()
    : ObSystemCmdStmt(stmt::T_TABLE_TTL),
      type_(obrpc::ObTTLRequestArg::TTL_INVALID_TYPE),
      opt_tenant_ids_(),
      ttl_all_(false)
  {}
  virtual ~ObTableTTLStmt()
  {}

  obrpc::ObTTLRequestArg::TTLRequestType get_type() const
  {
    return type_;
  }
  int set_type(const int64_t type)
  {
    int ret = common::OB_SUCCESS;

    if (type < 0 || type >= obrpc::ObTTLRequestArg::TTL_MOVE_TYPE) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid args", K(type));
    } else {
      type_ = static_cast<obrpc::ObTTLRequestArg::TTLRequestType>(type);
    }

    return ret;
  }
  inline common::ObSArray<uint64_t> &get_tenant_ids() { return opt_tenant_ids_; }
  bool is_ttl_all() const { return ttl_all_; }
  void set_ttl_all(bool ttl_all) { ttl_all_ = ttl_all; }

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(type),
               K_(opt_tenant_ids), K_(ttl_all));

private:
  uint64_t tenant_id_;
  obrpc::ObTTLRequestArg::TTLRequestType type_;
  common::ObSArray<uint64_t> opt_tenant_ids_;
  bool ttl_all_;
};

class ObBackupSetEncryptionStmt : public ObSystemCmdStmt
{
public:
  ObBackupSetEncryptionStmt();
  virtual ~ObBackupSetEncryptionStmt() {}
  share::ObBackupEncryptionMode::EncryptionMode get_mode() const { return mode_; }
  const ObString get_passwd() const { return encrypted_passwd_; }
  int set_param(const int64_t mode, const common::ObString &passwd);
  TO_STRING_KV(N_STMT_TYPE,  ((int)stmt_type_),
      "mode", share::ObBackupEncryptionMode::to_str(mode_), K_(encrypted_passwd));
private:
  share::ObBackupEncryptionMode::EncryptionMode mode_;
  char passwd_buf_[OB_MAX_PASSWORD_LENGTH];
  ObString encrypted_passwd_;
};

class ObBackupSetDecryptionStmt : public ObSystemCmdStmt
{
public:
  ObBackupSetDecryptionStmt();
  virtual ~ObBackupSetDecryptionStmt() {}
  ObString get_passwd_array() const { return passwd_array_; }
  int add_passwd(const ObString &passwd);
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(pos), K_(passwd_array));
private:
  char passwd_array_[OB_MAX_PASSWORD_ARRAY_LENGTH];
  int64_t pos_;
};

class ObSetRegionBandwidthStmt : public ObSystemCmdStmt
{
public:
  ObSetRegionBandwidthStmt();
  virtual ~ObSetRegionBandwidthStmt() {}
  const char* get_src_region() const { return src_region_; }
  const char* get_dst_region() const { return dst_region_; }
  int64_t get_max_bw() const { return max_bw_; }
  int set_param(const char *src_region, const char *dst_region, const int64_t max_bw);
  void set_src_region(const char *region_str)
  {
    snprintf(src_region_, MAX_REGION_LENGTH, "%s", region_str);
  }
  void set_dst_region(const char *region_str)
  {
    snprintf(dst_region_, MAX_REGION_LENGTH, "%s", region_str);
  }
  void set_dst_region(const int64_t max_bw) { max_bw_ = max_bw; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_) , K_(src_region), K_(dst_region), K_(max_bw));
private:
  // common::ObRegion src_region_;
  // common::ObRegion dst_region_;
  char src_region_[MAX_REGION_LENGTH];
  char dst_region_[MAX_REGION_LENGTH];
  int64_t max_bw_;
};

class ObAddRestoreSourceStmt : public ObSystemCmdStmt
{
public:
  ObAddRestoreSourceStmt();
  virtual ~ObAddRestoreSourceStmt() {}
  const ObString get_restore_source_array() const { return restore_source_array_; }
  int add_restore_source(const common::ObString &source);
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(restore_source_array));
private:
  static const int64_t MAX_RESTORE_SOURCE_LENGTH = 365 * 10 * share::OB_MAX_BACKUP_DEST_LENGTH;
  char restore_source_array_[MAX_RESTORE_SOURCE_LENGTH];
  int64_t pos_;
};

class ObClearRestoreSourceStmt : public ObSystemCmdStmt
{
public:
  ObClearRestoreSourceStmt() : ObSystemCmdStmt(stmt::T_CLEAR_RESTORE_SOURCE) {}
  virtual ~ObClearRestoreSourceStmt() {}
};

class ObCheckpointSlogStmt : public ObSystemCmdStmt
{
public:
  ObCheckpointSlogStmt()
    : ObSystemCmdStmt(stmt::T_CHECKPOINT_SLOG),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      server_()
  {}
  virtual ~ObCheckpointSlogStmt() {}
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(tenant_id), K_(server));

  uint64_t tenant_id_;
  common::ObAddr server_;
};

class ObRecoverTenantStmt : public ObSystemCmdStmt
{
public:
  ObRecoverTenantStmt()
    : ObSystemCmdStmt(stmt::T_RECOVER),
      rpc_arg_() {}
  virtual ~ObRecoverTenantStmt() {}

  obrpc::ObRecoverTenantArg &get_rpc_arg() { return rpc_arg_; }
private:
  obrpc::ObRecoverTenantArg rpc_arg_;
};

class ObRecoverTableStmt : public ObSystemCmdStmt
{
public:
  ObRecoverTableStmt()
    : ObSystemCmdStmt(stmt::T_RECOVER_TABLE), rpc_arg_() {}
  virtual ~ObRecoverTableStmt() {}
  obrpc::ObRecoverTableArg &get_rpc_arg() { return rpc_arg_; }
private:
  obrpc::ObRecoverTableArg rpc_arg_;
};

class ObResetConfigStmt : public ObSystemCmdStmt
{
public:
  ObResetConfigStmt() : ObSystemCmdStmt(stmt::T_ALTER_SYSTEM_RESET_PARAMETER) {}
  virtual ~ObResetConfigStmt() {}
  obrpc::ObAdminSetConfigArg &get_rpc_arg() { return rpc_arg_; }
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(rpc_arg));
private:
  obrpc::ObAdminSetConfigArg rpc_arg_;
};

class ObCancelCloneStmt : public ObSystemCmdStmt
{
public:
  ObCancelCloneStmt()
    : ObSystemCmdStmt(stmt::T_CANCEL_CLONE),
      clone_tenant_name_() {}
  virtual ~ObCancelCloneStmt() {}
  int set_clone_tenant_name(const ObString &tenant_name) { return clone_tenant_name_.assign(tenant_name); }
  const ObString get_clone_tenant_name() { return clone_tenant_name_.str(); }
	TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), K_(clone_tenant_name));
private:
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> clone_tenant_name_;
};
class ObTransferPartitionStmt : public ObSystemCmdStmt
{
public:
  ObTransferPartitionStmt()
    : ObSystemCmdStmt(stmt::T_TRANSFER_PARTITION),
      arg_() {}
  virtual ~ObTransferPartitionStmt() {}

  rootserver::ObTransferPartitionArg &get_arg() { return arg_; }
private:
  rootserver::ObTransferPartitionArg arg_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_RESOLVER_CMD_OB_ALTER_SYSTEM_STMT_
