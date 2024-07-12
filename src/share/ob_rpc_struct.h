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

#ifndef OCEANBASE_RPC_OB_RPC_STRUCT_H_
#define OCEANBASE_RPC_OB_RPC_STRUCT_H_

#include "common/ob_member.h"
#include "common/ob_member_list.h"
#include "common/ob_role.h"
#include "common/ob_role_mgr.h"
#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "common/ob_idc.h"
#include "common/ob_zone_type.h"
#include "common/ob_common_types.h"
#include "common/ob_store_format.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ddl_common.h"
#include "share/ob_debug_sync.h"
#include "share/ob_partition_modify.h"
#include "share/ob_zone_info.h"
#include "share/ob_server_status.h"
#include "share/ob_simple_batch.h"
#include "share/ob_tenant_id_schema_version.h"
#include "share/ob_cluster_role.h"            // ObClusterRole PRIMARY_CLUSTER
#include "share/ob_web_service_root_addr.h"
#include "share/ob_cluster_version.h"
#include "share/restore/ob_physical_restore_info.h"
#include "share/schema/ob_error_info.h"
#include "share/schema/ob_constraint.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_udf.h"
#include "share/schema/ob_dependency_info.h"
#include "share/schema/ob_trigger_info.h"
#include "share/ob_storage_format.h"
#include "share/io/ob_io_calibration.h"  // ObIOBenchResult
#include "rootserver/ob_rs_job_table_operator.h"
#include "sql/executor/ob_task_id.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "objit/common/ob_item_type.h"
#include "ob_i_tablet_scan.h"
#include "storage/ob_i_table.h"
#include "share/ob_ls_id.h"
#include "share/ls/ob_ls_info.h"
#include "share/ls/ob_ls_i_life_manager.h"//ObLSStatus
#include "share/ob_tablet_autoincrement_param.h"
#include "share/ob_tenant_info_proxy.h"//ObAllTenantInfo
#include "share/ob_alive_server_tracer.h"//ServerAddr
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_multi_data_source.h"
#include "share/unit/ob_unit_info.h" //ObUnit*
#include "share/backup/ob_backup_clean_struct.h"
#include "logservice/palf/palf_options.h"//access mode
#include "logservice/palf/palf_base_info.h"//PalfBaseInfo
#include "logservice/palf/log_define.h"//INVALID_PROPOSAL_ID
#ifdef OB_BUILD_ARBITRATION
#include "logservice/arbserver/ob_arb_gc_utils.h"                   // TenantLSIDS
#endif
#include "share/location_cache/ob_vtable_location_service.h" // share::ObVtableLocationType
#include "share/ob_balance_define.h" // share::ObTransferTaskID
#include "share/config/ob_config.h" // ObConfigArray
#include "logservice/palf/log_meta_info.h"//LogConfigVersion
#include "share/scn.h"//SCN
#include "share/ob_server_table_operator.h"
#include "share/restore/ob_import_arg.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"
#include "share/location_cache/ob_location_update_task.h"
#include "share/resource_limit_calculator/ob_resource_limit_calculator.h"//ObUserResourceCalculateArg
#include "share/ob_heartbeat_handler.h"
#include "storage/tablelock/ob_table_lock_common.h"       //ObTableLockPriority

namespace oceanbase
{
namespace rootserver
{
struct ObGlobalIndexTask;
}
namespace obrpc
{
typedef common::ObSArray<common::ObAddr> ObServerList;
static const int64_t MAX_COUNT = 128;
static const int64_t OB_DEFAULT_ARRAY_SIZE = 8;
typedef common::ObFixedLengthString<common::OB_MAX_CLUSTER_NAME_LENGTH> ObClusterName;
typedef common::ObFixedLengthString<common::OB_MAX_CONFIG_URL_LENGTH> ObConfigUrl;

enum ObUpgradeStage {
  OB_UPGRADE_STAGE_INVALID,
  OB_UPGRADE_STAGE_NONE,
  OB_UPGRADE_STAGE_PREUPGRADE,
  OB_UPGRADE_STAGE_DBUPGRADE,
  OB_UPGRADE_STAGE_POSTUPGRADE,
  OB_UPGRADE_STAGE_MAX
};
const char* get_upgrade_stage_str(ObUpgradeStage stage);
ObUpgradeStage get_upgrade_stage(const common::ObString &str);

enum class MigrateMode
{
  MT_LOCAL_FS_MODE = 0,
  MT_OFS_SINGLE_ZONE_MODE,
  MT_OFS_MULTI_ZONE_MODE,
  MT_MAX,
};

enum ObDefaultRoleFlag
{
  OB_DEFUALT_NONE = 0,
  OB_DEFAULT_ROLE_LIST = 1,
  OB_DEFAULT_ROLE_ALL = 2,
  OB_DEFAULT_ROLE_ALL_EXCEPT = 3,
  OB_DEFAULT_ROLE_NONE = 4,
  OB_DEFAULT_ROLE_DEFAULT = 5,
  OB_DEFAULT_ROLE_MAX,
};
struct Bool
{
  OB_UNIS_VERSION(1);

public:
  Bool(bool v = false)
      : v_(v) {}

  operator bool () { return v_; }
  operator bool () const { return v_; }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  bool v_;
};

struct Int64
{
  OB_UNIS_VERSION(1);

public:
  Int64(int64_t v = common::OB_INVALID_ID)
      : v_(v) {}

  inline void reset();
  bool is_valid() const { return true; }
  operator int64_t () { return v_; }
  operator int64_t () const { return v_; }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  int64_t v_;
};

struct UInt64
{
  OB_UNIS_VERSION(1);

public:
  UInt64(uint64_t v = common::OB_INVALID_ID)
      : v_(v) {}

  operator uint64_t () { return v_; }
  operator uint64_t () const { return v_; }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  uint64_t v_;
};

struct ObGetRootserverRoleResult
{
  OB_UNIS_VERSION(1);

public:
  ObGetRootserverRoleResult():
    role_(FOLLOWER), status_(share::status::MAX)
  {}
  inline const common::ObRole &get_role() const { return role_; }
  inline const share::status::ObRootServiceStatus &get_status() const { return status_; }
  int init(const common::ObRole &role, const share::status::ObRootServiceStatus &status);
  void reset();
  int assign(const ObGetRootserverRoleResult &other);
  DECLARE_TO_STRING;

private:
  common::ObRole role_;
  share::status::ObRootServiceStatus status_;
};

struct ObServerInfo
{
  OB_UNIS_VERSION(1);

public:
  common::ObZone zone_;
  common::ObAddr server_;
  common::ObRegion region_;

  // FIXME: (xiaochu.yh) Do you need to consider region_ comparison after adding region_? I don’t need to consider it for the time being
  bool operator <(const ObServerInfo &r) const { return zone_ < r.zone_; }
  DECLARE_TO_STRING;
};

struct ObPartitionId
{
  OB_UNIS_VERSION(1);

public:
  int64_t table_id_;
  int64_t partition_id_;

  ObPartitionId() : table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_INDEX) {}

  DECLARE_TO_STRING;
};

typedef common::ObSArray<ObServerInfo> ObServerInfoList;
typedef common::ObArray<ObServerInfoList> ObPartitionServerList;

struct ObDDLArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLArg() :
      ddl_stmt_str_(),
      exec_tenant_id_(common::OB_INVALID_TENANT_ID),
      ddl_id_str_(),
      sync_from_primary_(false),
      based_schema_object_infos_(),
      parallelism_(0),
      task_id_(0),
      consumer_group_id_(0),
      is_parallel_(false)
   { }
  virtual ~ObDDLArg() = default;
  bool is_need_check_based_schema_objects() const
  {
    return 0 < based_schema_object_infos_.count();
  }
  virtual bool is_allow_when_disable_ddl() const { return false; }
  virtual bool is_allow_when_upgrade() const { return false; }
  bool is_sync_from_primary() const
  {
    return sync_from_primary_;
  }
  //user tenant can not ddl in standby
  virtual bool is_allow_in_standby() const
  { return !is_user_tenant(exec_tenant_id_); }
  virtual int assign(const ObDDLArg &other);
  virtual bool contain_sensitive_data() const { return false; }
  void reset()
  {
    ddl_stmt_str_.reset();
    exec_tenant_id_ = common::OB_INVALID_TENANT_ID;
    ddl_id_str_.reset();
    sync_from_primary_ = false;
    based_schema_object_infos_.reset();
    parallelism_ = 0;
    task_id_ = 0;
    consumer_group_id_ = 0;
    is_parallel_ = false;
  }
  DECLARE_TO_STRING;

  common::ObString ddl_stmt_str_;
  uint64_t exec_tenant_id_;
  common::ObString ddl_id_str_;
  bool sync_from_primary_;
  common::ObSArray<share::schema::ObBasedSchemaObjectInfo> based_schema_object_infos_;
  int64_t parallelism_;
  int64_t task_id_;
  int64_t consumer_group_id_;
  //some parallel ddl is effect before 4220, this member is valid after 4220
  bool is_parallel_;
};

struct ObAlterResourceUnitArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObAlterResourceUnitArg() :
      ObDDLArg(),
      unit_config_()
  {}
  virtual ~ObAlterResourceUnitArg() {}
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual int assign(const ObAlterResourceUnitArg &other);

  int init(const common::ObString &name, const share::ObUnitResource &ur);

  const share::ObUnitConfig &get_unit_config() const { return unit_config_; }

  TO_STRING_KV(K_(unit_config));

protected:
  // Unit Config
  //
  // Not all properties are valid. Here only use ObUnitConfig to record user input
  //    * valid resource means "is specified by user"
  //    * non-valid resource means "is not specified by user"
  share::ObUnitConfig   unit_config_;
};

struct ObCreateResourceUnitArg : public ObAlterResourceUnitArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateResourceUnitArg() :
      ObAlterResourceUnitArg(),
      if_not_exist_(false)
  {}
  virtual ~ObCreateResourceUnitArg() {}
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual int assign(const ObCreateResourceUnitArg &other);

  bool get_if_not_exist() const { return if_not_exist_; }

  int init(const common::ObString &name, const share::ObUnitResource &ur, const bool if_not_exist);

  TO_STRING_KV(K_(if_not_exist), K_(unit_config));

private:
  bool if_not_exist_;
};


struct ObDropResourceUnitArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropResourceUnitArg():
    ObDDLArg(),
    if_exist_(false)
  {}
  virtual ~ObDropResourceUnitArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual int assign(const ObDropResourceUnitArg &other);
  DECLARE_TO_STRING;

  common::ObString unit_name_;
  bool if_exist_;
};

struct ObCreateResourcePoolArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateResourcePoolArg():
    ObDDLArg(),
    unit_num_(0),
    if_not_exist_(0),
    replica_type_(common::REPLICA_TYPE_FULL)
  {}
  virtual ~ObCreateResourcePoolArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual int assign(const ObCreateResourcePoolArg &other);
  int init(const ObString &pool_name,
           const ObString &unit_name,
           const int64_t unit_count,
           const common::ObIArray<common::ObZone> &zone_list,
           const common::ObReplicaType replica_type,
           const uint64_t exec_tenant_id,
           const bool if_not_exist);
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  common::ObString unit_;
  int64_t unit_num_;
  common::ObSArray<common::ObZone> zone_list_;
  bool if_not_exist_;
  common::ObReplicaType replica_type_;
};

struct ObSplitResourcePoolArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObSplitResourcePoolArg() : ObDDLArg(),
                             pool_name_(),
                             zone_list_(),
                             split_pool_list_() {}
  virtual ~ObSplitResourcePoolArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual int assign(const ObSplitResourcePoolArg &other);
  TO_STRING_KV(K_(pool_name), K_(zone_list), K_(split_pool_list));

  common::ObString pool_name_;
  common::ObSArray<common::ObZone> zone_list_;
  common::ObSArray<common::ObString> split_pool_list_;
};

struct ObAlterResourceTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObAlterResourceTenantArg() : ObDDLArg(),
                               tenant_name_(),
                               unit_num_(),
                               unit_group_id_array_() {}
  virtual ~ObAlterResourceTenantArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const override {
    return true;
  }
  virtual int assign(const ObAlterResourceTenantArg &that);
  TO_STRING_KV(K_(tenant_name), K_(unit_num), K_(unit_group_id_array));
public:
  void set_tenant_name(const common::ObString &tenant_name) {
    tenant_name_ = tenant_name;
  }
  void set_unit_num(const int64_t unit_num) {
    unit_num_ = unit_num;
  }
  int fill_unit_group_id(const uint64_t unit_group_id);
public:
  common::ObString tenant_name_;
  int64_t unit_num_;
  common::ObSArray<uint64_t> unit_group_id_array_;
};

struct ObMergeResourcePoolArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObMergeResourcePoolArg() : ObDDLArg(),
                             old_pool_list_(),//Before the merger
                             new_pool_list_() {}//After the merger
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual int assign(const ObMergeResourcePoolArg &other);
  TO_STRING_KV(K_(old_pool_list), K_(new_pool_list));

  common::ObSArray<common::ObString> old_pool_list_;
  common::ObSArray<common::ObString> new_pool_list_;
};

struct ObAlterResourcePoolArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObAlterResourcePoolArg()
    : ObDDLArg(),  pool_name_(), unit_(), unit_num_(0), zone_list_(), delete_unit_id_array_() {}
  virtual ~ObAlterResourcePoolArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual int assign(const ObAlterResourcePoolArg &other);
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  common::ObString unit_;
  int64_t unit_num_;
  common::ObSArray<common::ObZone> zone_list_;
  common::ObSArray<uint64_t> delete_unit_id_array_; // This array may be empty
};

struct ObDropResourcePoolArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropResourcePoolArg():
    ObDDLArg(),
    if_exist_(false),
    pool_id_(OB_INVALID_ID)
  {}
  virtual ~ObDropResourcePoolArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual int assign(const ObDropResourcePoolArg &other);
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  bool if_exist_;
  uint64_t pool_id_;
};

struct ObCmdArg
{
  OB_UNIS_VERSION(1);
public:
  common::ObString &get_sql_stmt() { return sql_text_; }
  const common::ObString &get_sql_stmt() const { return sql_text_; }
public:
  common::ObString sql_text_;
};

struct ObSysVarIdValue
{
  OB_UNIS_VERSION(1);
public:
  ObSysVarIdValue() : sys_id_(share::SYS_VAR_INVALID), value_() {}
  ObSysVarIdValue(share::ObSysVarClassType sys_id, common::ObString &value) : sys_id_(sys_id), value_(value) {}
  ~ObSysVarIdValue() {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  share::ObSysVarClassType sys_id_;
  common::ObString value_;
};

struct ObCreateTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTenantArg()
    : ObDDLArg(), tenant_schema_(), pool_list_(), if_not_exist_(false),
      sys_var_list_(), name_case_mode_(common::OB_NAME_CASE_INVALID), is_restore_(false),
      palf_base_info_(), compatible_version_(0), recovery_until_scn_(share::SCN::min_scn()),
      is_creating_standby_(false), log_restore_source_(), is_tmp_tenant_for_recover_(false),
      source_tenant_id_(OB_INVALID_TENANT_ID)  {}
  virtual ~ObCreateTenantArg() {};
  bool is_valid() const;
  bool is_clone_tenant() const { return OB_INVALID_TENANT_ID != source_tenant_id_; }
  bool is_restore_tenant() const { return is_restore_; }
  bool is_standby_tenant() const { return is_creating_standby_; }
  uint64_t get_source_tenant_id() const { return source_tenant_id_; }
  int check_valid() const;
  void reset();
  int assign(const ObCreateTenantArg &other);

  virtual bool is_allow_in_standby() const { return sync_from_primary_; }
  int init(const share::schema::ObTenantSchema &tenant_schema,
           const common::ObIArray<common::ObString> &pool_list,
           const bool is_sync_from_primary,
           const bool if_not_exist);

  DECLARE_TO_STRING;

  share::schema::ObTenantSchema tenant_schema_;
  common::ObSArray<common::ObString> pool_list_;
  bool if_not_exist_;
  common::ObSArray<ObSysVarIdValue> sys_var_list_;
  common::ObNameCaseMode name_case_mode_;
  bool is_restore_;
  //for restore tenant sys ls
  palf::PalfBaseInfo palf_base_info_;
  //for restore tenant, from backuped meta file
  uint64_t compatible_version_;
  share::SCN recovery_until_scn_;
  bool is_creating_standby_;
  common::ObString log_restore_source_; // for create standby tenant
  bool is_tmp_tenant_for_recover_; //tmp tenant for recover table
  uint64_t source_tenant_id_;           // for create clone tenant
};

struct ObCreateTenantEndArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTenantEndArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_TENANT_ID) {}
  virtual ~ObCreateTenantEndArg() {}
  bool is_valid() const;
  int assign(const ObCreateTenantEndArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
};

struct ObModifyTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum ModifiableOptions {
       REPLICA_NUM = 1,
       CHARSET_TYPE,
       COLLATION_TYPE,
       PRIMARY_ZONE,
       ZONE_LIST,
       RESOURCE_POOL_LIST,
       READ_ONLY,
       COMMENT,
       LOCALITY,
       LOGONLY_REPLICA_NUM,
       DEFAULT_TABLEGROUP,
       FORCE_LOCALITY,
       PROGRESSIVE_MERGE_NUM,
       ENABLE_EXTENDED_ROWID,
       ENABLE_ARBITRATION_SERVICE,
       MAX_OPTION,
  };
  ObModifyTenantArg() : ObDDLArg()
    { }
  bool is_valid() const;
  int check_normal_tenant_can_do(bool &normal_can_do) const;
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool is_allow_when_upgrade() const;
  int assign(const ObModifyTenantArg &other);

  DECLARE_TO_STRING;

  share::schema::ObTenantSchema tenant_schema_;
  common::ObSArray<common::ObString> pool_list_;
  //used to mark alter tenant options
  common::ObBitSet<> alter_option_bitset_;
  common::ObSArray<ObSysVarIdValue> sys_var_list_;
  common::ObString new_tenant_name_; // for tenant rename
};

struct ObLockTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObLockTenantArg():
    ObDDLArg(),
    is_locked_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString tenant_name_;
  bool is_locked_;
};

struct ObDropTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropTenantArg():
      ObDDLArg(),
      if_exist_(false),
      delay_to_drop_(true),
      force_drop_(false),//Obsolete field
      open_recyclebin_(true),
      tenant_id_(OB_INVALID_TENANT_ID),
      drop_only_in_restore_(false) {}
  virtual ~ObDropTenantArg() {}
  /*
   * drop tenant force最高优先级
   * 此时delay_to_drop_=false;
   * 其余情况delay_to_drop_=true;
   * open_recyclebin_根据回收站是否打开决定;
   * open_recyclebin_打开为true，进入回收站
   * open_recyclebin_关闭为false，延迟删除
  */
  int assign(const ObDropTenantArg &other);
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual bool is_allow_in_standby() const { return sync_from_primary_; };
  DECLARE_TO_STRING;

  common::ObString tenant_name_;
  bool if_exist_;
  bool delay_to_drop_;
  bool force_drop_;
  common::ObString object_name_;//no use in 4.0
  bool open_recyclebin_;
  uint64_t tenant_id_;          // drop tenant with tenant_id_ if it's valid.
  bool drop_only_in_restore_; // when cancel restore tenant, drop_only_in_restore_ is set to true
};

struct ObSequenceDDLArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObSequenceDDLArg():
      ObDDLArg(),
      stmt_type_(common::OB_INVALID_ID),
      option_bitset_(),
      seq_schema_(),
      database_name_(),
      ignore_exists_error_(false)
  {}
  bool is_valid() const
  {
    return !database_name_.empty();
  }
  virtual bool is_allow_when_upgrade() const { return sql::stmt::T_DROP_SEQUENCE == stmt_type_; }
  void set_stmt_type(int64_t type)
  {
    stmt_type_ = type;
  }
  int64_t get_stmt_type() const
  {
    return stmt_type_;
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    seq_schema_.set_tenant_id(tenant_id);
  }
  void set_is_system_generated()
  {
    seq_schema_.set_is_system_generated(true);
  }
  void set_sequence_id(const uint64_t sequence_id)
  {
    seq_schema_.set_sequence_id(sequence_id);
  }
  void set_sequence_name(const common::ObString &name)
  {
    seq_schema_.set_sequence_name(name);
  }
  void set_database_name(const common::ObString &name)
  {
    database_name_ = name;
  }
  share::ObSequenceOption &option()
  {
    return seq_schema_.get_sequence_option();
  }
  const common::ObString &get_database_name() const
  {
    return database_name_;
  }
  share::schema::ObSequenceSchema &sequence_schema()
  {
    return seq_schema_;
  }
  uint64_t get_sequence_id() const {
    return seq_schema_.get_sequence_id();
  }
  uint64_t get_tenant_id() const {
    return seq_schema_.get_tenant_id();
  }
  common::ObBitSet<> &get_option_bitset()
  {
    return option_bitset_;
  }
  const common::ObBitSet<> &get_option_bitset() const
  {
    return option_bitset_;
  }
  bool ignore_exists_error() const {
    return ignore_exists_error_;
  }
  void set_ignore_exists_error(bool ignore_error) {
    ignore_exists_error_ = ignore_error;
  }
  TO_STRING_KV(K_(stmt_type), K_(seq_schema), K_(database_name), K_(ignore_exists_error));
public:
  int64_t stmt_type_;
  common::ObBitSet<> option_bitset_;
  share::schema::ObSequenceSchema seq_schema_;
  common::ObString database_name_;
  bool ignore_exists_error_; // if exsit for drop sequence, if not exist for create sequence.
};

struct ObAddSysVarArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObAddSysVarArg() : sysvar_(), if_not_exist_(false), update_sys_var_(false) {}
  DECLARE_TO_STRING;
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  int assign(const ObAddSysVarArg &other);
  share::schema::ObSysVarSchema sysvar_;
  bool if_not_exist_;
  bool update_sys_var_; // Distinguish add/update sys var, for internal use only
};

struct ObModifySysVarArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObModifySysVarArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), is_inner_(false)
    { }
  DECLARE_TO_STRING;
  bool is_valid() const;
  int assign(const ObModifySysVarArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual bool is_allow_when_disable_ddl() const { return true; }
  uint64_t tenant_id_;
  common::ObSArray<share::schema::ObSysVarSchema> sys_var_list_;
  bool is_inner_;
};

struct ObCreateDatabaseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateDatabaseArg():
    ObDDLArg(),
    if_not_exist_(false)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  DECLARE_TO_STRING;

  share::schema::ObDatabaseSchema database_schema_;
  //used to mark alter database options
  common::ObBitSet<> alter_option_bitset_;
  bool if_not_exist_;
};

struct ObAlterDatabaseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum ModifiableOptions {
       REPLICA_NUM = 1,
       CHARSET_TYPE,
       COLLATION_TYPE,
       PRIMARY_ZONE,
       READ_ONLY,
       DEFAULT_TABLEGROUP,
       MAX_OPTION
  };

public:
  ObAlterDatabaseArg() : ObDDLArg()
    { }
  bool is_valid() const;
  bool only_alter_primary_zone() const
  { return (1 == alter_option_bitset_.num_members()
            && alter_option_bitset_.has_member(PRIMARY_ZONE)); }
  virtual bool is_allow_when_upgrade() const { return true; }
  DECLARE_TO_STRING;

  share::schema::ObDatabaseSchema database_schema_;
  //used to mark alter database options
  common::ObBitSet<> alter_option_bitset_;
};

struct ObDropDatabaseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropDatabaseArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    if_exist_(false),
    to_recyclebin_(false),
    is_add_to_scheduler_(false),
    compat_mode_(lib::Worker::CompatMode::INVALID)
  {}

  ObDropDatabaseArg &operator=(const ObDropDatabaseArg &other) = delete;
  ObDropDatabaseArg(const ObDropDatabaseArg &other) = delete;
  virtual ~ObDropDatabaseArg() = default;
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString database_name_;
  bool if_exist_;
  bool to_recyclebin_;
  bool is_add_to_scheduler_;
  lib::Worker::CompatMode compat_mode_;
};

struct ObCreateTablegroupArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTablegroupArg():
    ObDDLArg(),
    if_not_exist_(false)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  DECLARE_TO_STRING;

  share::schema::ObTablegroupSchema tablegroup_schema_;
  bool if_not_exist_;
};

struct ObDropTablegroupArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropTablegroupArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    if_exist_(false)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
  bool if_exist_;
};

struct ObTableItem;
struct ObAlterTablegroupArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObAlterTablegroupArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID), tablegroup_name_(), table_items_(),
    alter_option_bitset_(), alter_tablegroup_schema_()
  {}
  bool is_valid() const;
  bool is_alter_partitions() const;
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool is_allow_when_upgrade() const;
  DECLARE_TO_STRING;

  enum ModifiableOptions {
    LOCALITY = 1,
    PRIMARY_ZONE,
    ADD_PARTITION,
    DROP_PARTITION,
    PARTITIONED_TABLE,
    PARTITIONED_PARTITION,
    REORGANIZE_PARTITION,
    SPLIT_PARTITION,
    FORCE_LOCALITY,
    SHARDING,
    MAX_OPTION,
  };
  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
  common::ObSArray<ObTableItem> table_items_;
  common::ObBitSet<> alter_option_bitset_;
  share::schema::ObTablegroupSchema alter_tablegroup_schema_;
};

struct ObCreateVertialPartitionArg : ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateVertialPartitionArg() :
      ObDDLArg(),
      vertical_partition_columns_()
  {}
  virtual ~ObCreateVertialPartitionArg()
  {}
  bool is_valid() const;
  void reset()
  {
    vertical_partition_columns_.reset();
  }
  DECLARE_TO_STRING;

public:
  common::ObSEArray<common::ObString, 8> vertical_partition_columns_;
};


struct ObCheckFrozenScnArg
{
  OB_UNIS_VERSION(1);
public:
  ObCheckFrozenScnArg();
  virtual ~ObCheckFrozenScnArg() {}

  bool is_valid() const;
  TO_STRING_KV(K_(frozen_scn));
public:
  share::SCN frozen_scn_;
};

struct ObGetMinSSTableSchemaVersionArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetMinSSTableSchemaVersionArg() { tenant_id_arg_list_.reuse(); }

  virtual ~ObGetMinSSTableSchemaVersionArg() { tenant_id_arg_list_.reset(); }

  bool is_valid() const { return tenant_id_arg_list_.size() > 0; }
  TO_STRING_KV(K_(tenant_id_arg_list));
public:
  common::ObSArray<uint64_t> tenant_id_arg_list_;
};

struct ObCreateIndexArg;//Forward declaration
struct ObCreateForeignKeyArg;//Forward declaration

struct ObMVAdditionalInfo
{
  OB_UNIS_VERSION(1);
public:
  share::schema::ObTableSchema container_table_schema_;
  ObMVRefreshInfo mv_refresh_info_;

  int assign(const ObMVAdditionalInfo &other);

  TO_STRING_KV(K_(container_table_schema), K_(mv_refresh_info));
};

struct ObCreateTableArg : ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTableArg() :
      ObDDLArg(),
      if_not_exist_(false),
      last_replay_log_id_(0),
      is_inner_(false),
      error_info_(),
      is_alter_view_(false),
      sequence_ddl_arg_(),
      dep_infos_(),
      mv_ainfo_()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const;
  int assign(const ObCreateTableArg &other);
  DECLARE_TO_STRING;

  bool if_not_exist_;
  share::schema::ObTableSchema schema_;
  common::ObSArray<ObCreateIndexArg> index_arg_list_;
  common::ObSArray<ObCreateForeignKeyArg> foreign_key_arg_list_;
  common::ObSEArray<share::schema::ObConstraint, 4> constraint_list_;
  common::ObString db_name_;
  uint64_t last_replay_log_id_;
  bool is_inner_;
  common::ObSArray<ObCreateVertialPartitionArg> vertical_partition_arg_list_;
  share::schema::ObErrorInfo error_info_;
  // New members of ObCreateTableArg need to pay attention to the implementation of is_allow_when_upgrade
  bool is_alter_view_;
  ObSequenceDDLArg sequence_ddl_arg_;
  common::ObSArray<oceanbase::share::schema::ObDependencyInfo> dep_infos_;
  common::ObSArray<ObMVAdditionalInfo> mv_ainfo_;
};

struct ObCreateTableRes
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTableRes() :
      table_id_(OB_INVALID_ID),
      schema_version_(OB_INVALID_VERSION),
      task_id_(0),
      do_nothing_(false)
  {}
  int assign(const ObCreateTableRes &other) {
    table_id_ = other.table_id_;
    schema_version_ = other.schema_version_;
    task_id_ = other.task_id_;
    do_nothing_ = other.do_nothing_;
    return common::OB_SUCCESS;
  }
  TO_STRING_KV(K_(table_id), K_(schema_version), K_(task_id), K_(do_nothing));
  uint64_t table_id_;
  int64_t schema_version_;
  int64_t task_id_;
  bool do_nothing_;
};

struct ObCreateTableLikeArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateTableLikeArg():
      ObDDLArg(),
      if_not_exist_(false),
      tenant_id_(common::OB_INVALID_ID),
      table_type_(share::schema::USER_TABLE),
      origin_db_name_(),
      origin_table_name_(),
      new_db_name_(),
      new_table_name_(),
      create_host_(),
      sequence_ddl_arg_(),
      session_id_(0),
      define_user_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  int assign(const ObCreateTableLikeArg &other);
  DECLARE_TO_STRING;

  bool if_not_exist_;
  uint64_t tenant_id_;
  share::schema::ObTableType table_type_;
  common::ObString origin_db_name_;
  common::ObString origin_table_name_;
  common::ObString new_db_name_;
  common::ObString new_table_name_;
  common::ObString create_host_; //Temporary table is valid
  // oracle not support like, mysql not support identity, so here just define it for call create_user_tables()
  ObSequenceDDLArg sequence_ddl_arg_;
  int64_t session_id_;
  uint64_t define_user_id_;
};

struct ObCreateSynonymArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
ObCreateSynonymArg():
  or_replace_(false),
      synonym_info_(),
      db_name_()
      {}
  int assign(const ObCreateSynonymArg &other);
  TO_STRING_KV(K_(synonym_info), K_(db_name), K_(obj_db_name), K_(dependency_info));
  bool or_replace_;
  share::schema::ObSynonymInfo synonym_info_;
  common::ObString db_name_;
  common::ObString obj_db_name_;
  oceanbase::share::schema::ObDependencyInfo dependency_info_;
};

struct ObDropSynonymArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropSynonymArg(): tenant_id_(common::OB_INVALID_ID),
      is_force_(false),
      db_name_(),
      synonym_name_() {}
  virtual ~ObDropSynonymArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(is_force), K_(db_name), K_(synonym_name));

  uint64_t tenant_id_;
  bool is_force_;
  common::ObString db_name_;
  common::ObString synonym_name_;
};

struct ObLabelSePolicyDDLArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:

  ObLabelSePolicyDDLArg() : schema_(), ddl_type_() {}
  virtual bool is_allow_when_upgrade() const
  {
    return share::schema::OB_DDL_DROP_LABEL_SE_POLICY == ddl_type_;
  }

  TO_STRING_KV(K_(schema), K_(ddl_type));
  share::schema::ObLabelSePolicySchema schema_;
  share::schema::ObSchemaOperationType ddl_type_;
};

struct ObLabelSeComponentDDLArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:

  ObLabelSeComponentDDLArg() : schema_(), ddl_type_() {}
  virtual bool is_allow_when_upgrade() const
  {
    return share::schema::OB_DDL_DROP_LABEL_SE_LEVEL == ddl_type_;
  }

  TO_STRING_KV(K_(schema), K_(ddl_type), K_(policy_name));
  share::schema::ObLabelSeComponentSchema schema_;
  share::schema::ObSchemaOperationType ddl_type_;
  common::ObString policy_name_;
};

struct ObLabelSeLabelDDLArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:

  ObLabelSeLabelDDLArg() : schema_(), ddl_type_() {}
  virtual bool is_allow_when_upgrade() const
  {
    return share::schema::OB_DDL_DROP_LABEL_SE_LABEL == ddl_type_;
  }

  TO_STRING_KV(K_(schema), K_(ddl_type), K_(policy_name));
  share::schema::ObLabelSeLabelSchema schema_;
  share::schema::ObSchemaOperationType ddl_type_;
  common::ObString policy_name_;
};

struct ObLabelSeUserLevelDDLArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:

  ObLabelSeUserLevelDDLArg() : ddl_type_(), level_schema_() {}
  virtual bool is_allow_when_upgrade() const
  {
    return share::schema::OB_DDL_DROP_LABEL_SE_USER_LEVELS == ddl_type_;
  }

  TO_STRING_KV(K_(ddl_type), K_(level_schema), K_(policy_name));
  share::schema::ObSchemaOperationType ddl_type_;
  share::schema::ObLabelSeUserLevelSchema level_schema_;
  common::ObString policy_name_;
};

struct ObIndexArg : public ObDDLArg
{
  OB_UNIS_VERSION_V(1);
public:
  enum IndexActionType
  {
    INVALID_ACTION = 1,
    ADD_INDEX,
    DROP_INDEX,
    ALTER_INDEX,
    DROP_FOREIGN_KEY, // The foreign key is a 1.4 function, and rename_index needs to be placed at the back in consideration of compatibility
    RENAME_INDEX,
    ALTER_INDEX_PARALLEL,
    REBUILD_INDEX,
    ALTER_PRIMARY_KEY,
    ADD_PRIMARY_KEY,
    DROP_PRIMARY_KEY,
    ALTER_INDEX_TABLESPACE,
    ADD_MLOG,
    DROP_MLOG,
    RENAME_MLOG
  };

  static const char *to_type_str(const IndexActionType type)
  {
    const char *str = "";
    if (ADD_INDEX == type) {
      str = "add index";
    } else if (DROP_INDEX == type) {
      str = "drop index";
    } else if (ALTER_INDEX == type) {
      str = "alter index";
    } else if (DROP_FOREIGN_KEY == type) {
      str = "drop foreign key";
    } else if (RENAME_INDEX == type) {
      str = "rename index";
    } else if (ALTER_INDEX_PARALLEL == type) {
      str = "alter index parallel";
    } else if (REBUILD_INDEX == type) {
      str = "rebuild index";
    } else if (ALTER_PRIMARY_KEY == type) {
      str = "alter primary key";
    } else if (ADD_PRIMARY_KEY == type) {
      str = "add primary key";
    } else if (DROP_PRIMARY_KEY == type) {
      str = "drop primary key";
    } else if (ALTER_INDEX_TABLESPACE == type) {
      str = "alter index tablespace";
    } else if (ADD_MLOG == type) {
      str = "add materialized view log";
    } else if (DROP_MLOG == type) {
      str = "drop materialized view log";
    } else if (RENAME_MLOG == type) {
      str = "rename materialized view log";
    }
    return str;
  }

  uint64_t tenant_id_;
  uint64_t session_id_; //The session id is passed in when building the index, and the table schema is searched by rs according to the temporary table and then the ordinary table.
  common::ObString index_name_;
  common::ObString table_name_;
  common::ObString database_name_;
  IndexActionType index_action_type_;
  share::SortCompactLevel compact_level_;

  ObIndexArg():
      ObDDLArg(),
      tenant_id_(common::OB_INVALID_ID),
      session_id_(common::OB_INVALID_ID),
      index_name_(),
      table_name_(),
      database_name_(),
      index_action_type_(INVALID_ACTION),
      compact_level_(share::SORT_COMPACT_LEVEL)
  {}
  virtual ~ObIndexArg() {}
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    session_id_ = common::OB_INVALID_ID;
    index_name_.reset();
    table_name_.reset();
    database_name_.reset();
    index_action_type_ = INVALID_ACTION;
    compact_level_ = share::SORT_COMPACT_LEVEL;
    ObDDLArg::reset();
  }
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const;
  int assign(const ObIndexArg &other) {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObDDLArg::assign(other))) {
      SHARE_LOG(WARN, "assign ddl arg failed", K(ret));
    } else {
      tenant_id_ = other.tenant_id_;
      session_id_ = other.session_id_;
      index_name_ = other.index_name_;
      table_name_ = other.table_name_;
      database_name_ = other.database_name_;
      index_action_type_ = other.index_action_type_;
      compact_level_ = other.compact_level_;
    }
    return ret;
  }

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObUpdateStatCacheArg : public ObDDLArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObUpdateStatCacheArg()
    : tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      partition_ids_(),
      column_ids_(),
      no_invalidate_(false),
      update_system_stats_only_(false)
  {}
  virtual ~ObUpdateStatCacheArg() {}
  void rest()
  {
    tenant_id_ = common::OB_INVALID_ID,
    table_id_ = common::OB_INVALID_ID,
    partition_ids_.reset();
    column_ids_.reset();
    no_invalidate_ = false;
    update_system_stats_only_ = false;
  }
  bool is_valid() const;
  int assign(const ObUpdateStatCacheArg &other) {
    int ret = common::OB_SUCCESS;
    tenant_id_ = other.tenant_id_;
    table_id_ = other.table_id_;
    no_invalidate_ = other.no_invalidate_;
    update_system_stats_only_ = other.update_system_stats_only_;
    if (OB_FAIL(ObDDLArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign ddl arg", KR(ret));
    } else if (OB_FAIL(partition_ids_.assign(other.partition_ids_))) {
      SHARE_LOG(WARN, "fail to assign partition ids", KR(ret));
    } else if (OB_FAIL(column_ids_.assign(other.column_ids_))) {
      SHARE_LOG(WARN, "fail to assign column ids", KR(ret));
    } else { /*do nothing*/ }
    return ret;
  }
  virtual bool is_allow_when_upgrade() const { return true; }
  uint64_t tenant_id_;
  uint64_t table_id_;
  common::ObSArray<int64_t> partition_ids_;
  common::ObSArray<uint64_t> column_ids_;
  bool no_invalidate_;
  bool update_system_stats_only_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObDropIndexArg: public ObIndexArg
{
  OB_UNIS_VERSION(1);
  //if add new member,should add to_string and serialize function
public:
  ObDropIndexArg():
      ObIndexArg()
  {
    index_action_type_ = DROP_INDEX;
    index_table_id_ = common::OB_INVALID_ID;
    is_add_to_scheduler_ = false;
    is_hidden_ = false;
    is_in_recyclebin_ = false;
    is_inner_ = false;
  }
  virtual ~ObDropIndexArg() {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = DROP_INDEX;
    is_add_to_scheduler_ = false;
    is_hidden_ = false;
    is_in_recyclebin_ = false;
    is_inner_ = false;
  }
  bool is_valid() const { return ObIndexArg::is_valid(); }
  uint64_t index_table_id_;
  bool is_add_to_scheduler_;
  bool is_hidden_;
  bool is_in_recyclebin_;
  bool is_inner_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObDropIndexRes final
{
  OB_UNIS_VERSION(1);
public:
  ObDropIndexRes()
    : tenant_id_(common::OB_INVALID_ID), index_table_id_(common::OB_INVALID_ID), schema_version_(0), task_id_(0)
  {}
  ~ObDropIndexRes() = default;
  int assign(const ObDropIndexRes &other);
public:
  uint64_t tenant_id_;
  uint64_t index_table_id_;
  int64_t schema_version_;
  int64_t task_id_;
};

struct ObRebuildIndexArg: public ObIndexArg
{
  OB_UNIS_VERSION(1);
  //if add new member,should add to_string and serialize function
public:
  ObRebuildIndexArg() : ObIndexArg()
  {
    index_action_type_ = REBUILD_INDEX;
    index_table_id_ = common::OB_INVALID_ID;
  }
  virtual ~ObRebuildIndexArg() {}

  int assign(const ObRebuildIndexArg &other) {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObIndexArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign base", K(ret));
    } else {
      index_table_id_ = other.index_table_id_;
    }
    return ret;
  }

  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = REBUILD_INDEX;
  }
  bool is_valid() const { return ObIndexArg::is_valid(); }
  uint64_t index_table_id_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObAlterIndexParallelArg: public ObIndexArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObAlterIndexParallelArg() : ObIndexArg(), new_parallel_(common::OB_DEFAULT_TABLE_DOP)
  {
    index_action_type_ = ALTER_INDEX_PARALLEL;
  }
  virtual ~ObAlterIndexParallelArg()  {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = ALTER_INDEX_PARALLEL;
    new_parallel_ = common::OB_DEFAULT_TABLE_DOP;
  }
  bool is_valid() const
  {
    // parallel must be greater than 0
    return new_parallel_ > 0;
  }

  int64_t new_parallel_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObRenameIndexArg: public ObIndexArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObRenameIndexArg() : ObIndexArg(), origin_index_name_(), new_index_name_()
  {
    index_action_type_ = RENAME_INDEX;
  }
  virtual ~ObRenameIndexArg()  {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = RENAME_INDEX;
    origin_index_name_.reset();
    new_index_name_.reset();
  }
  bool is_valid() const;
  common::ObString origin_index_name_;
  common::ObString new_index_name_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObAlterIndexArg: public ObIndexArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObAlterIndexArg() : ObIndexArg(), index_visibility_(common::OB_DEFAULT_INDEX_VISIBILITY)
  {
    index_action_type_ = ALTER_INDEX;
  }
  virtual ~ObAlterIndexArg() {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = ALTER_INDEX;
    index_visibility_ = common::OB_DEFAULT_INDEX_VISIBILITY;
  }
  bool is_valid() const;
  uint64_t index_visibility_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObAlterIndexTablespaceArg: public ObIndexArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObAlterIndexTablespaceArg() : ObIndexArg(), tablespace_id_(common::OB_INVALID_ID),
                                encryption_()
  {
    index_action_type_ = ALTER_INDEX_TABLESPACE;
  }
  virtual ~ObAlterIndexTablespaceArg()  {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = ALTER_INDEX_TABLESPACE;
    tablespace_id_ = common::OB_INVALID_ID;
    encryption_.reset();
  }
  int assign(const ObAlterIndexTablespaceArg &other) {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObIndexArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign base", K(ret));
    } else {
      tablespace_id_ = other.tablespace_id_;
      encryption_ = other.encryption_;
    }
    return ret;
  }
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tablespace_id_;
  }

  uint64_t tablespace_id_;
  common::ObString encryption_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObTruncateTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObTruncateTableArg():
      ObDDLArg(),
      tenant_id_(common::OB_INVALID_ID),
      session_id_(common::OB_INVALID_ID),
      database_name_(),
      table_name_(),
      is_add_to_scheduler_(false),
      compat_mode_(lib::Worker::CompatMode::INVALID),
      foreign_key_checks_(false)
  {}

  ObTruncateTableArg &operator=(const ObTruncateTableArg &other) = delete;
  ObTruncateTableArg(const ObTruncateTableArg &other) = delete;
  virtual ~ObTruncateTableArg() = default;
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  uint64_t session_id_; //Pass in session id when truncate table
  common::ObString database_name_;
  common::ObString table_name_;
  bool is_add_to_scheduler_;
  lib::Worker::CompatMode compat_mode_;
  bool foreign_key_checks_;
};

struct ObRenameTableItem
{
  OB_UNIS_VERSION(1);
public:
  ObRenameTableItem():
      origin_db_name_(),
      new_db_name_(),
      origin_table_name_(),
      new_table_name_(),
      origin_table_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString origin_db_name_;
  common::ObString new_db_name_;
  common::ObString origin_table_name_;
  common::ObString new_table_name_;
  uint64_t origin_table_id_;//only used in work thread, no need add to SERIALIZE now
};

struct ObRenameTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObRenameTableArg():
      ObDDLArg(),
      tenant_id_(common::OB_INVALID_ID),
      rename_table_items_(),
      client_session_id_(0),
      client_session_create_ts_(0),
      lock_priority_(transaction::tablelock::ObTableLockPriority::NORMAL)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObSArray<ObRenameTableItem> rename_table_items_;
  uint32_t client_session_id_;
  int64_t client_session_create_ts_;
  transaction::tablelock::ObTableLockPriority lock_priority_;
};
struct ObStartRedefTableArg final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(orig_tenant_id),
               K_(orig_table_id),
               K_(target_tenant_id),
               K_(target_table_id),
               K_(session_id),
               K_(parallelism),
               K_(ddl_type),
               K_(ddl_stmt_str),
               K_(trace_id),
               K_(sql_mode),
               K_(tz_info_wrap),
               "nls_formats", common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX));
  ObStartRedefTableArg():
    orig_tenant_id_(common::OB_INVALID_ID),
    orig_table_id_(common::OB_INVALID_ID),
    target_tenant_id_(common::OB_INVALID_ID),
    target_table_id_(common::OB_INVALID_ID),
    session_id_(common::OB_INVALID_ID),
    ddl_type_(share::DDL_INVALID),
    ddl_stmt_str_(),
    trace_id_(),
    sql_mode_(0),
    tz_info_wrap_(),
    nls_formats_{}
  {}

  ~ObStartRedefTableArg()
  {
    allocator_.clear();
  }

  void reset()
  {
    orig_tenant_id_ = common::OB_INVALID_ID;
    orig_table_id_ = common::OB_INVALID_ID;
    target_tenant_id_ = common::OB_INVALID_ID;
    target_table_id_ = common::OB_INVALID_ID;
    session_id_ = common::OB_INVALID_ID;
    ddl_type_ = share::DDL_INVALID;
    ddl_stmt_str_.reset();
    sql_mode_ = 0;
  }

  inline void set_tz_info_map(const common::ObTZInfoMap *tz_info_map)
  {
    tz_info_wrap_.set_tz_info_map(tz_info_map);
    tz_info_.set_tz_info_map(tz_info_map);
  }
  int set_nls_formats(const common::ObString *nls_formats);
  int set_nls_formats(const common::ObString &nls_date_format,
                      const common::ObString &nls_timestamp_format,
                      const common::ObString &nls_timestamp_tz_format)
  {
    ObString tmp_str[ObNLSFormatEnum::NLS_MAX] = {nls_date_format, nls_timestamp_format,
                                                  nls_timestamp_tz_format};
    return set_nls_formats(tmp_str);
  }
  bool is_valid() const;
  int assign(const ObStartRedefTableArg &arg);
public:
  uint64_t orig_tenant_id_;
  uint64_t orig_table_id_;
  uint64_t target_tenant_id_;
  uint64_t target_table_id_;
  uint64_t session_id_;
  uint64_t parallelism_;
  share::ObDDLType ddl_type_;
  common::ObString ddl_stmt_str_;
  share::ObTaskId trace_id_;
  ObSQLMode sql_mode_;
  common::ObArenaAllocator allocator_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
};

struct ObStartRedefTableRes final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(schema_version));
  ObStartRedefTableRes() : task_id_(0), tenant_id_(common::OB_INVALID_ID), schema_version_(0){}
  ~ObStartRedefTableRes() = default;
  void reset()
  {
    task_id_ = 0;
    tenant_id_ = common::OB_INVALID_ID;
    schema_version_ = 0;
  }
  int assign(const ObStartRedefTableRes &res);
public:
  int64_t task_id_;
  uint64_t tenant_id_;
  int64_t schema_version_;
};

struct ObCopyTableDependentsArg final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(copy_indexes),
               K_(copy_triggers),
               K_(copy_constraints),
               K_(copy_foreign_keys),
               K_(ignore_errors));
  ObCopyTableDependentsArg() : task_id_(0), tenant_id_(common::OB_INVALID_ID), copy_indexes_(true), copy_triggers_(true),
                               copy_constraints_(true), copy_foreign_keys_(true), ignore_errors_(false) {}

  ~ObCopyTableDependentsArg() = default;
  bool is_valid() const;
  void reset()
  {
    task_id_ = 0;
    tenant_id_ = 0;
    copy_indexes_ = false;
    copy_triggers_ = false;
    copy_constraints_ = false;
    copy_foreign_keys_ = false;
    ignore_errors_ = false;
  }
  int assign(const ObCopyTableDependentsArg &arg);
public:
  int64_t task_id_;
  uint64_t tenant_id_;
  bool copy_indexes_;
  bool copy_triggers_;
  bool copy_constraints_;
  bool copy_foreign_keys_;
  bool ignore_errors_;
};

struct ObFinishRedefTableArg final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(task_id), K_(tenant_id));
  ObFinishRedefTableArg() :
    task_id_(0), tenant_id_(common::OB_INVALID_ID) {}
  ~ObFinishRedefTableArg() = default;
  bool is_valid() const;
  void reset()
  {
    task_id_ = 0;
    tenant_id_ = common::OB_INVALID_ID;
  }
  int assign(const ObFinishRedefTableArg &arg);
public:
  int64_t task_id_;
  uint64_t tenant_id_;
};


struct ObAbortRedefTableArg final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(task_id), K_(tenant_id));
  ObAbortRedefTableArg() : task_id_(0), tenant_id_(common::OB_INVALID_ID) {}
  ~ObAbortRedefTableArg() = default;
  bool is_valid() const;
  void reset()
  {
    task_id_ = 0;
    tenant_id_ = common::OB_INVALID_ID;
  }
  int assign(const ObAbortRedefTableArg &arg);
public:
  int64_t task_id_;
  uint64_t tenant_id_;
};
struct ObUpdateDDLTaskActiveTimeArg final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(task_id), K_(tenant_id));
  ObUpdateDDLTaskActiveTimeArg() : task_id_(0), tenant_id_(common::OB_INVALID_ID) {}
  ~ObUpdateDDLTaskActiveTimeArg() = default;
  bool is_valid() const;
  void reset()
  {
    task_id_ = 0;
    tenant_id_ = common::OB_INVALID_ID;
  }
  int assign(const ObUpdateDDLTaskActiveTimeArg &arg);
public:
  int64_t task_id_;
  uint64_t tenant_id_;
};

struct ObRecoverRestoreTableDDLArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObRecoverRestoreTableDDLArg() :
     ObDDLArg(),
     target_schema_(),
     src_tenant_id_(common::OB_INVALID_ID),
     src_table_id_(common::OB_INVALID_ID),
     ddl_task_id_(common::OB_INVALID_ID),
     tz_info_wrap_(),
     nls_formats_{},
     allocator_("RestorTableDDL")
  {
  }
  virtual ~ObRecoverRestoreTableDDLArg();
  int assign(const ObRecoverRestoreTableDDLArg &other);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(target_schema), K_(src_tenant_id), K_(src_table_id), K_(tz_info_wrap),
               "nls_formats", common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX));
public:
  share::schema::ObTableSchema target_schema_; // with dest tenant_id, database_id, and table name.
  uint64_t src_tenant_id_;
  int64_t src_table_id_;
  int64_t ddl_task_id_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
  common::ObArenaAllocator allocator_;
};

struct ObCreateHiddenTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(exec_tenant_id),
               K_(tenant_id),
               K_(table_id),
               K_(consumer_group_id),
               K_(dest_tenant_id),
               K_(session_id),
               K_(parallelism),
               K_(ddl_type),
               K_(ddl_stmt_str),
               K_(sql_mode),
               K_(tz_info_wrap),
               "nls_formats", common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX));
  ObCreateHiddenTableArg() :
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    table_id_(common::OB_INVALID_ID),
    consumer_group_id_(0),
    dest_tenant_id_(common::OB_INVALID_ID),
    session_id_(common::OB_INVALID_ID),
    ddl_type_(share::DDL_INVALID),
    ddl_stmt_str_(),
    sql_mode_(0),
    tz_info_wrap_(),
    nls_formats_{}
    {}
  ~ObCreateHiddenTableArg()
  {
    allocator_.clear();
  }
  bool is_valid() const;
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    consumer_group_id_ = 0;
    dest_tenant_id_ = common::OB_INVALID_ID;
    session_id_ = common::OB_INVALID_ID;
    ddl_type_ = share::DDL_INVALID;
    ddl_stmt_str_.reset();
    sql_mode_ = 0;
  }
  int assign(const ObCreateHiddenTableArg &arg);
public:
  uint64_t tenant_id_;
  int64_t table_id_;
  int64_t consumer_group_id_;
  uint64_t dest_tenant_id_;
  uint64_t session_id_;
  uint64_t parallelism_;
  share::ObDDLType ddl_type_;
  common::ObString ddl_stmt_str_;
  ObSQLMode sql_mode_;
  common::ObArenaAllocator allocator_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
};

struct ObCreateHiddenTableRes final
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(tenant_id),
               K_(table_id),
               K_(dest_tenant_id),
               K_(dest_table_id),
               K_(trace_id),
               K_(task_id),
               K_(schema_version));
  ObCreateHiddenTableRes() :
    tenant_id_(common::OB_INVALID_ID),
    table_id_(common::OB_INVALID_ID),
    dest_tenant_id_(common::OB_INVALID_ID),
    dest_table_id_(common::OB_INVALID_ID),
    task_id_(0),
    schema_version_(0) {}
  ~ObCreateHiddenTableRes() = default;
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    dest_tenant_id_ = common::OB_INVALID_ID;
    dest_table_id_ = common::OB_INVALID_ID;
    task_id_ = 0;
    schema_version_ = 0;
  }
  int assign(const ObCreateHiddenTableRes &res);
public:
  uint64_t tenant_id_;
  int64_t table_id_;
  uint64_t dest_tenant_id_;
  int64_t dest_table_id_;
  int64_t task_id_;
  int64_t schema_version_;
  share::ObTaskId trace_id_;
};

struct ObMViewCompleteRefreshArg final : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObMViewCompleteRefreshArg()
    : ObDDLArg(),
      tenant_id_(OB_INVALID_TENANT_ID),
      table_id_(OB_INVALID_ID),
      session_id_(OB_INVALID_ID),
      sql_mode_(0),
      last_refresh_scn_(),
      allocator_("MVRefDDL"),
      tz_info_(),
      tz_info_wrap_(),
      nls_formats_(),
      parent_task_id_(0)
  {
  }
  ~ObMViewCompleteRefreshArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObMViewCompleteRefreshArg &other);
  INHERIT_TO_STRING_KV("ObDDLArg", ObDDLArg,
                       K_(tenant_id),
                       K_(table_id),
                       K_(session_id),
                       K_(sql_mode),
                       K_(last_refresh_scn),
                       K_(tz_info),
                       K_(tz_info_wrap),
                       "nls_formats", common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX),
                       K_(parent_task_id));
public:
  uint64_t tenant_id_;
  uint64_t table_id_; // mview table id
  uint64_t session_id_;
  ObSQLMode sql_mode_;
  share::SCN last_refresh_scn_;
  common::ObArenaAllocator allocator_;
  common::ObTimeZoneInfo tz_info_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
  int64_t parent_task_id_;
};

struct ObMViewCompleteRefreshRes final
{
  OB_UNIS_VERSION(1);
public:
  ObMViewCompleteRefreshRes() : task_id_(0), trace_id_() {}
  ~ObMViewCompleteRefreshRes() = default;
  void reset()
  {
    task_id_ = 0;
    trace_id_.reset();
  }
  int assign(const ObMViewCompleteRefreshRes &other)
  {
    if (this != &other) {
      task_id_ = other.task_id_;
      trace_id_ = other.trace_id_;
    }
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(task_id), K_(trace_id));
public:
  int64_t task_id_;
  share::ObTaskId trace_id_;
};

struct ObMViewRefreshInfo
{
  OB_UNIS_VERSION(1);
public:
  ObMViewRefreshInfo()
    : mview_table_id_(OB_INVALID_ID),
      last_refresh_scn_(),
      refresh_scn_(),
      start_time_(OB_INVALID_TIMESTAMP),
      is_mview_complete_refresh_(false)
  {
  }
  ~ObMViewRefreshInfo() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObMViewRefreshInfo &other);
  TO_STRING_KV(K_(mview_table_id),
               K_(last_refresh_scn),
               K_(refresh_scn),
               K_(start_time),
               K_(is_mview_complete_refresh));
public:
  uint64_t mview_table_id_;
  share::SCN last_refresh_scn_;
  share::SCN refresh_scn_;
  int64_t start_time_;
  bool is_mview_complete_refresh_;
};

struct ObAlterTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum ModifiableTableColumns {
       AUTO_INCREMENT = 1,
       BLOCK_SIZE,
       CHARSET_TYPE,
       COLLATION_TYPE,
       COMPRESS_METHOD,
       COMMENT,
       EXPIRE_INFO,
       PRIMARY_ZONE,
       REPLICA_NUM,
       TABLET_SIZE,
       PCTFREE,
       PROGRESSIVE_MERGE_NUM,
       TABLE_NAME,
       TABLEGROUP_NAME,
       SEQUENCE_COLUMN_ID,
       USE_BLOOM_FILTER,
       READ_ONLY,
       LOCALITY,
       SESSION_ID,
       SESSION_ACTIVE_TIME,
       STORE_FORMAT,
       DUPLICATE_SCOPE,
       ENABLE_ROW_MOVEMENT,
       PROGRESSIVE_MERGE_ROUND,
       STORAGE_FORMAT_VERSION,
       FORCE_LOCALITY,
       TABLE_MODE,
       ENCRYPTION,
       TABLESPACE_ID,
       TABLE_DOP,
       INCREMENT_MODE,
       ENABLE_EXTENDED_ROWID,
       TTL_DEFINITION,
       KV_ATTRIBUTES,
       LOB_INROW_THRESHOLD,
       INCREMENT_CACHE_SIZE,
       MAX_OPTION = 1000
  };
  enum AlterPartitionType
  {
    ADD_PARTITION = -1,
    DROP_PARTITION,
    PARTITIONED_TABLE,
    PARTITIONED_PARTITION,
    REORGANIZE_PARTITION,
    SPLIT_PARTITION,
    TRUNCATE_PARTITION,
    ADD_SUB_PARTITION,
    DROP_SUB_PARTITION,
    TRUNCATE_SUB_PARTITION,
    REPARTITION_TABLE,
    // 1. convert range to interval in range part table
    // 2. modify interval range in interval part table
    SET_INTERVAL,
    // cnovert interval to range
    INTERVAL_TO_RANGE,
    RENAME_PARTITION,
    RENAME_SUB_PARTITION,
    AUTO_SPLIT_PARTITION,
    EXCHANGE_PARTITION,
    NO_OPERATION = 1000
  };
  enum AlterConstraintType
  {
    ADD_CONSTRAINT = -1,
    DROP_CONSTRAINT,
    ALTER_CONSTRAINT_STATE,
    CONSTRAINT_NO_OPERATION = 1000
  };
  enum AlterAlgorithm
  {
    DEFAULT = 0, // empty
    INSTANT = 1,
    INPLACE = 2,
  };
  ObAlterTableArg():
      ObDDLArg(),
      session_id_(common::OB_INVALID_ID),
      alter_part_type_(NO_OPERATION),
      alter_constraint_type_(CONSTRAINT_NO_OPERATION),
      index_arg_list_(),
      foreign_key_arg_list_(),
      allocator_(),
      alter_table_schema_(&allocator_),
      tz_info_wrap_(),
      nls_formats_{},
      sequence_ddl_arg_(),
      sql_mode_(0),
      ddl_task_type_(share::INVALID_TASK),
      compat_mode_(lib::Worker::CompatMode::INVALID),
      table_id_(common::OB_INVALID_ID),
      hidden_table_id_(common::OB_INVALID_ID),
      is_alter_columns_(false),
      is_alter_indexs_(false),
      is_alter_options_(false),
      is_alter_partitions_(false),
      is_inner_(false),
      is_update_global_indexes_(false),
      is_convert_to_character_(false),
      skip_sys_table_check_(false),
      need_rebuild_trigger_(false),
      foreign_key_checks_(true),
      is_add_to_scheduler_(false),
      inner_sql_exec_addr_(),
      local_session_var_(&allocator_),
      mview_refresh_info_(),
      alter_algorithm_(INPLACE),
      alter_auto_partition_attr_(false),
      rebuild_index_arg_list_(),
      client_session_id_(0),
      client_session_create_ts_(0),
      lock_priority_(transaction::tablelock::ObTableLockPriority::NORMAL)
  {
  }
  virtual ~ObAlterTableArg()
  {
    for (int64_t i = 0; i < index_arg_list_.size(); ++i) {
      ObIndexArg *index_arg = index_arg_list_.at(i);
      if (OB_NOT_NULL(index_arg)) {
        index_arg->~ObIndexArg();
      }
    }
  }
  bool is_valid() const;
  bool has_rename_action() const
  { return alter_table_schema_.alter_option_bitset_.has_member(TABLE_NAME); }
  bool need_progressive_merge() const {
    return alter_table_schema_.alter_option_bitset_.has_member(BLOCK_SIZE)
        || alter_table_schema_.alter_option_bitset_.has_member(COMPRESS_METHOD)
        || alter_table_schema_.alter_option_bitset_.has_member(PCTFREE)
        || alter_table_schema_.alter_option_bitset_.has_member(STORE_FORMAT)
        || alter_table_schema_.alter_option_bitset_.has_member(STORAGE_FORMAT_VERSION)
        || alter_table_schema_.alter_option_bitset_.has_member(PROGRESSIVE_MERGE_ROUND)
        || alter_table_schema_.alter_option_bitset_.has_member(PROGRESSIVE_MERGE_NUM)
        || alter_table_schema_.alter_option_bitset_.has_member(ENCRYPTION);
  }
  ObAlterTableArg &operator=(const ObAlterTableArg &other) = delete;
  ObAlterTableArg(const ObAlterTableArg &other) = delete;
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool is_allow_when_upgrade() const;
  bool is_refresh_sess_active_time() const;
  inline void set_tz_info_map(const common::ObTZInfoMap *tz_info_map)
  {
    tz_info_wrap_.set_tz_info_map(tz_info_map);
    tz_info_.set_tz_info_map(tz_info_map);
  }
  int is_alter_comment(bool &is_alter_comment) const;
  int set_nls_formats(const common::ObString *nls_formats);
  int set_nls_formats(const common::ObString &nls_date_format,
                      const common::ObString &nls_timestamp_format,
                      const common::ObString &nls_timestamp_tz_format)
  {
    ObString tmp_str[ObNLSFormatEnum::NLS_MAX] = {nls_date_format, nls_timestamp_format,
                                                  nls_timestamp_tz_format};
    return set_nls_formats(tmp_str);
  }
  TO_STRING_KV(K_(session_id),
               K_(index_arg_list),
               K_(foreign_key_arg_list),
               K_(alter_table_schema),
               K_(alter_constraint_type),
               "nls_formats", common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX),
               K_(ddl_task_type),
               K_(compat_mode),
               K_(is_alter_columns),
               K_(is_alter_indexs),
               K_(is_alter_options),
               K_(is_alter_partitions),
               K_(is_inner),
               K_(is_update_global_indexes),
               K_(is_convert_to_character),
               K_(skip_sys_table_check),
               K_(need_rebuild_trigger),
               K_(foreign_key_checks),
               K_(is_add_to_scheduler),
               K_(table_id),
               K_(hidden_table_id),
               K_(inner_sql_exec_addr),
               K_(local_session_var),
               K_(mview_refresh_info),
               K_(alter_algorithm),
               K_(alter_auto_partition_attr),
               K_(client_session_id),
               K_(client_session_create_ts),
               K_(lock_priority));
private:
  int alloc_index_arg(const ObIndexArg::IndexActionType index_action_type, ObIndexArg *&index_arg);
public:
  uint64_t session_id_; //Only used to update the last active time of the temporary table. At this time, the session id used to create the temporary table is passed in
  AlterPartitionType alter_part_type_;
  AlterConstraintType alter_constraint_type_;
  common::ObSArray<ObIndexArg *> index_arg_list_;
  common::ObSArray<ObCreateForeignKeyArg> foreign_key_arg_list_;
  common::ObArenaAllocator allocator_;
  share::schema::AlterTableSchema alter_table_schema_;
  common::ObTimeZoneInfo tz_info_;//unused now
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
  ObSequenceDDLArg sequence_ddl_arg_;
  ObSQLMode sql_mode_;
  share::ObDDLTaskType ddl_task_type_;
  lib::Worker::CompatMode compat_mode_;
  int64_t table_id_; // to check if the table we get is correct
  int64_t hidden_table_id_; // to check if the hidden table we get is correct
  bool is_alter_columns_;
  bool is_alter_indexs_;
  bool is_alter_options_;
  bool is_alter_partitions_;
  bool is_inner_;
  bool is_update_global_indexes_;
  bool is_convert_to_character_;
  bool skip_sys_table_check_;
  bool need_rebuild_trigger_;
  bool foreign_key_checks_;
  bool is_add_to_scheduler_;
  common::ObAddr inner_sql_exec_addr_;
  ObLocalSessionVar local_session_var_;
  ObMViewRefreshInfo mview_refresh_info_;
  AlterAlgorithm alter_algorithm_;
  bool alter_auto_partition_attr_;
  common::ObSArray<ObTableSchema> rebuild_index_arg_list_;  // pre split
  uint32_t client_session_id_;
  int64_t client_session_create_ts_;
  transaction::tablelock::ObTableLockPriority lock_priority_;
  int serialize_index_args(char *buf, const int64_t data_len, int64_t &pos) const;
  int deserialize_index_args(const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_index_args_serialize_size() const;
};

struct ObExchangePartitionArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObExchangePartitionArg():
      ObDDLArg(),
      session_id_(common::OB_INVALID_ID),
      tenant_id_(common::OB_INVALID_ID),
      exchange_partition_level_(PARTITION_LEVEL_MAX),
      base_table_id_(common::OB_INVALID_ID),
      base_table_part_name_(),
      inc_table_id_(common::OB_INVALID_ID),
      including_indexes_(true),
      without_validation_(true),
      update_global_indexes_(false)
  {
  }
  virtual ~ObExchangePartitionArg()
  {
  }
  bool is_valid() const;
  int assign(const ObExchangePartitionArg& other);
public:
  DECLARE_TO_STRING;
  uint64_t session_id_;
  uint64_t tenant_id_;
  ObPartitionLevel exchange_partition_level_;
  uint64_t base_table_id_; // PT table, always contains large amount pf data.
  ObString base_table_part_name_;
  uint64_t inc_table_id_; // NT table, always contains incremental data.
  bool including_indexes_; // default true.
  bool without_validation_; // default true.
  bool update_global_indexes_; // default false.
};

struct ObTableItem
{
  OB_UNIS_VERSION(1);
public:
  ObTableItem():
      mode_(common::OB_NAME_CASE_INVALID), //for compare
      database_name_(),
      table_name_(),
      is_hidden_(false),
      table_id_(OB_INVALID_ID)
  {}
  bool operator==(const ObTableItem &table_item) const;
  inline uint64_t hash() const;
  inline int hash(uint64_t &hash_val, uint64_t seed = 0) const;
  void reset() {
    mode_ = common::OB_NAME_CASE_INVALID;
    database_name_.reset();
    table_name_.reset();
    is_hidden_ = false;
    table_id_ = OB_INVALID_ID;
  }
  DECLARE_TO_STRING;

  common::ObNameCaseMode mode_;
  common::ObString database_name_;
  common::ObString table_name_;
  bool is_hidden_;
  uint64_t table_id_;
};

inline uint64_t ObTableItem::hash() const
{
  uint64_t val = 0;
  if (!database_name_.empty() && !table_name_.empty()) {
    val = common::murmurhash(database_name_.ptr(), database_name_.length(), val);
    val = common::murmurhash(table_name_.ptr(), table_name_.length(), val);
  }
  return val;
}

inline int ObTableItem::hash(uint64_t &hash_val, uint64_t seed) const
{
  hash_val = seed;
  if (!database_name_.empty() && !table_name_.empty()) {
    hash_val = common::murmurhash(database_name_.ptr(), database_name_.length(), hash_val);
    hash_val = common::murmurhash(table_name_.ptr(), table_name_.length(), hash_val);
  }
  return OB_SUCCESS;
}

struct ObDropTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropTableArg():
      ObDDLArg(),
      tenant_id_(common::OB_INVALID_ID),
      session_id_(common::OB_INVALID_ID),
      sess_create_time_(0),
      table_type_(share::schema::MAX_TABLE_TYPE),
      tables_(),
      if_exist_(false),
      to_recyclebin_(false),
      foreign_key_checks_(true),
      is_add_to_scheduler_(false),
      force_drop_(false),
      compat_mode_(lib::Worker::CompatMode::INVALID)
  {}
  bool is_valid() const;
  ObDropTableArg &operator=(const ObDropTableArg &other) = delete;
  ObDropTableArg(const ObDropTableArg &other) = delete;
  virtual ~ObDropTableArg() { tables_.reset(); }
  virtual bool is_allow_when_upgrade() const { return true; }

  int assign(const ObDropTableArg& other);
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  uint64_t session_id_; //Pass in session id when deleting table
  int64_t sess_create_time_; //When deleting oracle temporary table data, pass in the creation time of sess
  share::schema::ObTableType table_type_;
  common::ObSArray<ObTableItem> tables_;
  bool if_exist_;
  bool to_recyclebin_;
  bool foreign_key_checks_;
  bool is_add_to_scheduler_;
  bool force_drop_;
  lib::Worker::CompatMode compat_mode_;
  common::ObArenaAllocator allocator_;
};

struct ObOptimizeTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObOptimizeTableArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    tables_()
  {}
  DECLARE_TO_STRING;
  bool is_valid() const;
  uint64_t tenant_id_;
  common::ObSArray<ObTableItem> tables_;
};

struct ObOptimizeTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObOptimizeTenantArg():
      ObDDLArg(),
      tenant_name_()
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString tenant_name_;
};

struct ObOptimizeAllArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObOptimizeAllArg()
    : ObDDLArg()
  {}
  bool is_valid() const { return true; }
  DECLARE_TO_STRING;
};

struct ObColumnSortItem final
{
  OB_UNIS_VERSION(1);
public:
  ObColumnSortItem() : column_name_(),
                       prefix_len_(0),
                       order_type_(common::ObOrderType::ASC),
                       column_id_(common::OB_INVALID_ID),
                       is_func_index_(false)
  {}
  void reset()
  {
    column_name_.reset();
    prefix_len_ = 0;
    order_type_ = common::ObOrderType::ASC;
    column_id_ = common::OB_INVALID_ID;
    is_func_index_ = false;
  }
  inline uint64_t get_column_id() const { return column_id_; }

  DECLARE_TO_STRING;

  common::ObString column_name_;
  int32_t prefix_len_;
  common::ObOrderType order_type_;
  uint64_t column_id_;
  bool is_func_index_;   //Whether the mark is a function index, the default is false.
};

struct ObTableOption
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableOption() :
    block_size_(-1),
    replica_num_(0),
    index_status_(share::schema::INDEX_STATUS_UNAVAILABLE),
    use_bloom_filter_(false),
    compress_method_("none"),
    comment_(),
    progressive_merge_num_(common::OB_DEFAULT_PROGRESSIVE_MERGE_NUM),
    primary_zone_(),
    row_store_type_(common::MAX_ROW_STORE),
    store_format_(common::OB_STORE_FORMAT_INVALID),
    progressive_merge_round_(0),
    storage_format_version_(common::OB_STORAGE_FORMAT_VERSION_INVALID)
  {}
  virtual void reset()
  {
    block_size_ = common::OB_DEFAULT_SSTABLE_BLOCK_SIZE;
    replica_num_ = -1;
    index_status_ = share::schema::INDEX_STATUS_UNAVAILABLE;
    use_bloom_filter_ = false;
    compress_method_ = common::ObString::make_string("none");
    comment_.reset();
    tablegroup_name_.reset();
    progressive_merge_num_ = common::OB_DEFAULT_PROGRESSIVE_MERGE_NUM;
    primary_zone_.reset();
    row_store_type_ = common::MAX_ROW_STORE;
    store_format_ = common::OB_STORE_FORMAT_INVALID;
    progressive_merge_round_ = 0;
    storage_format_version_ = common::OB_STORAGE_FORMAT_VERSION_INVALID;
  }
  bool is_valid() const;
  DECLARE_TO_STRING;

  int64_t block_size_;
  int64_t replica_num_;
  share::schema::ObIndexStatus index_status_;
  bool use_bloom_filter_;
  common::ObString compress_method_;
  common::ObString comment_;
  common::ObString tablegroup_name_;
  int64_t progressive_merge_num_;
  common::ObString primary_zone_;
  common::ObRowStoreType row_store_type_;
  common::ObStoreFormatType  store_format_;
  int64_t progressive_merge_round_;
  int64_t storage_format_version_;
};

struct ObIndexOption : public ObTableOption
{
  OB_UNIS_VERSION(1);
public:
  ObIndexOption() :
    ObTableOption(),
    parser_name_(),
    index_attributes_set_(common::OB_DEFAULT_INDEX_ATTRIBUTES_SET)
  { }

  bool is_valid() const;
  void reset()
  {
    ObTableOption::reset();
    parser_name_.reset();
  }
  DECLARE_TO_STRING;

  common::ObString parser_name_;
  uint64_t index_attributes_set_;//flags, one bit for one attribute
};

struct ObCreateIndexArg : public ObIndexArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObCreateIndexArg()
      : index_type_(share::schema::INDEX_TYPE_IS_NOT),
        index_columns_(),
        store_columns_(),
        hidden_store_columns_(),
        fulltext_columns_(),
        index_option_(),
        data_table_id_(common::OB_INVALID_ID),
        index_table_id_(common::OB_INVALID_ID),
        if_not_exist_(false),
        with_rowid_(false),
        index_schema_(),
        is_inner_(false),
        nls_date_format_(),
        nls_timestamp_format_(),
        nls_timestamp_tz_format_(),
        sql_mode_(0),
        inner_sql_exec_addr_(),
        allocator_(),
        local_session_var_(&allocator_),
        exist_all_column_group_(false),
        index_cgs_()
  {
    index_action_type_ = ADD_INDEX;
    index_using_type_ = share::schema::USING_BTREE;
  }
  virtual ~ObCreateIndexArg() {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = ADD_INDEX;
    index_type_ = share::schema::INDEX_TYPE_IS_NOT;
    index_columns_.reset();
    store_columns_.reset();
    hidden_store_columns_.reset();
    fulltext_columns_.reset();
    index_option_.reset();
    index_using_type_ = share::schema::USING_BTREE;
    data_table_id_ = common::OB_INVALID_ID;
    index_table_id_ = common::OB_INVALID_ID;
    if_not_exist_ = false;
    with_rowid_ = false;
    index_schema_.reset();
    is_inner_ = false;
    nls_date_format_.reset();
    nls_timestamp_format_.reset();
    nls_timestamp_tz_format_.reset();
    sql_mode_ = 0;
    inner_sql_exec_addr_.reset();
    local_session_var_.reset();
    allocator_.reset();
    exist_all_column_group_ = false;
    index_cgs_.reset();
  }
  void set_index_action_type(const IndexActionType type) { index_action_type_  = type; }
  bool is_valid() const;
  int assign(const ObCreateIndexArg &other) {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObIndexArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign base", K(ret));
    } else if (OB_FAIL(index_columns_.assign(other.index_columns_))) {
      SHARE_LOG(WARN, "fail to assign index columns", K(ret));
    } else if (OB_FAIL(store_columns_.assign(other.store_columns_))) {
      SHARE_LOG(WARN, "fail to assign store columns", K(ret));
    } else if (OB_FAIL(hidden_store_columns_.assign(other.hidden_store_columns_))) {
      SHARE_LOG(WARN, "fail to assign hidden store columns", K(ret));
    } else if (OB_FAIL(fulltext_columns_.assign(other.fulltext_columns_))) {
      SHARE_LOG(WARN, "fail to assign fulltext columns", K(ret));
    } else if (OB_FAIL(index_cgs_.assign(other.index_cgs_))) {
      SHARE_LOG(WARN, "fail to assign index cgs", K(ret));
    } else if (OB_FAIL(index_schema_.assign(other.index_schema_))) {
      SHARE_LOG(WARN, "fail to assign index schema", K(ret));
    } else if (OB_FAIL(local_session_var_.deep_copy(other.local_session_var_))){
      SHARE_LOG(WARN, "fail to copy local session vars", K(ret));
    } else {
      index_type_ = other.index_type_;
      index_option_ = other.index_option_;
      index_using_type_ = other.index_using_type_;
      data_table_id_ = other.data_table_id_;
      index_table_id_ = other.index_table_id_;
      if_not_exist_ = other.if_not_exist_;
      with_rowid_ = other.with_rowid_;
      is_inner_ = other.is_inner_;
      nls_date_format_ = other.nls_date_format_;
      nls_timestamp_format_ = other.nls_timestamp_format_;
      nls_timestamp_tz_format_ = other.nls_timestamp_tz_format_;
      sql_mode_ = other.sql_mode_;
      inner_sql_exec_addr_ = other.inner_sql_exec_addr_;
      consumer_group_id_ = other.consumer_group_id_;
      exist_all_column_group_ = other.exist_all_column_group_;
    }
    return ret;
  }
  inline bool is_unique_primary_index() const
  {
    return ObSimpleTableSchemaV2::is_unique_index(index_type_)
            || share::schema::INDEX_TYPE_PRIMARY == index_type_;
  }
  DECLARE_VIRTUAL_TO_STRING;
  inline bool is_spatial_index() const { return ObSimpleTableSchemaV2::is_spatial_index(index_type_); }
  inline bool is_multivalue_index() const { return is_multivalue_index_aux(index_type_); }

//todo @qilu:only for each_cg now, when support customized cg ,refine this
  typedef common::ObSEArray<uint64_t, common::DEFAULT_CUSTOMIZED_CG_NUM> ObCGColumnList;
  struct ObIndexColumnGroupItem final
  {
    OB_UNIS_VERSION(1);
  public:
    ObIndexColumnGroupItem() : is_each_cg_(false), column_list_(), cg_type_(ObColumnGroupType::SINGLE_COLUMN_GROUP)
    {} /* to compat former version, force to set default value as single column group*/
    ~ObIndexColumnGroupItem()
    {
      reset();
    }
    bool is_valid() const
    {
      return cg_type_ < ObColumnGroupType::NORMAL_COLUMN_GROUP;
    }
    void reset()
    {
      is_each_cg_ = false;
      column_list_.reset();
      cg_type_ = ObColumnGroupType::SINGLE_COLUMN_GROUP;
    }
    int assign(const ObIndexColumnGroupItem &other);
    TO_STRING_KV(K(is_each_cg_), K(column_list_), K(cg_type_));

  public:
    bool is_each_cg_;
    ObCGColumnList column_list_; /* column list not used yet, wait user define cg*/
    ObColumnGroupType cg_type_;
  };

public:
  share::schema::ObIndexType index_type_;
  common::ObSEArray<ObColumnSortItem, common::OB_PREALLOCATED_NUM> index_columns_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> store_columns_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> hidden_store_columns_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> fulltext_columns_;
  ObIndexOption index_option_;
  share::schema::ObIndexUsingType index_using_type_;
  uint64_t data_table_id_;
  uint64_t index_table_id_; // Data_table_id and index_table_id will be given in SQL during recovery
  bool if_not_exist_;
  bool with_rowid_;
  share::schema::ObTableSchema index_schema_; // Index table schema
  bool is_inner_;
  //Nls_xx_format is required when creating a functional index
  common::ObString nls_date_format_;
  common::ObString nls_timestamp_format_;
  common::ObString nls_timestamp_tz_format_;
  ObSQLMode sql_mode_;
  common::ObAddr inner_sql_exec_addr_;
  common::ObArenaAllocator allocator_;
  ObLocalSessionVar local_session_var_;
  bool exist_all_column_group_;
  common::ObSEArray<ObIndexColumnGroupItem, 1/*each*/> index_cgs_;
};

struct ObGenerateAuxIndexSchemaArg : public ObDDLArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObGenerateAuxIndexSchemaArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      data_table_id_(OB_INVALID_ID)
  {}
  ~ObGenerateAuxIndexSchemaArg() {}
  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_TENANT_ID &&
           data_table_id_ != OB_INVALID_ID&&
           create_index_arg_.is_valid();
  }
  int assign(const ObGenerateAuxIndexSchemaArg &other);
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    data_table_id_ = OB_INVALID_ID;
    create_index_arg_.reset();
  }
  TO_STRING_KV(K(tenant_id_), K(data_table_id_), K(create_index_arg_));

public:
  uint64_t tenant_id_;
  uint64_t data_table_id_;
  ObCreateIndexArg create_index_arg_;
};

struct ObGenerateAuxIndexSchemaRes final
{
  OB_UNIS_VERSION_V(1);
public:
  ObGenerateAuxIndexSchemaRes()
    : aux_table_id_(OB_INVALID_ID),
      schema_generated_(false)
  {}
  ~ObGenerateAuxIndexSchemaRes() {}
  int assign(const ObGenerateAuxIndexSchemaRes &other)
  {
    int ret = OB_SUCCESS;
    aux_table_id_ = other.aux_table_id_;
    schema_generated_ = other.schema_generated_;
    return ret;
  }
  void reset()
  {
    aux_table_id_ = OB_INVALID_ID;
    schema_generated_ = false;
  }
  TO_STRING_KV(K(aux_table_id_), K(schema_generated_));

public:
  uint64_t aux_table_id_;
  bool schema_generated_;
};

typedef ObCreateIndexArg ObAlterPrimaryArg;

struct ObCreateMLogArg : public ObDDLArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObCreateMLogArg()
      : ObDDLArg(),
        database_name_(),
        table_name_(),
        mlog_name_(),
        tenant_id_(OB_INVALID_TENANT_ID),
        base_table_id_(common::OB_INVALID_ID),
        mlog_table_id_(common::OB_INVALID_ID),
        session_id_(common::OB_INVALID_ID),
        with_rowid_(false),
        with_primary_key_(false),
        with_sequence_(false),
        include_new_values_(false),
        purge_options_(),
        mlog_schema_(),
        store_columns_(),
        nls_date_format_(),
        nls_timestamp_format_(),
        nls_timestamp_tz_format_(),
        sql_mode_(0)
  {

  }
  virtual ~ObCreateMLogArg() {}
  bool is_valid() const;
  int assign(const ObCreateMLogArg &other) {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObDDLArg::assign(other))) {
      SHARE_LOG(WARN, "failed to assign base", KR(ret));
    } else if (OB_FAIL(mlog_schema_.assign(other.mlog_schema_))) {
      SHARE_LOG(WARN, "failed to assign mlog schema", KR(ret));
    } else if (OB_FAIL(store_columns_.assign(other.store_columns_))) {
      SHARE_LOG(WARN, "failed to assign store columns", KR(ret));
    } else {
      database_name_ = other.database_name_;
      table_name_ = other.table_name_;
      mlog_name_ = other.mlog_name_;
      tenant_id_ = other.tenant_id_;
      base_table_id_ = other.base_table_id_;
      mlog_table_id_ = other.mlog_table_id_;
      session_id_ = other.session_id_;
      with_rowid_ = other.with_rowid_;
      with_primary_key_ = other.with_primary_key_;
      with_sequence_ = other.with_sequence_;
      include_new_values_ = other.include_new_values_;
      purge_options_ = other.purge_options_;
      nls_date_format_ = other.nls_date_format_;
      nls_timestamp_format_ = other.nls_timestamp_format_;
      nls_timestamp_tz_format_ = other.nls_timestamp_tz_format_;
      sql_mode_ = other.sql_mode_;
    }

    return ret;
  }
  void reset()
  {
    database_name_.reset();
    table_name_.reset();
    mlog_name_.reset();
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    base_table_id_ = common::OB_INVALID_ID;
    mlog_table_id_ = common::OB_INVALID_ID;
    session_id_ = common::OB_INVALID_ID;
    with_rowid_ = false;
    with_primary_key_ = false;
    with_sequence_ = false;
    include_new_values_ = false;
    purge_options_.reset();
    mlog_schema_.reset();
    store_columns_.reset();
    nls_date_format_.reset();
    nls_timestamp_format_.reset();
    nls_timestamp_tz_format_.reset();
    sql_mode_ = 0;
    ObDDLArg::reset();
  }
  DECLARE_TO_STRING;

  struct PurgeOptions {
    OB_UNIS_VERSION(1);
  public:
    PurgeOptions() : purge_mode_(share::schema::ObMLogPurgeMode::MAX)
    {
    }
    ~PurgeOptions() {}
    void reset()
    {
      purge_mode_ = share::schema::ObMLogPurgeMode::MAX;
      start_datetime_expr_.reset();
      next_datetime_expr_.reset();
      exec_env_.reset();
    }
    bool is_valid() const
    {
      return (share::schema::ObMLogPurgeMode::MAX != purge_mode_) && !exec_env_.empty();
    }
    TO_STRING_KV(K_(purge_mode),
                 K_(start_datetime_expr),
                 K_(next_datetime_expr),
                 K_(exec_env));
    share::schema::ObMLogPurgeMode purge_mode_;
    common::ObObj start_datetime_expr_;
    common::ObString next_datetime_expr_;
    common::ObString exec_env_;
  };
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString mlog_name_; // for privilege check
  uint64_t tenant_id_;
  uint64_t base_table_id_;
  uint64_t mlog_table_id_;
  uint64_t session_id_;
  bool with_rowid_;
  bool with_primary_key_;
  bool with_sequence_;
  bool include_new_values_;
  PurgeOptions purge_options_;
  share::schema::ObTableSchema mlog_schema_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> store_columns_;
  common::ObString nls_date_format_;
  common::ObString nls_timestamp_format_;
  common::ObString nls_timestamp_tz_format_;
  ObSQLMode sql_mode_;
};

struct ObCreateMLogRes
{
  OB_UNIS_VERSION(1);
public:
  ObCreateMLogRes()
      : mlog_table_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION),
        task_id_(0)
  {}
  void reset()
  {
    mlog_table_id_ = OB_INVALID_ID;
    schema_version_ = OB_INVALID_VERSION;
    task_id_ = 0;
  }
  int assign(const ObCreateMLogRes &other) {
    int ret = common::OB_SUCCESS;
    mlog_table_id_ = other.mlog_table_id_;
    schema_version_ = other.schema_version_;
    task_id_ = other.task_id_;
    return ret;
  }
public:
  TO_STRING_KV(K_(mlog_table_id), K_(schema_version), K_(task_id));
  uint64_t mlog_table_id_;
  int64_t schema_version_;
  int64_t task_id_;
};

struct ObCreateForeignKeyArg : public ObIndexArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObCreateForeignKeyArg()
  : ObIndexArg(),
    parent_database_(),
    parent_table_(),
    child_columns_(),
    parent_columns_(),
    update_action_(share::schema::ACTION_INVALID),
    delete_action_(share::schema::ACTION_INVALID),
    foreign_key_name_(),
    enable_flag_(true),
    is_modify_enable_flag_(false),
    ref_cst_type_(share::schema::CONSTRAINT_TYPE_INVALID),
    ref_cst_id_(common::OB_INVALID_ID),
    validate_flag_(CST_FK_VALIDATED),
    is_modify_validate_flag_(false),
    rely_flag_(false),
    is_modify_rely_flag_(false),
    is_modify_fk_state_(false),
    need_validate_data_(true),
    is_parent_table_mock_(false),
    parent_database_id_(common::OB_INVALID_ID),
    parent_table_id_(common::OB_INVALID_ID),
    name_generated_type_(GENERATED_TYPE_UNKNOWN)
  {}
  virtual ~ObCreateForeignKeyArg()
  {}

  void reset()
  {
    ObIndexArg::reset();
    parent_database_.reset();
    parent_table_.reset();
    child_columns_.reset();
    parent_columns_.reset();
    update_action_ = share::schema::ACTION_INVALID;
    delete_action_ = share::schema::ACTION_INVALID;
    foreign_key_name_.reset();
    enable_flag_ = true;
    is_modify_enable_flag_ = false;
    ref_cst_type_ = share::schema::CONSTRAINT_TYPE_INVALID;
    ref_cst_id_ = common::OB_INVALID_ID;
    validate_flag_ = CST_FK_VALIDATED;
    is_modify_validate_flag_ = false;
    rely_flag_ = false;
    is_modify_rely_flag_ = false;
    is_modify_fk_state_ = false;
    need_validate_data_ = true;
    is_parent_table_mock_ = false;
    parent_database_id_ = common::OB_INVALID_ID;
    parent_table_id_ = common::OB_INVALID_ID;
    name_generated_type_ = GENERATED_TYPE_UNKNOWN;
  }
  bool is_valid() const;
  int assign(const ObCreateForeignKeyArg &other) {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObIndexArg::assign(other))) {
      SHARE_LOG(WARN, "assign index arg failed", K(ret), K(other));
    } else if (FALSE_IT(parent_database_ = other.parent_database_)) {
    } else if (FALSE_IT(parent_table_ = other.parent_table_)) {
    } else if (OB_FAIL(child_columns_.assign(other.child_columns_))) {
      SHARE_LOG(WARN, "assign child columns failed", K(ret), K(other.child_columns_));
    } else if (OB_FAIL(parent_columns_.assign(other.parent_columns_))) {
      SHARE_LOG(WARN, "assign parent columns failed", K(ret), K(other.parent_columns_));
    } else {
      update_action_ = other.update_action_;
      delete_action_ = other.delete_action_;
      foreign_key_name_ = other.foreign_key_name_;
      enable_flag_ = other.enable_flag_;
      is_modify_enable_flag_ = other.is_modify_enable_flag_;
      ref_cst_type_ = other.ref_cst_type_;
      ref_cst_id_ = other.ref_cst_id_;
      validate_flag_ = other.validate_flag_;
      is_modify_validate_flag_ = other.is_modify_validate_flag_;
      rely_flag_ = other.rely_flag_;
      is_modify_rely_flag_ = other.is_modify_rely_flag_;
      is_modify_fk_state_ = other.is_modify_fk_state_;
      need_validate_data_ = other.need_validate_data_;
      is_parent_table_mock_ = other.is_parent_table_mock_;
      parent_database_id_ = other.parent_database_id_;
      parent_table_id_ = other.parent_table_id_;
      name_generated_type_ = other.name_generated_type_;
    }
    return ret;
  }
  DECLARE_VIRTUAL_TO_STRING;

public:
  common::ObString parent_database_;
  common::ObString parent_table_;
  common::ObSEArray<common::ObString, 8> child_columns_;
  common::ObSEArray<common::ObString, 8> parent_columns_;
  share::schema::ObReferenceAction update_action_;
  share::schema::ObReferenceAction delete_action_;
  common::ObString foreign_key_name_;
  bool enable_flag_;
  bool is_modify_enable_flag_;
  share::schema::ObConstraintType ref_cst_type_;
  uint64_t ref_cst_id_;
  ObCstFkValidateFlag validate_flag_;
  bool is_modify_validate_flag_;
  bool rely_flag_;
  bool is_modify_rely_flag_;
  bool is_modify_fk_state_;
  bool need_validate_data_;
  bool is_parent_table_mock_;
  uint64_t parent_database_id_;  // used in ddl_service to store related object_id
  uint64_t parent_table_id_;     // used in ddl_service to store related object_id
  ObNameGeneratedType name_generated_type_;
};

struct ObDropForeignKeyArg : public ObIndexArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObDropForeignKeyArg()
  : ObIndexArg(),
    foreign_key_name_()
  {
    index_action_type_ = DROP_FOREIGN_KEY;
  }
  virtual ~ObDropForeignKeyArg()
  {}

  void reset()
  {
    ObIndexArg::reset();
    foreign_key_name_.reset();
  }
  bool is_valid() const;
  DECLARE_VIRTUAL_TO_STRING;

public:
  common::ObString foreign_key_name_;
};

struct ObFlashBackTableFromRecyclebinArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObFlashBackTableFromRecyclebinArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    origin_db_name_(),
    origin_table_name_(),
    new_db_name_(),
    new_table_name_(),
    origin_table_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  uint64_t tenant_id_;
  common::ObString origin_db_name_;
  common::ObString origin_table_name_;
  common::ObString new_db_name_;
  common::ObString new_table_name_;
  uint64_t origin_table_id_;//only used in work thread, no need add to SERIALIZE now

  TO_STRING_KV(K_(tenant_id),
               K_(origin_db_name),
               K_(origin_table_name),
               K_(new_db_name),
               K_(new_table_name),
               K_(origin_table_id));
};

struct ObFlashBackTableToScnArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
  public:
  ObFlashBackTableToScnArg():
    tenant_id_(common::OB_INVALID_ID),
    time_point_(-1),
    tables_(),
    query_end_time_(-1)
  {}
  bool is_valid() const;

  uint64_t tenant_id_;
  int64_t time_point_;
  common::ObSArray<ObTableItem> tables_;
  int64_t query_end_time_;

  TO_STRING_KV(K_(tenant_id),
               K_(time_point),
               K_(tables),
               K_(query_end_time));
};

struct ObFlashBackIndexArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObFlashBackIndexArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    origin_table_name_(),
    new_db_name_(),
    new_table_name_(),
    origin_table_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  uint64_t tenant_id_;
  common::ObString origin_table_name_;
  common::ObString new_db_name_;
  common::ObString new_table_name_;
  uint64_t origin_table_id_;//only used in work thread, no need add to SERIALIZE now

  TO_STRING_KV(K_(tenant_id),
               K_(origin_table_name),
               K_(new_db_name),
               K_(new_table_name),
               K_(origin_table_id));
};

struct ObFlashBackDatabaseArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObFlashBackDatabaseArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    origin_db_name_(),
    new_db_name_()
  {}
  bool is_valid() const;
  uint64_t tenant_id_;
  common::ObString origin_db_name_;
  common::ObString new_db_name_;

  TO_STRING_KV(K_(tenant_id),
               K_(origin_db_name),
               K_(new_db_name));
};

struct ObFlashBackTenantArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObFlashBackTenantArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    origin_tenant_name_(),
    new_tenant_name_()
  {}
  bool is_valid() const;
  uint64_t tenant_id_;
  common::ObString origin_tenant_name_;
  common::ObString new_tenant_name_;

  TO_STRING_KV(K_(tenant_id),
               K_(origin_tenant_name),
               K_(new_tenant_name));
};

struct ObPurgeTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObPurgeTableArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    database_id_(common::OB_INVALID_ID),
    table_name_()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString table_name_;
  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(table_name));
};

struct ObPurgeIndexArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObPurgeIndexArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    database_id_(common::OB_INVALID_ID),
    table_name_(),
    table_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  int init(const uint64_t tenant_id,
           const uint64_t exec_tenant_id,
           const ObString &table_name,
           const uint64_t table_id,
           const ObString &ddl_stmt_str);
  int assign(const ObPurgeIndexArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  void reset()
  {
    tenant_id_ = OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    table_name_.reset();
    ObDDLArg::reset();
  }
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString table_name_;
  uint64_t table_id_;//only used in work thread, no need add to SERIALIZE now

  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(table_name),
               K_(table_id));
};

struct ObPurgeDatabaseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObPurgeDatabaseArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    db_name_()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  uint64_t tenant_id_;
  common::ObString db_name_;
  TO_STRING_KV(K_(tenant_id),
               K_(db_name));
};


struct ObPurgeTenantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObPurgeTenantArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    tenant_name_()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  uint64_t tenant_id_;
  common::ObString tenant_name_;
  TO_STRING_KV(K_(tenant_id),
               K_(tenant_name));
};

struct ObPurgeRecycleBinArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  static const int DEFAULT_PURGE_EACH_TIME = 10;
  ObPurgeRecycleBinArg():
    ObDDLArg(),
    tenant_id_(common::OB_INVALID_ID),
    purge_num_(0),
    expire_time_(0),
    auto_purge_(false)
  {}
  virtual ~ObPurgeRecycleBinArg()
  {}
  bool is_valid() const;
  int assign(const ObPurgeRecycleBinArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  uint64_t tenant_id_;
  int64_t purge_num_;
  int64_t expire_time_;
  bool auto_purge_;
  TO_STRING_KV(K_(tenant_id), K_(purge_num), K_(expire_time), K_(auto_purge));
};

struct ObCreateDupLSArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateDupLSArg() : tenant_id_(OB_INVALID_TENANT_ID) {}
  ~ObCreateDupLSArg() {}
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_; }
  void reset() { tenant_id_ = OB_INVALID_TENANT_ID; }
  int assign(const ObCreateDupLSArg &arg);
  int init(const uint64_t tenant_id);
  int64_t get_tenant_id() const { return tenant_id_; }
  DECLARE_TO_STRING;
private:
  uint64_t tenant_id_;
private:
   DISALLOW_COPY_AND_ASSIGN(ObCreateDupLSArg);
};

struct ObCreateDupLSResult
{
  OB_UNIS_VERSION(1);
public:
  ObCreateDupLSResult(): ret_(common::OB_SUCCESS) {}
  ~ObCreateDupLSResult() {}
  bool is_valid() const;
  int assign(const ObCreateDupLSResult &other);
  void init(const int ret) { ret_ = ret; }
  TO_STRING_KV(K_(ret));
  int get_result() const { return ret_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateDupLSResult);
private:
  int ret_;
};

struct ObCreateLSArg
{
  OB_UNIS_VERSION(1);
public:
  enum CreateLSType
  {
    EMPTY_LS = 0,
    CREATE_WITH_PALF,
  };
  ObCreateLSArg() : tenant_id_(OB_INVALID_TENANT_ID), id_(),
                    replica_type_(REPLICA_TYPE_MAX),
                    replica_property_(), tenant_info_(),
                    create_scn_(),
                    compat_mode_(lib::Worker::CompatMode::INVALID),
                    create_ls_type_(EMPTY_LS),
                    palf_base_info_() {}
  ~ObCreateLSArg() {}
  bool is_valid() const;
  void reset();
  int assign(const ObCreateLSArg &arg);
  int init(const int64_t tenant_id,
           const share::ObLSID &id,
           const ObReplicaType replica_type,
           const common::ObReplicaProperty &replica_property,
           const share::ObAllTenantInfo &tenant_info,
           const share::SCN &create_scn,
           const lib::Worker::CompatMode &mode,
           const bool create_with_palf,
           const palf::PalfBaseInfo &palf_base_info);
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  const share::ObAllTenantInfo &get_tenant_info() const { return tenant_info_; }
  share::ObLSID get_ls_id() const
  {
    return id_;
  }
  ObReplicaType get_replica_type() const
  {
    return replica_type_;
  }
  common::ObReplicaProperty get_replica_property() const
  {
    return replica_property_;
  }
  const share::SCN &get_create_scn() const
  {
    return create_scn_;
  }
  lib::Worker::CompatMode get_compat_mode() const
  {
    return compat_mode_;
  }
  const palf::PalfBaseInfo& get_palf_base_info() const
  {
    return palf_base_info_;
  }
  bool is_create_ls_with_palf() const
  {
    return CREATE_WITH_PALF == create_ls_type_;
  }

  bool need_create_inner_tablets() const
  {
    return CREATE_WITH_PALF != create_ls_type_;
  }
  DECLARE_TO_STRING;

private:
  uint64_t tenant_id_;
  share::ObLSID id_;
  ObReplicaType replica_type_;
  common::ObReplicaProperty replica_property_;
  share::ObAllTenantInfo tenant_info_;
  share::SCN create_scn_;
  lib::Worker::CompatMode compat_mode_;
  CreateLSType create_ls_type_;
  palf::PalfBaseInfo palf_base_info_;
private:
   DISALLOW_COPY_AND_ASSIGN(ObCreateLSArg);
};

struct ObCreateLSResult
{
  OB_UNIS_VERSION(1);
public:
  ObCreateLSResult(): ret_(common::OB_SUCCESS), addr_(), replica_type_(REPLICA_TYPE_FULL) {}
  ~ObCreateLSResult() {}
  bool is_valid() const;
  int assign(const ObCreateLSResult &other);
  void init(const int ret, const ObAddr &addr, const ObReplicaType &replica_type)
  {
    ret_ = ret;
    addr_ = addr;
    replica_type_ = replica_type;
  }
  TO_STRING_KV(K_(ret), K_(addr), K_(replica_type));
  int get_result() const
  {
    return ret_;
  }
  const ObAddr &get_addr() const
  {
    return addr_;
  }
  const common::ObReplicaType &get_replica_type() const
  {
    return replica_type_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateLSResult);
private:
  int ret_;
  ObAddr addr_;//for async rpc, dests and results not one-by-one mapping
  common::ObReplicaType replica_type_;
};

#ifdef OB_BUILD_ARBITRATION
// send to arbitration service to create A-replica
struct ObCreateArbArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateArbArg() : tenant_id_(OB_INVALID_TENANT_ID),
                     ls_id_(),
                     tenant_role_() {}
  ~ObCreateArbArg() {}
  bool is_valid() const;
  void reset();
  int assign(const ObCreateArbArg &arg);
  int init(const int64_t tenant_id,
           const share::ObLSID &id,
           const share::ObTenantRole &tenant_role);
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  const share::ObTenantRole &get_tenant_role() const { return tenant_role_; }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  DECLARE_TO_STRING;
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::ObTenantRole tenant_role_;
private:
   DISALLOW_COPY_AND_ASSIGN(ObCreateArbArg);
};

struct ObCreateArbResult
{
  OB_UNIS_VERSION(1);
public:
  ObCreateArbResult(): ret_(common::OB_SUCCESS) {}
  ~ObCreateArbResult() {}
  bool is_valid() const;
  int assign(const ObCreateArbResult &other);
  TO_STRING_KV(K_(ret));
  void set_result(const int ret)
  {
    ret_ = ret;
  }
  int get_result() const
  {
    return ret_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateArbResult);
private:
  int ret_;
};

// send to arbitration service to delete A-replica
struct ObDeleteArbArg
{
  OB_UNIS_VERSION(1);
public:
  ObDeleteArbArg() : tenant_id_(OB_INVALID_TENANT_ID),
                     ls_id_() {}
  ~ObDeleteArbArg() {}
  bool is_valid() const;
  void reset();
  int assign(const ObDeleteArbArg &arg);
  int init(const int64_t tenant_id,
           const share::ObLSID &id);
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  DECLARE_TO_STRING;
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
private:
   DISALLOW_COPY_AND_ASSIGN(ObDeleteArbArg);
};

struct ObDeleteArbResult
{
  OB_UNIS_VERSION(1);
public:
  ObDeleteArbResult(): ret_(common::OB_SUCCESS) {}
  ~ObDeleteArbResult() {}
  bool is_valid() const;
  int assign(const ObDeleteArbResult &other);
  TO_STRING_KV(K_(ret));
  void set_result(const int ret)
  {
    ret_ = ret;
  }
  int get_result() const
  {
    return ret_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeleteArbResult);
private:
  int ret_;
};

struct ObAddArbResult
{
  OB_UNIS_VERSION(1);
public:
  ObAddArbResult(): ret_(common::OB_SUCCESS) {}
  ~ObAddArbResult() {}
  bool is_valid() const;
  int assign(const ObAddArbResult &other);
  TO_STRING_KV(K_(ret));
  void set_result(const int ret)
  {
    ret_ = ret;
  }
  int get_result() const
  {
    return ret_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObAddArbResult);
private:
  int ret_;
};

struct ObRemoveArbResult
{
  OB_UNIS_VERSION(1);
public:
  ObRemoveArbResult(): ret_(common::OB_SUCCESS) {}
  ~ObRemoveArbResult() {}
  bool is_valid() const;
  int assign(const ObRemoveArbResult &other);
  TO_STRING_KV(K_(ret));
  void set_result(const int ret)
  {
    ret_ = ret;
  }
  int get_result() const
  {
    return ret_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoveArbResult);
private:
  int ret_;
};

struct ObForceClearArbClusterInfoArg
{
  OB_UNIS_VERSION(1);
public:
  ObForceClearArbClusterInfoArg()
    : cluster_id_(OB_INVALID_ID) { }
  ObForceClearArbClusterInfoArg(const int64_t cluster_id)
    : cluster_id_(cluster_id) { }
  bool is_valid() const { return is_valid_cluster_id(cluster_id_); }
  int assign(const ObForceClearArbClusterInfoArg &other)
  {
    cluster_id_ = other.cluster_id_;
    return OB_SUCCESS;
  }
  int64_t get_cluster_id() const { return cluster_id_; }
  TO_STRING_KV(K_(cluster_id));
private:
   DISALLOW_COPY_AND_ASSIGN(ObForceClearArbClusterInfoArg);
private:
  int64_t cluster_id_;
};
#endif

struct ObSetMemberListArgV2
{
  OB_UNIS_VERSION(1);
public:
  ObSetMemberListArgV2() : tenant_id_(OB_INVALID_TENANT_ID), id_(),
                           member_list_(), paxos_replica_num_(0),
                           arbitration_service_(), learner_list_() {}
  ~ObSetMemberListArgV2() {}
  bool is_valid() const;
  void reset();
  int assign(const ObSetMemberListArgV2 &arg);
  int init(const int64_t tenant_id,
           const share::ObLSID &id,
           const int64_t paxos_replica_num,
           const ObMemberList &member_list,
           const ObMember &arbitration_service,
           const common::GlobalLearnerList &learner_list);
  DECLARE_TO_STRING;
  const ObMemberList& get_member_list() const
  {
    return member_list_;
  }
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return id_;
  }
  const ObMember& get_arbitration_service() const
  {
    return arbitration_service_;
  }
  int64_t get_paxos_replica_num() const
  {
    return paxos_replica_num_;
  }
  const common::GlobalLearnerList& get_learner_list() const
  {
    return learner_list_;
  }
private:
  int64_t tenant_id_;
  share::ObLSID id_;
  ObMemberList member_list_;
  int64_t paxos_replica_num_;
  ObMember arbitration_service_;
  common::GlobalLearnerList learner_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetMemberListArgV2);
};

struct ObSetMemberListResult
{
  OB_UNIS_VERSION(1);
public:
  ObSetMemberListResult(): ret_(common::OB_SUCCESS) {}
  ~ObSetMemberListResult() {}
  bool is_valid() const;
  int assign(const ObSetMemberListResult &other);
  TO_STRING_KV(K_(ret));
  void init(const int ret)
  {
    ret_ = ret;
  }
  int get_result() const
  {
    return ret_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObSetMemberListResult);
private:
  int ret_;
};

struct ObGetLSAccessModeInfoArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetLSAccessModeInfoArg(): tenant_id_(OB_INVALID_TENANT_ID),
                              ls_id_() {}
  ~ObGetLSAccessModeInfoArg() {}
  bool is_valid() const;
  int init(uint64_t tenant_id, const share::ObLSID &ls_id);
  int assign(const ObGetLSAccessModeInfoArg &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id));
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObGetLSAccessModeInfoArg);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

struct ObLSAccessModeInfo
{
  OB_UNIS_VERSION(1);
public:
  ObLSAccessModeInfo(): tenant_id_(OB_INVALID_TENANT_ID),
                        ls_id_(),
                        mode_version_(palf::INVALID_PROPOSAL_ID),
                        access_mode_(palf::AccessMode::INVALID_ACCESS_MODE),
                        ref_scn_(),
                        addr_(),
                        sys_ls_end_scn_() {}
  ~ObLSAccessModeInfo() {}
  bool is_valid() const;
  int init(uint64_t tenant_id, const share::ObLSID &ls_idd,
           const int64_t mode_version,
           const palf::AccessMode &access_mode,
           const share::SCN &ref_scn,
           const share::SCN &sys_ls_end_scn);
  int assign(const ObLSAccessModeInfo &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(mode_version),
               K_(access_mode), K_(ref_scn), K_(sys_ls_end_scn));
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  palf::AccessMode get_access_mode() const
  {
    return access_mode_;
  }
  int64_t get_mode_version() const
  {
    return mode_version_;
  }
  const share::SCN &get_ref_scn() const
  {
    return ref_scn_;
  }
  const share::SCN &get_sys_ls_end_scn() const
  {
    return sys_ls_end_scn_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSAccessModeInfo);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t mode_version_;
  palf::AccessMode access_mode_;
  share::SCN ref_scn_;
  ObAddr addr_;//no used, add in 4200 RC1
  share::SCN sys_ls_end_scn_; // new arg in V4.2.0
};

struct ObChangeLSAccessModeRes
{
  OB_UNIS_VERSION(1);
public:
  ObChangeLSAccessModeRes(): tenant_id_(OB_INVALID_TENANT_ID),
                              ls_id_(), ret_(common::OB_SUCCESS), wait_sync_scn_cost_(0), change_access_mode_cost_(0) {}
  ~ObChangeLSAccessModeRes() {}
  bool is_valid() const;
  int init(uint64_t tenant_id, const share::ObLSID& ls_id, const int result, const int64_t wait_sync_scn_cost, const int64_t change_access_mode_cost);
  int assign(const ObChangeLSAccessModeRes &other);
  TO_STRING_KV(K_(tenant_id), "ls_id", ls_id_.id(), K_(ret), K_(wait_sync_scn_cost), K_(change_access_mode_cost));
  int get_result() const
  {
    return ret_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  int64_t get_wait_sync_scn_cost() const { return wait_sync_scn_cost_; }
  int64_t get_change_access_mode_cost() const { return change_access_mode_cost_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObChangeLSAccessModeRes);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int ret_;
  int64_t wait_sync_scn_cost_;
  int64_t change_access_mode_cost_;
};

struct ObNotifySwitchLeaderArg
{
  OB_UNIS_VERSION(1);
public:
  enum SwitchLeaderComment
  {
    INVALID_COMMENT = -1,
    STOP_SERVER,
    STOP_ZONE,
    CHANGE_MEMBER,//remove member, migrate replica, type transfer
    MODIFY_PRIMARY_ZONE,
    MANUAL_SWITCH,
    CREATE_LS,
  };
  const char* comment_to_str() const;
  ObNotifySwitchLeaderArg() : tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), advise_leader_(),
  comment_(INVALID_COMMENT) {}
  ~ObNotifySwitchLeaderArg() {}
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(advise_leader), K_(comment), "commet_str", comment_to_str());
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObAddr &leader, const SwitchLeaderComment &comment);
  bool is_valid() const
  {
    return INVALID_COMMENT != comment_;
  }
  int assign(const ObNotifySwitchLeaderArg &other);
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObNotifySwitchLeaderArg);
private:
  //tenant_id is invalid weakup all tenant
  //tenant_id is valid weakup target tenant
  uint64_t tenant_id_;
  //ls_id is invalid iterator all ls
  //ls_id is valid, only check target ls
  share::ObLSID ls_id_;
  //advise_leader maybe invalid
  common::ObAddr advise_leader_;
  SwitchLeaderComment comment_;
};

struct ObNotifyTenantThreadArg
{
  OB_UNIS_VERSION(1);
public:
  enum TenantThreadType
  {
    INVALID_TYPE = -1,
    RECOVERY_LS_SERVICE,
  };
  ObNotifyTenantThreadArg() : tenant_id_(OB_INVALID_TENANT_ID), thread_type_(INVALID_TYPE) {}
  ~ObNotifyTenantThreadArg() {}
  TO_STRING_KV(K_(tenant_id), K_(thread_type));
  int init(const uint64_t tenant_id, const TenantThreadType thread_type);
  int assign(const ObNotifyTenantThreadArg &other);
  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_) && INVALID_TYPE != thread_type_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  TenantThreadType get_thread_type() const
  {
    return thread_type_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObNotifyTenantThreadArg);
private:
  uint64_t tenant_id_;
  TenantThreadType thread_type_;
};

struct ObCreateTabletInfo
{
  OB_UNIS_VERSION(1);
public:
  ObCreateTabletInfo() { reset(); }
  ~ObCreateTabletInfo() {}
  bool is_valid() const;
  void reset();
  int assign(const ObCreateTabletInfo &info);
  int init(const ObIArray<common::ObTabletID> &tablet_ids,
           const common::ObTabletID data_tablet_id,
           const common::ObIArray<int64_t> &table_schema_index,
           const lib::Worker::CompatMode &mode,
           const bool is_create_bind_hidden_tablets);
  common::ObTabletID get_data_tablet_id() const { return data_tablet_id_; }
  int64_t get_tablet_count() const { return tablet_ids_.count(); }
  DECLARE_TO_STRING;

  common::ObSArray<common::ObTabletID> tablet_ids_;
  common::ObTabletID data_tablet_id_; // or orig tablet id if is create hidden tablets
  //the index of table_schemas_ in ObBatchCreateTabletArg
  common::ObSArray<int64_t> table_schema_index_;
  lib::Worker::CompatMode compat_mode_;
  bool is_create_bind_hidden_tablets_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTabletInfo);
};

struct ObCreateTabletExtraInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObCreateTabletExtraInfo() { reset(); }
  ~ObCreateTabletExtraInfo() { reset(); }
  int init(const uint64_t tenant_data_version, const bool need_create_empty_major);
  void reset();
  int assign(const ObCreateTabletExtraInfo &other);
public:
  uint64_t tenant_data_version_;
  bool need_create_empty_major_;
  TO_STRING_KV(K_(tenant_data_version), K_(need_create_empty_major));
};

struct ObBatchCreateTabletArg
{
  OB_UNIS_VERSION(1);
public:
  ObBatchCreateTabletArg()
  { reset(); }
  ~ObBatchCreateTabletArg() {}
  bool is_valid() const;
  bool is_inited() const;
  void reset();
  int assign(const ObBatchCreateTabletArg &arg);
  int init_create_tablet(const share::ObLSID &id_,
                         const share::SCN &major_frozen_scn,
                         const bool need_check_tablet_cnt);
  int64_t get_tablet_count() const;
  int serialize_for_create_tablet_schemas(char *buf,
      const int64_t data_len,
      int64_t &pos) const;
  int64_t get_serialize_size_for_create_tablet_schemas() const;
  int deserialize_create_tablet_schemas(const char *buf,
      const int64_t data_len,
      int64_t &pos);
  static int is_old_mds(const char *buf,
      int64_t data_len,
      bool &is_old_mds);
  static int skip_unis_array_len(const char *buf,
      int64_t data_len,
      int64_t &pos);
  bool set_binding_info_outside_create() const;
  DECLARE_TO_STRING;

public:
  share::ObLSID id_;
  share::SCN major_frozen_scn_;
  common::ObSArray<share::schema::ObTableSchema> table_schemas_;
  common::ObSArray<ObCreateTabletInfo> tablets_;
  bool need_check_tablet_cnt_;
  bool is_old_mds_;
  common::ObSArray<storage::ObCreateTabletSchema*> create_tablet_schemas_;
  ObArenaAllocator allocator_;
  common::ObSArray<ObCreateTabletExtraInfo> tablet_extra_infos_;
  share::SCN clog_checkpoint_scn_;
};

struct ObBatchRemoveTabletArg
{
  OB_UNIS_VERSION(1);
public:
  ObBatchRemoveTabletArg() { reset(); }
  ~ObBatchRemoveTabletArg() {}
  bool is_valid() const;
  bool is_inited() const;
  void reset();
  int assign (const ObBatchRemoveTabletArg &arg);
  int init(const ObIArray<common::ObTabletID> &tablet_ids,
           const share::ObLSID id);
  static int is_old_mds(const char *buf,
      int64_t data_len,
      bool &is_old_mds);
  static int skip_array_len(const char *buf,
      int64_t data_len,
      int64_t &pos);
  DECLARE_TO_STRING;

public:
  share::ObLSID id_;
  common::ObSArray<common::ObTabletID> tablet_ids_;
  bool is_old_mds_;
};

struct ObRemoveTabletRes
{
  OB_UNIS_VERSION(1);

public:
  ObRemoveTabletRes()
    : ret_(common::OB_SUCCESS) {}
  ~ObRemoveTabletRes() {}
  inline void reset();

  DECLARE_TO_STRING;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoveTabletRes);
};

struct ObCreateTabletBatchRes
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTabletBatchRes()
    : ret_(common::OB_SUCCESS) {}
  ~ObCreateTabletBatchRes() {}
  inline void reset();

  DECLARE_TO_STRING;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTabletBatchRes);
};

struct ObCreateTabletBatchInTransRes
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTabletBatchInTransRes()
    : ret_(common::OB_SUCCESS), tx_result_() {}
  ~ObCreateTabletBatchInTransRes() {}
  inline void reset();

  DECLARE_TO_STRING;
  int ret_;
  transaction::ObTxExecResult tx_result_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTabletBatchInTransRes);
};

using ObRemoveTabletsInTransRes = ObCreateTabletBatchInTransRes;

struct ObFetchTabletSeqArg
{
  OB_UNIS_VERSION(1);
public:
  ObFetchTabletSeqArg() : tenant_id_(OB_INVALID_TENANT_ID), cache_size_(0), tablet_id_(), ls_id_() {}
  ~ObFetchTabletSeqArg() {}
  bool is_valid() const;
  void reset();
  int assign (const ObFetchTabletSeqArg &arg);
  int init(const uint64_t tenant_id,
           const uint64_t cache_size,
           const common::ObTabletID tablet_id,
           const share::ObLSID ls_id);
public:
  DECLARE_TO_STRING;
public:
  uint64_t tenant_id_;
  uint64_t cache_size_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFetchTabletSeqArg);

};

struct ObFetchTabletSeqRes
{
  OB_UNIS_VERSION(1);
public:
  ObFetchTabletSeqRes() : tenant_id_(OB_INVALID_TENANT_ID), cache_interval_() {}
  ~ObFetchTabletSeqRes() {}
  bool is_valid() const;
  void reset();
  int assign (const ObFetchTabletSeqRes &arg);
  int init(const uint64_t tenant_id,
           const share::ObTabletAutoincInterval cache_interval_);
  DECLARE_TO_STRING;
public:
  uint64_t tenant_id_;
  share::ObTabletAutoincInterval cache_interval_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFetchTabletSeqRes);
};

using ObClearTabletAutoincSeqCacheArg = ObBatchRemoveTabletArg;

struct ObGetMinSSTableSchemaVersionRes
{
  OB_UNIS_VERSION(1);

public:
  ObGetMinSSTableSchemaVersionRes()
    : ret_list_(){}
  ~ObGetMinSSTableSchemaVersionRes() { reset(); }
  inline void reset() { ret_list_.reset(); }
  inline void reuse() { ret_list_.reuse(); }

  TO_STRING_KV(K_(ret_list));
  // response includes all rets
  common::ObSArray<int64_t> ret_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetMinSSTableSchemaVersionRes);
};

struct ObCalcColumnChecksumRequestArg final
{
  OB_UNIS_VERSION(1);
public:
  ObCalcColumnChecksumRequestArg() { reset(); }
  ~ObCalcColumnChecksumRequestArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObCalcColumnChecksumRequestArg &other);
  TO_STRING_KV(K_(tenant_id), K_(target_table_id), K_(schema_version), K_(execution_id),
      K_(snapshot_version), K_(source_table_id), K_(task_id), K_(calc_items));
  struct SingleItem final
  {
    OB_UNIS_VERSION(1);
  public:
    SingleItem() { reset(); }
    ~SingleItem() = default;
    bool is_valid() const;
    void reset();
    int assign(const SingleItem &other);
    TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(calc_table_id));
    share::ObLSID ls_id_;
    common::ObTabletID tablet_id_;
    int64_t calc_table_id_;
  };
public:
  uint64_t tenant_id_;
  uint64_t target_table_id_;
  int64_t schema_version_;
  int64_t execution_id_;
  int64_t snapshot_version_;
  int64_t source_table_id_;
  int64_t task_id_;
  common::ObSEArray<SingleItem, 10> calc_items_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCalcColumnChecksumRequestArg);
};

struct ObCalcColumnChecksumRequestRes final
{
  OB_UNIS_VERSION(1);
public:
  common::ObSEArray<int, 10> ret_codes_;
};

struct ObCalcColumnChecksumResponseArg
{
  OB_UNIS_VERSION(2);
public:
  ObCalcColumnChecksumResponseArg() { reset(); }
  ~ObCalcColumnChecksumResponseArg() = default;
  bool is_valid() const;
  void reset();
  int assign(const ObCalcColumnChecksumResponseArg &other) {
    int ret = common::OB_SUCCESS;
    tablet_id_ = other.tablet_id_;
    target_table_id_ = other.target_table_id_;
    ret_code_ = other.ret_code_;
    source_table_id_ = other.source_table_id_;
    schema_version_ = other.schema_version_;
    task_id_ = other.task_id_;
    tenant_id_ = other.tenant_id_;
    return ret;
  }
  TO_STRING_KV(K_(task_id), K_(tablet_id), K_(target_table_id), K_(ret_code), K_(source_table_id), K_(schema_version), K_(tenant_id));
public:
  common::ObTabletID tablet_id_;
  uint64_t target_table_id_;
  int ret_code_;
  int64_t source_table_id_;
  int64_t schema_version_;
  int64_t task_id_;
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCalcColumnChecksumResponseArg);
};

struct ObRemoveNonPaxosReplicaBatchResult
{
public:
  ObRemoveNonPaxosReplicaBatchResult() : return_array_() {}
public:
  TO_STRING_KV(K_(return_array));
public:
  common::ObSArray<int> return_array_;

  OB_UNIS_VERSION(3);
};

struct ObLSMigrateReplicaArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObLSMigrateReplicaArg()
    : task_id_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_(),
      src_(),
      dst_(),
      data_source_(),
      paxos_replica_number_(0),
      skip_change_member_list_(),
      force_use_data_source_(false),
      force_data_source_() {}
public:
  int assign(const ObLSMigrateReplicaArg &that);

  int init(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObReplicaMember &src,
      const common::ObReplicaMember &dst,
      const common::ObReplicaMember &data_source,
      const int64_t paxos_replica_number,
      const bool skip_change_member_list,
      const common::ObReplicaMember &force_data_source);

  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(ls_id),
               K_(src),
               K_(dst),
               K_(data_source),
               K_(paxos_replica_number),
               K_(skip_change_member_list),
               K_(force_use_data_source),
               K_(force_data_source));

  bool is_valid() const {
    return !task_id_.is_invalid()
           && common::OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid()
           && src_.is_valid()
           && dst_.is_valid()
           && data_source_.is_valid()
           && paxos_replica_number_ > 0;
  }
public:
  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_source_;
  int64_t paxos_replica_number_;
  bool skip_change_member_list_;
  bool force_use_data_source_;
  // deprecated field, in order to fix the upgrade compatibility issue from 430rc2 to master
  common::ObReplicaMember force_data_source_;
};

struct ObLSAddReplicaArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObLSAddReplicaArg()
    : task_id_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_(),
      dst_(),
      data_source_(),
      orig_paxos_replica_number_(0),
      new_paxos_replica_number_(0),
      skip_change_member_list_(false),
      force_use_data_source_(false),
      force_data_source_() {}
public:
  int assign(const ObLSAddReplicaArg &that);

  int init(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObReplicaMember &dst,
      const common::ObReplicaMember &data_source,
      const int64_t orig_paxos_replica_number,
      const int64_t new_paxos_replica_number,
      const bool skip_change_member_list,
      const common::ObReplicaMember &force_data_source);

  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(ls_id),
               K_(dst),
               K_(data_source),
               K_(orig_paxos_replica_number),
               K_(new_paxos_replica_number),
               K_(skip_change_member_list),
               K_(force_use_data_source),
               K_(force_data_source));

  bool is_valid() const {
    return !task_id_.is_invalid()
           && common::OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid()
           && dst_.is_valid()
           && data_source_.is_valid()
           && orig_paxos_replica_number_ > 0
           && new_paxos_replica_number_ > 0;
  }
public:
  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_source_;
  int64_t orig_paxos_replica_number_;
  int64_t new_paxos_replica_number_;
  bool skip_change_member_list_;
  bool force_use_data_source_;
  // deprecated field, in order to fix the upgrade compatibility issue from 430rc2 to master
  common::ObReplicaMember force_data_source_;
};

struct ObLSChangeReplicaArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObLSChangeReplicaArg()
    : task_id_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_(),
      src_(),
      dst_(),
      data_source_(),
      orig_paxos_replica_number_(0),
      new_paxos_replica_number_(0),
      skip_change_member_list_(false) {}
public:
  int assign(const ObLSChangeReplicaArg &that);

  int init(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObReplicaMember &src,
      const common::ObReplicaMember &dst,
      const common::ObReplicaMember &data_source,
      const int64_t orig_paxos_replica_number,
      const int64_t new_paxos_replica_number,
      const bool skip_change_member_list);

  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(ls_id),
               K_(src),
               K_(dst),
               K_(data_source),
               K_(orig_paxos_replica_number),
               K_(new_paxos_replica_number),
               K_(skip_change_member_list));

  bool is_valid() const {
    return !task_id_.is_invalid()
           && common::OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid()
           && src_.is_valid()
           && dst_.is_valid()
           && data_source_.is_valid()
           && orig_paxos_replica_number_ > 0
           && new_paxos_replica_number_ > 0;
  }
public:
  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_source_;
  int64_t orig_paxos_replica_number_;
  int64_t new_paxos_replica_number_;
  bool skip_change_member_list_;
};

struct ObLSDropPaxosReplicaArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObLSDropPaxosReplicaArg()
    : task_id_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_(),
      remove_member_(),
      orig_paxos_replica_number_(0),
      new_paxos_replica_number_(0) {}
public:
  int assign(const ObLSDropPaxosReplicaArg &that);

  int init(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObReplicaMember &remove_member,
      const int64_t orig_paxos_replica_number,
      const int64_t new_paxos_replica_number);

  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(ls_id),
               K_(remove_member),
               K_(orig_paxos_replica_number),
               K_(new_paxos_replica_number));

  bool is_valid() const {
    return !task_id_.is_invalid()
           && common::OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid()
           && remove_member_.is_valid()
           && orig_paxos_replica_number_ > 0
           && new_paxos_replica_number_ > 0;
  }
public:
  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObReplicaMember remove_member_;
  int64_t orig_paxos_replica_number_;
  int64_t new_paxos_replica_number_;
};

struct ObLSDropNonPaxosReplicaArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObLSDropNonPaxosReplicaArg()
    : task_id_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_(),
      remove_member_() {}
public:
  int assign(const ObLSDropNonPaxosReplicaArg &that);

  int init(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObReplicaMember &remove_member);

  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(ls_id),
               K_(remove_member));

  bool is_valid() const {
    return !task_id_.is_invalid()
           && common::OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid()
           && remove_member_.is_valid();
  }
public:
  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObReplicaMember remove_member_;
};

struct ObLSModifyPaxosReplicaNumberArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObLSModifyPaxosReplicaNumberArg()
    : task_id_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_(),
      orig_paxos_replica_number_(0),
      new_paxos_replica_number_(0),
      member_list_() {}
public:
  int assign(const ObLSModifyPaxosReplicaNumberArg &that);

  int init(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t orig_paxos_replica_number,
      const int64_t new_paxos_replica_number,
      const common::ObMemberList &member_list);

  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(ls_id),
               K_(orig_paxos_replica_number),
               K_(new_paxos_replica_number),
               K_(member_list));

  bool is_valid() const {
    return !task_id_.is_invalid()
           && common::OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid()
           && orig_paxos_replica_number_ > 0
           && new_paxos_replica_number_ > 0
           && member_list_.is_valid();
  }
public:
  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t orig_paxos_replica_number_;
  int64_t new_paxos_replica_number_;
  common::ObMemberList member_list_;
};

struct ObDRTaskReplyResult
{
public:
  OB_UNIS_VERSION(1);
public:
  ObDRTaskReplyResult()
    : task_id_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_(),
      result_(common::OB_SUCCESS) {}
public:
  int assign(
      const ObDRTaskReplyResult &that);

  int init(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int result);

  TO_STRING_KV(K_(task_id),
               K_(tenant_id),
               K_(ls_id),
               K_(result));

  bool is_valid() const {
    return !task_id_.is_invalid()
           && OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid();
  }
public:
  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int result_;
};

struct ObDRTaskExistArg final
{
public:
  OB_UNIS_VERSION(1);
public:
  ObDRTaskExistArg()
    : task_id_(),
      tenant_id_(OB_INVALID_ID),
      ls_id_() {}
public:
  int assign(
      const ObDRTaskExistArg &that);

  int init(
      const share::ObTaskId &task_id,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id);

  TO_STRING_KV(K_(task_id),
               K_(tenant_id));

  bool is_valid() const {
    return !task_id_.is_invalid()
           && OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid();
  }
public:
  share::ObTaskId task_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

class ObAdminDRTaskType
{
  OB_UNIS_VERSION(1);
public:
  enum AdminDRTaskType
  {
    INVALID_TYPE = -1,
    ADD_REPLICA = 0,
    REMOVE_REPLICA = 1,
    MAX_TYPE
  };
public:
  ObAdminDRTaskType() : type_(INVALID_TYPE) {}
  explicit ObAdminDRTaskType(AdminDRTaskType type) : type_(type) {}
  ObAdminDRTaskType &operator=(const AdminDRTaskType type) { type_ = type; return *this; }
  ObAdminDRTaskType &operator=(const ObAdminDRTaskType &other) { type_ = other.type_; return *this; }
  bool operator==(const ObAdminDRTaskType &other) const { return other.type_ == type_; }
  bool operator!=(const ObAdminDRTaskType &other) const { return other.type_ != type_; }
  void reset() { type_ = INVALID_TYPE; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
  void assign(const ObAdminDRTaskType &other) { type_ = other.type_; }
  bool is_valid() const { return INVALID_TYPE < type_ && MAX_TYPE > type_; }
  bool is_add_task() const { return ADD_REPLICA == type_; }
  bool is_remove_task() const { return REMOVE_REPLICA == type_; }
  const AdminDRTaskType &get_type() const { return type_; }
  const char* get_type_str() const;
  const char* get_comment() const;
private:
  AdminDRTaskType type_;
};
struct ObAdminCommandArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObAdminCommandArg()
    : admin_command_(),
      task_type_(ObAdminDRTaskType::INVALID_TYPE) {}
  ~ObAdminCommandArg() {}
public:
  int assign(const ObAdminCommandArg &other);
  int init(const ObString &admin_command, const ObAdminDRTaskType &task_type);
  bool is_valid() const { return !admin_command_.is_empty() && task_type_.is_valid(); }
  void reset() { admin_command_.reset(); task_type_.reset(); }
  const ObString get_admin_command_str() const { return admin_command_.str(); }
  const ObAdminDRTaskType &get_task_type() const { return task_type_; }
  const char* get_type_str() const { return task_type_.get_type_str(); }
  const char* get_comment() const { return task_type_.get_comment(); }
  bool is_remove_task() const { return task_type_.is_remove_task(); }
  bool is_add_task() const { return task_type_.is_add_task(); }
  TO_STRING_KV(K(admin_command_), K(task_type_));
private:
  common::ObFixedLengthString<OB_MAX_ADMIN_COMMAND_LENGTH + 1> admin_command_;
  ObAdminDRTaskType task_type_;
};

#ifdef OB_BUILD_ARBITRATION
// send to leader to add A-replica for log stream
struct ObAddArbArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObAddArbArg()
    : tenant_id_(OB_INVALID_ID),
      ls_id_(),
      arb_member_(),
      timeout_us_(OB_INVALID_TIMESTAMP) {}
public:
  int assign(const ObAddArbArg &that);
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObMember &arb_member,
      const int64_t timeout_us);
  TO_STRING_KV(K_(tenant_id),
               K_(ls_id),
               K_(arb_member),
               K_(timeout_us));
  bool is_valid() const {
    return common::OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid()
           && ls_id_.is_valid_with_tenant(tenant_id_)
           && arb_member_.is_valid()
           && timeout_us_ != OB_INVALID_TIMESTAMP;
  }
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObMember arb_member_;
  int64_t timeout_us_;
};

// send to leader to remove A-replica for log stream
struct ObRemoveArbArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObRemoveArbArg()
    : tenant_id_(OB_INVALID_ID),
      ls_id_(),
      arb_member_(),
      timeout_us_(OB_INVALID_TIMESTAMP) {}
public:
  int assign(const ObRemoveArbArg &that);
  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const common::ObMember &arb_member,
      const int64_t timeout_us);
  TO_STRING_KV(K_(tenant_id),
               K_(ls_id),
               K_(arb_member),
               K_(timeout_us));
  bool is_valid() const {
    return common::OB_INVALID_ID != tenant_id_
           && ls_id_.is_valid()
           && ls_id_.is_valid_with_tenant(tenant_id_)
           && arb_member_.is_valid()
           && timeout_us_ != OB_INVALID_TIMESTAMP;
  }
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObMember arb_member_;
	int64_t timeout_us_;
};
#endif

struct ObBackupTaskRes
{
  OB_UNIS_VERSION(1);
public:
  ObBackupTaskRes() :
    task_id_(0),
    job_id_(0),
    tenant_id_(0),
    ls_id_(),
    src_server_(),
    result_(OB_SUCCESS) {}
public:
  bool is_valid() const;
  int assign(const ObBackupTaskRes &res);
  TO_STRING_KV(K_(task_id), K_(job_id), K_(tenant_id), K_(src_server), K_(ls_id), K_(result), K_(trace_id), K_(dag_id));
public:
  int64_t task_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObAddr src_server_;
  int result_;
  share::ObTaskId trace_id_;
  share::ObTaskId dag_id_;
};
struct ObBackupDataArg
{
  OB_UNIS_VERSION(1);
public:
  ObBackupDataArg()
    : trace_id_(),
      job_id_(0),
      tenant_id_(0),
      task_id_(0),
      backup_set_id_(0),
      incarnation_id_(0),
      backup_type_(),
      backup_date_(0),
      ls_id_(0),
      turn_id_(0),
      retry_id_(0),
      dst_server_(),
      backup_path_(),
      backup_data_type_() {}
public:
  int assign(const ObBackupDataArg &arg);
  bool is_valid() const;
  TO_STRING_KV(K_(trace_id), K_(job_id), K_(tenant_id), K_(task_id), K_(backup_set_id), K_(incarnation_id),
      K_(backup_type), K_(backup_date), K_(ls_id), K_(turn_id), K_(retry_id), K_(dst_server), K_(backup_path),
      K_(backup_data_type));
public:
  share::ObTaskId trace_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t backup_set_id_;
  int64_t incarnation_id_;
  share::ObBackupType::BackupType backup_type_;
  int64_t backup_date_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  common::ObAddr dst_server_;
  share::ObBackupPathString backup_path_;
  share::ObBackupDataType backup_data_type_;
};

struct ObBackupComplLogArg
{
  OB_UNIS_VERSION(1);
public:
  ObBackupComplLogArg()
    : trace_id_(),
      job_id_(0),
      tenant_id_(0),
      task_id_(0),
      backup_set_id_(0),
      incarnation_id_(0),
      backup_type_(),
      backup_date_(0),
      ls_id_(),
      dst_server_(),
      backup_path_(),
      start_scn_(),
      end_scn_(),
      is_only_calc_stat_(false) {}
public:
  int assign(const ObBackupComplLogArg &arg);
  bool is_valid() const;
  TO_STRING_KV(K_(trace_id), K_(job_id), K_(tenant_id), K_(task_id), K_(backup_set_id), K_(incarnation_id),
    K_(backup_type), K_(backup_date), K_(ls_id), K_(dst_server), K_(backup_path), K_(start_scn), K_(end_scn));
public:
  share::ObTaskId trace_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t backup_set_id_;
  int64_t incarnation_id_;
  share::ObBackupType::BackupType backup_type_;
  int64_t backup_date_;
  share::ObLSID ls_id_;
  common::ObAddr dst_server_;
  share::ObBackupPathString backup_path_;
  share::SCN start_scn_;
  share::SCN end_scn_;
  bool is_only_calc_stat_;
};

struct ObBackupBuildIdxArg
{
  OB_UNIS_VERSION(1);
public:
  ObBackupBuildIdxArg()
    : job_id_(0),
      task_id_(0),
      trace_id_(),
      backup_path_(),
      tenant_id_(0),
      backup_set_id_(0),
      incarnation_id_(0),
      backup_date_(),
      backup_type_(),
      turn_id_(-1),
      retry_id_(-1),
      start_turn_id_(),
      dst_server_(),
      backup_data_type_() {}
public:
  int assign(const ObBackupBuildIdxArg &arg);
  bool is_valid() const;
  TO_STRING_KV(K_(job_id), K_(task_id), K_(trace_id), K_(backup_path), K_(tenant_id), K_(backup_set_id),
    K_(incarnation_id), K_(backup_date), K_(backup_type), K_(turn_id), K_(retry_id), K_(start_turn_id), K_(dst_server),
    K_(backup_data_type));
public:
  int64_t job_id_;
  int64_t task_id_;
  share::ObTaskId trace_id_;
  share::ObBackupPathString backup_path_;
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  int64_t incarnation_id_;
  int64_t backup_date_;
  share::ObBackupType::BackupType backup_type_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t start_turn_id_;
  common::ObAddr dst_server_;
  share::ObBackupDataType backup_data_type_;
};

struct ObBackupCheckTaskArg
{
  OB_UNIS_VERSION(1);
public:
  ObBackupCheckTaskArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      trace_id_() {}
  ~ObBackupCheckTaskArg() {}
  int assign(const ObBackupCheckTaskArg& that);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(trace_id));
public:
  uint64_t tenant_id_;
  share::ObTaskId trace_id_;
};

struct ObBackupMetaArg
{
  OB_UNIS_VERSION(1);
public:
  ObBackupMetaArg()
    : trace_id_(),
      job_id_(0),
      tenant_id_(0),
      task_id_(0),
      backup_set_id_(0),
      incarnation_id_(0),
      backup_type_(),
      backup_date_(0),
      ls_id_(),
      turn_id_(0),
      retry_id_(0),
      start_scn_(),
      dst_server_(),
      backup_path_() {}
public:
  int assign(const ObBackupMetaArg &arg);
  bool is_valid() const;
  TO_STRING_KV(K_(trace_id), K_(job_id), K_(tenant_id), K_(task_id), K_(backup_set_id), K_(incarnation_id),
      K_(backup_type), K_(backup_date), K_(ls_id), K_(turn_id), K_(retry_id), K_(start_scn), K_(dst_server), K_(backup_path));
public:
  share::ObTaskId trace_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t task_id_;
  int64_t backup_set_id_;
  int64_t incarnation_id_;
  share::ObBackupType::BackupType backup_type_;
  int64_t backup_date_;
  share::ObLSID ls_id_;
  int64_t  turn_id_;
  int64_t  retry_id_;
  share::SCN  start_scn_;
  common::ObAddr dst_server_;
  share::ObBackupPathString backup_path_;
};

struct ObBackupCheckTabletArg
{
  OB_UNIS_VERSION(1);
public:
  ObBackupCheckTabletArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      ls_id_(),
      backup_scn_(),
      tablet_ids_() {}
public:
  int assign(const ObBackupCheckTabletArg &arg);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(backup_scn), K_(tablet_ids));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::SCN backup_scn_;
  ObSArray<ObTabletID> tablet_ids_;
};

struct ObBackupBatchArg //TODO(xiuming): delete it after ob_rebalance_task not use it anymore
{
  OB_UNIS_VERSION(1);
public:
  ObBackupBatchArg() :  timeout_ts_(0), task_id_() {}
public:
  bool is_valid() const;
  TO_STRING_KV(K_(timeout_ts), K_(task_id));
public:
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
};

enum ObReplicaMovingType : int8_t
{
  REPLICA_MOVING_TYPE_INVALID = 0,
  REPLICA_MOVING_TYPE_ADD_REPLICA,
  REPLICA_MOVING_TYPE_DROP_REPLICA,
  REPLICA_MOVING_TYPE_MAX
};

//----Structs for managing privileges----

struct ObSwitchLeaderArg
{
  OB_UNIS_VERSION(1);

public:
  ObSwitchLeaderArg() : ls_id_(-1), role_(-1), tenant_id_(OB_INVALID_TENANT_ID) {}
  ObSwitchLeaderArg(const int64_t ls_id,
                    const int64_t role,
                    const int64_t tenant_id,
                    const common::ObAddr &dest_server):
                    ls_id_(ls_id),
                    role_(role),
                    tenant_id_(tenant_id),
                    dest_server_(dest_server) {}
  ~ObSwitchLeaderArg() {}
  inline void reset();
  bool is_valid() const { return (role_ == 0 || role_ == 1) && tenant_id_ != OB_INVALID_TENANT_ID; }

  DECLARE_TO_STRING;

  int64_t ls_id_;
  int64_t role_;
  int64_t tenant_id_;
  common::ObAddr dest_server_;
};

struct ObSwitchSchemaArg
{
  OB_UNIS_VERSION(1);
public:
  explicit ObSwitchSchemaArg()
    : schema_info_(),
      force_refresh_(false),
      is_async_(false) {}
  explicit ObSwitchSchemaArg(
       const share::schema::ObRefreshSchemaInfo &schema_info,
       const bool force_refresh,
       const bool is_async)
    : schema_info_(schema_info),
      force_refresh_(force_refresh),
      is_async_(is_async) {}
  ~ObSwitchSchemaArg() {}
  void reset();
  bool is_valid() const { return schema_info_.get_schema_version() > 0; }

  DECLARE_TO_STRING;

  share::schema::ObRefreshSchemaInfo schema_info_;
  bool force_refresh_;
  bool is_async_;
};

struct ObLSTabletPair final
{
  OB_UNIS_VERSION(1);
public:
  bool is_valid() const { return ls_id_.is_valid() && tablet_id_.is_valid(); }
  uint64_t hash() const { return ls_id_.hash() + tablet_id_.hash(); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator == (const ObLSTabletPair &other) const { return ls_id_ == other.ls_id_ && tablet_id_ == other.tablet_id_; }
  bool operator < (const ObLSTabletPair &other) const { return ls_id_ != other.ls_id_ ? ls_id_ < other.ls_id_ : tablet_id_ < other.tablet_id_; }
  TO_STRING_KV(K_(ls_id), K_(tablet_id));
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};

struct ObCheckSchemaVersionElapsedArg final
{
  OB_UNIS_VERSION(1);
public:
  ObCheckSchemaVersionElapsedArg()
    : tenant_id_(), schema_version_(0), need_wait_trans_end_(true), ddl_task_id_(0)
  {}
  bool is_valid() const;
  void reuse();
  TO_STRING_KV(K_(tenant_id), K_(schema_version), K_(need_wait_trans_end), K_(tablets), K_(ddl_task_id));

  uint64_t tenant_id_;
  int64_t schema_version_;
  bool need_wait_trans_end_;
  ObSEArray<ObLSTabletPair, 10> tablets_;
  int64_t ddl_task_id_;
};

struct ObDDLCheckTabletMergeStatusArg final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLCheckTabletMergeStatusArg()
    : tenant_id_(), ls_id_(), tablet_ids_(), snapshot_version_()
  {}
  ~ObDDLCheckTabletMergeStatusArg() = default;
  bool is_valid() const {
    return OB_INVALID_TENANT_ID != tenant_id_ &&
      common::OB_INVALID_TIMESTAMP != snapshot_version_ &&
      ls_id_.is_valid() &&
      tablet_ids_.count() > 0;
  }
  int assign(const ObDDLCheckTabletMergeStatusArg &other);
  void reset() {
    ls_id_.reset();
    tablet_ids_.reset();
  }
public:
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_ids), K_(snapshot_version));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObSArray<common::ObTabletID> tablet_ids_;
  int64_t snapshot_version_;
};

struct ObCheckModifyTimeElapsedArg final
{
  OB_UNIS_VERSION(1);
public:
  ObCheckModifyTimeElapsedArg() : tenant_id_(OB_INVALID_ID), sstable_exist_ts_(0), ddl_task_id_(0) {}
  bool is_valid() const;
  void reuse();
  TO_STRING_KV(K_(tenant_id), K_(sstable_exist_ts), K_(tablets), K_(ddl_task_id));
  uint64_t tenant_id_;
  int64_t sstable_exist_ts_;
  ObSEArray<ObLSTabletPair, 10> tablets_;
  int64_t ddl_task_id_;
};

struct ObCheckTransElapsedResult final
{
  OB_UNIS_VERSION(1);
public:
  ObCheckTransElapsedResult()
    : snapshot_(common::OB_INVALID_TIMESTAMP), pending_tx_id_(), ret_code_(OB_SUCCESS) {}
  bool is_valid() const { return snapshot_ != common::OB_INVALID_TIMESTAMP; }
  void reuse() { snapshot_ = common::OB_INVALID_TIMESTAMP; pending_tx_id_.reset(); }
  TO_STRING_KV(K_(snapshot), K_(pending_tx_id), K_(ret_code));
  int64_t snapshot_;
  transaction::ObTransID pending_tx_id_;
  int ret_code_;
};

struct ObDDLCheckTabletMergeStatusResult
{
  OB_UNIS_VERSION(1);
public:
  ObDDLCheckTabletMergeStatusResult()
    : merge_status_() {}
  void reset() { merge_status_.reset(); }
public:
  TO_STRING_KV(K_(merge_status));
  common::ObSArray<bool> merge_status_;
};

struct ObCheckSchemaVersionElapsedResult
{
  OB_UNIS_VERSION(1);
public:
  ObCheckSchemaVersionElapsedResult() {}
  bool is_valid() const;
  void reuse() { results_.reuse(); }
  TO_STRING_KV(K_(results));
  ObSEArray<ObCheckTransElapsedResult, 10> results_;
};

typedef ObCheckSchemaVersionElapsedResult ObCheckModifyTimeElapsedResult;

class CandidateStatus
{
  OB_UNIS_VERSION(1);
public:
  CandidateStatus() : candidate_status_(0) {}
  virtual ~CandidateStatus() {}
public:
  void set_in_black_list(const bool in_black_list) {
    if (in_black_list) {
      in_black_list_ = 1;
    } else {
      in_black_list_ = 0;
    }
  }
  bool get_in_black_list() const {
    bool ret_in_black_list = false;
    if (0 == in_black_list_) { // false, do nothing
    } else {
      ret_in_black_list = true;
    }
    return ret_in_black_list;
  }
  TO_STRING_KV("in_black_list", get_in_black_list());
private:
  union {
    uint64_t candidate_status_;
    struct {
      uint64_t in_black_list_ : 1; // Boolean
      uint64_t reserved_ : 63;
    };
  };
};

typedef common::ObSArray<CandidateStatus> CandidateStatusList;

//----Structs for managing privileges----

struct ObAccountArg
{
  OB_UNIS_VERSION(1);

public:
  ObAccountArg() : user_name_(), host_name_() , is_role_(false) {}
  ObAccountArg(const common::ObString &user_name, const common::ObString &host_name)
    : user_name_(user_name), host_name_(host_name), is_role_(false)  {}
  ObAccountArg(const char *user_name, const  char *host_name)
    : user_name_(user_name), host_name_(host_name), is_role_(false)  {}
  ObAccountArg(const common::ObString &user_name, const common::ObString &host_name, const bool is_role)
    : user_name_(user_name), host_name_(host_name), is_role_(is_role)  {}
  ObAccountArg(const char *user_name, const  char *host_name, const bool is_role)
    : user_name_(user_name), host_name_(host_name), is_role_(is_role)  {}
  bool is_valid() const { return !user_name_.empty(); }
  bool is_default_host_name() const { return 0 == host_name_.compare(common::OB_DEFAULT_HOST_NAME); }
  TO_STRING_KV(K_(user_name), K_(host_name), K_(is_role));

  common::ObString user_name_;
  common::ObString host_name_;
  bool is_role_;
};

struct ObSchemaReviseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum SchemaReviseType
  {
    INVALID_SCHEMA_REVISE_TYPE = 0,
    REVISE_CONSTRAINT_COLUMN_INFO = 1,
    REVISE_NOT_NULL_CONSTRAINT = 2,
    MAX_SCHEMA_REVISE_TYPE = 1000
  };
  ObSchemaReviseArg() :
    ObDDLArg(),
    type_(INVALID_SCHEMA_REVISE_TYPE),
    tenant_id_(common::OB_INVALID_ID),
    table_id_(common::OB_INVALID_ID),
    csts_array_()
  {}
  virtual ~ObSchemaReviseArg()
  {}
  bool is_valid() const;
  int assign(const ObSchemaReviseArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(type), K_(tenant_id), K_(table_id));
  SchemaReviseType type_;
  uint64_t tenant_id_;
  uint64_t table_id_;
  common::ObSArray<share::schema::ObConstraint> csts_array_;
};

struct ObCreateUserArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateUserArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), if_not_exist_(false),
                      creator_id_(common::OB_INVALID_ID), primary_zone_(), is_create_role_(false)
  {}
  virtual ~ObCreateUserArg()
  {}
  bool is_valid() const;
  int assign(const ObCreateUserArg &other);
  virtual bool contain_sensitive_data() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(user_infos));

  uint64_t tenant_id_;
  bool if_not_exist_;
  common::ObSArray<share::schema::ObUserInfo> user_infos_;
  uint64_t creator_id_;
  common::ObString primary_zone_; // only used in oracle mode
  bool is_create_role_;
};

struct ObDropUserArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObDropUserArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), is_role_(false)
  { }
  virtual ~ObDropUserArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(users), K_(hosts), K_(is_role));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> users_;
  common::ObSArray<common::ObString> hosts_;//can not use ObAccountArg for compatibility
  bool is_role_;
};

struct ObAlterRoleArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObAlterRoleArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObAlterRoleArg()
  {}
  int assign(const ObAlterRoleArg &other);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(role_name), K_(host_name), K_(pwd_enc));

  uint64_t tenant_id_;
  ObString role_name_;
  ObString host_name_;
  ObString pwd_enc_;
};

struct ObRenameUserArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObRenameUserArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID)
  { }
  virtual ~ObRenameUserArg() {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(old_users), K_(old_hosts), K_(new_users), K_(new_hosts));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> old_users_;
  common::ObSArray<common::ObString> new_users_;
  common::ObSArray<common::ObString> old_hosts_;
  common::ObSArray<common::ObString> new_hosts_;
};

struct ObSetPasswdArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObSetPasswdArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID),
  ssl_type_(share::schema::ObSSLType::SSL_TYPE_NOT_SPECIFIED),
  modify_max_connections_(false),
  max_connections_per_hour_(OB_INVALID_ID), max_user_connections_(OB_INVALID_ID)
  { }
  virtual ~ObSetPasswdArg() {}
  bool is_valid() const;
  virtual bool contain_sensitive_data() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(user), K_(host), K_(passwd), K_(ssl_type),
               K_(ssl_cipher), K_(x509_issuer), K_(x509_subject),
               K_(max_connections_per_hour), K_(max_user_connections));

  uint64_t tenant_id_;
  common::ObString user_;
  common::ObString passwd_;
  common::ObString host_;
  share::schema::ObSSLType ssl_type_;
  common::ObString ssl_cipher_;
  common::ObString x509_issuer_;
  common::ObString x509_subject_;
  bool modify_max_connections_;
  uint64_t max_connections_per_hour_;
  uint64_t max_user_connections_;
};

struct ObLockUserArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObLockUserArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), locked_(false)
  { }
  virtual ~ObLockUserArg() {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(users), K_(hosts), K_(locked));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> users_;
  common::ObSArray<common::ObString> hosts_;
  bool locked_;
};

struct ObAlterUserProfileArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObAlterUserProfileArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_TENANT_ID),
    user_name_(), host_name_(), profile_name_(), user_id_(common::OB_INVALID_TENANT_ID),
    default_role_flag_(common::OB_INVALID_TENANT_ID), role_id_array_(), user_ids_()
  { }
  virtual ~ObAlterUserProfileArg() {}
  int assign(const ObAlterUserProfileArg &other);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_name), K_(host_name), K_(profile_name));

  uint64_t tenant_id_;
  common::ObString user_name_;
  common::ObString host_name_;
  common::ObString profile_name_;
  uint64_t user_id_;
  uint64_t default_role_flag_;
  common::ObSEArray<uint64_t, 4> role_id_array_;
  common::ObSEArray<uint64_t, 4> user_ids_; //for set default role to multiple users
};

struct ObAlterUserProxyArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObAlterUserProxyArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_TENANT_ID),
    client_user_ids_(), proxy_user_ids_(),
    credential_type_(0), flags_(0), role_ids_()
  {}
  virtual ~ObAlterUserProxyArg() {}
  int assign(const ObAlterUserProxyArg &other);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(client_user_ids), K_(proxy_user_ids), K_(role_ids), K_(flags));

  uint64_t tenant_id_;
  ObSEArray<uint64_t, 4> client_user_ids_;
  ObSEArray<uint64_t, 4> proxy_user_ids_;
  uint64_t credential_type_;
  uint64_t flags_;
  bool is_grant_;
  common::ObSEArray<uint64_t, 4> role_ids_;
};

struct ObCreateDirectoryArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateDirectoryArg()
    : ObDDLArg(),
      or_replace_(false),
      user_id_(common::OB_INVALID_ID),
      schema_()
  {
  }
  virtual ~ObCreateDirectoryArg()
  {
  }

  int assign(const ObCreateDirectoryArg &other);
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != user_id_) && schema_.is_valid();
  }
  TO_STRING_KV(K_(or_replace), K_(user_id), K_(schema));

  bool or_replace_;
  uint64_t user_id_; // grant privilege
  share::schema::ObDirectorySchema schema_;
};

struct ObDropDirectoryArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropDirectoryArg()
    : ObDDLArg(),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      directory_name_()
  {
  }
  virtual ~ObDropDirectoryArg()
  {
  }

  int assign(const ObDropDirectoryArg &other);
  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_)
        && directory_name_.length() > 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(directory_name));

  uint64_t tenant_id_;
  common::ObString directory_name_;
};

struct ObGrantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObGrantArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID),
                 priv_level_(share::schema::OB_PRIV_INVALID_LEVEL),
                 priv_set_(0), users_passwd_(), hosts_(), need_create_user_(false),
                 has_create_user_priv_(false), roles_(), option_(0),
                 sys_priv_array_(), obj_priv_array_(),
                 object_type_(share::schema::ObObjectType::INVALID),
                 object_id_(common::OB_INVALID_ID), ins_col_ids_(),
                 upd_col_ids_(), ref_col_ids_(),
                 grantor_id_(common::OB_INVALID_ID), remain_roles_(), is_inner_(false),
		             sel_col_ids_(), column_names_priv_(), grantor_(), grantor_host_()
  { }
  virtual ~ObGrantArg() {}
  bool is_valid() const;
  int assign(const ObGrantArg &other);
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool contain_sensitive_data() const { return true; }

  TO_STRING_KV(K_(tenant_id), K_(priv_level), K_(db), K_(table), K_(priv_set),
               K_(users_passwd), K_(hosts), K_(need_create_user), K_(has_create_user_priv),
               K_(option), K_(object_type), K_(object_id), K_(grantor_id), K_(ins_col_ids),
               K_(upd_col_ids), K_(ref_col_ids), K_(grantor_id), K_(column_names_priv),
               K_(grantor), K_(grantor_host));

  uint64_t tenant_id_;
  share::schema::ObPrivLevel priv_level_;
  common::ObString db_;
  common::ObString table_;
  ObPrivSet priv_set_;
  common::ObSArray<common::ObString> users_passwd_;//user_name1, pwd1; user_name2, pwd2
  common::ObSArray<common::ObString> hosts_;//hostname1, hostname2, ..
  bool need_create_user_;
  bool has_create_user_priv_;
  common::ObSArray<common::ObString> roles_;
  uint64_t option_;
  share::ObRawPrivArray sys_priv_array_;
  share::ObRawObjPrivArray obj_priv_array_;
  share::schema::ObObjectType object_type_;
  uint64_t object_id_;
  common::ObSEArray<uint64_t, 4> ins_col_ids_;
  common::ObSEArray<uint64_t, 4> upd_col_ids_;
  common::ObSEArray<uint64_t, 4> ref_col_ids_;
  uint64_t grantor_id_;
  // used to save the user_name and host_name that cannot be stored in role[0] and role[1]
  // to support grant xxx to multiple user in oracle mode
  common::ObSArray<common::ObString> remain_roles_;
  bool is_inner_;
  common::ObSEArray<uint64_t, 4> sel_col_ids_;
  common::ObSEArray<std::pair<ObString, ObPrivType>, 4> column_names_priv_;
  common::ObString grantor_;
  common::ObString grantor_host_;
};

struct ObStandbyGrantArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObStandbyGrantArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), user_id_(0),
    db_(), table_(), priv_level_(share::schema::OB_PRIV_INVALID_LEVEL), priv_set_()
  {}
  virtual ~ObStandbyGrantArg() {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(priv_level), K_(priv_set),
               K_(db), K_(table), K_(priv_level));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  common::ObString table_;
  share::schema::ObPrivLevel priv_level_;
  ObPrivSet priv_set_;
  share::ObRawObjPrivArray obj_priv_array_;
};

struct ObRevokeUserArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObRevokeUserArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
                      priv_set_(0), revoke_all_(false), role_ids_()
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               "priv_set", share::schema::ObPrintPrivSet(priv_set_),
               K_(revoke_all),
               K_(role_ids));

  uint64_t tenant_id_;
  uint64_t user_id_;
  ObPrivSet priv_set_;
  bool revoke_all_;
  common::ObSArray<uint64_t> role_ids_;
};

struct ObRevokeDBArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObRevokeDBArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
                         priv_set_(0)
  { }
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               K_(db),
               "priv_set", share::schema::ObPrintPrivSet(priv_set_));

  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  ObPrivSet priv_set_;
};

struct ObRevokeTableArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObRevokeTableArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
                            priv_set_(0), grant_(true), obj_id_(common::OB_INVALID_ID),
                            obj_type_(common::OB_INVALID_ID), grantor_id_(common::OB_INVALID_ID),
                            obj_priv_array_(), revoke_all_ora_(false), sel_col_ids_(), ins_col_ids_(),
                            upd_col_ids_(), ref_col_ids_(), column_names_priv_(),
                            grantor_(), grantor_host_()
  { }

  int assign(const ObRevokeTableArg& other);
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               K_(db),
               K_(table),
               "priv_set", share::schema::ObPrintPrivSet(priv_set_),
               K_(grant),
               K_(obj_id),
               K_(obj_type),
               K_(grantor_id),
               K_(obj_priv_array),
               K_(column_names_priv),
               K_(grantor),
               K_(grantor_host));

  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString  db_;
  common::ObString table_;
  ObPrivSet priv_set_;
  bool grant_;
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t grantor_id_;
  share::ObRawObjPrivArray obj_priv_array_;
  bool revoke_all_ora_;
  common::ObSEArray<uint64_t, 4> sel_col_ids_;
  common::ObSEArray<uint64_t, 4> ins_col_ids_;
  common::ObSEArray<uint64_t, 4> upd_col_ids_;
  common::ObSEArray<uint64_t, 4> ref_col_ids_;
  common::ObSEArray<std::pair<ObString, ObPrivType>, 4> column_names_priv_;
  common::ObString grantor_;
  common::ObString grantor_host_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRevokeTableArg);
};

struct ObRevokeRoutineArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObRevokeRoutineArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID),
                            priv_set_(0), grant_(true), obj_id_(common::OB_INVALID_ID),
                            obj_type_(common::OB_INVALID_ID), grantor_id_(common::OB_INVALID_ID),
                            obj_priv_array_(), revoke_all_ora_(false), grantor_(), grantor_host_()
  { }
  bool is_valid() const;
  int assign(const ObRevokeRoutineArg &other);
  TO_STRING_KV(K_(tenant_id),
               K_(user_id),
               K_(db),
               K_(routine),
               "priv_set", share::schema::ObPrintPrivSet(priv_set_),
               K_(grant),
               K_(obj_id),
               K_(obj_type),
               K_(grantor_id),
               K_(obj_priv_array),
               K_(grantor),
               K_(grantor_host));

  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  common::ObString routine_;
  ObPrivSet priv_set_;
  bool grant_;
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t grantor_id_;
  share::ObRawObjPrivArray obj_priv_array_;
  bool revoke_all_ora_;
  common::ObString grantor_;
  common::ObString grantor_host_;
};

struct ObRevokeSysPrivArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObRevokeSysPrivArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID),
                      grantee_id_(common::OB_INVALID_ID),
                      sys_priv_array_(), role_ids_()
  { }
  virtual ~ObRevokeSysPrivArg() {}
  int assign(const ObRevokeSysPrivArg &other);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id),
               K_(grantee_id),
               K_(sys_priv_array),
               K_(role_ids));

  uint64_t tenant_id_;
  uint64_t grantee_id_;
  share::ObRawPrivArray sys_priv_array_;
  common::ObSArray<uint64_t> role_ids_;
};

struct ObCreateRoleArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);

public:
  ObCreateRoleArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObCreateRoleArg(){}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_infos));

  uint64_t tenant_id_;
  // role and user share the same user schema structure
  common::ObSArray<share::schema::ObUserInfo> user_infos_;
};

//----End of structs for managing privileges----

// system admin (alter system ...) rpc argument define
struct ObAdminServerArg
{
  OB_UNIS_VERSION(1);
public:
 enum AdminServerOp
  {
    INVALID_OP = 0,
    ADD = 1,
    DELETE = 2,
    CANCEL_DELETE = 3,
    START = 4,
    STOP = 5,
    FORCE_STOP = 6,
    ISOLATE = 7,
  };

  ObAdminServerArg() : servers_(), zone_(), force_stop_(false), op_(INVALID_OP) {}
  ~ObAdminServerArg() {}
  // zone can be empty, so don't check it
  bool is_valid() const { return servers_.count() > 0; }
  TO_STRING_KV(K_(servers), K_(zone), K_(force_stop), K_(op));

  ObServerList servers_;
  common::ObZone zone_;
  bool force_stop_;
  AdminServerOp op_;
};

struct ObAdminZoneArg
{
  OB_UNIS_VERSION(1);
public:
  enum AdminZoneOp
  {
    ADD = 1,
    DELETE = 2,
    START = 3,
    STOP = 4,
    MODIFY = 5,
    FORCE_STOP = 6,
    ISOLATE= 7,
  };
  enum ALTER_ZONE_OPTION
  {
    ALTER_ZONE_REGION = 0,
    ALTER_ZONE_IDC = 1,
    ALTER_ZONE_TYPE = 2,
    ALTER_ZONE_MAX = 128,
  };
public:
  ObAdminZoneArg() : zone_(),
                     region_(),
                     idc_(),
                     zone_type_(common::ObZoneType::ZONE_TYPE_INVALID),
                     force_stop_(false),
                     op_(ADD) {}
  ~ObAdminZoneArg() {}

  bool is_valid() const { return !zone_.is_empty(); }
  TO_STRING_KV(K_(zone),
               K_(region),
               K_(idc),
               K_(zone_type),
               K_(sql_stmt_str),
               K_(force_stop),
               K_(op));

  common::ObZone zone_;
  common::ObRegion region_;
  common::ObIDC idc_;
  common::ObZoneType zone_type_;
  common::ObString sql_stmt_str_;

  common::ObBitSet<ALTER_ZONE_MAX> alter_zone_options_;
  bool force_stop_;
  AdminZoneOp op_;
};

struct ObAdminSwitchReplicaRoleArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminSwitchReplicaRoleArg()
      : role_(common::FOLLOWER), ls_id_(-1), server_(), zone_(), tenant_name_() {}
  ~ObAdminSwitchReplicaRoleArg() {}

  bool is_valid() const;
  TO_STRING_KV(K_(role), K_(ls_id), K_(server), K_(zone), K_(tenant_name));

  common::ObRole role_;
  int64_t ls_id_;
  common::ObAddr server_;
  common::ObZone zone_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
};

struct ObAdminSwitchRSRoleArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminSwitchRSRoleArg()
      : role_(common::FOLLOWER), server_(), zone_() {}
  ~ObAdminSwitchRSRoleArg() {}

  bool is_valid() const;
  TO_STRING_KV(K_(role), K_(server), K_(zone));

  common::ObRole role_;
  common::ObAddr server_;
  common::ObZone zone_;
};

// TODO: @wanhong.wwh to implement change replica stmt
struct ObAdminChangeReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminChangeReplicaArg() : force_cmd_(false) {}
  ~ObAdminChangeReplicaArg() {}

  bool is_valid() const { return true; }
  TO_STRING_KV(K_(force_cmd));
  bool force_cmd_;
};

struct ObAdminDropReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminDropReplicaArg()
      : force_cmd_(false) {}
  ~ObAdminDropReplicaArg() {}

  bool is_valid() const {return true; }
  TO_STRING_KV(K_(force_cmd));
  bool force_cmd_;
};

struct ObAdminAddDiskArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminAddDiskArg()
      : diskgroup_name_(), disk_path_(), alias_name_(), server_(), zone_() {}
  ~ObAdminAddDiskArg() {}

  bool is_valid() const;
  TO_STRING_KV(K_(diskgroup_name), K_(disk_path), K_(alias_name), K_(server), K_(zone));

  common::ObString diskgroup_name_;
  common::ObString disk_path_;
  common::ObString alias_name_;
  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminDropDiskArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminDropDiskArg()
      : diskgroup_name_(), alias_name_(), server_(), zone_() {}
  ~ObAdminDropDiskArg() {}

  TO_STRING_KV(K_(diskgroup_name), K_(alias_name), K_(server), K_(zone));

  common::ObString diskgroup_name_;
  common::ObString alias_name_;
  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminMigrateReplicaArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminMigrateReplicaArg()
      : force_cmd_(false)
  {}
  ~ObAdminMigrateReplicaArg() {}

  bool is_valid() const { return true; }
  TO_STRING_KV(K_(force_cmd));
  bool force_cmd_;
};

struct ObPhysicalRestoreTenantArg : public ObCmdArg
{
  OB_UNIS_VERSION(1);

public:
  ObPhysicalRestoreTenantArg();
  virtual ~ObPhysicalRestoreTenantArg() {}
  bool is_valid() const;
  int assign(const ObPhysicalRestoreTenantArg &other);
  int add_table_item(const ObTableItem &item);
  TO_STRING_KV(K_(tenant_name),
               K_(uri),
               K_(restore_option),
               K_(restore_scn),
               K_(passwd_array),
               K_(kms_info),
               K_(table_items),
               K_(with_restore_scn),
               K_(encrypt_key),
               K_(kms_uri),
               K_(kms_encrypt_key),
               K_(restore_timestamp),
               K_(initiator_job_id),
               K_(initiator_tenant_id));

  common::ObString tenant_name_;
  common::ObString uri_;
  common::ObString restore_option_;
  share::SCN restore_scn_;
  common::ObString kms_info_;  //Encryption use
  common::ObString passwd_array_; // Password verification
  common::ObSArray<ObTableItem> table_items_;
  common::ObString multi_uri_; // 备份拆分用
  common::ObString description_;
  bool with_restore_scn_;
  common::ObString encrypt_key_;
  common::ObString kms_uri_;
  common::ObString kms_encrypt_key_;
  common::ObString restore_timestamp_;
  uint64_t initiator_job_id_;
  uint64_t initiator_tenant_id_;
};

struct ObServerZoneArg
{
  OB_UNIS_VERSION(1);

public:
  ObServerZoneArg() : server_(), zone_() {}

  // server can be invalid, zone can be empty
  virtual bool is_valid() const { return true; }
  VIRTUAL_TO_STRING_KV(K_(server), K_(zone));

  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminReportReplicaArg : public ObServerZoneArg
{
};

struct ObAdminRecycleReplicaArg : public ObServerZoneArg
{
};

struct ObAdminRefreshSchemaArg : public ObServerZoneArg
{
};

struct ObAdminRefreshMemStatArg : public ObServerZoneArg
{
};

struct ObAdminWashMemFragmentationArg : public ObServerZoneArg
{
};

struct ObRefreshIOCalibrationArg
{
  OB_UNIS_VERSION(1);
public:
  ObRefreshIOCalibrationArg()
    : storage_name_(), only_refresh_(false), calibration_list_() {}
  ~ObRefreshIOCalibrationArg() {}
  bool is_valid() const;
  int assign(const ObRefreshIOCalibrationArg &other);
  TO_STRING_KV(K_(storage_name), K_(only_refresh), K_(calibration_list));
public:
  common::ObString storage_name_;
  bool only_refresh_;
  common::ObSArray<common::ObIOBenchResult> calibration_list_;
};

struct ObAdminRefreshIOCalibrationArg : public ObServerZoneArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminRefreshIOCalibrationArg()
    : storage_name_(), only_refresh_(false), calibration_list_() {}
  virtual ~ObAdminRefreshIOCalibrationArg() {}
  virtual bool is_valid() const;
  int assign(const ObAdminRefreshIOCalibrationArg &other);
  INHERIT_TO_STRING_KV("server_zone_arg", ObServerZoneArg,
      K_(storage_name), K_(only_refresh), K_(calibration_list));
public:
  common::ObString storage_name_;
  bool only_refresh_;
  common::ObSArray<common::ObIOBenchResult> calibration_list_;
};

struct ObAdminClearLocationCacheArg : public ObServerZoneArg
{
};

struct ObRunJobArg : public ObServerZoneArg
{
  OB_UNIS_VERSION(1);

public:
  ObRunJobArg() : ObServerZoneArg(), job_() {}

  bool is_valid() const { return ObServerZoneArg::is_valid() && !job_.empty(); }
  TO_STRING_KV(K_(server), K_(zone), K_(job));

  common::ObString job_;
};

struct ObUpgradeJobArg
{
  OB_UNIS_VERSION(1);
public:
  enum Action {
    INVALID_ACTION,
    UPGRADE_POST_ACTION,
    STOP_UPGRADE_JOB,
    UPGRADE_SYSTEM_VARIABLE,
    UPGRADE_SYSTEM_TABLE,
    UPGRADE_BEGIN,
    UPGRADE_VIRTUAL_SCHEMA,
    UPGRADE_SYSTEM_PACKAGE,
    UPGRADE_ALL_POST_ACTION,
    UPGRADE_INSPECTION,
    UPGRADE_END,
    UPGRADE_ALL
  };
public:
  ObUpgradeJobArg();
  bool is_valid() const;
  int assign(const ObUpgradeJobArg &other);
  TO_STRING_KV(K_(action), K_(version), K_(tenant_ids));
public:
  Action action_;
  int64_t version_;
  ObSArray<uint64_t> tenant_ids_; // empty means all tenant
};

struct ObUpgradeTableSchemaArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObUpgradeTableSchemaArg()
    : ObDDLArg(),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      table_id_(common::OB_INVALID_ID),
      upgrade_virtual_schema_(false) {}
  ~ObUpgradeTableSchemaArg() {}
  int init(const uint64_t tenant_id,
           const uint64_t table_id,
           const bool upgrade_virtual_schema);
  bool is_valid() const;
  int assign(const ObUpgradeTableSchemaArg &other);
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_table_id() const { return table_id_; }
  bool upgrade_virtual_schema() const { return upgrade_virtual_schema_; }
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(upgrade_virtual_schema));
private:
  uint64_t tenant_id_;
  // 1. upgrade_virtual_schema_ = false, table_id_ should be sys table(except sys index/lob table).
  // 2. upgrade_virtual_schema_ = true, table_id_ is invalid.
  uint64_t table_id_;
  bool upgrade_virtual_schema_;
};

struct ObAdminMergeArg
{
  OB_UNIS_VERSION(1);

public:
  enum Type {
    START_MERGE = 1,
    SUSPEND_MERGE = 2,
    RESUME_MERGE = 3,
  };

  ObAdminMergeArg()
    : type_(START_MERGE), affect_all_(false), tenant_ids_(), affect_all_user_(false),
      affect_all_meta_(false) {}
  bool is_valid() const;
  TO_STRING_KV(K_(type), K_(affect_all), K_(tenant_ids), K_(affect_all_user), K_(affect_all_meta));

  int assign(const ObAdminMergeArg &other);

  Type type_;
  bool affect_all_; // the reason affect_all_ is not removed is for RPC compatibility
  common::ObSArray<uint64_t> tenant_ids_;
  bool affect_all_user_;
  bool affect_all_meta_;
};

class ObAdminRecoveryArg
{
  OB_UNIS_VERSION(1);
public:
  enum Type {
    SUSPEND_RECOVERY = 1,
    RESUME_RECOVERY = 2,
  };

  ObAdminRecoveryArg(): type_(SUSPEND_RECOVERY), zone_() {}
  bool is_valid() const;
  TO_STRING_KV(K_(type), K_(zone));

  int assign(const ObAdminMergeArg &other);

  Type type_;
  common::ObZone zone_;
};

struct ObAdminClearRoottableArg
{
  OB_UNIS_VERSION(1);

public:
  ObAdminClearRoottableArg() : tenant_name_() {}

  // tenant_name be empty means all tenant
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(tenant_name));

  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
};

struct ObAdminSetConfigItem
{
  OB_UNIS_VERSION(1);
public:
  ObAdminSetConfigItem() : name_(), value_(), comment_(), zone_(), server_(), tenant_name_(),
                           exec_tenant_id_(common::OB_SYS_TENANT_ID), tenant_ids_(),
                           want_to_set_tenant_config_(false) {}
  TO_STRING_KV(K_(name), K_(value), K_(comment), K_(zone), K_(server), K_(tenant_name),
               K_(exec_tenant_id), K_(tenant_ids), K_(want_to_set_tenant_config));

  common::ObFixedLengthString<common::OB_MAX_CONFIG_NAME_LEN> name_;
  common::ObFixedLengthString<common::OB_MAX_CONFIG_VALUE_LEN> value_;
  common::ObFixedLengthString<common::OB_MAX_CONFIG_INFO_LEN> comment_;
  common::ObZone zone_;
  common::ObAddr server_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
  uint64_t exec_tenant_id_;
  common::ObSArray<uint64_t> tenant_ids_;
  bool want_to_set_tenant_config_;
};

struct ObAdminSetConfigArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminSetConfigArg() : items_(), is_inner_(false), is_backup_config_(false) {}
  ~ObAdminSetConfigArg() {}

  bool is_valid() const { return items_.count() > 0; }

  int assign(const ObAdminSetConfigArg &arg);

  TO_STRING_KV(K_(items), K_(is_inner));

  common::ObSArray<ObAdminSetConfigItem> items_;
  bool is_inner_;
  bool is_backup_config_;
};

struct ObAdminFlushCacheArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminFlushCacheArg() :
    cache_type_(CACHE_TYPE_INVALID),
    is_fine_grained_(false),
    ns_type_(sql::ObLibCacheNameSpace::NS_INVALID),
    schema_id_(common::OB_INVALID_ID)
  {
  }
  virtual ~ObAdminFlushCacheArg() {}
  bool is_valid() const
  {
    return cache_type_ > CACHE_TYPE_INVALID && cache_type_ < CACHE_TYPE_MAX;
  }
  int push_tenant(uint64_t tenant_id) { return tenant_ids_.push_back(tenant_id); }
  int push_database(uint64_t db_id) { return db_ids_.push_back(db_id); }
  int assign(const ObAdminFlushCacheArg &other);
  TO_STRING_KV(K_(tenant_ids), K_(cache_type), K_(db_ids), K_(sql_id), K_(is_fine_grained), K_(ns_type));

  common::ObSEArray<uint64_t, 8> tenant_ids_;
  ObCacheType cache_type_;
  common::ObSEArray<uint64_t, 8> db_ids_;
  common::ObString sql_id_;
  bool is_fine_grained_;
  sql::ObLibCacheNameSpace ns_type_;
  uint64_t schema_id_;
};

struct ObAdminMigrateUnitArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminMigrateUnitArg():
    unit_id_(0), is_cancel_(false), destination_()
  {}
  ~ObAdminMigrateUnitArg() {}

  bool is_valid() const { return common::OB_INVALID_ID != unit_id_ && (destination_.is_valid() || is_cancel_); }
  TO_STRING_KV(K_(unit_id), K_(is_cancel), K_(destination));

  uint64_t unit_id_;
  bool is_cancel_;
  common::ObAddr destination_;
};

#ifdef OB_BUILD_ARBITRATION
struct ObAdminAddArbitrationServiceArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminAddArbitrationServiceArg():
    arbitration_service_()
  {}
  ~ObAdminAddArbitrationServiceArg() {}
  int init(const ObString &arbitration_service);
  int assign(const ObAdminAddArbitrationServiceArg &other) { return arbitration_service_.assign(other.arbitration_service_); }
  bool is_valid() const { return !arbitration_service_.is_empty(); }
  const ObString get_arbitration_service() const { return arbitration_service_.str(); }
  TO_STRING_KV(K(arbitration_service_));
private:
  common::ObFixedLengthString<OB_MAX_ARBITRATION_SERVICE_LENGTH + 1> arbitration_service_;
};

struct ObAdminRemoveArbitrationServiceArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminRemoveArbitrationServiceArg():
    arbitration_service_()
  {}
  ~ObAdminRemoveArbitrationServiceArg() {}
  int init(const ObString &arbitration_service);
  int assign(const ObAdminRemoveArbitrationServiceArg &other) { return arbitration_service_.assign(other.arbitration_service_); }
  const ObString get_arbitration_service() const { return arbitration_service_.str(); }
  bool is_valid() const { return !arbitration_service_.is_empty(); }
  TO_STRING_KV(K(arbitration_service_));
private:
  common::ObFixedLengthString<OB_MAX_ARBITRATION_SERVICE_LENGTH + 1> arbitration_service_;
};

struct ObAdminReplaceArbitrationServiceArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminReplaceArbitrationServiceArg():
    arbitration_service_(),
    previous_arbitration_service_()
  {}
  ~ObAdminReplaceArbitrationServiceArg() {}
  int init(const ObString &arbitration_service, const ObString &previous_arbitration_service);
  int assign(const ObAdminReplaceArbitrationServiceArg &other);
  bool is_valid() const { return !arbitration_service_.is_empty() && !previous_arbitration_service_.is_empty(); }
  const ObString get_arbitration_service() const { return arbitration_service_.str(); }
  const ObString get_previous_arbitration_service() const { return previous_arbitration_service_.str(); }
  TO_STRING_KV(K(arbitration_service_), K(previous_arbitration_service_));
private:
  common::ObFixedLengthString<OB_MAX_ARBITRATION_SERVICE_LENGTH + 1> arbitration_service_;
  common::ObFixedLengthString<OB_MAX_ARBITRATION_SERVICE_LENGTH + 1> previous_arbitration_service_;
};

struct ObRemoveClusterInfoFromArbServerArg
{
  OB_UNIS_VERSION(1);
public:
  ObRemoveClusterInfoFromArbServerArg():
    arbitration_service_()
  {}
  ~ObRemoveClusterInfoFromArbServerArg() {}
  int init(const ObString &arbitration_service);
  int assign(const ObRemoveClusterInfoFromArbServerArg &other);
  bool is_valid() const { return !arbitration_service_.is_empty(); }
  const ObString get_arbitration_service() const { return arbitration_service_.str(); }
  TO_STRING_KV(K(arbitration_service_));
private:
  common::ObFixedLengthString<OB_MAX_ARBITRATION_SERVICE_LENGTH + 1> arbitration_service_;
};
#endif

struct ObCheckpointSlogArg
{
  OB_UNIS_VERSION(1);
public:
  ObCheckpointSlogArg(): tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  ~ObCheckpointSlogArg() {}

  bool is_valid() const { return common::OB_INVALID_TENANT_ID != tenant_id_; }
  TO_STRING_KV(K_(tenant_id));

  uint64_t tenant_id_;
};


struct ObAutoincSyncArg
{
  OB_UNIS_VERSION(1);

public:
  ObAutoincSyncArg()
    : tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      column_id_(common::OB_INVALID_ID),
      sync_value_(0),
      table_part_num_(0),
      auto_increment_(0)
  {}
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(column_id), K_(sync_value), K_(table_part_num), K_(auto_increment));

  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t column_id_;
  uint64_t sync_value_;
  //TODO may need add first_part_num. As table_part_num not see usefull.
  //Not add first_part_num now.
  uint64_t table_part_num_;
  uint64_t auto_increment_;  // only for sync table option auto_increment
};

struct ObDumpMemtableArg
{
  OB_UNIS_VERSION(1);
public:
  ObDumpMemtableArg() : tenant_id_(), ls_id_(), tablet_id_() {}
  bool is_valid() const
  {
    return ls_id_ >= 0
      && OB_INVALID_TENANT_ID != tenant_id_
      && tablet_id_.is_valid();
  }

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id));

  uint64_t tenant_id_;
  int64_t ls_id_;
  common::ObTabletID tablet_id_;
};

struct ObDumpTxDataMemtableArg
{
  OB_UNIS_VERSION(1);
public:
  ObDumpTxDataMemtableArg() : tenant_id_(), ls_id_() {}
  bool is_valid() const
  {
    return ls_id_ >= 0
      && OB_INVALID_TENANT_ID != tenant_id_;
  }

  TO_STRING_KV(K_(tenant_id), K_(ls_id));

  uint64_t tenant_id_;
  int64_t ls_id_;
};

struct ObDumpSingleTxDataArg
{
  OB_UNIS_VERSION(1);
public:
  ObDumpSingleTxDataArg() : tenant_id_(0), ls_id_(0), tx_id_(0) {}
  bool is_valid() const
  {
    return ls_id_ >= 0 && OB_INVALID_TENANT_ID != tenant_id_ && tx_id_ > 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tx_id));

  uint64_t tenant_id_;
  int64_t ls_id_;
  int64_t tx_id_;
};

struct ObUpdateIndexStatusArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObUpdateIndexStatusArg():
    ObDDLArg(),
    index_table_id_(common::OB_INVALID_ID),
    status_(share::schema::INDEX_STATUS_MAX),
    convert_status_(true),
    in_offline_ddl_white_list_(false),
    data_table_id_(common::OB_INVALID_ID),
    database_name_(),
    task_id_(0),
    error_code_(OB_SUCCESS)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual bool is_in_offline_ddl_white_list() const { return in_offline_ddl_white_list_; }
  int assign(const ObUpdateIndexStatusArg &other_arg);
  TO_STRING_KV(K_(index_table_id), K_(status), K_(convert_status), K_(in_offline_ddl_white_list), K_(task_id), K_(error_code), K_(data_table_id), K_(database_name));

  uint64_t index_table_id_;
  share::schema::ObIndexStatus status_;
  bool convert_status_;
  bool in_offline_ddl_white_list_;
  uint64_t data_table_id_;
  ObString database_name_;
  int64_t task_id_;
  int error_code_;
};

struct ObUpdateMViewStatusArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObUpdateMViewStatusArg():
    ObDDLArg(),
    mview_table_id_(common::OB_INVALID_ID),
    mv_available_flag_(ObMVAvailableFlag::IS_MV_UNAVAILABLE),
    convert_status_(true),
    in_offline_ddl_white_list_(false)
  {}
  bool is_valid() const;
  int assign(const ObUpdateMViewStatusArg &other);
  virtual bool is_allow_when_disable_ddl() const { return false; };
  virtual bool is_allow_when_upgrade() const { return true; }
  virtual bool is_in_offline_ddl_white_list() const { return in_offline_ddl_white_list_; }
  TO_STRING_KV(K_(mview_table_id), K_(mv_available_flag), K_(convert_status), K_(in_offline_ddl_white_list));

public:
  uint64_t mview_table_id_;
  enum ObMVAvailableFlag mv_available_flag_;
  bool convert_status_;
  bool in_offline_ddl_white_list_;
};

struct ObMergeFinishArg
{
  OB_UNIS_VERSION(1);
public:
  ObMergeFinishArg():
    frozen_version_(0)
  {}

  bool is_valid() const { return server_.is_valid() && frozen_version_ > 0; }
  TO_STRING_KV(K_(server), K_(frozen_version));

  common::ObAddr server_;
  int64_t frozen_version_;
};

struct ObDebugSyncActionArg
{
  OB_UNIS_VERSION(1);
public:
  ObDebugSyncActionArg():
    reset_(false),
    clear_(false)
  {}

  bool is_valid() const { return reset_ || clear_ || action_.is_valid(); }
  TO_STRING_KV(K_(reset), K_(clear), K_(action));

  bool reset_;
  bool clear_;
  common::ObDebugSyncAction action_;
};

struct ObRootMajorFreezeArg
{
  OB_UNIS_VERSION(1);
public:
  ObRootMajorFreezeArg()
    : try_frozen_versions_(),
      svr_(),
      tenant_ids_(),
      freeze_all_(false)
  {}
  inline void reset();
  inline bool is_valid() const { return try_frozen_versions_.size() >= 0; }

  TO_STRING_KV(K_(try_frozen_versions),
               K(svr_),
               K(tenant_ids_),
               K_(freeze_all));

  int assign(const ObRootMajorFreezeArg &other);

  common::ObSArray<int64_t> try_frozen_versions_;
  common::ObAddr svr_;
  common::ObSArray<uint64_t> tenant_ids_;
  bool freeze_all_;
};

struct ObMinorFreezeArg
{
  OB_UNIS_VERSION(1);
public:
  ObMinorFreezeArg() {}
  int assign(const ObMinorFreezeArg &other);
  void reset()
  {
    tenant_ids_.reset();
    tablet_id_.reset();
    ls_id_.reset();
  }

  bool is_valid() const
  {
    return true;
  }

  TO_STRING_KV(K_(tenant_ids), K_(ls_id), K_(tablet_id));

  common::ObSArray<uint64_t> tenant_ids_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
};

struct ObRootMinorFreezeArg
{
  OB_UNIS_VERSION(2);
public:
  ObRootMinorFreezeArg()
  {}
  int assign(const ObRootMinorFreezeArg &other);
  void reset()
  {
    tenant_ids_.reset();
    server_list_.reset();
    zone_.reset();
    tablet_id_.reset();
    ls_id_.reset();
  }

  bool is_valid() const
  {
    return true;
  }

  TO_STRING_KV(K_(tenant_ids), K_(server_list), K_(zone), K_(tablet_id), K_(ls_id));

  common::ObSArray<uint64_t> tenant_ids_;
  common::ObSArray<common::ObAddr> server_list_;
  common::ObZone zone_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
};

struct ObTabletMajorFreezeArg
{
  OB_UNIS_VERSION(1);
public:
  ObTabletMajorFreezeArg()
    : tenant_id_(0),
      ls_id_(),
      tablet_id_(),
      is_rebuild_column_group_(false)
    {}
  ~ObTabletMajorFreezeArg() = default;
  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_) && ls_id_.is_valid() && tablet_id_.is_valid();
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(is_rebuild_column_group));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  bool is_rebuild_column_group_;
};

struct ObSyncPGPartitionMTFinishArg
{
  OB_UNIS_VERSION(1);
public:
  ObSyncPGPartitionMTFinishArg() : server_(), version_(0) {}

  inline bool is_valid() const { return server_.is_valid() && version_ > 0; }
  TO_STRING_KV(K_(server), K_(version));

  common::ObAddr server_;
  int64_t version_;
};

struct ObCheckDanglingReplicaFinishArg
{
  OB_UNIS_VERSION(1);
public:
  ObCheckDanglingReplicaFinishArg() : server_(), version_(0), dangling_count_(common::OB_INVALID_ID) {}

  inline bool is_valid() const { return server_.is_valid() && version_ > 0; }
  TO_STRING_KV(K_(server), K_(version), K_(dangling_count));

  common::ObAddr server_;
  int64_t version_;
  int64_t dangling_count_;
};

struct ObGetMemberListAndLeaderResult final
{
  OB_UNIS_VERSION(1);
public:
  ObGetMemberListAndLeaderResult()
    : member_list_(),
    leader_(),
    self_(),
    lower_list_(),
    replica_type_(common::REPLICA_TYPE_MAX),
    property_() {}
  void reset();
  inline bool is_valid() const {
    return member_list_.count() > 0
      && self_.is_valid()
      && common::REPLICA_TYPE_MAX != replica_type_
      && property_.is_valid();
  }

  int assign(const ObGetMemberListAndLeaderResult &other);
  TO_STRING_KV(K_(member_list), K_(leader), K_(self), K_(lower_list), K_(replica_type), K_(property));

  common::ObSEArray<common::ObMember, common::OB_MAX_MEMBER_NUMBER,
      common::ObNullAllocator, false> member_list_; // copy won't fail
  common::ObAddr leader_;
  common::ObAddr self_;
  common::ObSEArray<common::ObReplicaMember, common::OB_MAX_CHILD_MEMBER_NUMBER> lower_list_; //Cascaded downstream information
  common::ObReplicaType replica_type_; //The type of copy actually stored in the local copy
  common::ObReplicaProperty property_;
};

struct ObMemberListAndLeaderArg
{
  OB_UNIS_VERSION(1);
public:
  ObMemberListAndLeaderArg()
    : member_list_(),
      leader_(),
      self_(),
      lower_list_(),
      replica_type_(common::REPLICA_TYPE_MAX),
      property_(),
      role_(common::INVALID_ROLE) {}
  void reset();
  bool is_valid() const;
  bool check_leader_is_valid() const;
  int assign(const ObMemberListAndLeaderArg &other);
  TO_STRING_KV(K_(member_list), K_(leader), K_(self), K_(lower_list),
               K_(replica_type), K_(property), K_(role));

  common::ObSArray<common::ObAddr> member_list_; // copy won't fail
  common::ObAddr leader_;
  common::ObAddr self_;
  common::ObSArray<common::ObReplicaMember> lower_list_; //Cascaded downstream information
  common::ObReplicaType replica_type_; //The type of copy actually stored in the local copy
  common::ObReplicaProperty property_;
  common::ObRole role_;
};

struct ObBatchGetRoleResult
{
  OB_UNIS_VERSION(1);
public:
  ObBatchGetRoleResult() : results_() {}
  virtual ~ObBatchGetRoleResult() {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(results));
  common::ObSArray<int> results_;
};
struct ObGetPartitionCountResult
{
  OB_UNIS_VERSION(1);

public:
  ObGetPartitionCountResult() : partition_count_(0) {}
  void reset() { partition_count_ = 0; }
  TO_STRING_KV(K_(partition_count));

  int64_t partition_count_;
};

inline void Int64::reset()
{
  v_ = common::OB_INVALID_ID;
}

inline void ObSwitchLeaderArg::reset()
{
  ls_id_ = -1;
  role_ = -1;
  tenant_id_ = OB_INVALID_TENANT_ID;
  dest_server_.reset();
}

inline void ObRootMajorFreezeArg::reset()
{
  try_frozen_versions_.reset();
  freeze_all_ = false;
}

struct ObCreateUserDefinedFunctionArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateUserDefinedFunctionArg(): ObDDLArg(), udf_() {}
  virtual ~ObCreateUserDefinedFunctionArg() {}
  bool is_valid() const {
    return !udf_.get_name_str().empty();
  }
  TO_STRING_KV(K_(udf));

  share::schema::ObUDF udf_;
};

struct ObDropUserDefinedFunctionArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropUserDefinedFunctionArg(): tenant_id_(common::OB_INVALID_ID), name_(), if_exist_(false) {}
  virtual ~ObDropUserDefinedFunctionArg() {}
  bool is_valid() const {
    return !name_.empty();
  }
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(name));

  uint64_t tenant_id_;
  common::ObString name_;
  bool if_exist_;
};

struct ObCreateOutlineArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateOutlineArg(): ObDDLArg(), or_replace_(false), outline_info_(), db_name_() {}
  virtual ~ObCreateOutlineArg() {}
  bool is_valid() const;
  TO_STRING_KV(K_(or_replace), K_(outline_info), K_(db_name));

  bool or_replace_;
  share::schema::ObOutlineInfo outline_info_;
  common::ObString db_name_;
};

struct ObAlterOutlineArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum AlterOutlineOptions {
    ADD_OUTLINE_CONTENT = 1,
    ADD_CONCURRENT_LIMIT,
    MAX_OPTION
  };
  ObAlterOutlineArg(): ObDDLArg(), alter_outline_info_(), db_name_() {}
  virtual ~ObAlterOutlineArg() {}
  bool is_valid() const
  {
    return (!db_name_.empty() && !alter_outline_info_.get_signature_str().empty()
            && (!alter_outline_info_.get_outline_content_str().empty()
                || alter_outline_info_.has_outline_params()));
  }
  TO_STRING_KV(K_(alter_outline_info), K_(db_name));

  share::schema::ObAlterOutlineInfo alter_outline_info_;
  common::ObString db_name_;
};

struct ObDropOutlineArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropOutlineArg(): ObDDLArg(), tenant_id_(common::OB_INVALID_ID), db_name_(), outline_name_(), is_format_(false) {}
  virtual ~ObDropOutlineArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(db_name), K_(outline_name), K_(is_format));

  uint64_t tenant_id_;
  common::ObString db_name_;
  common::ObString outline_name_;
  bool is_format_;
};

struct ObCreateDbLinkArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateDbLinkArg() : ObDDLArg(), dblink_info_() {}
  virtual ~ObCreateDbLinkArg() {}
  bool is_valid() const;
  virtual bool contain_sensitive_data() const { return true; }
  TO_STRING_KV(K_(dblink_info));
  share::schema::ObDbLinkInfo dblink_info_;
};

struct ObDropDbLinkArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropDbLinkArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), dblink_name_(), if_exist_(false) {}
  virtual ~ObDropDbLinkArg() {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(dblink_name), K_(if_exist));
  uint64_t tenant_id_;
  common::ObString dblink_name_;
  bool if_exist_;
};

struct ObUseDatabaseArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObUseDatabaseArg() : ObDDLArg()
  { }
  virtual ~ObUseDatabaseArg() {}
};

struct ObFetchAliveServerArg
{
  OB_UNIS_VERSION(1);
public:
  ObFetchAliveServerArg(const int64_t cluster_id = 0)
    : cluster_id_(cluster_id) {}
  TO_STRING_KV(K_(cluster_id));
  bool is_valid() const { return cluster_id_ >= 0; }

  int64_t cluster_id_;
};

struct ObFetchAliveServerResult
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(active_server_list));
  bool is_valid() const { return !active_server_list_.empty(); }

  ObServerList active_server_list_;
  ObServerList inactive_server_list_;
};

struct ObFetchActiveServerAddrResult
{
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(server_addr_list));
  bool is_valid() const { return !server_addr_list_.empty(); }

  int assign(const ObFetchActiveServerAddrResult &other);
  common::ObSArray<share::ObAliveServerTracer::ServerAddr> server_addr_list_;
};

struct ObFlushCacheArg
{
  OB_UNIS_VERSION(1);
public:
  ObFlushCacheArg() :
    is_all_tenant_(false),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    cache_type_(CACHE_TYPE_INVALID),
    is_fine_grained_(false),
    ns_type_(sql::ObLibCacheNameSpace::NS_INVALID),
    schema_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObFlushCacheArg() {}
  bool is_valid() const
  {
    return cache_type_ > CACHE_TYPE_INVALID && cache_type_ < CACHE_TYPE_MAX;
  }
  int push_database(uint64_t db_id) { return db_ids_.push_back(db_id); }
  int assign(const ObFlushCacheArg &other);
  TO_STRING_KV(K(is_all_tenant_),
               K_(tenant_id),
               K_(cache_type),
               K_(db_ids),
               K_(sql_id),
               K_(is_fine_grained),
               K_(ns_type));

  bool is_all_tenant_;
  uint64_t tenant_id_;
  ObCacheType cache_type_;
  common::ObSEArray<uint64_t, 8> db_ids_;
  common::ObString sql_id_;
  bool is_fine_grained_;
  sql::ObLibCacheNameSpace ns_type_;
  uint64_t schema_id_;
};

struct ObGetAllSchemaArg
{
  OB_UNIS_VERSION(1);

public:
  ObGetAllSchemaArg()
    : schema_version_(common::OB_INVALID_VERSION)
  {}
  TO_STRING_KV(K_(schema_version), K_(tenant_name));

  int64_t schema_version_;
  common::ObString tenant_name_;
};

struct ObAdminSetTPArg : public ObServerZoneArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminSetTPArg() : event_no_(0),
                      occur_(0),
                      trigger_freq_(1),
                      error_code_(0),
                      cond_(0)
      {}

   inline bool is_valid() const {
     return( error_code_ <= 0
             && (trigger_freq_ >= 0)); }

   TO_STRING_KV(K_(event_no),
                K_(event_name),
                K_(occur),
                K_(trigger_freq),
                K_(error_code),
                K_(cond),
                K_(server),
                K_(zone));

   int64_t event_no_;                 // tracepoint no
   common::ObString event_name_;      // tracepoint name
   int64_t occur_;            // number of occurrences
   int64_t trigger_freq_;         // trigger frequency
   int64_t error_code_;        // error code to return
   int64_t cond_;              // condition to match
};

struct ObCreateRoutineArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateRoutineArg()
    : routine_info_(),
    db_name_(),
    is_or_replace_(false),
    is_need_alter_(false),
    error_info_(),
    dependency_infos_(),
    with_if_not_exist_(false) {}
  virtual ~ObCreateRoutineArg() {}
  bool is_valid() const;
  int assign(const ObCreateRoutineArg &other);
  TO_STRING_KV(K_(routine_info),
               K_(db_name),
               K_(is_or_replace),
               K_(is_need_alter),
               K_(error_info),
               K_(dependency_infos),
               K_(with_if_not_exist));

  share::schema::ObRoutineInfo routine_info_;
  common::ObString db_name_;
  bool is_or_replace_;
  bool is_need_alter_; // used in mysql mode
  share::schema::ObErrorInfo error_info_;
  common::ObSArray<oceanbase::share::schema::ObDependencyInfo> dependency_infos_;
  bool with_if_not_exist_;
};

struct ObDropRoutineArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropRoutineArg()
    : tenant_id_(common::OB_INVALID_ID),
      db_name_(),
      routine_name_(),
      routine_type_(share::schema::INVALID_ROUTINE_TYPE),
      if_exist_(false),
      error_info_() {}
  virtual ~ObDropRoutineArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id),
               K_(db_name),
               K_(routine_name),
               K_(routine_type),
               K_(if_exist),
               K_(error_info));

  uint64_t tenant_id_;
  common::ObString db_name_;
  common::ObString routine_name_;
  share::schema::ObRoutineType routine_type_;
  bool if_exist_;
  share::schema::ObErrorInfo error_info_;
};

struct ObCreatePackageArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreatePackageArg()
      : is_replace_(false),
        is_editionable_(false),
        db_name_(),
        package_info_(),
        error_info_() {}
  virtual ~ObCreatePackageArg() {}
  bool is_valid() const;
  int assign(const ObCreatePackageArg &other);
  TO_STRING_KV(K_(is_replace), K_(is_editionable), K_(db_name),
               K_(package_info), K_(public_routine_infos), K(error_info_),
               K_(dependency_infos));

  bool is_replace_;
  bool is_editionable_;
  common::ObString db_name_;
  share::schema::ObPackageInfo package_info_;
  common::ObSArray<share::schema::ObRoutineInfo> public_routine_infos_;
  share::schema::ObErrorInfo error_info_;
  common::ObSArray<oceanbase::share::schema::ObDependencyInfo> dependency_infos_;
};

struct ObAlterPackageArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObAlterPackageArg()
    : tenant_id_(common::OB_INVALID_ID),
      db_name_(),
      package_name_(),
      package_type_(share::schema::INVALID_PACKAGE_TYPE),
      compatible_mode_(-1),
      public_routine_infos_(),
      error_info_(),
      exec_env_(),
      dependency_infos_()
      {}
  virtual ~ObAlterPackageArg() {}
  bool is_valid() const;
  int assign(const ObAlterPackageArg &other);
  TO_STRING_KV(K_(tenant_id), K_(db_name), K_(package_name), K_(package_type),
               K_(compatible_mode), K_(public_routine_infos), K_(error_info),
               K_(exec_env), K_(dependency_infos));

  uint64_t tenant_id_;
  common::ObString db_name_;
  common::ObString package_name_;
  share::schema::ObPackageType package_type_;
  int64_t compatible_mode_;
  common::ObSArray<share::schema::ObRoutineInfo> public_routine_infos_;
  share::schema::ObErrorInfo error_info_;
  common::ObString exec_env_;
  common::ObSArray<oceanbase::share::schema::ObDependencyInfo> dependency_infos_;
};

struct ObDropPackageArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropPackageArg()
    : tenant_id_(common::OB_INVALID_ID),
      db_name_(),
      package_name_(),
      package_type_(share::schema::INVALID_PACKAGE_TYPE),
      compatible_mode_(-1),
      error_info_() {}
  virtual ~ObDropPackageArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(db_name), K_(package_name), K_(package_type), K_(compatible_mode), K_(error_info));

  uint64_t tenant_id_;
  common::ObString db_name_;
  common::ObString package_name_;
  share::schema::ObPackageType package_type_;
  int64_t compatible_mode_;
  share::schema::ObErrorInfo error_info_;
};

struct ObCreateTriggerArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateTriggerArg()
    : ObDDLArg(),
      trigger_info_(),
      flags_(0),
      error_info_()
  {}
  virtual ~ObCreateTriggerArg() {}
  bool is_valid() const;
  int assign(const ObCreateTriggerArg &other);
  TO_STRING_KV(K(trigger_database_),
               K(base_object_database_),
               K(base_object_name_),
               K(trigger_info_),
               K(with_replace_),
               K(in_second_stage_),
               K(with_if_not_exist_),
               K(error_info_),
               K(dependency_infos_));
public:
  common::ObString trigger_database_;
  common::ObString base_object_database_;
  common::ObString base_object_name_;
  share::schema::ObTriggerInfo trigger_info_;
  union
  {
    uint32_t flags_;
    struct
    {
      uint32_t with_replace_:1;
      uint32_t in_second_stage_:1; // is second create trigger stage
      uint32_t with_if_not_exist_:1;
      uint32_t reserved_:29;
    };
  };
  share::schema::ObErrorInfo error_info_;
  common::ObSArray<share::schema::ObDependencyInfo> dependency_infos_;
};

struct ObRoutineDDLRes
{
  OB_UNIS_VERSION(1);

public:
  ObRoutineDDLRes() :
    store_routine_schema_version_(OB_INVALID_VERSION)
  {}
  int assign(const ObRoutineDDLRes &other) {
    store_routine_schema_version_ = other.store_routine_schema_version_;
    return common::OB_SUCCESS;
  }
  TO_STRING_KV(K_(store_routine_schema_version));
  int64_t store_routine_schema_version_;
};

struct ObCreateTriggerRes
{
  OB_UNIS_VERSION(1);

public:
  ObCreateTriggerRes() :
    table_schema_version_(OB_INVALID_VERSION),
    trigger_schema_version_(OB_INVALID_VERSION)
  {}
  int assign(const ObCreateTriggerRes &other) {
    table_schema_version_ = other.table_schema_version_;
    trigger_schema_version_ = other.trigger_schema_version_;
    return common::OB_SUCCESS;
  }
  TO_STRING_KV(K_(table_schema_version), K_(trigger_schema_version));
  int64_t table_schema_version_;
  int64_t trigger_schema_version_;
};

struct ObDropTriggerArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropTriggerArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      trigger_database_(),
      trigger_name_(),
      if_exist_(false)
  {}
  virtual ~ObDropTriggerArg() {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K(tenant_id_),
               K(trigger_database_),
               K(trigger_name_),
               K(if_exist_));

  uint64_t tenant_id_;
  common::ObString trigger_database_;
  common::ObString trigger_name_;
  bool if_exist_;
};

struct ObAlterTriggerArg: public ObDDLArg
{
OB_UNIS_VERSION(1);
public:
  ObAlterTriggerArg()
      :
      ObDDLArg(), trigger_database_(), trigger_info_(), trigger_infos_(),
      is_set_status_(false),is_alter_compile_(false)
  {}
  virtual ~ObAlterTriggerArg()
  {}
  bool is_valid() const;
  int assign(const ObAlterTriggerArg &other);
  TO_STRING_KV(K(trigger_database_),
      K(trigger_info_),
      K(trigger_infos_),
      K(is_set_status_),
      K(is_alter_compile_))
  ;
public:
  common::ObString trigger_database_;           // 废弃
  share::schema::ObTriggerInfo trigger_info_;   // 废弃
  common::ObSArray<share::schema::ObTriggerInfo> trigger_infos_;
  bool is_set_status_;
  bool is_alter_compile_;
};

struct ObNotifyTenantSnapshotSchedulerArg
{
  OB_UNIS_VERSION(1);
public:
  ObNotifyTenantSnapshotSchedulerArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  ~ObNotifyTenantSnapshotSchedulerArg() {}
  bool is_valid() const;
  int assign(const ObNotifyTenantSnapshotSchedulerArg &other);
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  TO_STRING_KV(K_(tenant_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObNotifyTenantSnapshotSchedulerArg);
private:
  uint64_t tenant_id_;
};

struct ObNotifyTenantSnapshotSchedulerResult
{
  OB_UNIS_VERSION(1);
public:
  ObNotifyTenantSnapshotSchedulerResult(): ret_(common::OB_SUCCESS) {}
  ~ObNotifyTenantSnapshotSchedulerResult() {}
  bool is_valid() const;
  int assign(const ObNotifyTenantSnapshotSchedulerResult &other);
  void init(const int ret) { ret_ = ret; }
  TO_STRING_KV(K_(ret));
  int get_result() const { return ret_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObNotifyTenantSnapshotSchedulerResult);
private:
  int ret_;
};

struct ObFlushLSArchiveArg
{
  OB_UNIS_VERSION(1);
public:
  ObFlushLSArchiveArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  virtual ~ObFlushLSArchiveArg() {}
  bool is_valid() const;
  int assign(const ObFlushLSArchiveArg &other);
  TO_STRING_KV(K_(tenant_id));
public:
  uint64_t tenant_id_;
};

struct ObInnerCreateTenantSnapshotArg
{
  OB_UNIS_VERSION(1);
public:
  ObInnerCreateTenantSnapshotArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      tenant_snapshot_id_()
  {}
  ~ObInnerCreateTenantSnapshotArg()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    tenant_snapshot_id_.reset();
  }
  bool is_valid() const;
  int assign(const ObInnerCreateTenantSnapshotArg &other);
  const share::ObTenantSnapshotID &get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  void set_tenant_snapshot_id(const share::ObTenantSnapshotID &tenant_snapshot_id)
  {
    tenant_snapshot_id_ = tenant_snapshot_id;
  }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  TO_STRING_KV(K(tenant_id_), K(tenant_snapshot_id_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerCreateTenantSnapshotArg);
private:
  uint64_t tenant_id_;
  share::ObTenantSnapshotID tenant_snapshot_id_;
};

struct ObInnerCreateTenantSnapshotResult
{
  OB_UNIS_VERSION(1);
public:
  ObInnerCreateTenantSnapshotResult(): ret_(common::OB_SUCCESS) {}
  ~ObInnerCreateTenantSnapshotResult() {}
  bool is_valid() const;
  int assign(const ObInnerCreateTenantSnapshotResult &other);
  void init(const int ret) { ret_ = ret; }
  TO_STRING_KV(K_(ret));
  int get_result() const { return ret_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerCreateTenantSnapshotResult);
private:
  int ret_;
};

struct ObInnerDropTenantSnapshotArg
{
  OB_UNIS_VERSION(1);
public:
  ObInnerDropTenantSnapshotArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      tenant_snapshot_id_()
  {}
  ~ObInnerDropTenantSnapshotArg()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    tenant_snapshot_id_.reset();
  }
  bool is_valid() const;
  int assign(const ObInnerDropTenantSnapshotArg &other);
  const share::ObTenantSnapshotID &get_tenant_snapshot_id() const { return tenant_snapshot_id_; }
  void set_tenant_snapshot_id(const share::ObTenantSnapshotID &tenant_snapshot_id)
  {
    tenant_snapshot_id_ = tenant_snapshot_id;
  }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  TO_STRING_KV(K(tenant_id_), K(tenant_snapshot_id_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerDropTenantSnapshotArg);
private:
  uint64_t tenant_id_;
  share::ObTenantSnapshotID tenant_snapshot_id_;
};

struct ObInnerDropTenantSnapshotResult
{
  OB_UNIS_VERSION(1);
public:
  ObInnerDropTenantSnapshotResult(): ret_(common::OB_SUCCESS) {}
  ~ObInnerDropTenantSnapshotResult() {}
  bool is_valid() const;
  int assign(const ObInnerDropTenantSnapshotResult &other);
  void init(const int ret) { ret_ = ret; }
  TO_STRING_KV(K_(ret));
  int get_result() const { return ret_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObInnerDropTenantSnapshotResult);
private:
  int ret_;
};

struct ObCreateUDTArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateUDTArg()
    : udt_info_(),
      db_name_(),
      is_or_replace_(false),
      error_info_(),
      is_force_(true),
      exist_valid_udt_(false) {}
  virtual ~ObCreateUDTArg() {}
  bool is_valid() const;
  int assign(const ObCreateUDTArg &other);
  TO_STRING_KV(K_(udt_info),
               K_(db_name),
               K_(is_or_replace),
               K_(error_info),
               K_(public_routine_infos),
               K_(dependency_infos),
               K_(is_force),
               K_(exist_valid_udt));

  share::schema::ObUDTTypeInfo udt_info_;
  common::ObString db_name_;
  bool is_or_replace_;
  share::schema::ObErrorInfo error_info_;
  common::ObSArray<share::schema::ObRoutineInfo> public_routine_infos_;
  common::ObSArray<share::schema::ObDependencyInfo> dependency_infos_;
  bool is_force_;
  bool exist_valid_udt_;
};

struct ObDropUDTArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropUDTArg()
    : tenant_id_(common::OB_INVALID_ID),
      db_name_(),
      udt_name_(),
      if_exist_(false),
      is_type_body_(false),
      force_or_validate_(1), // default force for backward compatibility
      exist_valid_udt_(false) {}
  virtual ~ObDropUDTArg() {}
  bool is_valid() const;
  int assign(const ObDropUDTArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id),
               K_(db_name),
               K_(udt_name),
               K_(if_exist),
               K_(is_type_body),
               K_(force_or_validate),
               K_(exist_valid_udt));

  uint64_t tenant_id_;
  common::ObString db_name_;
  common::ObString udt_name_;
  bool if_exist_;
  bool is_type_body_;
  int64_t force_or_validate_;
  bool exist_valid_udt_;
};

struct ObCancelTaskArg : public ObServerZoneArg
{
  OB_UNIS_VERSION(2);
public:
  ObCancelTaskArg() : task_id_()
  {}
  TO_STRING_KV(K_(task_id));
  share::ObTaskId task_id_;
};

struct ObReportSingleReplicaArg
{
  OB_UNIS_VERSION(1);
public:
  ObReportSingleReplicaArg() : tenant_id_(), ls_id_() {}
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid(); }
  int assign(const ObReportSingleReplicaArg &other);

  TO_STRING_KV(K_(tenant_id), K_(ls_id));

  int64_t tenant_id_;
  share::ObLSID ls_id_;
};

struct ObSetDiskValidArg
{
  OB_UNIS_VERSION(1);
public:
  ObSetDiskValidArg() {}
  bool is_valid() const { return true; }
};

struct ObAdminClearDRTaskArg
{
  OB_UNIS_VERSION(1);
public:
  enum class TaskType : int64_t
  {
    AUTO = 0,
    MANUAL,
    ALL,
    MAX_TYPE
  };
public:
  ObAdminClearDRTaskArg() : tenant_ids_(),
                            type_(TaskType::ALL),
                            zone_names_() {}
  virtual ~ObAdminClearDRTaskArg() {}
public:
  int assign(
      const ObAdminClearDRTaskArg &that);

  TO_STRING_KV(K_(tenant_ids),
               K_(zone_names),
               K_(type));
public:
  common::ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> tenant_ids_;
  TaskType type_;
  common::ObSEArray<common::ObZone, common::OB_PREALLOCATED_NUM> zone_names_;
};

struct ObAdminClearBalanceTaskArg
{
  OB_UNIS_VERSION(1);
public:
  enum TaskType
  {
    AUTO = 0,
    MANUAL,
    ALL,
    MAX_TYPE
  };

  ObAdminClearBalanceTaskArg() : tenant_ids_(), type_(ALL), zone_names_() {}
  ~ObAdminClearBalanceTaskArg() {}

  TO_STRING_KV(K_(tenant_ids), K_(type), K_(zone_names));
  common::ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> tenant_ids_;
  TaskType type_;
  common::ObSEArray<common::ObZone, common::OB_PREALLOCATED_NUM> zone_names_;
};

class ObMCLogInfo
{
  OB_UNIS_VERSION(1);
public:
  ObMCLogInfo() : log_id_(common::OB_INVALID_ID), timestamp_(common::OB_INVALID_TIMESTAMP) {}
  ~ObMCLogInfo() {};
  bool is_valid() const { return common::OB_INVALID_ID != log_id_ && common::OB_INVALID_TIMESTAMP != timestamp_;}
public:
  uint64_t log_id_;
  int64_t timestamp_;
  TO_STRING_KV(K_(log_id), K_(timestamp));
};

struct ObForceSwitchILogFileArg
{
  OB_UNIS_VERSION(1);
public:
  ObForceSwitchILogFileArg() : force_(true) {}
  ~ObForceSwitchILogFileArg() {}

  DECLARE_TO_STRING;
  bool force_;
};

struct ObForceSetAllAsSingleReplicaArg
{
  OB_UNIS_VERSION(1);
public:
  ObForceSetAllAsSingleReplicaArg() : force_(true) {}
  ~ObForceSetAllAsSingleReplicaArg() {}

  DECLARE_TO_STRING;
  bool force_;
};

struct ObForceSetServerListArg
{
  OB_UNIS_VERSION(1);
public:
  ObForceSetServerListArg() : server_list_(), replica_num_(0) {}
  ~ObForceSetServerListArg() {}

  DECLARE_TO_STRING;
  obrpc::ObServerList server_list_;
  int64_t replica_num_;
};

struct ObForceCreateSysTableArg
{
  OB_UNIS_VERSION(1);
public:
  ObForceCreateSysTableArg() :
          tenant_id_(common::OB_INVALID_TENANT_ID),
          table_id_(common::OB_INVALID_ID),
          last_replay_log_id_(common::OB_INVALID_ID) {}
  ~ObForceCreateSysTableArg() {}
  bool is_valid() const;

  DECLARE_TO_STRING;
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t last_replay_log_id_;
};

struct ObForceSetLocalityArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:

  ObForceSetLocalityArg():
    ObDDLArg(),
    locality_()
  {}
  virtual ~ObForceSetLocalityArg() {}
  bool is_valid() const;
  int assign(const ObForceSetLocalityArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  common::ObString locality_;
  TO_STRING_KV(K_(exec_tenant_id), K_(locality));
};

struct ObSplitPartitionArg
{
  OB_UNIS_VERSION(1);
public:
  ObSplitPartitionArg(): split_info_() {}
  ~ObSplitPartitionArg() {}
  TO_STRING_KV(K_(split_info));
  bool is_valid() const { return split_info_.is_valid(); }
  void reset() { split_info_.reset(); }
public:
  share::ObSplitPartition split_info_;
};

struct ObSplitPartitionResult
{
  OB_UNIS_VERSION(1);
public:
  ObSplitPartitionResult() : results_() {}
  ~ObSplitPartitionResult() {}
  void reset() { results_.reuse(); }
  common::ObSArray<share::ObPartitionSplitProgress> &get_result() { return results_; }
  const common::ObSArray<share::ObPartitionSplitProgress> &get_result() const { return results_; }
  TO_STRING_KV(K_(results));
private:
  common::ObSArray<share::ObPartitionSplitProgress> results_;
};

struct ObSplitPartitionBatchArg
{
  OB_UNIS_VERSION(1);

public:
  ObSplitPartitionBatchArg() {}
  ~ObSplitPartitionBatchArg() {}
  bool is_valid() const;
  int assign(const ObSplitPartitionBatchArg &other);
  void reset() { return split_info_.reset(); }
  DECLARE_TO_STRING;
  share::ObSplitPartition split_info_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSplitPartitionBatchArg);
};

struct ObBootstrapArg
{
  OB_UNIS_VERSION(1);
public:
  ObBootstrapArg() : server_list_(), cluster_role_(common::PRIMARY_CLUSTER) {}
  ~ObBootstrapArg() {}
  TO_STRING_KV(K_(server_list), K_(cluster_role));
  int assign(const ObBootstrapArg &arg);
  ObServerInfoList server_list_;
  common::ObClusterRole cluster_role_;
};

struct ObForceSetLSAsSingleReplicaArg
{
  OB_UNIS_VERSION(1);
public:
  ObForceSetLSAsSingleReplicaArg(): tenant_id_(OB_INVALID_TENANT_ID), ls_id_() {}
  ~ObForceSetLSAsSingleReplicaArg() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id);
  int assign(const ObForceSetLSAsSingleReplicaArg &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id));

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObForceSetLSAsSingleReplicaArg);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

struct ObGetLSSyncScnArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetLSSyncScnArg(): tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), check_sync_to_latest_(false) {}
  ~ObGetLSSyncScnArg() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, const bool check_sync_to_latest);
  int assign(const ObGetLSSyncScnArg &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(check_sync_to_latest));

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }

  bool check_sync_to_latest() const
  {
    return check_sync_to_latest_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetLSSyncScnArg);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  bool check_sync_to_latest_;
};

struct ObGetLSSyncScnRes
{
  OB_UNIS_VERSION(1);
public:
  ObGetLSSyncScnRes(): tenant_id_(OB_INVALID_TENANT_ID),
                       ls_id_(),
                       cur_sync_scn_(share::SCN::min_scn()),
                       cur_restore_source_max_scn_(share::SCN::min_scn()) {}
  ~ObGetLSSyncScnRes() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &cur_sync_scn, const share::SCN &cur_restore_source_max_scn);
  int assign(const ObGetLSSyncScnRes &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(cur_sync_scn), K_(cur_restore_source_max_scn));
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  share::SCN get_cur_sync_scn() const
  {
    return cur_sync_scn_;
  }
  share::SCN get_cur_restore_source_max_scn() const
  {
    return cur_restore_source_max_scn_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetLSSyncScnRes);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::SCN cur_sync_scn_;
  share::SCN cur_restore_source_max_scn_;
};

struct ObRefreshTenantInfoArg
{
  OB_UNIS_VERSION(1);
public:
  ObRefreshTenantInfoArg(): tenant_id_(OB_INVALID_TENANT_ID) {}
  ~ObRefreshTenantInfoArg() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id);
  int assign(const ObRefreshTenantInfoArg &other);
  TO_STRING_KV(K_(tenant_id));

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObRefreshTenantInfoArg);
private:
  uint64_t tenant_id_;
};

struct ObRefreshTenantInfoRes
{
  OB_UNIS_VERSION(1);
public:
  ObRefreshTenantInfoRes(): tenant_id_(OB_INVALID_TENANT_ID) {}
  ~ObRefreshTenantInfoRes() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id);
  int assign(const ObRefreshTenantInfoRes &other);
  TO_STRING_KV(K_(tenant_id));
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObRefreshTenantInfoRes);
private:
  uint64_t tenant_id_;
};

struct ObUpdateTenantInfoCacheArg
{
  OB_UNIS_VERSION(1);
public:
  ObUpdateTenantInfoCacheArg(): tenant_id_(OB_INVALID_TENANT_ID), tenant_info_(), ora_rowscn_(0), finish_data_version_(0) {
    data_version_barrier_scn_.set_min();
  }
  ~ObUpdateTenantInfoCacheArg() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id, const share::ObAllTenantInfo &tenant_info,
           const int64_t ora_rowscn, const uint64_t finish_data_version,
           const share::SCN &data_version_barrier_scn);
  int assign(const ObUpdateTenantInfoCacheArg &other);
  TO_STRING_KV(K_(tenant_id), K_(tenant_info), K_(ora_rowscn), K_(finish_data_version),
               K_(data_version_barrier_scn));

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }

  const share::ObAllTenantInfo &get_tenant_info() const
  {
    return tenant_info_;
  }

  int64_t get_ora_rowscn() const
  {
    return ora_rowscn_;
  }

  uint64_t get_finish_data_version() const
  {
    return finish_data_version_;
  }

  const share::SCN &get_data_version_barrier_scn() const
  {
    return data_version_barrier_scn_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObUpdateTenantInfoCacheArg);
private:
  uint64_t tenant_id_;
  share::ObAllTenantInfo tenant_info_;
  int64_t ora_rowscn_;
  uint64_t finish_data_version_;
  share::SCN data_version_barrier_scn_;
};

struct ObUpdateTenantInfoCacheRes
{
  OB_UNIS_VERSION(1);
public:
  ObUpdateTenantInfoCacheRes(): tenant_id_(OB_INVALID_TENANT_ID) {}
  ~ObUpdateTenantInfoCacheRes() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id);
  int assign(const ObUpdateTenantInfoCacheRes &other);
  TO_STRING_KV(K_(tenant_id));
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObUpdateTenantInfoCacheRes);
private:
  uint64_t tenant_id_;
};

struct ObCheckpoint
{
  OB_UNIS_VERSION(1);
public:
  ObCheckpoint():
    ls_id_(),
    cur_sync_scn_(share::SCN::min_scn()),
    cur_restore_source_max_scn_(share::SCN::min_scn()) {}
  explicit ObCheckpoint(const int64_t id, const share::SCN &cur_sync_scn, const share::SCN &cur_restore_source_max_scn):
    ls_id_(id),
    cur_sync_scn_(cur_sync_scn),
    cur_restore_source_max_scn_(cur_restore_source_max_scn) {}

  explicit ObCheckpoint(const share::ObLSID id, const share::SCN &cur_sync_scn, const share::SCN &cur_restore_source_max_scn):
    ls_id_(id),
    cur_sync_scn_(cur_sync_scn),
    cur_restore_source_max_scn_(cur_restore_source_max_scn) {}

  bool is_valid() const;
  bool operator==(const obrpc::ObCheckpoint &r) const;
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  share::SCN get_cur_sync_scn() const
  {
    return cur_sync_scn_;
  }
  share::SCN get_cur_restore_source_max_scn() const
  {
    return cur_restore_source_max_scn_;
  }

  bool is_sync_to_latest() const
  {
    return (cur_sync_scn_ >= cur_restore_source_max_scn_
            && cur_sync_scn_.is_valid_and_not_min() && cur_restore_source_max_scn_.is_valid_and_not_min());
  }

  TO_STRING_KV(K_(ls_id), K_(cur_sync_scn), K_(cur_restore_source_max_scn));

  share::ObLSID ls_id_;
  share::SCN cur_sync_scn_;
  share::SCN cur_restore_source_max_scn_;
};

struct ObGetTenantResArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetTenantResArg() : tenant_id_(OB_INVALID_TENANT_ID) {}
  ObGetTenantResArg(const uint64_t tenant_id) : tenant_id_(tenant_id) {}
  ~ObGetTenantResArg() {}
  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_);
  }
  int assign(const ObGetTenantResArg &other)
  {
    tenant_id_ = other.tenant_id_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(tenant_id));
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetTenantResArg);
  uint64_t tenant_id_;
};
struct ObTenantLogicalRes
{
  OB_UNIS_VERSION(1);
public:
  ObTenantLogicalRes() : server_(), arg_() {}
  ~ObTenantLogicalRes() {}
  bool is_valid() const
  {
    return server_.is_valid();
  }
  int init(const ObAddr &server, const share::ObUserResourceCalculateArg &arg);
  int assign(const ObTenantLogicalRes &other);
  TO_STRING_KV(K_(server), K_(arg));
  ObAddr get_server() const
  {
    return server_;
  }
  const share::ObUserResourceCalculateArg& get_arg() const
  {
    return arg_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantLogicalRes);
  ObAddr server_;
  share::ObUserResourceCalculateArg arg_;

};
struct ObGetLSReplayedScnArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetLSReplayedScnArg(): tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), all_replica_(false) {}
  ~ObGetLSReplayedScnArg() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, const bool all_replica);
  int assign(const ObGetLSReplayedScnArg &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(all_replica));

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  bool is_all_replica() const
  {
    return all_replica_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetLSReplayedScnArg);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  bool all_replica_;//add in 4.3.0, for get all ls replica readable_scn
};

struct ObGetLSReplayedScnRes
{
  OB_UNIS_VERSION(1);
public:
  ObGetLSReplayedScnRes(): tenant_id_(OB_INVALID_TENANT_ID),
                           ls_id_(),
                           cur_readable_scn_(share::SCN::min_scn()),
                           offline_scn_(),
                           self_addr_() {}
  ~ObGetLSReplayedScnRes() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &cur_readable_scn,
           const share::SCN &offline_scn, const common::ObAddr &server);
  int assign(const ObGetLSReplayedScnRes &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(cur_readable_scn), K_(offline_scn), K(self_addr_));
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  share::SCN get_cur_readable_scn() const
  {
    return cur_readable_scn_;
  }
  common::ObAddr get_server() const
  {
    return self_addr_;
  }
  share::SCN get_offline_scn() const
  {
    return offline_scn_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObGetLSReplayedScnRes);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::SCN cur_readable_scn_;
  share::SCN offline_scn_;//add in 4.2.2.0
  common::ObAddr self_addr_;//add in 4.3.0
};


struct ObSwitchTenantArg
{
  OB_UNIS_VERSION(1);
public:
  enum OpType
  {
    INVALID = -1,
    FAILOVER_TO_PRIMARY = 0,
    SWITCH_TO_STANDBY = 1,
    SWITCH_TO_PRIMARY = 2,
  };
  ObSwitchTenantArg() : exec_tenant_id_(OB_INVALID_TENANT_ID),
                        op_type_(INVALID),
                        tenant_name_(),
                        stmt_str_(),
                        is_verify_(false) {}
  ~ObSwitchTenantArg() {}
  int init(
    const uint64_t exec_tenant_id,
    const OpType op_type,
    const ObString &tenant_name,
    const bool is_verify);
  bool is_valid() const {
    return OB_INVALID_TENANT_ID != exec_tenant_id_
           && INVALID != op_type_;
  }
  int assign(const ObSwitchTenantArg &other);
  void set_stmt_str(const ObString &stmt_str) { stmt_str_ = stmt_str; }

  TO_STRING_KV(K_(exec_tenant_id), K_(op_type), K_(stmt_str), K_(tenant_name), K_(is_verify));

  const char *get_alter_type_str() const
  {
    const char *cstr = "invalid";
    switch (op_type_) {
      case SWITCH_TO_STANDBY:
        cstr = "switchover to physical standby";
        break;
      case SWITCH_TO_PRIMARY:
        cstr = "switchover to primary";
        break;
      case FAILOVER_TO_PRIMARY:
        cstr = "failover to primary";
        break;
      default :
        cstr = "invalid";
        break;
    }
    return cstr;
  }
#define Property_declare_var(variable_type, variable_name)\
private:\
  variable_type variable_name##_;\
public:\
  variable_type get_##variable_name() const \
  { return variable_name##_;} \

  Property_declare_var(uint64_t, exec_tenant_id)
  Property_declare_var(OpType, op_type)
  Property_declare_var(ObString, tenant_name)
  Property_declare_var(ObString, stmt_str)
  Property_declare_var(bool, is_verify)
#undef Property_declare_var
};

struct ObRecoverTenantArg
{
  OB_UNIS_VERSION(1);
public:
  enum RecoverType
  {
    INVALID = -1,
    UNTIL = 0,
    CANCEL = 1
  };
  ObRecoverTenantArg() : exec_tenant_id_(OB_INVALID_TENANT_ID),
                         tenant_name_(),
                         type_(RecoverType::INVALID),
                         recovery_until_scn_(share::SCN::min_scn()),
                         stmt_str_() {}
  ~ObRecoverTenantArg() {}
  int init(
    const uint64_t exec_tenant_id,
    const ObString &tenant_name,
    const RecoverType type,
    const share::SCN &recovery_until_scn);
  bool is_valid() const {
    return OB_INVALID_TENANT_ID != exec_tenant_id_
           && is_valid(type_, recovery_until_scn_);
  }
  static bool is_valid(const RecoverType &type, const share::SCN &recovery_until_scn) {
    return ((RecoverType::UNTIL == type && recovery_until_scn.is_valid_and_not_min())
               || (RecoverType::CANCEL == type && recovery_until_scn.is_min()));
  }

  int assign(const ObRecoverTenantArg &other);
  void set_stmt_str(const ObString &stmt_str) { stmt_str_ = stmt_str; }

  TO_STRING_KV(K_(exec_tenant_id), K_(tenant_name), K_(type), K_(recovery_until_scn), K_(stmt_str));

private:

#define Property_declare_var(variable_type, variable_name)\
private:\
  variable_type variable_name##_;\
public:\
  variable_type get_##variable_name() const \
  { return variable_name##_;} \

  Property_declare_var(uint64_t, exec_tenant_id)
  Property_declare_var(ObString, tenant_name)
  Property_declare_var(RecoverType, type)
  Property_declare_var(share::SCN, recovery_until_scn)
  /**
   * @description: recovery_until_scn
   *  UNTIL UNLIMITED          : SCN::max_scn()
   *  UNTIL specified TIME/SCN : valid scn
   *  CANCEL                   : min_scn()
   */
  Property_declare_var(ObString, stmt_str)
#undef Property_declare_var
};

struct ObDDLNopOpreatorArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDDLNopOpreatorArg(): schema_operation_() {}
  ~ObDDLNopOpreatorArg() {}
public:
  share::schema::ObSchemaOperation schema_operation_;
  bool is_valid() const {
    return schema_operation_.is_valid();
  }
  virtual bool is_allow_when_upgrade() const { return true; }
  void reset() {
    schema_operation_.reset();
  }
  TO_STRING_KV(K_(schema_operation));
private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLNopOpreatorArg);
};

struct ObTablespaceDDLArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObTablespaceDDLArg():
    ObDDLArg(),
    type_(CREATE_TABLESPACE),
    schema_()
  {}
  enum DDLType
  {
    CREATE_TABLESPACE = 0,
    ALTER_TABLESPACE,
    DROP_TABLESPACE,
  };

  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const { return DROP_TABLESPACE == type_; }
  DDLType type_;
  share::schema::ObTablespaceSchema schema_;
  DECLARE_TO_STRING;
};

// end for ddl arg
//////////////////////////////////////////////////

struct ObEstPartArgElement
{
  ObEstPartArgElement() : batch_(), scan_flag_(),
    index_id_(common::OB_INVALID_ID), range_columns_count_(0), tablet_id_(), ls_id_(), tenant_id_(0), tx_id_()
  {}
  // Essentially, we can use ObIArray<ObNewRange> here
  // For compatibility reason, we still use ObSimpleBatch
  common::ObSimpleBatch batch_;
  common::ObQueryFlag scan_flag_;
  int64_t index_id_;
  int64_t range_columns_count_;
  ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  uint64_t tenant_id_;
  transaction::ObTransID tx_id_;

  TO_STRING_KV(
      K(scan_flag_),
      K(index_id_),
      K(batch_),
      K(range_columns_count_),
      K(tablet_id_),
      K(ls_id_),
      K(tenant_id_),
      K(tx_id_));
  int64_t get_serialize_size(void) const;
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator,
                  const char *buf,
                  const int64_t data_len,
                  int64_t &pos);
};

struct ObEstPartArg
{
  //Deserialization use
  common::ObArenaAllocator allocator_;

  int64_t schema_version_;
  common::ObSEArray<ObEstPartArgElement, 4, common::ModulePageAllocator, true> index_params_;

  ObEstPartArg()
      : allocator_(common::ObModIds::OB_SQL_QUERY_RANGE),
        schema_version_(0),
        index_params_()
  {}
  ~ObEstPartArg() { reset(); }

  void reset();

  TO_STRING_KV(K_(schema_version),
               K_(index_params));

  OB_UNIS_VERSION(1);
};

struct ObEstPartResElement
{
  int64_t logical_row_count_;
  int64_t physical_row_count_;
  /**
   * @brief reliable_
   * storage estimation is not successfully called,
   * we use ndv to estimate row count in the following
   */
  bool reliable_;
  common::ObSEArray<common::ObEstRowCountRecord, 2, common::ModulePageAllocator, true> est_records_;

  ObEstPartResElement() {
    reset();
  }

  void reset()
  {
    logical_row_count_ = common::OB_INVALID_COUNT;
    physical_row_count_ = common::OB_INVALID_COUNT;
    reliable_ = false;
    est_records_.reset();
  }

  TO_STRING_KV(K(logical_row_count_), K(physical_row_count_), K(reliable_), K(est_records_));
  OB_UNIS_VERSION(1);
};

struct ObEstPartRes
{
  common::ObSEArray<ObEstPartResElement, 4, common::ModulePageAllocator, true> index_param_res_;

  ObEstPartRes() : index_param_res_()
  {}

  TO_STRING_KV(K(index_param_res_));

  OB_UNIS_VERSION(1);
};

struct ObGetWRSArg
{
  OB_UNIS_VERSION(1);
public:
  enum Scope
  {
    INVALID_RANGE = 0,
    INNER_TABLE,
    USER_TABLE,
    ALL_TABLE
  };
  TO_STRING_KV(K_(tenant_id), K_(scope), K_(need_filter));
  bool is_valid() const;
  int64_t tenant_id_;
  Scope scope_; //The machine-readable timestamp can be calculated separately for the timestamp of the system table or user table, or collectively calculated together
  bool need_filter_;

  ObGetWRSArg() :
      tenant_id_(common::OB_INVALID_TENANT_ID),
      scope_(ALL_TABLE),  // Statistics of all types of tables by default
      need_filter_(false) // Unreadable partitions are not filtered by default
  {}

  explicit ObGetWRSArg(const int64_t tenant_id) :
      tenant_id_(tenant_id),
      scope_(ALL_TABLE),  // Statistics of all types of tables by default
      need_filter_(false) // Unreadable partitions are not filtered by default
  {}
};

struct ObGetWRSResult
{
  OB_UNIS_VERSION(1);

public:
  ObGetWRSResult() : self_addr_(), err_code_(0)
  {}

  void reset()
  {
    self_addr_.reset();
    err_code_ = 0;
  }

public:
  common::ObAddr  self_addr_;
  int err_code_;
  TO_STRING_KV(K_(err_code),
      K_(self_addr));
};

struct ObTenantSchemaVersions
{
  OB_UNIS_VERSION(1);
public:
  ObTenantSchemaVersions() : tenant_schema_versions_() {}
  common::ObSArray<share::TenantIdAndSchemaVersion> tenant_schema_versions_;
  int add(const int64_t tenant_id, const int64_t schema_version);
  void reset() { return tenant_schema_versions_.reset(); }
  bool is_valid() const
  {
    return 0 < tenant_schema_versions_.count();
  }
  int assign(const ObTenantSchemaVersions &arg)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(tenant_schema_versions_.assign(arg.tenant_schema_versions_))) {
      SHARE_LOG(WARN, "failed to assign tenant schema version", KR(ret), K(arg));
    }
    return ret;
  }
  TO_STRING_KV(K_(tenant_schema_versions));
};

struct ObGetSchemaArg : public ObDDLArg
{
   OB_UNIS_VERSION(1);
public:
  ObGetSchemaArg() : reserve_(0), ignore_fail_(false) {}
  virtual bool is_allow_when_upgrade() const { return true; }
  int64_t reserve_;
  bool ignore_fail_;
};

struct TenantIdAndStats
{
  OB_UNIS_VERSION(1);
public:
  TenantIdAndStats() :
    tenant_id_(common::OB_INVALID_TENANT_ID),
    refreshed_schema_version_(0),
    ddl_lag_(0),
    min_sys_table_scn_(0),
    min_user_table_scn_(0) {}

  TenantIdAndStats(
      const uint64_t tenant_id,
      const int64_t refreshed_schema_version,
      const int64_t ddl_lag,
      const int64_t min_sys_table_scn,
      const int64_t min_user_table_scn) :
        tenant_id_(tenant_id),
        refreshed_schema_version_(refreshed_schema_version),
        ddl_lag_(ddl_lag),
        min_sys_table_scn_(min_sys_table_scn),
        min_user_table_scn_(min_user_table_scn) {}

  TO_STRING_KV(K_(tenant_id), K_(refreshed_schema_version), K_(ddl_lag),
               K_(min_sys_table_scn), K_(min_user_table_scn));

  void reset() {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    refreshed_schema_version_ = 0;
    ddl_lag_ = 0;
    min_sys_table_scn_ = 0;
    min_user_table_scn_ = 0;
  }

  uint64_t tenant_id_;
  int64_t refreshed_schema_version_;
  int64_t ddl_lag_;
  int64_t min_sys_table_scn_;
  int64_t min_user_table_scn_;
};

struct ObBroadcastSchemaArg
{
  OB_UNIS_VERSION(1);
public:
  ObBroadcastSchemaArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      schema_version_(common::OB_INVALID_VERSION) {}
  void reset();
public:
  uint64_t tenant_id_;
  int64_t schema_version_;
  TO_STRING_KV(K_(tenant_id), K_(schema_version));
};

struct ObGetRecycleSchemaVersionsArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetRecycleSchemaVersionsArg()
    : tenant_ids_() {}
  virtual ~ObGetRecycleSchemaVersionsArg() {}
  bool is_valid() const;
  void reset();
  int assign(const ObGetRecycleSchemaVersionsArg &other);
public:
  common::ObSArray<uint64_t> tenant_ids_;
  TO_STRING_KV(K_(tenant_ids));
};

struct ObGetRecycleSchemaVersionsResult
{
  OB_UNIS_VERSION(1);
public:
  ObGetRecycleSchemaVersionsResult()
    : recycle_schema_versions_() {}
  virtual ~ObGetRecycleSchemaVersionsResult() {}
  bool is_valid() const;
  void reset();
  int assign(const ObGetRecycleSchemaVersionsResult &other);
public:
  common::ObSArray<share::TenantIdAndSchemaVersion> recycle_schema_versions_;
  TO_STRING_KV(K_(recycle_schema_versions));
};


class ObHaGtsPingRequest
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsPingRequest(): gts_id_(common::OB_INVALID_ID),
                        req_id_(common::OB_INVALID_ID),
                        epoch_id_(common::OB_INVALID_TIMESTAMP),
                        request_ts_(common::OB_INVALID_TIMESTAMP) {}
  ~ObHaGtsPingRequest() {}
public:
  void reset()
  {
    gts_id_ = common::OB_INVALID_ID;
    req_id_ = common::OB_INVALID_ID;
    epoch_id_ = common::OB_INVALID_TIMESTAMP;
    request_ts_ = common::OB_INVALID_TIMESTAMP;
  }
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && common::OB_INVALID_ID != req_id_
      && epoch_id_ > 0 && request_ts_ > 0;
  }
  void set(const uint64_t gts_id,
           const uint64_t req_id,
           const int64_t epoch_id,
           const int64_t request_ts)
  {
    gts_id_ = gts_id;
    req_id_ = req_id;
    epoch_id_ = epoch_id;
    request_ts_ = request_ts;
  }
  uint64_t get_gts_id() const { return gts_id_; }
  uint64_t get_req_id() const { return req_id_; }
  int64_t get_epoch_id() const { return epoch_id_; }
  int64_t get_request_ts() const { return request_ts_; }
  TO_STRING_KV(K(gts_id_), K(req_id_), K(epoch_id_), K(request_ts_));
private:
  uint64_t gts_id_;
  uint64_t req_id_;
  int64_t epoch_id_;
  int64_t request_ts_;
};

class ObHaGtsPingResponse
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsPingResponse(): gts_id_(common::OB_INVALID_ID),
                         req_id_(common::OB_INVALID_ID),
                         epoch_id_(common::OB_INVALID_TIMESTAMP),
                         response_ts_(common::OB_INVALID_TIMESTAMP) {}
  ~ObHaGtsPingResponse() {}
public:
  void reset()
  {
    gts_id_ = common::OB_INVALID_ID;
    req_id_ = common::OB_INVALID_ID;
    epoch_id_ = common::OB_INVALID_TIMESTAMP;
    response_ts_ = common::OB_INVALID_TIMESTAMP;
  }
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && common::OB_INVALID_ID != req_id_ && epoch_id_ > 0
      && response_ts_ > 0;
  }
  void set(const uint64_t gts_id,
           const uint64_t req_id,
           const int64_t epoch_id,
           const int64_t response_ts)
  {
    gts_id_ = gts_id;
    req_id_ = req_id;
    epoch_id_ = epoch_id;
    response_ts_ = response_ts;
  }
  uint64_t get_gts_id() const { return gts_id_; }
  uint64_t get_req_id() const { return req_id_; }
  int64_t get_epoch_id() const { return epoch_id_; }
  int64_t get_response_ts() const { return response_ts_; }
  TO_STRING_KV(K(gts_id_), K(req_id_), K(epoch_id_), K(response_ts_));
private:
  uint64_t gts_id_;
  uint64_t req_id_;
  int64_t epoch_id_;
  int64_t response_ts_;
};

class ObHaGtsGetRequest
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsGetRequest(): gts_id_(common::OB_INVALID_ID),
                       self_addr_(),
                       tenant_id_(common::OB_INVALID_TENANT_ID),
                       srr_(common::OB_INVALID_TIMESTAMP) {}
  ~ObHaGtsGetRequest() {}
public:
  void reset()
  {
    gts_id_ = common::OB_INVALID_ID;
    self_addr_.reset();
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    srr_.reset();
  }
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && self_addr_.is_valid()
           && srr_.is_valid();
  }
  void set(const uint64_t gts_id,
           const common::ObAddr &self_addr,
           const uint64_t tenant_id,
           const transaction::MonotonicTs srr)
  {
    gts_id_ = gts_id;
    self_addr_ = self_addr;
    tenant_id_ = tenant_id;
    srr_ = srr;
  }
  uint64_t get_gts_id() const { return gts_id_; }
  const common::ObAddr &get_self_addr() const { return self_addr_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  transaction::MonotonicTs get_srr() const { return srr_; }
  TO_STRING_KV(K(gts_id_), K(self_addr_), K(tenant_id_), K(srr_));
private:
  uint64_t gts_id_;
  common::ObAddr self_addr_;
  // TODO: To be deleted
  uint64_t tenant_id_;
  transaction::MonotonicTs srr_;
};

class ObHaGtsGetResponse
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsGetResponse() : gts_id_(common::OB_INVALID_ID),
                         tenant_id_(common::OB_INVALID_TENANT_ID),
                         srr_(common::OB_INVALID_TIMESTAMP),
                         gts_(common::OB_INVALID_TIMESTAMP) {}
  ~ObHaGtsGetResponse() {}
public:
  void reset()
  {
    gts_id_ = common::OB_INVALID_ID;
    tenant_id_ = common::OB_INVALID_TENANT_ID;
    srr_.reset();
    gts_ = common::OB_INVALID_TIMESTAMP;
  }
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_)  && srr_.is_valid() && gts_ > 0;
  }
  void set(const uint64_t gts_id,
           const uint64_t tenant_id,
           const transaction::MonotonicTs srr,
           const int64_t gts)
  {
    gts_id_ = gts_id;
    tenant_id_ = tenant_id;
    srr_ = srr;
    gts_ = gts;
  }
  int64_t get_gts_id() const { return gts_id_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  transaction::MonotonicTs get_srr() const { return srr_; }
  int64_t get_gts() const { return gts_; }
  TO_STRING_KV(K(gts_id_), K(tenant_id_), K(srr_), K(gts_));
private:
  uint64_t gts_id_;
  // TODO: To be deleted
  uint64_t tenant_id_;
  transaction::MonotonicTs srr_;
  int64_t gts_;
};

class ObHaGtsHeartbeat
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsHeartbeat() : gts_id_(common::OB_INVALID_ID), addr_() {}
  ~ObHaGtsHeartbeat() {}
public:
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && addr_.is_valid();
  }
  void set(const uint64_t gts_id, const common::ObAddr &addr)
  {
    gts_id_ = gts_id;
    addr_ = addr;
  }
  uint64_t get_gts_id() const { return gts_id_; }
  const common::ObAddr &get_addr() const { return addr_; }
  TO_STRING_KV(K(gts_id_), K(addr_));
private:
  uint64_t gts_id_;
  common::ObAddr addr_;
};

class ObHaGtsUpdateMetaRequest
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsUpdateMetaRequest() : gts_id_(common::OB_INVALID_ID),
                               epoch_id_(common::OB_INVALID_TIMESTAMP),
                               member_list_(),
                               local_ts_(common::OB_INVALID_TIMESTAMP) {}
  ~ObHaGtsUpdateMetaRequest() {}
public:
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && epoch_id_ > 0
      && member_list_.get_member_number() > 0 && member_list_.get_member_number() <= 3
      && local_ts_ > 0;
  }
  void set(const uint64_t gts_id,
           const int64_t epoch_id,
           const common::ObMemberList &member_list,
           const int64_t local_ts)
  {
    gts_id_ = gts_id;
    epoch_id_ = epoch_id;
    member_list_ = member_list;
    local_ts_ = local_ts;
  }
  uint64_t get_gts_id() const { return gts_id_; }
  int64_t get_epoch_id() const { return epoch_id_; }
  const common::ObMemberList &get_member_list() const { return member_list_; }
  int64_t get_local_ts() const { return local_ts_; }
  TO_STRING_KV(K(gts_id_), K(epoch_id_), K(member_list_), K(local_ts_));
private:
  uint64_t gts_id_;
  int64_t epoch_id_;
  common::ObMemberList member_list_;
  int64_t local_ts_;
};

class ObHaGtsUpdateMetaResponse
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsUpdateMetaResponse() : local_ts_(common::OB_INVALID_TIMESTAMP) {}
  ~ObHaGtsUpdateMetaResponse() {}
public:
  bool is_valid() const
  {
    return local_ts_ > 0;
  }
  void set(const int64_t local_ts)
  {
    local_ts_ = local_ts;
  }
  int64_t get_local_ts() const { return local_ts_; }
  TO_STRING_KV(K(local_ts_));
private:
  int64_t local_ts_;
};

class ObHaGtsChangeMemberRequest
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsChangeMemberRequest() : gts_id_(common::OB_INVALID_ID), offline_replica_() {}
  ~ObHaGtsChangeMemberRequest() {}
public:
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && offline_replica_.is_valid();
  }
  void set(const uint64_t gts_id, const common::ObAddr &offline_replica)
  {
    gts_id_ = gts_id;
    offline_replica_ = offline_replica;
  }
  uint64_t get_gts_id() const { return gts_id_; }
  const common::ObAddr &get_offline_replica() const { return offline_replica_; }
  TO_STRING_KV(K(gts_id_), K(offline_replica_));
private:
  uint64_t gts_id_;
  common::ObAddr offline_replica_;
};

class ObHaGtsChangeMemberResponse
{
  OB_UNIS_VERSION(1);
public:
  ObHaGtsChangeMemberResponse() : ret_value_(common::OB_SUCCESS) {}
  ~ObHaGtsChangeMemberResponse() {}
public:
  void set(const int ret_value)
  {
    ret_value_ = ret_value;
  }
  int get_ret_value() const { return ret_value_; }
  TO_STRING_KV(K(ret_value_));
private:
  int ret_value_;
};

struct ObSecurityAuditArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObSecurityAuditArg() : ObDDLArg(),
                         tenant_id_(OB_INVALID_ID),
                         modify_type_(share::schema::AUDIT_MT_INVALID),
                         audit_type_(share::schema::AUDIT_INVALID),
                         operation_types_(),
                         stmt_user_ids_(),
                         obj_object_id_(OB_INVALID_ID),
                         by_type_(share::schema::AUDIT_BY_SESSION),
                         when_type_(share::schema::AUDIT_WHEN_NOT_SET)
  {}
  virtual ~ObSecurityAuditArg() {}
  bool is_valid() const
  {
    return ((tenant_id_ != OB_INVALID_ID)
            && (share::schema::AUDIT_MT_ADD == modify_type_
                || share::schema::AUDIT_MT_DEL == modify_type_)
            && (share::schema::AUDIT_INVALID <= audit_type_
                && audit_type_ < share::schema::AUDIT_MAX)
            && !operation_types_.empty()
            && (share::schema::AUDIT_BY_SESSION == by_type_
                || share::schema::AUDIT_BY_ACCESS == by_type_)
            && (share::schema::AUDIT_WHEN_NOT_SET <= when_type_
                && when_type_ <= share::schema::AUDIT_WHEN_SUCCESS)
            && ((share::schema::AUDIT_STMT == audit_type_ && !stmt_user_ids_.empty())
                || (share::schema::AUDIT_STMT_ALL_USER == audit_type_
                    && stmt_user_ids_.empty())
                || (share::schema::AUDIT_OBJ_DEFAULT == audit_type_
                    && OB_INVALID_ID == obj_object_id_)
                || (share::schema::AUDIT_TABLE <= audit_type_
                    && audit_type_ < share::schema::AUDIT_MAX
                    && obj_object_id_ != OB_INVALID_ID)));
  }
  virtual bool is_allow_when_upgrade() const { return false; }
  TO_STRING_KV(K_(tenant_id), K_(modify_type), K_(audit_type), K_(operation_types),
               K_(stmt_user_ids), K_(obj_object_id), K_(by_type), K_(when_type));

public:
  uint64_t tenant_id_;
  share::schema::ObSAuditModifyType modify_type_;
  share::schema::ObSAuditType audit_type_;

  common::ObSEArray<share::schema::ObSAuditOperationType, OB_DEFAULT_ARRAY_SIZE> operation_types_;
  common::ObSEArray<uint64_t, OB_DEFAULT_ARRAY_SIZE> stmt_user_ids_;
  uint64_t obj_object_id_;

  share::schema::ObSAuditOperByType by_type_;
  share::schema::ObSAuditOperWhenType when_type_;
};

struct ObAlterTableResArg
{
  OB_UNIS_VERSION(1);
public:
  ObAlterTableResArg() :
  schema_type_(share::schema::OB_MAX_SCHEMA),
  schema_id_(common::OB_INVALID_ID),
  schema_version_(common::OB_INVALID_VERSION),
  part_object_id_(common::OB_INVALID_ID)
  {}
  ObAlterTableResArg(
      const share::schema::ObSchemaType schema_type,
      const uint64_t schema_id,
      const int64_t schema_version)
      : schema_type_(schema_type),
        schema_id_(schema_id),
        schema_version_(schema_version)
  {}
    ObAlterTableResArg(
      const share::schema::ObSchemaType schema_type,
      const uint64_t schema_id,
      const int64_t schema_version,
      const int64_t part_object_id)
      : schema_type_(schema_type),
        schema_id_(schema_id),
        schema_version_(schema_version),
        part_object_id_(part_object_id)
  {}
  void reset();
public:
  TO_STRING_KV(K_(schema_type), K_(schema_id), K_(schema_version), K_(part_object_id));
  share::schema::ObSchemaType schema_type_;
  uint64_t schema_id_;
  int64_t schema_version_;
  int64_t part_object_id_;
};

struct ObDDLRes final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLRes()
    : tenant_id_(common::OB_INVALID_ID), schema_id_(common::OB_INVALID_ID), task_id_(0)
  {}
  ~ObDDLRes() = default;
  int assign(const ObDDLRes &other);
  void reset() {
    tenant_id_ = common::OB_INVALID_ID;
    schema_id_ = common::OB_INVALID_ID;
    task_id_ = 0;
  }
  bool is_valid() {
    return common::OB_INVALID_ID != tenant_id_
        && common::OB_INVALID_ID != schema_id_
        && task_id_ > 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(schema_id), K_(task_id));
public:
  uint64_t tenant_id_;
  uint64_t schema_id_;
  int64_t task_id_;
};

struct ObAlterTableRes
{
  OB_UNIS_VERSION(1);
public:
  ObAlterTableRes() :
  index_table_id_(common::OB_INVALID_ID),
  constriant_id_(common::OB_INVALID_ID),
  schema_version_(common::OB_INVALID_VERSION),
  res_arg_array_(),
  ddl_type_(share::DDL_INVALID),
  task_id_(0),
  ddl_res_array_(),
  ddl_need_retry_at_executor_(false)
  {}
  void reset();
  int assign(const ObAlterTableRes &other) {
    int ret = common::OB_SUCCESS;
    index_table_id_ = other.index_table_id_;
    constriant_id_ = other.constriant_id_;
    schema_version_ = other.schema_version_;
    if (OB_FAIL(res_arg_array_.assign(other.res_arg_array_))) {
      SHARE_LOG(WARN, "assign res_arg_array failed", K(ret), K(other.res_arg_array_));
    } else if (OB_FAIL(ddl_res_array_.assign(other.ddl_res_array_))) {
      SHARE_LOG(WARN, "assign ddl res array failed", K(ret));
    } else {
      ddl_type_ = other.ddl_type_;
      task_id_ = other.task_id_;
      ddl_need_retry_at_executor_ = other.ddl_need_retry_at_executor_;
    }
    return ret;
  }
public:
  TO_STRING_KV(K_(index_table_id), K_(constriant_id), K_(schema_version),
  K_(res_arg_array), K_(ddl_type), K_(task_id), K_(ddl_need_retry_at_executor));
  uint64_t index_table_id_;
  uint64_t constriant_id_;
  int64_t schema_version_;
  common::ObSArray<ObAlterTableResArg> res_arg_array_;
  share::ObDDLType ddl_type_;
  int64_t task_id_;
  common::ObSArray<ObDDLRes> ddl_res_array_;
  bool ddl_need_retry_at_executor_;
};

struct ObDropDatabaseRes final
{
  OB_UNIS_VERSION(1);
public:
  ObDropDatabaseRes()
    : ddl_res_(),
    affected_row_(0)
  {}
  void reset();
  int assign(const ObDropDatabaseRes &other);
  bool is_valid() {
    return ddl_res_.is_valid();
  }
public:
  TO_STRING_KV(K_(ddl_res), K_(affected_row));
  ObDDLRes ddl_res_;
  UInt64 affected_row_;
};
struct ObGetTenantSchemaVersionArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetTenantSchemaVersionArg() : tenant_id_(common::OB_INVALID_TENANT_ID) {}
  bool is_valid() const { return common::OB_INVALID_TENANT_ID != tenant_id_; }

  TO_STRING_KV(K_(tenant_id));
  uint64_t tenant_id_;
};

struct ObGetTenantSchemaVersionResult
{
  OB_UNIS_VERSION(1);
public:
  ObGetTenantSchemaVersionResult() : schema_version_(common::OB_INVALID_VERSION) {}
  bool is_valid() const { return schema_version_ > 0; }

  TO_STRING_KV(K_(schema_version));
  int64_t schema_version_;
};

struct ObTenantMemoryArg
{
  OB_UNIS_VERSION(1);
public:
  ObTenantMemoryArg() : tenant_id_(0), memory_size_(0), refresh_interval_(0) {}
  bool is_valid() const { return tenant_id_ > 0 && memory_size_ > 0 && refresh_interval_ >= 0; }

  TO_STRING_KV(K_(tenant_id), K_(memory_size));
  int64_t tenant_id_;
  int64_t memory_size_;
  int64_t refresh_interval_;
};

struct ObKeystoreDDLArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObKeystoreDDLArg():
    ObDDLArg(),
    type_(CREATE_KEYSTORE),
    schema_(),
    is_kms_(false)
  {}
  enum DDLType
  {
    CREATE_KEYSTORE = 0,
    ALTER_KEYSTORE_PASSWORD,
    ALTER_KEYSTORE_SET_KEY,
    ALTER_KEYSTORE_CLOSE,
    ALTER_KEYSTORE_OPEN,
  };
  bool is_valid() const;
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool is_allow_when_upgrade() const { return false; }
  virtual bool contain_sensitive_data() const { return true; }
  DDLType type_;
  share::schema::ObKeystoreSchema schema_;
  bool is_kms_;
  TO_STRING_KV(K_(type));
};

struct ObProfileDDLArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:

  ObProfileDDLArg():
    ObDDLArg(),
    schema_(),
    ddl_type_(),
    is_cascade_(false)
  {}
  virtual bool is_allow_when_upgrade() const {
    return share::schema::OB_DDL_DROP_PROFILE == ddl_type_
        || share::schema::OB_DDL_ALTER_PROFILE == ddl_type_;
  }
  virtual bool is_allow_when_disable_ddl() const { return true; }

  share::schema::ObProfileSchema schema_;
  share::schema::ObSchemaOperationType ddl_type_;
  bool is_cascade_;
  TO_STRING_KV(K_(schema), K_(ddl_type), K_(is_cascade));
};

struct ObDependencyObjDDLArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObDependencyObjDDLArg()
    : tenant_id_(OB_INVALID_ID),
      reset_view_column_infos_(false)
  {
  }
  bool is_valid() const { return tenant_id_ != OB_INVALID_ID; }
  int assign(const ObDependencyObjDDLArg &other);
  virtual bool is_allow_when_upgrade() const { return false; }
  TO_STRING_KV(K_(tenant_id),
               K_(insert_dep_objs),
               K_(update_dep_objs),
               K_(delete_dep_objs),
               K_(reset_view_column_infos));

  uint64_t tenant_id_;
  share::schema::ObReferenceObjTable::DependencyObjKeyItemPairs insert_dep_objs_;
  share::schema::ObReferenceObjTable::DependencyObjKeyItemPairs update_dep_objs_;
  share::schema::ObReferenceObjTable::DependencyObjKeyItemPairs delete_dep_objs_;
  share::schema::ObTableSchema schema_;
  bool reset_view_column_infos_; // for oracle mode, recompile invalid view will reset its data type to undefined
};

struct ObCheckServerEmptyArg
{
  OB_UNIS_VERSION(1);
public:
  enum Mode {
    BOOTSTRAP,
    ADD_SERVER
  };

  ObCheckServerEmptyArg(): mode_(BOOTSTRAP), sys_data_version_(0), server_id_(OB_INVALID_ID) {}
  ObCheckServerEmptyArg(const Mode mode,
                        const uint64_t sys_data_version)
    : mode_(mode), sys_data_version_(sys_data_version), server_id_(OB_INVALID_ID) {}
  TO_STRING_KV(K_(mode), K_(sys_data_version), K_(server_id));
  int assign(const ObCheckServerEmptyArg &other);
  Mode mode_;
  uint64_t sys_data_version_;
  uint64_t server_id_;
};
struct ObCheckServerForAddingServerArg
{
  OB_UNIS_VERSION(1);
public:
  enum Mode {
    ADD_SERVER,
    BOOTSTRAP
  };

  ObCheckServerForAddingServerArg(): mode_(ADD_SERVER), sys_tenant_data_version_(0), server_id_(OB_INVALID_ID) {}
  TO_STRING_KV(K_(mode), K_(sys_tenant_data_version), K_(server_id));
  int init(const Mode &mode, const uint64_t sys_tenant_data_version, const uint64_t server_id);
  int assign(const ObCheckServerForAddingServerArg &other);
  bool is_valid() const;
  void reset();
  Mode get_mode() const
  {
    return mode_;
  }
  uint64_t get_sys_tenant_data_version() const
  {
    return sys_tenant_data_version_;
  }
  uint64_t get_server_id() const
  {
    return server_id_;
  }
private:
  Mode mode_;
  uint64_t sys_tenant_data_version_;
  uint64_t server_id_;
};
struct ObCheckServerForAddingServerResult
{
  OB_UNIS_VERSION(1);
public:
  enum ObServerMode {
    INVALID_MODE = 0
  };
  ObCheckServerForAddingServerResult()
      : is_server_empty_(false),
      zone_(),
      sql_port_(0),
      build_version_(),
      startup_mode_(ObServerMode::INVALID_MODE)
  {
  }
  TO_STRING_KV(K_(is_server_empty), K_(zone), K_(sql_port), K_(build_version), K_(startup_mode));
  int init(
      const bool is_server_empty,
      const ObZone &zone,
      const int64_t sql_port,
      const share::ObServerInfoInTable::ObBuildVersion &build_version);
  int assign(const ObCheckServerForAddingServerResult &other);
  bool get_is_server_empty() const
  {
    return is_server_empty_;
  }
  const ObZone& get_zone() const
  {
    return zone_;
  }
  int64_t get_sql_port() const
  {
    return sql_port_;
  }
  const share::ObServerInfoInTable::ObBuildVersion& get_build_version() const
  {
    return build_version_;
  }
private:
  bool is_server_empty_;
  ObZone zone_;
  int64_t sql_port_;
  share::ObServerInfoInTable::ObBuildVersion build_version_;
  ObServerMode startup_mode_; // not used, only as placeholder
};

struct ObArchiveLogArg
{
  OB_UNIS_VERSION(1);
public:
  ObArchiveLogArg(): enable_(true), tenant_id_(OB_INVALID_TENANT_ID), archive_tenant_ids_() {}

  int assign(const ObArchiveLogArg &other);

  TO_STRING_KV(K_(enable), K_(tenant_id), K_(archive_tenant_ids));
  bool enable_;
  uint64_t tenant_id_;
  common::ObSArray<uint64_t> archive_tenant_ids_;
};

struct ObBackupDatabaseArg
{
  OB_UNIS_VERSION(1);
public:
  ObBackupDatabaseArg();
  int assign(const ObBackupDatabaseArg &arg);
  bool is_valid() const;
	TO_STRING_KV(K_(tenant_id), K_(initiator_tenant_id), K_(initiator_job_id), K_(backup_tenant_ids),
      K_(is_incremental), K_(backup_dest), K_(backup_description), K_(is_compl_log), K_(encryption_mode),
      K_(passwd));
  uint64_t tenant_id_; // target tenant to do backup
  uint64_t initiator_tenant_id_; // the tenant id who initiate the backup
  int64_t initiator_job_id_; // only used when sys tenant send rpc to user teannt to insert a backup job
  common::ObSArray<uint64_t> backup_tenant_ids_; // tenants which need to backup
  bool is_incremental_;
  bool is_compl_log_;
  share::ObBackupPathString backup_dest_;
  share::ObBackupDescription backup_description_;
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
};

struct ObBackupManageArg
{
  OB_UNIS_VERSION(1);
public:
  enum Type
  {
    CANCEL_BACKUP = 0,
    SUSPEND_BACKUP = 1,
    RESUME_BACKUP = 2,
    VALIDATE_DATABASE = 4,
    VALIDATE_BACKUPSET = 5,
    CANCEL_VALIDATE = 6,
    CANCEL_BACKUP_BACKUPSET = 7,
    CANCEL_BACKUP_BACKUPPIECE = 8,
    CANCEL_ALL_BACKUP_FORCE = 9,
    MAX_TYPE
  };
  int assign(const ObBackupManageArg &arg);
  ObBackupManageArg(): tenant_id_(common::OB_INVALID_TENANT_ID), managed_tenant_ids_(), type_(MAX_TYPE), value_(0), copy_id_(0) {}
  TO_STRING_KV(K_(tenant_id), K_(managed_tenant_ids) ,K_(type), K_(value), K_(copy_id));
  uint64_t tenant_id_;
  common::ObSArray<uint64_t> managed_tenant_ids_;
  Type type_;
  int64_t value_;
  int64_t copy_id_;
};

struct ObBackupCleanArg
{
  OB_UNIS_VERSION(1);
public:
  ObBackupCleanArg() :
      tenant_id_(common::OB_INVALID_TENANT_ID),
      initiator_tenant_id_(common::OB_INVALID_TENANT_ID),
      initiator_job_id_(0),
      type_(share::ObNewBackupCleanType::MAX),
      value_(0),
      dest_id_(0),
      description_(),
      clean_tenant_ids_()
  {

  }
  bool is_valid() const;
  int assign(const ObBackupCleanArg &arg);
  TO_STRING_KV(K_(type), K_(tenant_id), K_(initiator_tenant_id), K_(initiator_job_id), K_(value), K_(dest_id), K_(description), K_(clean_tenant_ids));
  uint64_t tenant_id_;
  uint64_t initiator_tenant_id_;
  int64_t initiator_job_id_;
  share::ObNewBackupCleanType::TYPE type_;
  int64_t value_;
  int64_t dest_id_;
  share::ObBackupDescription description_;
  common::ObSArray<uint64_t> clean_tenant_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupCleanArg);
};

struct ObNotifyArchiveArg
{
  OB_UNIS_VERSION(1);
public:
  ObNotifyArchiveArg() :
      tenant_id_(common::OB_INVALID_TENANT_ID)
  {
  }
public:
  bool is_valid() const;
  int assign(const ObNotifyArchiveArg &arg);
  TO_STRING_KV(K_(tenant_id));
public:
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObNotifyArchiveArg);
};

struct ObLSBackupCleanArg
{
  OB_UNIS_VERSION(1);
public:
  ObLSBackupCleanArg()
    : trace_id_(),
      job_id_(0),
      tenant_id_(0),
      incarnation_(0),
      task_id_(0),
      ls_id_(0),
      task_type_(),
      id_(),
      dest_id_(0),
      round_id_(0)
  {
  }
public:
  bool is_valid() const;
  int assign(const ObLSBackupCleanArg &arg);
  TO_STRING_KV(K_(trace_id), K_(job_id), K_(tenant_id), K_(incarnation), K_(task_id), K_(ls_id), K_(task_type), K_(id), K_(dest_id), K_(round_id));
public:
  share::ObTaskId trace_id_;
  int64_t job_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  uint64_t task_id_;
  share::ObLSID ls_id_;
  share::ObBackupCleanTaskType::TYPE task_type_;
  uint64_t id_;
  int64_t dest_id_;
  int64_t round_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupCleanArg);
};

struct ObDeletePolicyArg
{
  OB_UNIS_VERSION(1);
public:
  ObDeletePolicyArg() :
      initiator_tenant_id_(common::OB_INVALID_TENANT_ID),
      type_(share::ObPolicyOperatorType::MAX),
      policy_name_(),
      recovery_window_(),
      redundancy_(0),
      backup_copies_(0),
      clean_tenant_ids_()
  {
  }
  bool is_valid() const;
  int assign(const ObDeletePolicyArg &arg);
  TO_STRING_KV(K_(initiator_tenant_id), K_(type), K_(policy_name),
      K_(recovery_window), K_(redundancy),  K_(backup_copies), K_(clean_tenant_ids));

  uint64_t initiator_tenant_id_;
  share::ObPolicyOperatorType type_;
  char policy_name_[share::OB_BACKUP_DELETE_POLICY_NAME_LENGTH];
  char recovery_window_[share::OB_BACKUP_RECOVERY_WINDOW_LENGTH];
  int64_t redundancy_;
  int64_t backup_copies_;
  common::ObSArray<uint64_t> clean_tenant_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDeletePolicyArg);
};

struct CheckLeaderRpcIndex
{
  OB_UNIS_VERSION(1);
public:
  int64_t switchover_timestamp_; //Switch logo
  int64_t epoch_;  //(switchover_timestamp, epoch) uniquely identifies a statistical information during a switching process
  int64_t tenant_id_;
  int64_t ml_pk_index_;  //Position the coordinates of pkey
  int64_t pkey_info_start_index_; //Position the coordinates of pkey
  CheckLeaderRpcIndex()
    : switchover_timestamp_(0), epoch_(0), tenant_id_(common::OB_INVALID_TENANT_ID),
      ml_pk_index_(0), pkey_info_start_index_(0) {};
  ~CheckLeaderRpcIndex() {reset();}
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(switchover_timestamp), K_(epoch), K_(tenant_id),
               K_(ml_pk_index), K_(pkey_info_start_index));
};

struct ObBatchCheckRes
{
  OB_UNIS_VERSION(1);
public:
  common::ObSArray<bool> results_; //Corresponding to the above pkeys--, true means there is a master, false means no master
  CheckLeaderRpcIndex index_;
  ObBatchCheckRes() {reset();}
  ~ObBatchCheckRes() {}
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(index), K_(results));
};

struct ObRebuildIndexInRestoreArg
{
  OB_UNIS_VERSION(1);
public:
  ObRebuildIndexInRestoreArg() : tenant_id_(common::OB_INVALID_TENANT_ID) {}
  ~ObRebuildIndexInRestoreArg() {}
  bool is_valid() const { return common::OB_INVALID_TENANT_ID != tenant_id_; }
  void reset() { tenant_id_ = common::OB_INVALID_TENANT_ID; }
public:
  uint64_t tenant_id_;
  TO_STRING_KV(K_(tenant_id));
};

struct ObRsListArg
{
  OB_UNIS_VERSION(1);
public:
  ObRsListArg() : rs_list_(), master_rs_() {}
  ~ObRsListArg() {}
  bool is_valid() const { return rs_list_.count() > 0 && master_rs_.is_valid(); }
  void reset() { rs_list_.reset(); master_rs_.reset(); }
  TO_STRING_KV(K_(rs_list), K_(master_rs));
  int assign(const ObRsListArg &that);
  ObServerList rs_list_;
  ObAddr master_rs_;
};

#ifdef OB_BUILD_TDE_SECURITY
struct ObWaitMasterKeyInSyncArg
{
  OB_UNIS_VERSION(1);
public:
  ObWaitMasterKeyInSyncArg() : tenant_max_key_version_(), tenant_config_version_(), rs_list_arg_() {}
  TO_STRING_KV(K_(rs_list_arg));
  void reset() { tenant_max_key_version_.reset(); tenant_config_version_.reset(); rs_list_arg_.reset(); }
  int assign(const ObWaitMasterKeyInSyncArg &that);
  common::ObSEArray<std::pair<uint64_t, share::ObLeaseResponse::TLRpKeyVersion>, 10> tenant_max_key_version_;
  common::ObSEArray<std::pair<uint64_t, int64_t>, 10> tenant_config_version_;
  ObRsListArg rs_list_arg_;
};
#endif

struct ObCheckDeploymentModeArg
{
  OB_UNIS_VERSION(1);
public:
  enum ObServerMode {
    INVALID_MODE = 0
  };
  ObCheckDeploymentModeArg() : single_zone_deployment_on_(false), startup_mode_(ObServerMode::INVALID_MODE) {}
  TO_STRING_KV(K_(single_zone_deployment_on), K_(startup_mode));
  int assign(const ObCheckDeploymentModeArg &other);
  bool single_zone_deployment_on_;
  ObServerMode startup_mode_;	// not used, only as placeholder
};

struct ObPreProcessServerArg
{
  OB_UNIS_VERSION(1);
public:
  ObPreProcessServerArg() : server_(), svr_seq_(OB_INVALID_SVR_SEQ), rescue_server_() {}
  TO_STRING_KV(K_(server), K_(svr_seq), K_(rescue_server));
  bool is_valid() const { return server_.is_valid() && svr_seq_ > 0 && rescue_server_.is_valid(); }
  int init(const common::ObAddr &server, const int64_t svr_seq, const common::ObAddr &rescue_server);
public:
  common::ObAddr server_;
  int64_t svr_seq_;
  common::ObAddr rescue_server_;
};

struct ObAdminRollingUpgradeArg
{
  OB_UNIS_VERSION(1);
public:
  ObAdminRollingUpgradeArg() : stage_(OB_UPGRADE_STAGE_MAX) {}
  ~ObAdminRollingUpgradeArg() {}
  bool is_valid() const;
  TO_STRING_KV(K_(stage));

  ObUpgradeStage stage_;
};

struct ObFetchLocationArg
{
  OB_UNIS_VERSION(1);
public:
  ObFetchLocationArg()
    : vtable_type_() {}
  ~ObFetchLocationArg() {}
  int init(const share::ObVtableLocationType &vtable_type);
  int assign(const ObFetchLocationArg &other);
  const share::ObVtableLocationType &get_vtable_type() const { return vtable_type_; }
  bool is_valid() const;
  TO_STRING_KV(K_(vtable_type));
private:
  share::ObVtableLocationType vtable_type_;
};

#ifdef OB_BUILD_TDE_SECURITY
struct ObGetMasterKeyResultArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetMasterKeyResultArg() : str_(share::OB_CLOG_ENCRYPT_MASTER_KEY_LEN, 0, buf_) {}
  ~ObGetMasterKeyResultArg() {}
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(str));
  char buf_[share::OB_CLOG_ENCRYPT_MASTER_KEY_LEN];
  common::ObString str_;
};

struct ObRestoreKeyArg
{
  OB_UNIS_VERSION(1);
public:
  ObRestoreKeyArg()
    : tenant_id_(OB_INVALID_ID),
      backup_dest_(),
      encrypt_key_()
  {}
  ~ObRestoreKeyArg() {}
  bool is_valid() const { return OB_INVALID_ID != tenant_id_ && !backup_dest_.empty(); }
  int assign(const ObRestoreKeyArg &other);
  TO_STRING_KV(K_(tenant_id), K_(backup_dest), K_(encrypt_key));
  uint64_t tenant_id_;
  common::ObString backup_dest_;
  common::ObString encrypt_key_;
};

struct ObRestoreKeyResult
{
  OB_UNIS_VERSION(1);
public:
  ObRestoreKeyResult() : ret_(common::OB_ERROR) {}
  ~ObRestoreKeyResult() {}
  int assign(const ObRestoreKeyResult &other);
  void set_ret(int ret) { ret_ = ret; }
  int64_t get_ret() const { return ret_; }
  TO_STRING_KV(K_(ret));
private:
  int ret_;
};

enum RootKeyType
{
  INVALID = 0,
  DEFAULT,
  NORMAL
};

struct ObRootKeyArg
{
  OB_UNIS_VERSION(1);
public:
  ObRootKeyArg()
    : tenant_id_(OB_INVALID_ID),
      is_set_(false),
      key_type_(RootKeyType::INVALID),
      root_key_()
  {}
  ~ObRootKeyArg() {}
  bool is_valid() const { return OB_INVALID_ID != tenant_id_; }
  int assign(const ObRootKeyArg &other);
  int init_for_get(const uint64_t tenant_id);
  int init(const uint64_t tenant_id, RootKeyType key_type,
            ObString &root_key);
  TO_STRING_KV(K_(tenant_id), K_(is_set), K_(key_type), K_(root_key));
  uint64_t tenant_id_;
  bool is_set_;
  enum RootKeyType key_type_;
  common::ObString root_key_;
};

struct ObRootKeyResult
{
  OB_UNIS_VERSION(1);
public:
  ObRootKeyResult()
    : key_type_(RootKeyType::INVALID),
      root_key_(),
      allocator_()
  {}
  ~ObRootKeyResult() {}
  int assign(const ObRootKeyResult &other);
  void reset();
  TO_STRING_KV(K_(key_type), K_(root_key));
  enum RootKeyType key_type_;
  common::ObString root_key_;
private:
  common::ObArenaAllocator allocator_;
};

struct ObReloadMasterKeyArg
{
  OB_UNIS_VERSION(1);
public:
  ObReloadMasterKeyArg(): tenant_id_(OB_INVALID_ID)
  {}
  ~ObReloadMasterKeyArg() {}
  int assign(const ObReloadMasterKeyArg &other);
  bool is_valid() const { return OB_INVALID_ID != tenant_id_; }
  TO_STRING_KV(K_(tenant_id));
  uint64_t tenant_id_;
};

struct ObReloadMasterKeyResult
{
  OB_UNIS_VERSION(1);
public:
  ObReloadMasterKeyResult(): tenant_id_(OB_INVALID_ID), master_key_id_(OB_INVALID_ID)
  {}
  ~ObReloadMasterKeyResult() {}
  int assign(const ObReloadMasterKeyResult &other);
  TO_STRING_KV(K_(tenant_id), K_(master_key_id));
  uint64_t tenant_id_;
  uint64_t master_key_id_;
};
#endif

struct ObAlterUserProxyRes
{
  OB_UNIS_VERSION(1);
public:
  ObAlterUserProxyRes()
    : ret_(OB_SUCCESS)
  {}
  ~ObAlterUserProxyRes() {}
  int assign(const ObAlterUserProxyRes &other);
  void set_ret(int ret) { ret_ = ret; }
  int64_t get_ret() const { return ret_; }
  void reset() { ret_ = OB_SUCCESS; };
  TO_STRING_KV(K_(ret));
private:
  int ret_;
};

struct TenantServerUnitConfig
{
public:
  TenantServerUnitConfig()
    : tenant_id_(common::OB_INVALID_ID),
      unit_id_(common::OB_INVALID_ID),
      compat_mode_(lib::Worker::CompatMode::INVALID),
      unit_config_(),
      replica_type_(common::ObReplicaType::REPLICA_TYPE_MAX),
      if_not_grant_(false),
      is_delete_(false)
#ifdef OB_BUILD_TDE_SECURITY
      , with_root_key_(false),
      root_key_()
#endif
      {}
  int assign(const TenantServerUnitConfig &other);
  int init(const uint64_t tenant_id,
           const uint64_t unit_id,
           const lib::Worker::CompatMode compat_mode,
           const share::ObUnitConfig &unit_config,
           const common::ObReplicaType replica_type,
           const bool if_not_grant,
           const bool is_delete
#ifdef OB_BUILD_TDE_SECURITY
           , const ObRootKeyResult &root_key
#endif
           );
  int init_for_dropping(const uint64_t tenant_id,
                        const bool is_delete);
  void reset();
  uint64_t tenant_id_;
  uint64_t unit_id_;
  lib::Worker::CompatMode compat_mode_;
  share::ObUnitConfig unit_config_;
  common::ObReplicaType replica_type_;
  bool if_not_grant_;
  bool is_delete_;
#ifdef OB_BUILD_TDE_SECURITY
  bool with_root_key_;  // true if root_key_ is explicitly assigned
  ObRootKeyResult root_key_;
#endif
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id),
               K_(unit_id),
               K_(unit_config),
               K_(compat_mode),
               K_(replica_type),
               K_(if_not_grant),
               K_(is_delete)
#ifdef OB_BUILD_TDE_SECURITY
               , K_(with_root_key)
               , K_(root_key)
#endif
               );
public:
  OB_UNIS_VERSION(1);
};

enum TransToolCmd
{
  MODIFY = 0,
  DUMP = 1,
  KILL = 2
};

struct ObTrxToolArg
{
  OB_UNIS_VERSION(1);
public:
  ObTrxToolArg()
    : status_(0),
      trans_version_(0),
      end_log_ts_(0),
      cmd_(-1){}
  ~ObTrxToolArg() {}
  bool is_valid() const
  { return trans_id_.is_valid(); }
  TO_STRING_KV(K_(trans_id), K_(status), K_(trans_version),
               K_(end_log_ts), K_(cmd));
  transaction::ObTransID trans_id_;
  int32_t status_;
  int64_t trans_version_;
  int64_t end_log_ts_;
  int32_t cmd_;
};

struct ObTrxToolRes
{
  OB_UNIS_VERSION(1);
public:
  ObTrxToolRes()
    : buf_(NULL) {}
  ~ObTrxToolRes() {}
  int reset();
public:
  static const int64_t BUF_LENGTH = 2 * 1024 * 1024;
  common::ObString trans_info_;
  char *buf_;
  TO_STRING_KV(K_(trans_info));
private:
  ObArenaAllocator allocator_;
};

struct ObPhysicalRestoreResult
{
  OB_UNIS_VERSION(1);
public:
  ObPhysicalRestoreResult();
  virtual ~ObPhysicalRestoreResult() {}
  bool is_valid() const;
  int assign(const ObPhysicalRestoreResult &other);
  TO_STRING_KV(K_(job_id), K_(return_ret), K_(mod), K_(tenant_id), K_(trace_id), K_(addr));

  int64_t job_id_;
  int32_t return_ret_;
  share::PhysicalRestoreMod mod_;
  uint64_t tenant_id_;
  common::ObCurTraceId::TraceId trace_id_;
  common::ObAddr addr_;
};

struct ObRefreshTimezoneArg
{
  OB_UNIS_VERSION(1);
public:
  ObRefreshTimezoneArg() : tenant_id_(common::OB_INVALID_TENANT_ID) {}
  ObRefreshTimezoneArg(uint64_t tenant_id) : tenant_id_(tenant_id) {}
  ~ObRefreshTimezoneArg() {}
  bool is_valid() const { return common::OB_INVALID_TENANT_ID != tenant_id_; }
  TO_STRING_KV(K_(tenant_id));
  uint64_t tenant_id_;
};

struct ObCreateRestorePointArg
{
  OB_UNIS_VERSION(1);
public:
  ObCreateRestorePointArg() :
      tenant_id_(0),
      name_()
   { }
  int assign(const ObCreateRestorePointArg &arg)
  {
    int ret = common::OB_SUCCESS;
    tenant_id_ = arg.tenant_id_;
    name_ = arg.name_;
    return ret;
  }
  bool is_valid() const
  {
    return tenant_id_ > 0 && !name_.empty();
  }
  int64_t tenant_id_;
  common::ObString name_;
  TO_STRING_KV(K(tenant_id_), K(name_));
};

struct ObDropRestorePointArg
{
  OB_UNIS_VERSION(1);
public:
  ObDropRestorePointArg() :
      tenant_id_(0),
      name_()
   { }
  int assign(const ObDropRestorePointArg &arg)
  {
    int ret = common::OB_SUCCESS;
    tenant_id_ = arg.tenant_id_;
    name_ = arg.name_;
    return ret;
  }
  bool is_valid() const
  {
    return tenant_id_ > 0 && !name_.empty();
  }
  int64_t tenant_id_;
  common::ObString name_;
  TO_STRING_KV(K(tenant_id_), K(name_));
};

struct ObCheckBuildIndexTaskExistArg
{
  OB_UNIS_VERSION(1);
public:
  ObCheckBuildIndexTaskExistArg() :
      tenant_id_(0), task_id_(), scheduler_id_(0)
  {
  }

  int assign(const ObCheckBuildIndexTaskExistArg &arg)
  {
    tenant_id_ = arg.tenant_id_;
    task_id_ = arg.task_id_;
    scheduler_id_ = arg.scheduler_id_;
    return common::OB_SUCCESS;
  }

  bool is_valid() const
  {
    return tenant_id_ > 0 && task_id_.is_valid();
  }

  int64_t tenant_id_;
  sql::ObTaskID task_id_;
  uint64_t scheduler_id_;

  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(scheduler_id));
};

struct ObDDLBuildSingleReplicaRequestArg final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLBuildSingleReplicaRequestArg() : tenant_id_(OB_INVALID_ID), ls_id_(), source_tablet_id_(), dest_tablet_id_(),
                                        source_table_id_(OB_INVALID_ID), dest_schema_id_(OB_INVALID_ID),
                                        schema_version_(0), snapshot_version_(0), ddl_type_(0), task_id_(0), parallelism_(0), execution_id_(-1), tablet_task_id_(0),
                                        data_format_version_(0), consumer_group_id_(0), dest_tenant_id_(OB_INVALID_ID), dest_ls_id_(), dest_schema_version_(0),
                                        compaction_scn_(0), can_reuse_macro_block_(false), split_sstable_type_(share::ObSplitSSTableType::SPLIT_BOTH),
                                        lob_col_idxs_(), parallel_datum_rowkey_list_()
  {}
  bool is_valid() const {
    return OB_INVALID_ID != tenant_id_ && ls_id_.is_valid() && source_tablet_id_.is_valid() && dest_tablet_id_.is_valid()
           && OB_INVALID_ID != source_table_id_ && OB_INVALID_ID != dest_schema_id_ && schema_version_ > 0
           && snapshot_version_ > 0 && dest_schema_version_ > 0 && task_id_ > 0 && parallelism_ > 0 && tablet_task_id_ > 0
           && data_format_version_ > 0 && consumer_group_id_ >= 0;
  }
  int assign(const ObDDLBuildSingleReplicaRequestArg &other);
  TO_STRING_KV(K_(tenant_id), K_(dest_tenant_id), K_(ls_id), K_(dest_ls_id), K_(source_tablet_id),
    K_(dest_tablet_id), K_(source_table_id), K_(dest_schema_id), K_(schema_version), K_(dest_schema_version),
    K_(snapshot_version), K_(task_id), K_(parallelism), K_(execution_id), K_(tablet_task_id), K_(data_format_version),
    K_(consumer_group_id),
    K_(compaction_scn), K_(can_reuse_macro_block), K_(split_sstable_type),
    K_(lob_col_idxs), K_(parallel_datum_rowkey_list));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTabletID source_tablet_id_;
  ObTabletID dest_tablet_id_;
  int64_t source_table_id_;
  int64_t dest_schema_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t ddl_type_;
  int64_t task_id_;
  int64_t parallelism_;
  int64_t execution_id_;
  int64_t tablet_task_id_;
  uint64_t data_format_version_;
  int64_t consumer_group_id_;
  uint64_t dest_tenant_id_;
  share::ObLSID dest_ls_id_;
  int64_t dest_schema_version_;
  int64_t compaction_scn_;
  bool can_reuse_macro_block_;
  share::ObSplitSSTableType split_sstable_type_;
  ObSArray<uint64_t> lob_col_idxs_;
  common::ObSArray<blocksstable::ObDatumRowkey> parallel_datum_rowkey_list_;
};

struct ObDDLBuildSingleReplicaRequestResult final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLBuildSingleReplicaRequestResult()
    : ret_code_(OB_SUCCESS), row_inserted_(0), row_scanned_(0), physical_row_count_(0)
  {}
  ~ObDDLBuildSingleReplicaRequestResult() = default;
  int assign(const ObDDLBuildSingleReplicaRequestResult &other);
  TO_STRING_KV(K_(ret_code), K_(row_inserted), K_(row_scanned), K_(physical_row_count))
public:
  int64_t ret_code_;
  int64_t row_inserted_;
  int64_t row_scanned_;
  int64_t physical_row_count_;
};

struct ObDDLBuildSingleReplicaResponseArg final
{
  OB_UNIS_VERSION(1);
public:
  ObDDLBuildSingleReplicaResponseArg()
    : tenant_id_(OB_INVALID_ID),
      ls_id_(),
      tablet_id_(),
      source_table_id_(OB_INVALID_ID),
      dest_schema_id_(OB_INVALID_ID),
      ret_code_(OB_SUCCESS),
      snapshot_version_(0),
      schema_version_(0),
      task_id_(0),
      execution_id_(-1),
      row_scanned_(0),
      row_inserted_(0),
      dest_tenant_id_(OB_INVALID_ID),
      dest_ls_id_(),
      dest_schema_version_(0),
      server_addr_(),
      physical_row_count_(0)
  {}
  ~ObDDLBuildSingleReplicaResponseArg() = default;
  bool is_valid() const { return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != dest_tenant_id_
                          && ls_id_.is_valid() && dest_ls_id_.is_valid() && tablet_id_.is_valid()
                          && OB_INVALID_ID != source_table_id_ && OB_INVALID_ID != dest_schema_id_
                          && snapshot_version_ > 0 && schema_version_ > 0 && dest_schema_version_ > 0
                          && task_id_ > 0 && execution_id_ >= 0; }
  int assign(const ObDDLBuildSingleReplicaResponseArg &other);
  TO_STRING_KV(K_(tenant_id), K_(dest_tenant_id), K_(ls_id), K_(dest_ls_id),
               K_(tablet_id), K_(source_table_id), K_(dest_schema_id), K_(ret_code),
               K_(snapshot_version), K_(schema_version), K_(dest_schema_version), K_(task_id),
               K_(execution_id), K_(row_scanned), K_(row_inserted), K_(server_addr), K_(physical_row_count));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  int64_t source_table_id_;
  int64_t dest_schema_id_;
  int ret_code_;
  int64_t snapshot_version_;
  int64_t schema_version_;
  int64_t task_id_;
  int64_t execution_id_;
  int64_t row_scanned_;
  int64_t row_inserted_;
  uint64_t dest_tenant_id_;
  share::ObLSID dest_ls_id_;
  int64_t dest_schema_version_;
  common::ObAddr server_addr_;
  int64_t physical_row_count_;
};

struct ObLogReqLoadProxyRequest
{
  OB_UNIS_VERSION(1);

public:
  ObLogReqLoadProxyRequest() :
    agency_addr_seq_(),
    principal_addr_seq_(),
    principal_crashed_ts_(OB_INVALID_TIMESTAMP)
  {}
  bool is_valid() const
  {
    return agency_addr_seq_.is_valid() && principal_addr_seq_.is_valid()
           && OB_INVALID_TIMESTAMP != principal_crashed_ts_;
  }
  TO_STRING_KV(K_(agency_addr_seq), K_(principal_addr_seq));

public:
  common::ObAddrWithSeq agency_addr_seq_;
  common::ObAddrWithSeq principal_addr_seq_;
  // principal server crashed timestamp
  int64_t principal_crashed_ts_;
};

struct ObLogReqLoadProxyResponse
{
  OB_UNIS_VERSION(1);

public:
  ObLogReqLoadProxyResponse() : err_(OB_SUCCESS)
  {}
  void set_err(const int err) { err_ = err; };
  int get_err() { return err_; };
  TO_STRING_KV(K_(err));

public:
  int err_;
};

struct ObLogReqUnloadProxyRequest
{
  OB_UNIS_VERSION(1);

public:
  ObLogReqUnloadProxyRequest() : agency_addr_seq_(), principal_addr_seq_()
  {}
  bool is_valid() const
  {
    return agency_addr_seq_.is_valid() && principal_addr_seq_.is_valid();
  }
  TO_STRING_KV(K_(agency_addr_seq), K_(principal_addr_seq));

public:
  common::ObAddrWithSeq agency_addr_seq_;
  common::ObAddrWithSeq principal_addr_seq_;
};


struct ObLogReqUnloadProxyResponse
{
  OB_UNIS_VERSION(1);

public:
  ObLogReqUnloadProxyResponse() : err_(OB_SUCCESS)
  {}
  void set_err(const int err) { err_ = err; };
  int get_err() { return err_; };
  TO_STRING_KV(K_(err));

public:
  int err_;
};

struct ObLogReqLoadProxyProgressRequest
{
  OB_UNIS_VERSION(1);

public:
  ObLogReqLoadProxyProgressRequest() : agency_addr_seq_(), principal_addr_seq_()
  {}
  bool is_valid() const
  {
    return agency_addr_seq_.is_valid() && principal_addr_seq_.is_valid();
  }
  TO_STRING_KV(K_(agency_addr_seq), K_(principal_addr_seq));

public:
  common::ObAddrWithSeq agency_addr_seq_;
  common::ObAddrWithSeq principal_addr_seq_;
};

struct ObLogReqLoadProxyProgressResponse
{
  OB_UNIS_VERSION(1);

public:
  ObLogReqLoadProxyProgressResponse() : err_(OB_SUCCESS), progress_(0)
  {}
  void set_err(const int err) { err_ = err; }
  void set_progress(const int progress) { progress_ = progress; }
  int get_err() { return err_; }
  int get_progress() { return progress_; };
  TO_STRING_KV(K_(err), K_(progress));

public:
  int err_;
  int progress_;
};

#ifdef OB_BUILD_TDE_SECURITY
struct ObDumpCacheMasterKeyResultArg
{
  OB_UNIS_VERSION(1);
public:
  struct ObMasterKeyVersionPair
  {
    OB_UNIS_VERSION(1);
  public:
    ObMasterKeyVersionPair() : tenant_id_(-1), master_key_version_(-1),
                               master_key_(share::OB_CLOG_ENCRYPT_MASTER_KEY_LEN, 0, buf_) {}
    ~ObMasterKeyVersionPair() {}
    int assign(const ObMasterKeyVersionPair &other);
    TO_STRING_KV(K_(master_key_version), K_(master_key));
    int64_t tenant_id_;
    int64_t master_key_version_;
    char buf_[share::OB_CLOG_ENCRYPT_MASTER_KEY_LEN];
    common::ObString master_key_;
  };
  ObDumpCacheMasterKeyResultArg() {}
  ~ObDumpCacheMasterKeyResultArg() {}
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(master_key_version_array));
  common::ObSEArray<ObMasterKeyVersionPair, 64> master_key_version_array_;
};
#endif

struct ObDetectMasterRsArg
{
  OB_UNIS_VERSION(1);
public:
  ObDetectMasterRsArg();
  ~ObDetectMasterRsArg();
  int init(const ObAddr &addr, const int64_t &cluster_id);
  int assign(const ObDetectMasterRsArg &other);
  void reset();
  bool is_valid() const;

  void set_addr(const ObAddr &addr);
  void set_cluster_id(const int64_t &cluster_id);
  ObAddr get_addr() const;
  int64_t get_cluster_id() const;

  TO_STRING_KV(K_(addr), K_(cluster_id));
private:
  ObAddr addr_;
  int64_t cluster_id_;
};

struct ObDetectMasterRsLSResult
{
  OB_UNIS_VERSION(1);
public:
  ObDetectMasterRsLSResult();
  ~ObDetectMasterRsLSResult();
  int init(
      const common::ObRole &role,
      const common::ObAddr &master_rs,
      const share::ObLSReplica &replica,
      const share::ObLSInfo &ls_info);
  int assign(const ObDetectMasterRsLSResult &other);
  void reset();

  bool is_valid() const;
  void set_role(const common::ObRole &role);
  void set_master_rs(const common::ObAddr &master_rs);
  int set_replica(const share::ObLSReplica &replica);
  int set_ls_info(const share::ObLSInfo &ls_info);

  common::ObRole get_role() const;
  common::ObAddr get_master_rs() const;
  const share::ObLSReplica &get_replica() const;
  const share::ObLSInfo &get_ls_info() const;

  TO_STRING_KV(K_(role), K_(master_rs), K_(replica), K_(ls_info));
private:
  /*
   * INVALID_ROLE : __all_core_table's replica doesn't exist.
   * LEADER : __all_core_table's replica exists and it's leader.
   * FOLLOWER : __all_core_table's replica exists and it's follower.
   */
  common::ObRole role_;
  common::ObAddr master_rs_;               // local master_rs_ from ObRsMgr
  share::ObLSReplica replica_;      // it's valid if role_ is LEADER or FOLLOWER
  share::ObLSInfo ls_info_; // it's valid if role_ is LEADER
};

struct ObBatchBroadcastSchemaArg
{
  OB_UNIS_VERSION(1);
public:
  ObBatchBroadcastSchemaArg();
  ~ObBatchBroadcastSchemaArg();
  int init(const uint64_t tenant_id,
           const int64_t sys_schema_version,
           const common::ObIArray<share::schema::ObTableSchema> &tables);
  int assign(const ObBatchBroadcastSchemaArg &other);
  int deep_copy_tables(const common::ObIArray<share::schema::ObTableSchema> &tables);
  void reset();
  bool is_valid() const;

  uint64_t get_tenant_id() const;
  int64_t get_sys_schema_version() const;
  const common::ObIArray<share::schema::ObTableSchema> &get_tables() const;
  TO_STRING_KV(K_(tenant_id), K_(sys_schema_version), K_(tables));
private:
  uint64_t tenant_id_;
  int64_t sys_schema_version_;
  common::ObArenaAllocator allocator_;
  common::ObSArray<share::schema::ObTableSchema> tables_;
};

struct ObBatchBroadcastSchemaResult
{
  OB_UNIS_VERSION(1);
public:
  ObBatchBroadcastSchemaResult();
  ~ObBatchBroadcastSchemaResult();
  int assign(const ObBatchBroadcastSchemaResult &other);
  void reset();

  void set_ret(int ret);
  int get_ret() const;
  TO_STRING_KV(K_(ret));
private:
  int ret_;
};

struct ObRpcRemoteWriteDDLRedoLogArg
{
  OB_UNIS_VERSION(1);
public:
  ObRpcRemoteWriteDDLRedoLogArg();
  ~ObRpcRemoteWriteDDLRedoLogArg() = default;
  int init(const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const storage::ObDDLMacroBlockRedoInfo &redo_info,
           const int64_t task_id);
  bool is_valid() const { return tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && redo_info_.is_valid() && task_id_ != 0; }
  TO_STRING_KV(K_(tenant_id), K(ls_id_), K_(redo_info), K(task_id_));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  storage::ObDDLMacroBlockRedoInfo redo_info_;
  int64_t task_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRemoteWriteDDLRedoLogArg);
};

struct ObRpcRemoteWriteDDLCommitLogArg final
{
  OB_UNIS_VERSION(1);
public:
  ObRpcRemoteWriteDDLCommitLogArg();
  ~ObRpcRemoteWriteDDLCommitLogArg() = default;
  int init(const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const storage::ObITable::TableKey &table_key,
           const share::SCN &start_scn);
  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && table_key_.is_valid() && start_scn_.is_valid_and_not_min();
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(table_key), K_(start_scn), K_(table_id),
               K_(execution_id), K_(ddl_task_id));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  storage::ObITable::TableKey table_key_;
  share::SCN start_scn_;
  int64_t table_id_; // depercated
  int64_t execution_id_; // depercated
  int64_t ddl_task_id_; // depercated
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRemoteWriteDDLCommitLogArg);
};

struct ObRpcRemoteWriteDDLIncCommitLogArg final
{
  OB_UNIS_VERSION(1);
public:
  ObRpcRemoteWriteDDLIncCommitLogArg();
  ~ObRpcRemoteWriteDDLIncCommitLogArg();
  int init(const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const common::ObTabletID tablet_id,
           const common::ObTabletID lob_meta_tablet_id,
           transaction::ObTxDesc *tx_desc);
  int release();
  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && tablet_id_.is_valid() &&
           OB_NOT_NULL(tx_desc_) && tx_desc_->is_valid();
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(lob_meta_tablet_id), KP_(tx_desc));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID lob_meta_tablet_id_;
  transaction::ObTxDesc *tx_desc_;
  bool need_release_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRemoteWriteDDLIncCommitLogArg);
};

struct ObRpcRemoteWriteDDLIncCommitLogRes final
{
  OB_UNIS_VERSION(1);
public:
  ObRpcRemoteWriteDDLIncCommitLogRes() : tx_result_() {}
  ~ObRpcRemoteWriteDDLIncCommitLogRes() {}
  TO_STRING_KV(K_(tx_result));
public:
  transaction::ObTxExecResult tx_result_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRpcRemoteWriteDDLIncCommitLogRes);
};

struct ObCheckLSCanOfflineArg
{
  OB_UNIS_VERSION(1);
public:
  ObCheckLSCanOfflineArg() : tenant_id_(OB_INVALID_TENANT_ID), id_(), current_ls_status_(share::ObLSStatus::OB_LS_EMPTY) {}
  ~ObCheckLSCanOfflineArg() {}
  bool is_valid() const;
  void reset();
  int assign(const ObCheckLSCanOfflineArg &arg);
  int init(const uint64_t tenant_id,
           const share::ObLSID &id,
           const share::ObLSStatus &ls_status);
  DECLARE_TO_STRING;

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return id_;
  }
  share::ObLSStatus get_ls_status() const
  {
    return current_ls_status_;
  }
private:
  uint64_t tenant_id_;
  share::ObLSID id_;
  share::ObLSStatus current_ls_status_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckLSCanOfflineArg);
};

struct ObRegisterTxDataArg
{
  OB_UNIS_VERSION(1);

public:
  ObRegisterTxDataArg();
  ~ObRegisterTxDataArg() = default;
  bool is_valid() const;
  void reset();
  void inc_request_id(const int64_t base_request_id);
  int init(const uint64_t tenant_id,
           const transaction::ObTxDesc &tx_desc,
           const share::ObLSID &ls_id,
           const transaction::ObTxDataSourceType &type,
           const common::ObString &buf,
           const transaction::ObTxSEQ seq_no,
           const int64_t base_request_id,
           const transaction::ObRegisterMdsFlag &register_flag);
  TO_STRING_KV(K_(tenant_id),
               KPC_(tx_desc),
               K_(ls_id),
               K_(type),
               KP(buf_.length()),
               K_(seq_no),
               K_(request_id),
               K_(register_flag));

public:
  uint64_t tenant_id_;
  transaction::ObTxDesc *tx_desc_;
  share::ObLSID ls_id_;
  transaction::ObTxDataSourceType type_;
  common::ObString buf_;
  transaction::ObTxSEQ seq_no_;
  int64_t request_id_;

  transaction::ObRegisterMdsFlag register_flag_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRegisterTxDataArg);
};

struct ObRegisterTxDataResult
{
  OB_UNIS_VERSION(1);
public:
  ObRegisterTxDataResult() : result_(common::OB_SUCCESS), tx_result_() {}
  ~ObRegisterTxDataResult() {}
  bool is_valid() const;
  void reset();
  int init(const transaction::ObTxExecResult &tx_result);
  TO_STRING_KV(K_(result), K_(tx_result));
public:
  int64_t result_;
  transaction::ObTxExecResult tx_result_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObRegisterTxDataResult);
};

struct ObRemoveSysLsArg
{
  OB_UNIS_VERSION(1);
public:
  ObRemoveSysLsArg() : server_() {}
  explicit ObRemoveSysLsArg(const common::ObAddr &server) : server_(server) {}
  bool is_valid() const { return server_.is_valid(); }
  int init(const common::ObAddr &server);
  int assign(const ObRemoveSysLsArg &other);
public:
  common::ObAddr server_;
  TO_STRING_KV(K_(server));
};

struct ObQueryLSIsValidMemberRequest
{
  OB_UNIS_VERSION(1);
public:
  ObQueryLSIsValidMemberRequest() : tenant_id_(0), self_addr_(), ls_array_() {}
  ~ObQueryLSIsValidMemberRequest() {}
  void reset()
  {
    self_addr_.reset();
    ls_array_.reset();
  }

  uint64_t tenant_id_;
  common::ObAddr self_addr_;
  share::ObLSArray ls_array_;
  TO_STRING_KV(K(tenant_id_), K(self_addr_), K(ls_array_));
};

enum LogMemberGCStat
{
  LOG_MEMBER_GC_STAT_INVALID = 0,
  LOG_MEMBER_NORMAL_GC_STAT = 1,
  LOG_LEARNER_IN_MIGRATING = 2,
  LOG_MEMBER_GC_STAT_MAX = 256
};

struct ObQueryLSIsValidMemberResponse
{
  OB_UNIS_VERSION(1);
public:
  ObQueryLSIsValidMemberResponse() : ret_value_(common::OB_SUCCESS), ls_array_(),
    candidates_status_(), ret_array_(), gc_stat_array_() {}
  ~ObQueryLSIsValidMemberResponse() {}
  void reset()
  {
    ret_value_ = common::OB_SUCCESS;
    ls_array_.reset();
    candidates_status_.reset();
    ret_array_.reset();
    gc_stat_array_.reset();
  }

  int ret_value_;
  share::ObLSArray ls_array_;
  common::ObSEArray<bool, 16> candidates_status_;
  common::ObSEArray<int, 16> ret_array_;
  common::ObSEArray<LogMemberGCStat, 16> gc_stat_array_;
  TO_STRING_KV(K(ret_value_), K(ls_array_), K(candidates_status_), K(ret_array_), K(gc_stat_array_));
};

struct ObSwitchSchemaResult
{
  OB_UNIS_VERSION(1);
public:
  ObSwitchSchemaResult() : ret_(common::OB_SUCCESS) {}
  ~ObSwitchSchemaResult() {}
  int assign(const ObSwitchSchemaResult &other)
  {
    ret_ = other.ret_;
    return common::OB_SUCCESS;
  }
  void reset() { ret_ = common::OB_SUCCESS; }
  void set_ret(int ret) { ret_ = ret; }
  int get_ret() const { return ret_; }
  TO_STRING_KV(K_(ret));
private:
  int ret_;
};

struct ObContextDDLArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObContextDDLArg():
      ObDDLArg(),
      stmt_type_(common::OB_INVALID_ID),
      ctx_schema_(),
      or_replace_(false)
  {}
  int assign(const ObContextDDLArg &other);
  bool is_valid() const
  {
    return OB_INVALID_ID != ctx_schema_.get_tenant_id()
           && 0 != ctx_schema_.get_namespace().length();
  }
  virtual bool is_allow_when_upgrade() const { return true; }
  void set_stmt_type(int64_t type)
  {
    stmt_type_ = type;
  }
  int64_t get_stmt_type() const
  {
    return stmt_type_;
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    ctx_schema_.set_tenant_id(tenant_id);
  }
  void set_context_id(const uint64_t context_id)
  {
    ctx_schema_.set_context_id(context_id);
  }
  int set_namespace(const ObString &ctx_namespace)
  {
    return ctx_schema_.set_namespace(ctx_namespace);
  }
  int set_schema_name(const ObString &schema_name)
  {
    return ctx_schema_.set_schema_name(schema_name);
  }
  int set_package_name(const ObString &package_name)
  {
    return ctx_schema_.set_trusted_package(package_name);
  }
  void set_context_type(ObContextType type)
  {
    ctx_schema_.set_context_type(type);
  }
  void set_origin_con_id(int64_t id)
  {
    ctx_schema_.set_origin_con_id(id);
  }
  void set_is_tracking(bool is_tracking)
  {
    ctx_schema_.set_is_tracking(is_tracking);
  }
  share::schema::ObContextSchema &context_schema()
  {
    return ctx_schema_;
  }
  TO_STRING_KV(K_(stmt_type), K_(ctx_schema));
public:
  int64_t stmt_type_;
  share::schema::ObContextSchema ctx_schema_;
  bool or_replace_;
};

struct ObTenantConfigArg
{
  OB_UNIS_VERSION(1);
public:
  ObTenantConfigArg() : tenant_id_(0), config_str_() {}
  bool is_valid() const { return tenant_id_ > 0 && !config_str_.empty(); }
  int assign(const ObTenantConfigArg &other);
  int64_t tenant_id_;
  common::ObString config_str_;
  TO_STRING_KV(K_(tenant_id), K_(config_str));
};

struct ObCheckBackupConnectivityArg final
{
  OB_UNIS_VERSION(1);
public:
  ObCheckBackupConnectivityArg();
  ~ObCheckBackupConnectivityArg() {}
public:
  int assign(const ObCheckBackupConnectivityArg &res);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(backup_path), K_(check_path));
public:
  uint64_t tenant_id_;
  char backup_path_[share::OB_MAX_BACKUP_PATH_LENGTH];
  char check_path_[share::OB_MAX_BACKUP_CHECK_FILE_LENGTH];
};
struct ObModifyPlanBaselineArg
{
  OB_UNIS_VERSION(1);
public:
  enum Action {
    INVALID = 0,
    ACCEPT = 1,
    EVICT_EVOLVE = 1 << 1,
    EVICT_BASELINE = 1 << 2,
    EVICT_ALL = 1 << 3,
    MAX_ACTION = 1 << 4
  };
  ObModifyPlanBaselineArg()
  : tenant_id_(),
    database_id_(0),
    sql_id_(),
    with_plan_hash_(false),
    plan_hash_(0)
  {}
  bool is_valid() const
  {
    return database_id_ > 0 && action_ > Action::INVALID && action_ < Action::MAX_ACTION;
  }
  int assign(const ObModifyPlanBaselineArg &other);

  int64_t tenant_id_;
  uint64_t database_id_;
  common::ObString sql_id_;
  bool with_plan_hash_;
  uint64_t plan_hash_;
  Action action_;
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(sql_id), K_(with_plan_hash), K_(plan_hash), K_(action));
};

struct ObLoadPlanBaselineArg
{
  OB_UNIS_VERSION(1);
public:
  ObLoadPlanBaselineArg()
  : tenant_id_(),
    database_id_(0),
    sql_id_(),
    with_plan_hash_(false),
    plan_hash_value_(common::OB_INVALID_ID),
    fixed_(false),
    enabled_(true)
  {}
  virtual ~ObLoadPlanBaselineArg() {}
  int assign(const ObLoadPlanBaselineArg &other);
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(sql_id), K_(with_plan_hash), K_(plan_hash_value), K_(fixed), K_(enabled));

  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString sql_id_;
  bool with_plan_hash_;
  uint64_t plan_hash_value_;
  bool fixed_;
  bool enabled_;
};

struct ObFlushOptStatArg
{
  OB_UNIS_VERSION(1);
public:
  ObFlushOptStatArg() : tenant_id_(0), is_flush_col_usage_(false), is_flush_dml_stat_(false) {}
  ObFlushOptStatArg(const uint64_t tenant_id,
                    const bool is_flush_col_usage,
                    const bool is_flush_dml_stat) :
    tenant_id_(tenant_id),
    is_flush_col_usage_(is_flush_col_usage),
    is_flush_dml_stat_(is_flush_dml_stat)
  {}
  bool is_valid() const { return tenant_id_ > 0; }
  int assign(const ObFlushOptStatArg &other);
  uint64_t tenant_id_;
  bool is_flush_col_usage_;
  bool is_flush_dml_stat_;
  TO_STRING_KV(K_(tenant_id), K_(is_flush_col_usage), K_(is_flush_dml_stat));
};

struct ObCancelDDLTaskArg final
{
  OB_UNIS_VERSION(1);
public:
  ObCancelDDLTaskArg();
  explicit ObCancelDDLTaskArg(const ObCurTraceId::TraceId &task_id);
  ~ObCancelDDLTaskArg() = default;
  bool is_valid() const { return !task_id_.is_invalid(); }
  int assign(const ObCancelDDLTaskArg &other);
  const ObCurTraceId::TraceId &get_task_id() const{ return task_id_; }
  TO_STRING_KV(K_(task_id));
private:
  ObCurTraceId::TraceId task_id_;
};

struct ObEstBlockArgElement
{
  OB_UNIS_VERSION(1);
public:
  ObEstBlockArgElement() : tenant_id_(0), tablet_id_(), ls_id_(), column_group_ids_() {}
  bool is_valid() const { return tenant_id_ > 0 && tablet_id_.is_valid() && ls_id_.is_valid(); }
  int assign(const ObEstBlockArgElement &other);
  uint64_t tenant_id_;
  ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  common::ObSEArray<uint64_t, 4> column_group_ids_;
  TO_STRING_KV(K_(tenant_id), K_(tablet_id), K_(ls_id), K_(column_group_ids));
};

struct ObEstBlockArg
{
  OB_UNIS_VERSION(1);
public:
  common::ObSEArray<ObEstBlockArgElement, 4> tablet_params_arg_;
  bool is_valid() const;
  int assign(const ObEstBlockArg &other);
  ObEstBlockArg() : tablet_params_arg_() {}
  TO_STRING_KV(K(tablet_params_arg_));
};

struct ObEstBlockResElement
{
  OB_UNIS_VERSION(1);
public:
  int64_t macro_block_count_;
  int64_t micro_block_count_;
  int64_t sstable_row_count_;
  int64_t memtable_row_count_;
  common::ObSEArray<int64_t, 4> cg_macro_cnt_arr_;
  common::ObSEArray<int64_t, 4> cg_micro_cnt_arr_;
  bool is_valid() const { return true; }
  int assign(const ObEstBlockResElement &other);
  ObEstBlockResElement() : macro_block_count_(0), micro_block_count_(0), sstable_row_count_(0), memtable_row_count_(0) {}
  TO_STRING_KV(K(macro_block_count_), K(micro_block_count_),
      K(sstable_row_count_), K(memtable_row_count_), K(cg_macro_cnt_arr_), K(cg_micro_cnt_arr_));
};

struct ObEstBlockRes
{
  OB_UNIS_VERSION(1);
public:
  common::ObSEArray<ObEstBlockResElement, 4> tablet_params_res_;
  bool is_valid() const { return true; }
  int assign(const ObEstBlockRes &other);
  ObEstBlockRes() : tablet_params_res_() {}
  TO_STRING_KV(K(tablet_params_res_));
};

class ObReportBackupJobResultArg final
{
  OB_UNIS_VERSION(1);
public:
  ObReportBackupJobResultArg();
  ~ObReportBackupJobResultArg() {}
public:
  int assign(const ObReportBackupJobResultArg &that);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(job_id), K_(result));
public:
  uint64_t tenant_id_;
  int64_t job_id_;
  int result_;
};

struct ObBatchGetTabletAutoincSeqArg final
{
  OB_UNIS_VERSION(1);
public:
  ObBatchGetTabletAutoincSeqArg()
    : tenant_id_(OB_INVALID_ID), ls_id_(), src_tablet_ids_(), dest_tablet_ids_()
  {}
  ~ObBatchGetTabletAutoincSeqArg() {}
public:
  int assign(const ObBatchGetTabletAutoincSeqArg &other);
  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && src_tablet_ids_.count() > 0
            && src_tablet_ids_.count() == dest_tablet_ids_.count();
  }
  int init(const uint64_t tenant_id_, const share::ObLSID &ls_id, const ObIArray<share::ObMigrateTabletAutoincSeqParam> &params);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(src_tablet_ids), K_(dest_tablet_ids));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObSEArray<common::ObTabletID, 1> src_tablet_ids_;
  common::ObSEArray<common::ObTabletID, 1> dest_tablet_ids_;
};

struct ObBatchGetTabletAutoincSeqRes final
{
  OB_UNIS_VERSION(1);
public:
  ObBatchGetTabletAutoincSeqRes() : autoinc_params_() {}
  ~ObBatchGetTabletAutoincSeqRes() {}
public:
  int assign(const ObBatchGetTabletAutoincSeqRes &other);
  bool is_valid() const { return autoinc_params_.count() > 0; }
  TO_STRING_KV(K_(autoinc_params));
public:
  common::ObSEArray<share::ObMigrateTabletAutoincSeqParam, 1> autoinc_params_;
};

struct ObBatchSetTabletAutoincSeqArg final
{
  OB_UNIS_VERSION(1);
public:
  ObBatchSetTabletAutoincSeqArg()
    : tenant_id_(OB_INVALID_ID), ls_id_(), autoinc_params_()
  {}
  ~ObBatchSetTabletAutoincSeqArg() {}
public:
  int assign(const ObBatchSetTabletAutoincSeqArg &other);
  bool is_valid() const { return tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && autoinc_params_.count() > 0; }
  int init(const uint64_t tenant_id_, const share::ObLSID &ls_id, const ObIArray<share::ObMigrateTabletAutoincSeqParam> &params);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(autoinc_params));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObSEArray<share::ObMigrateTabletAutoincSeqParam, 1> autoinc_params_;
};

struct ObBatchSetTabletAutoincSeqRes final
{
  OB_UNIS_VERSION(1);
public:
  ObBatchSetTabletAutoincSeqRes() : autoinc_params_() {}
  ~ObBatchSetTabletAutoincSeqRes() {}
public:
  int assign(const ObBatchSetTabletAutoincSeqRes &other);
  bool is_valid() const { return autoinc_params_.count() > 0; }
  TO_STRING_KV(K_(autoinc_params));
public:
  common::ObSEArray<share::ObMigrateTabletAutoincSeqParam, 1> autoinc_params_;
};

struct ObBatchGetTabletBindingArg final
{
  OB_UNIS_VERSION(1);
public:
  ObBatchGetTabletBindingArg()
    : tenant_id_(OB_INVALID_ID), ls_id_(), tablet_ids_(), check_committed_(false)
  {}
  ~ObBatchGetTabletBindingArg() {}
public:
  int assign(const ObBatchGetTabletBindingArg &other);
  bool is_valid() const { return tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && tablet_ids_.count() > 0; }
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, const common::ObIArray<common::ObTabletID> &tablet_ids, const bool check_committed);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_ids), K_(check_committed));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObSArray<common::ObTabletID> tablet_ids_;
  bool check_committed_;
};

struct ObBatchGetTabletBindingRes final
{
  OB_UNIS_VERSION(1);
public:
  ObBatchGetTabletBindingRes() : binding_datas_() {}
  ~ObBatchGetTabletBindingRes() {}
public:
  int assign(const ObBatchGetTabletBindingRes &other);
  bool is_valid() const { return binding_datas_.count() > 0; }
  TO_STRING_KV(K_(binding_datas));
public:
  common::ObSArray<ObTabletBindingMdsUserData> binding_datas_;
};

struct ObFetchLocationResult
{
public:
  OB_UNIS_VERSION(1);
public:
  ObFetchLocationResult() : servers_() {}
public:
  int assign(const ObFetchLocationResult &other);
  bool is_valid() const { return !servers_.empty();}
  int set_servers(const common::ObSArray<common::ObAddr> &servers);
  const common::ObSArray<common::ObAddr> &get_servers() { return servers_; }
  TO_STRING_KV(K_(servers));
private:
  common::ObSArray<common::ObAddr> servers_;
};

struct ObStartTransferTaskArg final
{
  OB_UNIS_VERSION(1);
public:
  ObStartTransferTaskArg(): tenant_id_(OB_INVALID_TENANT_ID), task_id_(), src_ls_() {}
  ~ObStartTransferTaskArg() {}
  int init(const uint64_t tenant_id, const share::ObTransferTaskID &task_id, const share::ObLSID &src_ls);
  uint64_t get_tenant_id() const { return tenant_id_; }
  const share::ObTransferTaskID get_task_id() const { return task_id_; }
  const share::ObLSID &get_src_ls() { return src_ls_; }
  bool is_valid() const { return is_valid_tenant_id(tenant_id_) && task_id_.is_valid(); }
  int assign(const ObStartTransferTaskArg &other);
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(src_ls));

private:
  uint64_t tenant_id_;
  share::ObTransferTaskID task_id_;
  share::ObLSID src_ls_;

  DISALLOW_COPY_AND_ASSIGN(ObStartTransferTaskArg);
};

struct ObFinishTransferTaskArg final
{
  OB_UNIS_VERSION(1);
public:
  ObFinishTransferTaskArg(): task_id_() {}
  int init(const uint64_t tenant_id, const share::ObTransferTaskID &task_id);
  uint64_t get_tenant_id() const { return tenant_id_; }
  const share::ObTransferTaskID get_task_id() const { return task_id_; }
  bool is_valid() const { return is_valid_tenant_id(tenant_id_) && task_id_.is_valid(); }
  int assign(const ObFinishTransferTaskArg &other);
  TO_STRING_KV(K_(tenant_id), K_(task_id));

private:
  uint64_t tenant_id_;
  share::ObTransferTaskID task_id_;

  DISALLOW_COPY_AND_ASSIGN(ObFinishTransferTaskArg);
};

#ifdef OB_BUILD_ARBITRATION
struct ObArbGCNotifyArg
{
public:
  OB_UNIS_VERSION(1);
public:
  static const int64_t DEFAULT_NUM = 16;
public:
  ObArbGCNotifyArg() {}
  ~ObArbGCNotifyArg() {}
  int assign(const ObArbGCNotifyArg &other);
  int init(const arbserver::GCMsgEpoch &epoch,
           const arbserver::TenantLSIDSArray &ls_ids);
  bool is_valid() const;
  const arbserver::GCMsgEpoch &get_epoch() const;
  arbserver::TenantLSIDSArray &get_ls_ids();
  TO_STRING_KV(K_(epoch), K_(ls_ids));
private:
  DISALLOW_COPY_AND_ASSIGN(ObArbGCNotifyArg);
  arbserver::GCMsgEpoch epoch_;
  arbserver::TenantLSIDSArray ls_ids_;
};

struct ObArbGCNotifyResult
{
public:
  OB_UNIS_VERSION(1);
public:
  ObArbGCNotifyResult(): ret_(common::OB_SUCCESS) {}
  ~ObArbGCNotifyResult() {}
  bool is_valid() const
  {
    return true;
  }
  int assign(const ObArbGCNotifyResult &other)
  {
    ret_ = other.ret_;
    return common::OB_SUCCESS;
  }
  void set_result(const int ret)
  {
    ret_ = ret;
  }
  int get_result() const
  {
    return ret_;
  }
  TO_STRING_KV(K_(ret));
private:
  DISALLOW_COPY_AND_ASSIGN(ObArbGCNotifyResult);
private:
  int ret_;
};

struct ObArbClusterOpArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObArbClusterOpArg()
      : type_(MSG_TYPE::INVALID_MSG),
        cluster_id_(OB_INVALID_CLUSTER_ID),
        cluster_name_(),
        epoch_() {}
  ~ObArbClusterOpArg() { reset(); }
  void reset();
  int assign(const ObArbClusterOpArg &other);
  int init(const int64_t cluster_id,
           const ObString &cluster_name,
           const arbserver::GCMsgEpoch &epoch,
           const bool is_add_arb);
  bool is_valid() const;
  int64_t get_cluster_id() const { return cluster_id_; }
  const ObString &get_cluster_name() const { return cluster_name_; }
  const arbserver::GCMsgEpoch &get_epoch() const { return epoch_; }
  bool is_cluster_add_arb_req() const { return type_ == MSG_TYPE::CLUSTER_ADD_ARB; }
  TO_STRING_KV(K_(type), K_(cluster_id), K_(cluster_name), K_(epoch));
private:
  enum MSG_TYPE
  {
    INVALID_MSG = 0,
    CLUSTER_ADD_ARB,
    CLUSTER_REMOVE_ARB,
  };
  DISALLOW_COPY_AND_ASSIGN(ObArbClusterOpArg);
private:
  MSG_TYPE type_;
  int64_t cluster_id_;
  common::ObString cluster_name_;
  arbserver::GCMsgEpoch epoch_;
};

struct ObArbClusterOpResult
{
public:
  OB_UNIS_VERSION(1);
public:
  ObArbClusterOpResult(): ret_(common::OB_SUCCESS) {}
  ~ObArbClusterOpResult() {}
  bool is_valid() const { return true; }
  int assign(const ObArbClusterOpResult &other)
  {
    ret_ = other.ret_;
    return common::OB_SUCCESS;
  }
  void set_result(const int ret)
  {
    ret_ = ret;
  }
  int get_result() const
  {
    return ret_;
  }
  TO_STRING_KV(K_(ret));
private:
  DISALLOW_COPY_AND_ASSIGN(ObArbClusterOpResult);
private:
  int ret_;
};
#endif

struct ObSyncRewriteRuleArg
{
  OB_UNIS_VERSION(1);
public:
  ObSyncRewriteRuleArg(): tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  ~ObSyncRewriteRuleArg() {}
  int assign(const ObSyncRewriteRuleArg &other)
  {
    tenant_id_ = other.tenant_id_;
    return common::OB_SUCCESS;
  }
  bool is_valid() const { return common::OB_INVALID_TENANT_ID != tenant_id_; }
  TO_STRING_KV(K_(tenant_id));

  uint64_t tenant_id_;
};

struct ObGetLeaderLocationsArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetLeaderLocationsArg() : addr_() {}
  ~ObGetLeaderLocationsArg() {}
  bool is_valid() const { return addr_.is_valid(); }
  void reset() { addr_.reset(); }
  int assign(const ObGetLeaderLocationsArg &other)
  {
    int ret = common::OB_SUCCESS;
    addr_ = other.addr_;
    return ret;
  }
  void set_addr(const common::ObAddr &addr) { addr_ = addr; }
  TO_STRING_KV(K_(addr));
private:
  common::ObAddr addr_;
};

struct ObGetLeaderLocationsResult
{
  OB_UNIS_VERSION(1);
public:
  ObGetLeaderLocationsResult() : addr_(), leader_replicas_() {}
  ~ObGetLeaderLocationsResult() {}
  bool is_valid() const { return addr_.is_valid(); }
  void reset()
  {
    addr_.reset();
    leader_replicas_.reset();
  }
  int assign(const ObGetLeaderLocationsResult &other)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(leader_replicas_.assign(other.leader_replicas_))) {
      SHARE_LOG(WARN, "fail to assign leader replicas", KR(ret));
    } else {
      addr_ = other.addr_;
    }
    return ret;
  }
  const ObAddr &get_addr() const { return addr_; }
  const common::ObIArray<share::ObLSLeaderLocation> &get_leader_replicas() const
  {
    return leader_replicas_;
  }
  void set_addr(const common::ObAddr &addr) { addr_ = addr; }
  int add_leader_replica(const share::ObLSLeaderLocation &replica)
  {
    return leader_replicas_.push_back(replica);
  }
  TO_STRING_KV(K_(addr), K_(leader_replicas));
private:
  common::ObAddr addr_;
  common::ObSArray<share::ObLSLeaderLocation> leader_replicas_;
};

struct ObInitTenantConfigArg
{
  OB_UNIS_VERSION(1);
public:
  ObInitTenantConfigArg() : tenant_configs_() {}
  ~ObInitTenantConfigArg() {}
  bool is_valid() const { return tenant_configs_.count() > 0; }
  int assign(const ObInitTenantConfigArg &other);
  int add_tenant_config(const ObTenantConfigArg &arg);
  const common::ObSArray<ObTenantConfigArg> &get_tenant_configs() const { return tenant_configs_; }
  TO_STRING_KV(K_(tenant_configs));
private:
  common::ObSArray<ObTenantConfigArg> tenant_configs_;
};

struct ObInitTenantConfigRes
{
  OB_UNIS_VERSION(1);
public:
  ObInitTenantConfigRes() : ret_(common::OB_ERROR) {}
  ~ObInitTenantConfigRes() {}
  int assign(const ObInitTenantConfigRes &other);
  void set_ret(int ret) { ret_ = ret; }
  int64_t get_ret() const { return ret_; }
  TO_STRING_KV(K_(ret));
private:
  int ret_;
};

struct ObRlsPolicyDDLArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  enum AlterOption {
    ENABLE = 1,
    ADD_ATTRIBUTE,
    REMOVE_ATTRIBUTE,
    MAX_OPTION = 100
  };
  ObRlsPolicyDDLArg() : ObDDLArg(), schema_(), ddl_type_(), option_bitset_() {}
  int assign(const ObRlsPolicyDDLArg &other);
  TO_STRING_KV(K_(schema), K_(ddl_type), K_(option_bitset));
  share::schema::ObRlsPolicySchema schema_;
  share::schema::ObSchemaOperationType ddl_type_;
  common::ObBitSet<> option_bitset_;
};

struct ObRlsGroupDDLArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObRlsGroupDDLArg() : ObDDLArg(), schema_(), ddl_type_() {}
  int assign(const ObRlsGroupDDLArg &other);
  TO_STRING_KV(K_(schema), K_(ddl_type));
  share::schema::ObRlsGroupSchema schema_;
  share::schema::ObSchemaOperationType ddl_type_;
};

struct ObRlsContextDDLArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObRlsContextDDLArg() : ObDDLArg(), schema_(), ddl_type_() {}
  int assign(const ObRlsContextDDLArg &other);
  TO_STRING_KV(K_(schema), K_(ddl_type));
  share::schema::ObRlsContextSchema schema_;
  share::schema::ObSchemaOperationType ddl_type_;
};

struct ObRecompileAllViewsBatchArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObRecompileAllViewsBatchArg()
    : tenant_id_(OB_INVALID_ID),
      view_ids_()
  {
  }
  bool is_valid() const { return tenant_id_ != OB_INVALID_ID && !view_ids_.empty(); }
  int assign(const ObRecompileAllViewsBatchArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(view_ids));

  uint64_t tenant_id_;
  ObSArray<uint64_t> view_ids_;
};

struct ObTryAddDepInofsForSynonymBatchArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObTryAddDepInofsForSynonymBatchArg()
    : tenant_id_(OB_INVALID_ID),
      synonym_ids_()
  {
  }
  bool is_valid() const { return tenant_id_ != OB_INVALID_ID && !synonym_ids_.empty(); }
  int assign(const ObTryAddDepInofsForSynonymBatchArg &other);
  virtual bool is_allow_when_upgrade() const { return true; }
  TO_STRING_KV(K_(tenant_id), K_(synonym_ids));

  uint64_t tenant_id_;
  ObSArray<uint64_t> synonym_ids_;
};

// session info self-verification arg
struct ObSessInfoVerifyArg
{
  OB_UNIS_VERSION(1);
public:
  ObSessInfoVerifyArg() : sess_id_(0), proxy_sess_id_(0) {}
  ~ObSessInfoVerifyArg() {}
  bool is_valid() const;
  void reset() { sess_id_ = 0;
                 proxy_sess_id_ = 0; }
  int assign(const ObSessInfoVerifyArg &other)
  {
    int ret = common::OB_SUCCESS;
    sess_id_ = other.sess_id_;
    proxy_sess_id_ = other.proxy_sess_id_;
    return ret;
  }
  void set_sess_id(uint32_t sess_id) { sess_id_ = sess_id; }
  uint32_t get_sess_id() { return sess_id_; }
  void set_proxy_sess_id(uint64_t proxy_sess_id) { proxy_sess_id_ = proxy_sess_id; }
  uint64_t get_proxy_sess_id() { return proxy_sess_id_; }
  TO_STRING_KV(K_(sess_id), K_(proxy_sess_id));
private:
  uint32_t sess_id_;
  uint64_t proxy_sess_id_;
};

// session info self-verification result
struct ObSessionInfoVeriRes
{
  OB_UNIS_VERSION(1);
public:
  ObSessionInfoVeriRes() : verify_info_buf_(),
    allocator_(common::ObModIds::OB_SQL_SESSION), need_verify_(false) {}
  ~ObSessionInfoVeriRes() {}
  bool is_valid() const;
  void reset() {  verify_info_buf_.reset();
                  need_verify_ = false;
                  allocator_.reset(); }
  int assign(const ObSessionInfoVeriRes &other)
  {
    int ret = common::OB_SUCCESS;
    verify_info_buf_.assign_ptr(other.verify_info_buf_.ptr(),
                            static_cast<int32_t>(other.verify_info_buf_.length()));
    need_verify_ = other.need_verify_;
    return ret;
  }
  void set_info_check_buf(common::ObString verify_info_buf) { verify_info_buf_ = verify_info_buf; }
  common::ObString &get_info_check_buf() {return verify_info_buf_;}
  TO_STRING_KV(K_(verify_info_buf), K_(need_verify));
  common::ObString verify_info_buf_;
  common::ObArenaAllocator allocator_;
  bool need_verify_;
};

struct ObGetServerResourceInfoArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetServerResourceInfoArg() : rs_addr_() {}
  TO_STRING_KV(K_(rs_addr));
  int init(const common::ObAddr &rs_addr);
  int assign(const ObGetServerResourceInfoArg &other);
  bool is_valid() const;
  void reset();
  const common::ObAddr &get_rs_addr() const { return rs_addr_; }
private:
  common::ObAddr rs_addr_;
};

struct ObGetServerResourceInfoResult
{
  OB_UNIS_VERSION(1);
public:
  ObGetServerResourceInfoResult() : server_(), resource_info_() {}
  TO_STRING_KV(K_(server), K_(resource_info));
  int init(const common::ObAddr &server, const share::ObServerResourceInfo &resource_info);
  int assign(const ObGetServerResourceInfoResult &other);
  bool is_valid() const;
  void reset();
  const common::ObAddr &get_server() const { return server_; }
  const share::ObServerResourceInfo &get_resource_info() const { return resource_info_; }
private:
  common::ObAddr server_;
  share::ObServerResourceInfo resource_info_;
};

struct ObBroadcastConsensusVersionArg
{
  OB_UNIS_VERSION(1);
public:
  ObBroadcastConsensusVersionArg()
    : tenant_id_(OB_INVALID_ID),
      consensus_version_(OB_INVALID_VERSION) {}
  ~ObBroadcastConsensusVersionArg() {}
  int init(const uint64_t tenant_id, const int64_t consensus_version);
  bool is_valid() const;
  int assign(const ObBroadcastConsensusVersionArg &other);
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int64_t get_consensus_version() const
  {
    return consensus_version_;
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  void set_consensus_version(const int64_t consensus_version)
  {
    consensus_version_ = consensus_version;
  }
  TO_STRING_KV(K_(tenant_id), K_(consensus_version));
private:
  DISALLOW_COPY_AND_ASSIGN(ObBroadcastConsensusVersionArg);
private:
  uint64_t tenant_id_;
  int64_t consensus_version_;
};

struct ObBroadcastConsensusVersionRes
{
  OB_UNIS_VERSION(1);
public:
  ObBroadcastConsensusVersionRes()
    : ret_(OB_SUCCESS) {}
  ~ObBroadcastConsensusVersionRes() {}
  void set_ret(int ret) { ret_ = ret; }
  TO_STRING_KV(K_(ret));
private:
  DISALLOW_COPY_AND_ASSIGN(ObBroadcastConsensusVersionRes);
private:
  int ret_;
};

struct ObRecoverTableArg
{
public:
  OB_UNIS_VERSION(1);
public:
  enum Action
  {
    INITIATE = 0,
    CANCEL,
    MAX
  };
  ObRecoverTableArg();
  ~ObRecoverTableArg() {}
  bool is_valid() const;
  int assign(const ObRecoverTableArg &that);
  TO_STRING_KV(K_(tenant_id), K_(tenant_name), K_(import_arg), K_(restore_tenant_arg), K_(action));
public:
  uint64_t tenant_id_; // tenant which is the table recover to.
  common::ObString tenant_name_;
  share::ObImportArg import_arg_;
  ObPhysicalRestoreTenantArg restore_tenant_arg_;
  Action action_;
};

struct ObLoadBaselineRes
{
  OB_UNIS_VERSION(1);
public:
  ObLoadBaselineRes()
    : load_count_(0)
  {}
  int assign(const ObLoadBaselineRes &other);
  TO_STRING_KV(K_(load_count));
  uint64_t load_count_;
};

struct ObTTLRequestArg final
{
  OB_UNIS_VERSION(1);
public:
  enum TTLRequestType {
    TTL_TRIGGER_TYPE = 0,
    TTL_SUSPEND_TYPE = 1,
    TTL_RESUME_TYPE = 2,
    TTL_CANCEL_TYPE = 3,
    TTL_MOVE_TYPE = 4,
    TTL_INVALID_TYPE = 5
  };

  ObTTLRequestArg()
    : cmd_code_(-1), trigger_type_(-1), task_id_(OB_INVALID_ID), tenant_id_(OB_INVALID_ID)
  {}
  ~ObTTLRequestArg() = default;
  bool is_valid() const {
    // return cmd_code_ != -1 && OB_INVALID_ID != task_id_ && trigger_type_ != -1 && tenant_id_ != OB_INVALID_ID;
    return cmd_code_ != -1 && trigger_type_ != -1 && tenant_id_ != OB_INVALID_ID;
  }
  int assign(const ObTTLRequestArg &other);
  TO_STRING_KV(K_(cmd_code), K_(trigger_type), K_(task_id), K_(tenant_id));
public:
  int32_t cmd_code_; // enum TTLCmdType
  int32_t trigger_type_; // system or user
  int64_t task_id_;  // task id
  uint64_t tenant_id_; // tenand_id array
};

struct ObTTLResponseArg {
  OB_UNIS_VERSION(1);

public:
  ObTTLResponseArg();
  int assign(const ObTTLResponseArg &other);
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(server_addr), K_(task_status));
public:
  uint64_t tenant_id_;
  int64_t task_id_;
  ObAddr server_addr_;
  uint8_t task_status_;
  int err_code_;
};
struct ObAdminUnlockMemberListOpArg final
{
  OB_UNIS_VERSION(1);
public:
  ObAdminUnlockMemberListOpArg();
  ~ObAdminUnlockMemberListOpArg();
  int set(const uint64_t tenant_id,
          const share::ObLSID &ls_id,
          const int64_t lock_id);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(lock_id));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t lock_id_;
};
// kill client session arg
struct ObKillClientSessionArg
{
  OB_UNIS_VERSION(1);
public:
  ObKillClientSessionArg() : create_time_(0), client_sess_id_(0) {}
  ~ObKillClientSessionArg() {}
  bool is_valid() const;
  void reset() { create_time_ = 0;
                 client_sess_id_ = 0; }
  int assign(const ObKillClientSessionArg &other)
  {
    int ret = common::OB_SUCCESS;
    create_time_ = other.create_time_;
    client_sess_id_ = other.client_sess_id_;
    return ret;
  }
  void set_create_time(int64_t create_time) { create_time_ = create_time; }
  int64_t get_create_time() { return create_time_; }
  void set_client_sess_id(uint32_t client_sess_id) { client_sess_id_ = client_sess_id; }
  uint32_t get_client_sess_id() { return client_sess_id_; }
  TO_STRING_KV(K_(create_time), K_(client_sess_id));
private:
  int64_t create_time_;
  uint32_t client_sess_id_;
};

// kill client session result
struct ObKillClientSessionRes
{
  OB_UNIS_VERSION(1);
public:
  ObKillClientSessionRes() : can_kill_client_sess_(false) {}
  ~ObKillClientSessionRes() {}
  bool is_valid() const;
  void reset() {  can_kill_client_sess_ = false;}
  int assign(const ObKillClientSessionRes &other)
  {
    int ret = common::OB_SUCCESS;
    can_kill_client_sess_ = other.can_kill_client_sess_;
    return ret;
  }
  void set_can_kill_client_sess(bool can_kill_client_sess) { can_kill_client_sess_ = can_kill_client_sess; }
  bool get_can_kill_client_sess() {return can_kill_client_sess_;}
  TO_STRING_KV(K_(can_kill_client_sess));
  bool can_kill_client_sess_;
};

// kill client session arg & Authentication
struct ObClientSessionCreateTimeAndAuthArg
{
  OB_UNIS_VERSION(1);
public:
  ObClientSessionCreateTimeAndAuthArg() : client_sess_id_(0),
    tenant_id_(0), user_id_(0), has_user_super_privilege_(false) {}
  ~ObClientSessionCreateTimeAndAuthArg() {}
  bool is_valid() const;
  void reset()
  {
    client_sess_id_ = 0;
    tenant_id_ = 0;
    user_id_ = 0;
    has_user_super_privilege_ = false;
  }
  int assign(const ObClientSessionCreateTimeAndAuthArg &other)
  {
    int ret = common::OB_SUCCESS;
    client_sess_id_ = other.client_sess_id_;
    tenant_id_ = other.tenant_id_;
    user_id_ = other.user_id_;
    has_user_super_privilege_ = other.has_user_super_privilege_;
    return ret;
  }
  void set_client_sess_id(uint32_t client_sess_id) { client_sess_id_ = client_sess_id; }
  uint32_t get_client_sess_id() { return client_sess_id_; }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() { return tenant_id_; }
  void set_user_id(uint64_t user_id) { user_id_ = user_id; }
  uint64_t get_user_id() { return user_id_; }
  void set_has_user_super_privilege(bool has_user_super_privilege)
  {
    has_user_super_privilege_ = has_user_super_privilege;
  }
  bool is_has_user_super_privilege() { return has_user_super_privilege_; }
  TO_STRING_KV(K_(client_sess_id), K_(tenant_id), K_(user_id), K_(has_user_super_privilege));
private:
  uint32_t client_sess_id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  bool has_user_super_privilege_;
};

// kill client session result
struct ObClientSessionCreateTimeAndAuthRes
{
  OB_UNIS_VERSION(1);
public:
  ObClientSessionCreateTimeAndAuthRes() : client_sess_create_time_(0), have_kill_auth_(false) {}
  ~ObClientSessionCreateTimeAndAuthRes() {}
  bool is_valid() const;
  void reset()
  {
    client_sess_create_time_ = 0;
    have_kill_auth_ = false;
  }
  int assign(const ObClientSessionCreateTimeAndAuthRes &other)
  {
    int ret = common::OB_SUCCESS;
    client_sess_create_time_ = other.client_sess_create_time_;
    have_kill_auth_ = other.have_kill_auth_;
    return ret;
  }
  void set_client_create_time(int64_t client_sess_create_time)
  {
    client_sess_create_time_ = client_sess_create_time;
  }
  int64_t get_client_create_time() { return client_sess_create_time_; }
  void set_have_kill_auth(bool have_kill_auth) { have_kill_auth_ = have_kill_auth; }
  bool is_have_kill_auth() {return have_kill_auth_;}
  TO_STRING_KV(K_(client_sess_create_time), K_(have_kill_auth));
  int64_t client_sess_create_time_;
  bool have_kill_auth_;
};

struct ObCloneResourcePoolArg : public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObCloneResourcePoolArg():
    ObDDLArg(),
    source_tenant_id_(OB_INVALID_TENANT_ID),
    resource_pool_id_(OB_INVALID_ID) {}

  virtual ~ObCloneResourcePoolArg() {}
  bool is_valid() const { return !pool_name_.is_empty() && !unit_config_name_.is_empty() &&
                                 OB_INVALID_TENANT_ID != source_tenant_id_ &&
                                 OB_INVALID_ID != resource_pool_id_; }
  virtual int assign(const ObCloneResourcePoolArg &other);
  int init(const ObString &pool_name,
           const ObString &unit_config_name,
           const uint64_t source_tenant_id,
           const uint64_t resource_pool_id);

  const ObString get_pool_name() const { return pool_name_.str(); }
  const ObString get_unit_config_name() const { return unit_config_name_.str(); }
  uint64_t get_source_tenant_id() const { return source_tenant_id_; }
  uint64_t get_resource_pool_id() const { return resource_pool_id_; }
  DECLARE_TO_STRING;

private:
  common::ObFixedLengthString<common::MAX_RESOURCE_POOL_LENGTH + 1> pool_name_;
  common::ObFixedLengthString<common::MAX_UNIT_CONFIG_LENGTH + 1> unit_config_name_;
  uint64_t source_tenant_id_;
  uint64_t resource_pool_id_;
};

struct ObCloneTenantArg : public ObCmdArg
{
public:
  OB_UNIS_VERSION(1);
public:
  ObCloneTenantArg()
    : new_tenant_name_(),
      source_tenant_name_(),
      tenant_snapshot_name_(),
      resource_pool_name_(),
      unit_config_name_()
  {}
  virtual ~ObCloneTenantArg() {}
  bool is_valid() const;
  int assign(const ObCloneTenantArg &other);
  int init(const ObString &new_tenant_name,
           const ObString &source_tenant_name,
           const ObString &tenant_snapshot_name,
           const ObString &resource_pool_name,
           const ObString &unit_config_name);
  const ObString get_new_tenant_name() const { return new_tenant_name_.str(); }
  const ObString get_source_tenant_name() const { return source_tenant_name_.str(); }
  const ObString get_tenant_snapshot_name() const { return tenant_snapshot_name_.str(); }
  const ObString get_resource_pool_name() const { return resource_pool_name_.str(); }
  const ObString get_unit_config_name() const { return unit_config_name_.str(); }
  TO_STRING_KV(K_(new_tenant_name), K_(source_tenant_name),
               K_(tenant_snapshot_name), K_(resource_pool_name),
               K_(unit_config_name));
private:
  DISALLOW_COPY_AND_ASSIGN(ObCloneTenantArg);
private:
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> new_tenant_name_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> source_tenant_name_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_SNAPSHOT_NAME_LENGTH + 1> tenant_snapshot_name_;
  common::ObFixedLengthString<common::MAX_RESOURCE_POOL_LENGTH + 1> resource_pool_name_;
  common::ObFixedLengthString<common::MAX_UNIT_CONFIG_LENGTH + 1> unit_config_name_;
};

struct ObCloneTenantRes
{
  OB_UNIS_VERSION(1);
public:
  ObCloneTenantRes():
    job_id_(OB_INVALID_ID) {}
  ~ObCloneTenantRes() {}
  bool is_valid() const { return 0 < job_id_; }
  void reset();
  int assign(const ObCloneTenantRes &other);
  int64_t get_job_id() const { return job_id_; }
  void set_job_id(const int64_t job_id) { job_id_ = job_id; }
  TO_STRING_KV(K_(job_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObCloneTenantRes);
private:
  int64_t job_id_;
};

struct ObNotifyCloneSchedulerArg
{
  OB_UNIS_VERSION(1);
public:
  ObNotifyCloneSchedulerArg()
    : tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  ~ObNotifyCloneSchedulerArg() {}
  bool is_valid() const;
  int assign(const ObNotifyCloneSchedulerArg &other);
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  TO_STRING_KV(K_(tenant_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObNotifyCloneSchedulerArg);
private:
  uint64_t tenant_id_;
};

struct ObNotifyCloneSchedulerResult
{
  OB_UNIS_VERSION(1);
public:
  ObNotifyCloneSchedulerResult(): ret_(common::OB_SUCCESS) {}
  ~ObNotifyCloneSchedulerResult() {}
  bool is_valid() const;
  int assign(const ObNotifyCloneSchedulerResult &other);
  void init(const int ret) { ret_ = ret; }
  TO_STRING_KV(K_(ret));
  int get_result() const { return ret_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObNotifyCloneSchedulerResult);
private:
  int ret_;
};

struct ObCloneKeyArg
{
  OB_UNIS_VERSION(1);
public:
  ObCloneKeyArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      source_tenant_id_(OB_INVALID_TENANT_ID)
  {}
  ~ObCloneKeyArg() {}
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_source_tenant_id() const { return source_tenant_id_; }
  void set_source_tenant_id(const uint64_t source_tenant_id) { source_tenant_id_ = source_tenant_id; }
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_
                                 && OB_INVALID_TENANT_ID != source_tenant_id_; }
  int assign(const ObCloneKeyArg &other);
  TO_STRING_KV(K_(tenant_id), K_(source_tenant_id));
private:
  uint64_t tenant_id_;
  uint64_t source_tenant_id_;
};

struct ObCloneKeyResult
{
  OB_UNIS_VERSION(1);
public:
  ObCloneKeyResult() : ret_(common::OB_ERROR) {}
  ~ObCloneKeyResult() {}
  int assign(const ObCloneKeyResult &other);
  void set_ret(int ret) { ret_ = ret; }
  int64_t get_ret() const { return ret_; }
  TO_STRING_KV(K_(ret));
private:
  int ret_;
};

struct ObTrimKeyListArg
{
  OB_UNIS_VERSION(1);
public:
  ObTrimKeyListArg()
    : tenant_id_(OB_INVALID_TENANT_ID),
      latest_master_key_id_(0)
  {}
  ~ObTrimKeyListArg() {}
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_latest_master_key_id() const { return latest_master_key_id_; }
  void set_latest_master_key_id(const uint64_t latest_master_key_id) { latest_master_key_id_ = latest_master_key_id; }
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_
                                 && OB_INVALID_ID != latest_master_key_id_; }
  int assign(const ObTrimKeyListArg &other);
  TO_STRING_KV(K_(tenant_id), K_(latest_master_key_id));
private:
  uint64_t tenant_id_;
  uint64_t latest_master_key_id_;
};

struct ObTrimKeyListResult
{
  OB_UNIS_VERSION(1);
public:
  ObTrimKeyListResult() : ret_(common::OB_ERROR) {}
  ~ObTrimKeyListResult() {}
  int assign(const ObTrimKeyListResult &other);
  void set_ret(int ret) { ret_ = ret; }
  int64_t get_ret() const { return ret_; }
  TO_STRING_KV(K_(ret));
private:
  int ret_;
};

struct ObTabletLocationSendArg final
{
  OB_UNIS_VERSION(1);
public:
  ObTabletLocationSendArg();
  ~ObTabletLocationSendArg();
  int assign(const ObTabletLocationSendArg &other);
  int set(const ObIArray<share::ObTabletLocationBroadcastTask> &tasks);
  const ObSArray<share::ObTabletLocationBroadcastTask> &get_tasks() const { return tasks_; }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tasks));
public:
  ObSArray<share::ObTabletLocationBroadcastTask> tasks_;
};

struct ObTabletLocationSendResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTabletLocationSendResult();
  ~ObTabletLocationSendResult();
  int assign(const ObTabletLocationSendResult &other);
  void reset();
  void set_ret(int ret);
  int get_ret() const;
  TO_STRING_KV(K_(ret));
private:
  int ret_;
};

struct ObCancelGatherStatsArg
{
  OB_UNIS_VERSION(1);
public:
  ObCancelGatherStatsArg() : tenant_id_(0), task_id_() {}
  int assign(const ObCancelGatherStatsArg &other);
  bool is_valid() const { return tenant_id_ > 0; }
  uint64_t tenant_id_;
  common::ObString task_id_;
  TO_STRING_KV(K(tenant_id_), K(task_id_));
};

struct ObForceSetTenantLogDiskArg final
{
  OB_UNIS_VERSION(1);
public:
  ObForceSetTenantLogDiskArg() { reset(); }
  ~ObForceSetTenantLogDiskArg() { reset(); }
  int set(const uint64_t tenant_id,
          const int64_t log_disk_size);
  int assign(const ObForceSetTenantLogDiskArg &arg);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(log_disk_size));
public:
  uint64_t tenant_id_;
  int64_t log_disk_size_;
};
struct ObDumpServerUsageRequest final
{
  OB_UNIS_VERSION(1);
public:
  ObDumpServerUsageRequest() : tenant_id_(OB_SYS_TENANT_ID) {}
  ~ObDumpServerUsageRequest() { tenant_id_ = OB_INVALID_TENANT_ID; }
  uint64_t tenant_id_;
};
struct ObDumpServerUsageResult final
{
  OB_UNIS_VERSION(1);
public:
  struct ObServerInfo {
    OB_UNIS_VERSION(1);
  public:
    ObServerInfo() : log_disk_capacity_(0), log_disk_assigned_(0)
    {}
    ~ObServerInfo() {}
    int64_t log_disk_capacity_;
    int64_t log_disk_assigned_;
    TO_STRING_KV(K_(log_disk_capacity), K_(log_disk_assigned));
  };
  struct ObUnitInfo {
    OB_UNIS_VERSION(1);
  public:
    ObUnitInfo() : tenant_id_(OB_INVALID_TENANT_ID), log_disk_size_(0), log_disk_in_use_(0)
    {}
    ~ObUnitInfo() {}
  public:
    uint64_t tenant_id_;
    int64_t log_disk_size_;
    int64_t log_disk_in_use_;
    TO_STRING_KV(K_(tenant_id), K_(log_disk_size), K_(log_disk_in_use));
  };
public:
  ObDumpServerUsageResult() : server_info_(), unit_info_() {}
  ~ObDumpServerUsageResult() {}
  int assign(const ObDumpServerUsageResult &rhs);
  TO_STRING_KV(K_(server_info), K_(unit_info));
  // server info
  ObServerInfo server_info_;
  // unit info
  ObSArray<ObUnitInfo> unit_info_;
};
struct ObCheckServerMachineStatusArg final
{
  OB_UNIS_VERSION(1);
public:
  ObCheckServerMachineStatusArg() : rs_addr_(), target_addr_() {}
  ~ObCheckServerMachineStatusArg() {}
  int init(const common::ObAddr &rs_addr, const common::ObAddr &target_addr);
  bool is_valid() const { return rs_addr_.is_valid() && target_addr_.is_valid(); }
  void reset() { rs_addr_.reset(); target_addr_.reset(); }
  int assign(const ObCheckServerMachineStatusArg &other);
  TO_STRING_KV(K_(rs_addr), K_(target_addr));

private:
  common::ObAddr rs_addr_;
  common::ObAddr target_addr_;
};

struct ObCheckServerMachineStatusResult final
{
  OB_UNIS_VERSION(1);
public:
  ObCheckServerMachineStatusResult() : server_health_status_() {}
  ~ObCheckServerMachineStatusResult() {}
  int init(const share::ObServerHealthStatus &server_health_status);
  int assign(const ObCheckServerMachineStatusResult &other);
  void reset() { server_health_status_.reset(); }
  bool is_valid() const { return server_health_status_.is_valid(); }
  const share::ObServerHealthStatus &get_server_health_status() const { return server_health_status_; }
  TO_STRING_KV(K_(server_health_status));
private:
  share::ObServerHealthStatus server_health_status_;
};
}//end namespace obrpc
}//end namespace oceanbase
#endif
