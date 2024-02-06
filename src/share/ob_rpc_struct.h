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
#include "common/ob_partition_key.h"
#include "common/ob_role.h"
#include "common/ob_role_mgr.h"
#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "common/ob_idc.h"
#include "common/ob_zone_type.h"
#include "common/ob_common_types.h"
#include "common/ob_unit_info.h"
#include "common/ob_store_format.h"
#include "share/ob_debug_sync.h"
#include "share/ob_partition_modify.h"
#include "share/ob_zone_info.h"
#include "share/ob_server_status.h"
#include "share/ob_simple_batch.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_cluster_type.h"  // ObClusterType PRIMARY_CLUSTER
#include "share/ob_web_service_root_addr.h"
#include "share/ob_cluster_version.h"
#include "share/ob_i_data_access_service.h"
#include "share/ob_multi_cluster_util.h"
#include "share/ob_cluster_switchover_info.h"  // ObClusterSwitchoverInfoWrap
#include "share/backup/ob_physical_restore_info.h"
#include "share/schema/ob_error_info.h"
#include "share/schema/ob_constraint.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_udf.h"
#include "share/ob_storage_format.h"
#include "share/ob_replica_wrs_info.h"      // ObReplicaWrsInfoList
#include "share/restore/ob_restore_args.h"  // ObRestoreArgs
#include "rootserver/ob_rs_job_table_operator.h"
#include "sql/executor/ob_task_id.h"
#include "sql/parser/ob_item_type.h"                           // ObCacheType
#include "share/partition_table/ob_partition_location_task.h"  // ObPartitionBroadcastTask

namespace oceanbase {
namespace rootserver {
class ObGlobalIndexTask;
}
namespace obrpc {
typedef common::ObSArray<common::ObAddr> ObServerList;
static const int64_t MAX_COUNT = 128;
static const int64_t OB_DEFAULT_ARRAY_SIZE = 8;
typedef common::ObSEArray<common::ObPartitionKey, MAX_COUNT> ObPkeyArray;

enum ObUpgradeStage {
  OB_UPGRADE_STAGE_INVALID,
  OB_UPGRADE_STAGE_NONE,
  OB_UPGRADE_STAGE_PREUPGRADE,
  OB_UPGRADE_STAGE_DBUPGRADE,
  OB_UPGRADE_STAGE_POSTUPGRADE,
  OB_UPGRADE_STAGE_MAX
};
const char* get_upgrade_stage_str(ObUpgradeStage stage);
ObUpgradeStage get_upgrade_stage(const common::ObString& str);

enum class MigrateMode {
  MT_LOCAL_FS_MODE = 0,
  MT_SINGLE_ZONE_MODE = 1,
  MT_MAX,
};

enum ObCreateTableMode {
  OB_CREATE_TABLE_MODE_INVALID = -1,
  OB_CREATE_TABLE_MODE_LOOSE = 0,
  OB_CREATE_TABLE_MODE_STRICT = 1,
  OB_CREATE_TABLE_MODE_RESTORE = 2,
  OB_CREATE_TABLE_MODE_PHYSICAL_RESTORE = 3,  // Distinguish from logical recovery
  OB_CREATE_TABLE_MODE_MAX,
};

enum ObDefaultRoleFlag {
  OB_DEFUALT_NONE = 0,
  OB_DEFAULT_ROLE_LIST = 1,
  OB_DEFAULT_ROLE_ALL = 2,
  OB_DEFAULT_ROLE_ALL_EXCEPT = 3,
  OB_DEFAULT_ROLE_NONE = 4,
  OB_DEFAULT_ROLE_MAX,
};

struct Bool {
  OB_UNIS_VERSION(1);

public:
  Bool(bool v = false) : v_(v)
  {}

  operator bool()
  {
    return v_;
  }
  operator bool() const
  {
    return v_;
  }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  bool v_;
};

struct Int64 {
  OB_UNIS_VERSION(1);

public:
  Int64(int64_t v = common::OB_INVALID_ID) : v_(v)
  {}

  inline void reset();
  bool is_valid() const
  {
    return true;
  }
  operator int64_t()
  {
    return v_;
  }
  operator int64_t() const
  {
    return v_;
  }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  int64_t v_;
};

struct UInt64 {
  OB_UNIS_VERSION(1);

public:
  UInt64(uint64_t v = common::OB_INVALID_ID) : v_(v)
  {}

  operator uint64_t()
  {
    return v_;
  }
  operator uint64_t() const
  {
    return v_;
  }
  DEFINE_TO_STRING(BUF_PRINTO(v_));

private:
  uint64_t v_;
};

struct ObGetRootserverRoleResult {
  OB_UNIS_VERSION(1);

public:
  ObGetRootserverRoleResult()
      : role_(0), zone_(), type_(common::REPLICA_TYPE_MAX), status_(share::status::MAX), replica_(), partition_info_()
  {}
  void reset();
  DECLARE_TO_STRING;

  int32_t role_;
  common::ObZone zone_;
  common::ObReplicaType type_;
  share::status::ObRootServiceStatus status_;
  share::ObPartitionReplica replica_;
  share::ObPartitionInfo partition_info_;
};

struct ObServerInfo {
  OB_UNIS_VERSION(1);

public:
  common::ObZone zone_;
  common::ObAddr server_;
  common::ObRegion region_;

  // FIXME: () Do you need to consider region_ comparison after adding region_? I don't need to consider it for the time
  // being
  bool operator<(const ObServerInfo& r) const
  {
    return zone_ < r.zone_;
  }
  DECLARE_TO_STRING;
};

struct ObPartitionId {
  OB_UNIS_VERSION(1);

public:
  int64_t table_id_;
  int64_t partition_id_;

  ObPartitionId() : table_id_(common::OB_INVALID_ID), partition_id_(common::OB_INVALID_INDEX)
  {}

  DECLARE_TO_STRING;
};

struct ObPartitionStat {
  OB_UNIS_VERSION(1);

public:
  enum PartitionStat {
    RECOVERING = 0,
    WRITABLE = 1,
  };
  ObPartitionStat() : partition_key_(), stat_(RECOVERING)
  {}
  void reset()
  {
    partition_key_.reset();
    stat_ = RECOVERING;
  }
  common::ObPartitionKey partition_key_;
  PartitionStat stat_;

  DECLARE_TO_STRING;
};

typedef common::ObSArray<ObServerInfo> ObServerInfoList;
typedef common::ObSArray<common::ObPartitionKey> ObPartitionList;
typedef common::ObSArray<ObPartitionStat> ObPartitionStatList;
typedef common::ObArray<ObServerInfoList> ObPartitionServerList;

struct ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDDLArg()
      : ddl_stmt_str_(),
        exec_tenant_id_(common::OB_INVALID_TENANT_ID),
        ddl_id_str_(),
        primary_schema_versions_(),
        is_replay_schema_(false),
        based_schema_object_infos_()
  {}
  bool is_sync_primary_query() const
  {
    return 0 < primary_schema_versions_.count();
  }
  bool is_need_check_based_schema_objects() const
  {
    return 0 < based_schema_object_infos_.count();
  }
  virtual bool is_allow_when_disable_ddl() const
  {
    return false;
  }
  virtual bool is_allow_when_upgrade() const
  {
    return false;
  }
  virtual int assign(const ObDDLArg& other);
  TO_STRING_KV(
      K_(ddl_stmt_str), K_(exec_tenant_id), K_(ddl_id_str), K_(is_replay_schema), K_(based_schema_object_infos));
  common::ObString ddl_stmt_str_;
  uint64_t exec_tenant_id_;
  common::ObString ddl_id_str_;
  common::ObSEArray<int64_t, 5> primary_schema_versions_;
  bool is_replay_schema_;
  common::ObSEArray<share::schema::ObBasedSchemaObjectInfo, OB_DEFAULT_ARRAY_SIZE> based_schema_object_infos_;
};

struct ObCreateResourceUnitArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateResourceUnitArg()
      : ObDDLArg(),
        min_cpu_(0),
        min_iops_(0),
        min_memory_(0),
        max_cpu_(0),
        max_memory_(0),
        max_disk_size_(0),
        max_iops_(0),
        max_session_num_(0),
        if_not_exist_(false)
  {}
  virtual ~ObCreateResourceUnitArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObCreateResourceUnitArg& other);
  DECLARE_TO_STRING;

  common::ObString unit_name_;
  double min_cpu_;
  int64_t min_iops_;
  int64_t min_memory_;
  double max_cpu_;
  int64_t max_memory_;
  int64_t max_disk_size_;
  int64_t max_iops_;
  int64_t max_session_num_;
  bool if_not_exist_;
};

struct ObAlterResourceUnitArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObAlterResourceUnitArg()
      : ObDDLArg(),
        min_cpu_(0),
        min_iops_(0),
        min_memory_(0),
        max_cpu_(0),
        max_memory_(0),
        max_disk_size_(0),
        max_iops_(0),
        max_session_num_(0)
  {}
  virtual ~ObAlterResourceUnitArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObAlterResourceUnitArg& other);
  DECLARE_TO_STRING;

  common::ObString unit_name_;
  double min_cpu_;
  int64_t min_iops_;
  int64_t min_memory_;
  double max_cpu_;
  int64_t max_memory_;
  int64_t max_disk_size_;
  int64_t max_iops_;
  int64_t max_session_num_;
};

struct ObDropResourceUnitArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropResourceUnitArg() : ObDDLArg(), if_exist_(false)
  {}
  virtual ~ObDropResourceUnitArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObDropResourceUnitArg& other);
  DECLARE_TO_STRING;

  common::ObString unit_name_;
  bool if_exist_;
};

struct ObCreateResourcePoolArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateResourcePoolArg()
      : ObDDLArg(), unit_num_(0), if_not_exist_(0), replica_type_(common::REPLICA_TYPE_FULL), is_tenant_sys_pool_(false)
  {}
  virtual ~ObCreateResourcePoolArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObCreateResourcePoolArg& other);
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  common::ObString unit_;
  int64_t unit_num_;
  common::ObSArray<common::ObZone> zone_list_;
  bool if_not_exist_;
  common::ObReplicaType replica_type_;
  bool is_tenant_sys_pool_;
};

struct ObSplitResourcePoolArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObSplitResourcePoolArg() : ObDDLArg(), pool_name_(), zone_list_(), split_pool_list_()
  {}
  virtual ~ObSplitResourcePoolArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObSplitResourcePoolArg& other);
  TO_STRING_KV(K_(pool_name), K_(zone_list), K_(split_pool_list));

  common::ObString pool_name_;
  common::ObSArray<common::ObZone> zone_list_;
  common::ObSArray<common::ObString> split_pool_list_;
};

struct ObMergeResourcePoolArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObMergeResourcePoolArg()
      : ObDDLArg(),
        old_pool_list_(),  // Before the merger
        new_pool_list_()
  {}  // After the merger
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObMergeResourcePoolArg& other);
  TO_STRING_KV(K_(old_pool_list), K_(new_pool_list));

  common::ObSArray<common::ObString> old_pool_list_;
  common::ObSArray<common::ObString> new_pool_list_;
};

struct ObAlterResourcePoolArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObAlterResourcePoolArg() : ObDDLArg(), pool_name_(), unit_(), unit_num_(0), zone_list_(), delete_unit_id_array_()
  {}
  virtual ~ObAlterResourcePoolArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObAlterResourcePoolArg& other);
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  common::ObString unit_;
  int64_t unit_num_;
  common::ObSArray<common::ObZone> zone_list_;
  common::ObSArray<uint64_t> delete_unit_id_array_;  // This array may be empty
};

struct ObDropResourcePoolArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropResourcePoolArg() : ObDDLArg(), if_exist_(false)
  {}
  virtual ~ObDropResourcePoolArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObDropResourcePoolArg& other);
  DECLARE_TO_STRING;

  common::ObString pool_name_;
  bool if_exist_;
};

struct ObCmdArg {
  OB_UNIS_VERSION(1);

public:
  common::ObString& get_sql_stmt()
  {
    return sql_text_;
  }
  const common::ObString& get_sql_stmt() const
  {
    return sql_text_;
  }

public:
  common::ObString sql_text_;
};

struct ObSysVarIdValue {
  OB_UNIS_VERSION(1);

public:
  ObSysVarIdValue() : sys_id_(share::SYS_VAR_INVALID), value_()
  {}
  ObSysVarIdValue(share::ObSysVarClassType sys_id, common::ObString& value) : sys_id_(sys_id), value_(value)
  {}
  ~ObSysVarIdValue()
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  share::ObSysVarClassType sys_id_;
  common::ObString value_;
};

struct ObCreateTenantArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateTenantArg()
      : ObDDLArg(),
        tenant_schema_(),
        pool_list_(),
        if_not_exist_(false),
        sys_var_list_(),
        name_case_mode_(common::OB_NAME_CASE_INVALID),
        is_restore_(false),
        restore_frozen_status_()
  {}
  virtual ~ObCreateTenantArg(){};
  bool is_valid() const;
  int check_valid() const;
  int assign(const ObCreateTenantArg& other);
  DECLARE_TO_STRING;

  share::schema::ObTenantSchema tenant_schema_;
  common::ObSArray<common::ObString> pool_list_;
  bool if_not_exist_;
  common::ObSArray<ObSysVarIdValue> sys_var_list_;
  common::ObNameCaseMode name_case_mode_;
  bool is_restore_;
  common::ObSArray<ObPartitionKey> restore_pkeys_;      // For physical restore, partitions that should be created with
                                                        // is_restore = REPLICA_RESTORE_DATA
  common::ObSArray<ObPartitionKey> restore_log_pkeys_;  // For physical restore, partitions that should be created with
                                                        // is_restore = REPLICA_RESTORE_ARCHIVE_DATA
  share::ObSimpleFrozenStatus restore_frozen_status_;
};

struct ObCreateTenantEndArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateTenantEndArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  virtual ~ObCreateTenantEndArg()
  {}
  bool is_valid() const;
  int assign(const ObCreateTenantEndArg& other);
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
};

struct ObModifyTenantArg : public ObDDLArg {
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
    REWRITE_MERGE_VERSION,
    LOCALITY,
    LOGONLY_REPLICA_NUM,
    STORAGE_FORMAT_VERSION,
    STORAGE_FORMAT_WORK_VERSION,
    DEFAULT_TABLEGROUP,
    FORCE_LOCALITY,
    PROGRESSIVE_MERGE_NUM,
    MAX_OPTION,
  };
  ObModifyTenantArg() : ObDDLArg()
  {}
  bool is_valid() const;
  int check_normal_tenant_can_do(bool& normal_can_do) const;
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool is_allow_when_upgrade() const;

  DECLARE_TO_STRING;

  share::schema::ObTenantSchema tenant_schema_;
  common::ObSArray<common::ObString> pool_list_;
  // used to mark alter tenant options
  common::ObBitSet<> alter_option_bitset_;
  common::ObSArray<ObSysVarIdValue> sys_var_list_;
  common::ObString new_tenant_name_;  // for tenant rename
};

struct ObLockTenantArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObLockTenantArg() : ObDDLArg(), is_locked_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString tenant_name_;
  bool is_locked_;
};

struct ObDropTenantArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropTenantArg()
      : ObDDLArg(),
        if_exist_(false),
        delay_to_drop_(true),
        force_drop_(false),  // Obsolete field
        open_recyclebin_(true)
  {}
  virtual ~ObDropTenantArg()
  {}
  /*
   * drop tenant force highest priority
   * At this time delay_to_drop_=false;
   * In other cases delay_to_drop_=true;
   * open_recyclebin_ is determined according to whether the recycle bin is open or not;
   * open_recyclebin_ open as true, enter the recycle bin
   * open_recyclebin_ closed to false, delayed deletion
   */
  int assign(const ObDropTenantArg& other)
  {
    tenant_name_ = other.tenant_name_;
    if_exist_ = other.if_exist_;
    delay_to_drop_ = other.delay_to_drop_;
    force_drop_ = other.force_drop_;
    object_name_ = other.object_name_;
    open_recyclebin_ = other.object_name_;
    return common::OB_SUCCESS;
  }
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  DECLARE_TO_STRING;

  common::ObString tenant_name_;
  bool if_exist_;
  bool delay_to_drop_;
  bool force_drop_;
  common::ObString object_name_;  // Synchronize the name of the recycle bin in the main library
  bool open_recyclebin_;
};

struct ObSequenceDDLArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObSequenceDDLArg() : ObDDLArg(), stmt_type_(common::OB_INVALID_ID), option_bitset_(), seq_schema_(), database_name_()
  {}
  bool is_valid() const
  {
    return !database_name_.empty();
  }
  virtual bool is_allow_when_upgrade() const
  {
    return sql::stmt::T_DROP_SEQUENCE == stmt_type_;
  }
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
  void set_sequence_id(const uint64_t sequence_id)
  {
    seq_schema_.set_sequence_id(sequence_id);
  }
  void set_sequence_name(const common::ObString& name)
  {
    seq_schema_.set_sequence_name(name);
  }
  void set_database_name(const common::ObString& name)
  {
    database_name_ = name;
  }
  share::ObSequenceOption& option()
  {
    return seq_schema_.get_sequence_option();
  }
  const common::ObString& get_database_name() const
  {
    return database_name_;
  }
  share::schema::ObSequenceSchema& sequence_schema()
  {
    return seq_schema_;
  }
  common::ObBitSet<>& get_option_bitset()
  {
    return option_bitset_;
  }
  const common::ObBitSet<>& get_option_bitset() const
  {
    return option_bitset_;
  }
  TO_STRING_KV(K_(stmt_type), K_(seq_schema), K_(database_name));

public:
  int64_t stmt_type_;
  common::ObBitSet<> option_bitset_;
  share::schema::ObSequenceSchema seq_schema_;
  common::ObString database_name_;
};

struct ObAddSysVarArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObAddSysVarArg() : sysvar_(), if_not_exist_(false)
  {}
  DECLARE_TO_STRING;
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  share::schema::ObSysVarSchema sysvar_;
  bool if_not_exist_;
};

struct ObModifySysVarArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObModifySysVarArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), is_inner_(false)
  {}
  DECLARE_TO_STRING;
  bool is_valid() const;
  int assign(const ObModifySysVarArg& other);
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual bool is_allow_when_disable_ddl() const
  {
    return true;
  }
  uint64_t tenant_id_;
  common::ObSArray<share::schema::ObSysVarSchema> sys_var_list_;
  bool is_inner_;
};

struct ObCreateDatabaseArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateDatabaseArg() : ObDDLArg(), if_not_exist_(false)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  DECLARE_TO_STRING;

  share::schema::ObDatabaseSchema database_schema_;
  // used to mark alter database options
  common::ObBitSet<> alter_option_bitset_;
  bool if_not_exist_;
};

struct ObAlterDatabaseArg : public ObDDLArg {
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
  {}
  bool is_valid() const;
  bool only_alter_primary_zone() const
  {
    return (1 == alter_option_bitset_.num_members() && alter_option_bitset_.has_member(PRIMARY_ZONE));
  }
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  DECLARE_TO_STRING;

  share::schema::ObDatabaseSchema database_schema_;
  // used to mark alter database options
  common::ObBitSet<> alter_option_bitset_;
};

struct ObDropDatabaseArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropDatabaseArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), if_exist_(false), to_recyclebin_(false)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString database_name_;
  bool if_exist_;
  bool to_recyclebin_;
};

struct ObCreateTablegroupArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateTablegroupArg() : ObDDLArg(), if_not_exist_(false), create_mode_(obrpc::OB_CREATE_TABLE_MODE_STRICT)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  DECLARE_TO_STRING;

  share::schema::ObTablegroupSchema tablegroup_schema_;
  bool if_not_exist_;
  obrpc::ObCreateTableMode create_mode_;
};

struct ObDropTablegroupArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropTablegroupArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), if_exist_(false)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
  bool if_exist_;
};

struct ObTableItem;
struct ObAlterTablegroupArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObAlterTablegroupArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        tablegroup_name_(),
        table_items_(),
        alter_option_bitset_(),
        create_mode_(OB_CREATE_TABLE_MODE_STRICT),
        alter_tablegroup_schema_()
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
    MAX_OPTION,
  };
  uint64_t tenant_id_;
  common::ObString tablegroup_name_;
  common::ObSArray<ObTableItem> table_items_;
  common::ObBitSet<> alter_option_bitset_;
  ObCreateTableMode create_mode_;
  share::schema::ObTablegroupSchema alter_tablegroup_schema_;
};

struct ObReachPartitionLimitArg {
  OB_UNIS_VERSION(1);

public:
  ObReachPartitionLimitArg() : batch_cnt_(0), tenant_id_(common::OB_INVALID_ID), is_pg_arg_(false)
  {}

  ObReachPartitionLimitArg(const int64_t batch_cnt, const uint64_t tenant_id, const bool is_pg_arg)
      : batch_cnt_(batch_cnt), tenant_id_(tenant_id), is_pg_arg_(is_pg_arg)
  {}

  virtual ~ObReachPartitionLimitArg()
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(batch_cnt), K_(tenant_id), K_(is_pg_arg));

public:
  int64_t batch_cnt_;
  uint64_t tenant_id_;
  bool is_pg_arg_;
};

struct ObCheckFrozenVersionArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckFrozenVersionArg() : frozen_version_(0)
  {}

  virtual ~ObCheckFrozenVersionArg()
  {}

  bool is_valid() const
  {
    return frozen_version_ > 0;
  }
  TO_STRING_KV(K_(frozen_version));

public:
  int64_t frozen_version_;
};

struct ObGetMinSSTableSchemaVersionArg {
  OB_UNIS_VERSION(1);

public:
  ObGetMinSSTableSchemaVersionArg()
  {
    tenant_id_arg_list_.reuse();
  }

  virtual ~ObGetMinSSTableSchemaVersionArg()
  {
    tenant_id_arg_list_.reset();
  }

  bool is_valid() const
  {
    return tenant_id_arg_list_.size() > 0;
  }
  TO_STRING_KV(K_(tenant_id_arg_list));

public:
  common::ObSArray<uint64_t> tenant_id_arg_list_;
};

struct ObCreateIndexArg;       // Forward declaration
struct ObCreateForeignKeyArg;  // Forward declaration
struct ObCreateTableArg : ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateTableArg()
      : ObDDLArg(),
        if_not_exist_(false),
        create_mode_(OB_CREATE_TABLE_MODE_STRICT),
        last_replay_log_id_(0),
        is_inner_(false),
        error_info_()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const;
  DECLARE_TO_STRING;

  bool if_not_exist_;
  share::schema::ObTableSchema schema_;
  common::ObSArray<ObCreateIndexArg> index_arg_list_;
  common::ObSArray<ObCreateForeignKeyArg> foreign_key_arg_list_;
  common::ObSEArray<share::schema::ObConstraint, 4> constraint_list_;
  common::ObString db_name_;
  ObCreateTableMode create_mode_;
  uint64_t last_replay_log_id_;
  bool is_inner_;
  share::schema::ObErrorInfo error_info_;
  // New members of ObCreateTableArg need to pay attention to the implementation of is_allow_when_upgrade
};

struct ObCreateTableRes {
  OB_UNIS_VERSION(1);

public:
  ObCreateTableRes() : table_id_(OB_INVALID_ID), schema_version_(OB_INVALID_VERSION)
  {}
  int assign(const ObCreateTableRes &other)
  {
    table_id_ = other.table_id_;
    schema_version_ = other.schema_version_;
    return common::OB_SUCCESS;
  }
  TO_STRING_KV(K_(table_id), K_(schema_version));
  uint64_t table_id_;
  int64_t schema_version_;
};

struct ObCreateTableLikeArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateTableLikeArg()
      : ObDDLArg(),
        if_not_exist_(false),
        tenant_id_(common::OB_INVALID_ID),
        table_type_(share::schema::USER_TABLE),
        origin_db_name_(),
        origin_table_name_(),
        new_db_name_(),
        new_table_name_(),
        create_host_(),
        create_mode_(OB_CREATE_TABLE_MODE_STRICT)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  bool if_not_exist_;
  uint64_t tenant_id_;
  share::schema::ObTableType table_type_;
  common::ObString origin_db_name_;
  common::ObString origin_table_name_;
  common::ObString new_db_name_;
  common::ObString new_table_name_;
  common::ObString create_host_;  // Temporary table is valid
  ObCreateTableMode create_mode_;
};

struct ObCreateSynonymArg : ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateSynonymArg() : or_replace_(false), synonym_info_(), db_name_()
  {}
  TO_STRING_KV(K_(synonym_info), K_(db_name), K_(obj_db_name));
  bool or_replace_;
  share::schema::ObSynonymInfo synonym_info_;
  common::ObString db_name_;
  common::ObString obj_db_name_;
};

struct ObDropSynonymArg : ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropSynonymArg() : tenant_id_(common::OB_INVALID_ID), is_force_(false), db_name_(), synonym_name_()
  {}
  virtual ~ObDropSynonymArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  TO_STRING_KV(K_(tenant_id), K_(is_force), K_(db_name), K_(synonym_name));

  uint64_t tenant_id_;
  bool is_force_;
  common::ObString db_name_;
  common::ObString synonym_name_;
};

struct ObAlterPlanBaselineArg : ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  enum FieldUpdated {
    FIXED = 0x1,
    ENABLED = 0x02,
    OUTLINE_DATA = 0x4,
  };

  ObAlterPlanBaselineArg() : plan_baseline_info_(), field_update_bitmap_(0)
  {}

  TO_STRING_KV(K_(plan_baseline_info), K_(field_update_bitmap));
  share::schema::ObPlanBaselineInfo plan_baseline_info_;
  uint64_t field_update_bitmap_;

  void restore(const share::schema::ObPlanBaselineInfo& old);
};

struct ObCreatePlanBaselineArg : ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreatePlanBaselineArg() : plan_baseline_info_(), is_replace_(false)
  {}
  TO_STRING_KV(K_(plan_baseline_info), K(is_replace_));
  share::schema::ObPlanBaselineInfo plan_baseline_info_;
  bool is_replace_;
};

struct ObDropPlanBaselineArg : ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropPlanBaselineArg()
      : tenant_id_(common::OB_INVALID_ID),
        sql_id_(),
        plan_hash_value_(common::OB_INVALID_ID),
        plan_baseline_id_(common::OB_INVALID_ID),
        db_name_()
  {}
  virtual ~ObDropPlanBaselineArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  TO_STRING_KV(K_(tenant_id), K_(sql_id), K_(plan_hash_value));

  uint64_t tenant_id_;
  common::ObString sql_id_;
  uint64_t plan_hash_value_;
  uint64_t plan_baseline_id_;
  common::ObString db_name_;
};

struct ObIndexArg : public ObDDLArg {
  OB_UNIS_VERSION_V(1);

public:
  enum IndexActionType {
    INVALID_ACTION = 1,
    ADD_INDEX,
    DROP_INDEX,
    ALTER_INDEX,
    DROP_FOREIGN_KEY,  // The foreign key is a 1.4 function, and rename_index needs to be placed at the back in
                       // consideration of compatibility
    RENAME_INDEX,
    ALTER_INDEX_PARALLEL,
    REBUILD_INDEX
  };

  uint64_t tenant_id_;
  uint64_t session_id_;  // The session id is passed in when building the index, and the table schema is searched by rs
                         // according to the temporary table and then the ordinary table.
  common::ObString index_name_;
  common::ObString table_name_;
  common::ObString database_name_;
  IndexActionType index_action_type_;

  ObIndexArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        session_id_(common::OB_INVALID_ID),
        index_name_(),
        table_name_(),
        database_name_(),
        index_action_type_(INVALID_ACTION)
  {}
  virtual ~ObIndexArg()
  {}
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    session_id_ = common::OB_INVALID_ID;
    index_name_.reset();
    table_name_.reset();
    database_name_.reset();
    index_action_type_ = INVALID_ACTION;
  }
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const;
  int assign(const ObIndexArg& other)
  {
    tenant_id_ = other.tenant_id_;
    session_id_ = other.session_id_;
    index_name_ = other.index_name_;
    table_name_ = other.table_name_;
    database_name_ = other.database_name_;
    index_action_type_ = other.index_action_type_;
    return common::OB_SUCCESS;
  }

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObUpdateStatCacheArg : public ObDDLArg {
  OB_UNIS_VERSION_V(1);

public:
  ObUpdateStatCacheArg()
      : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID), partition_ids_(), column_ids_()
  {}
  virtual ~ObUpdateStatCacheArg()
  {}
  void rest()
  {
    tenant_id_ = common::OB_INVALID_ID, table_id_ = common::OB_INVALID_ID, partition_ids_.reset();
    column_ids_.reset();
  }
  bool is_valid() const;
  int assign(const ObUpdateStatCacheArg& other)
  {
    int ret = common::OB_SUCCESS;
    tenant_id_ = other.tenant_id_;
    table_id_ = other.table_id_;
    if (OB_FAIL(ObDDLArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign ddl arg", KR(ret));
    } else if (OB_FAIL(partition_ids_.assign(other.partition_ids_))) {
      SHARE_LOG(WARN, "fail to assign partition ids", KR(ret));
    } else if (OB_FAIL(column_ids_.assign(other.column_ids_))) {
      SHARE_LOG(WARN, "fail to assign column ids", KR(ret));
    } else { /*do nothing*/
    }
    return ret;
  }
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  uint64_t tenant_id_;
  uint64_t table_id_;
  common::ObSArray<int64_t> partition_ids_;
  common::ObSArray<uint64_t> column_ids_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObDropIndexArg : public ObIndexArg {
  OB_UNIS_VERSION(1);
  // if add new member,should add to_string and serialize function
public:
  ObDropIndexArg()
  {
    index_action_type_ = DROP_INDEX;
    to_recyclebin_ = false;
    index_table_id_ = common::OB_INVALID_ID;
  }
  virtual ~ObDropIndexArg()
  {}
  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = DROP_INDEX;
  }
  bool is_valid() const
  {
    return ObIndexArg::is_valid();
  }
  bool to_recyclebin() const
  {
    return to_recyclebin_;
  }
  bool to_recyclebin_;
  uint64_t index_table_id_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObRebuildIndexArg : public ObIndexArg {
  OB_UNIS_VERSION(1);
  // if add new member,should add to_string and serialize function
public:
  ObRebuildIndexArg() : ObIndexArg()
  {
    create_mode_ = OB_CREATE_TABLE_MODE_STRICT;
    index_action_type_ = REBUILD_INDEX;
    index_table_id_ = common::OB_INVALID_ID;
  }
  virtual ~ObRebuildIndexArg()
  {}

  int assign(const ObRebuildIndexArg& other)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(ObIndexArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign base", K(ret));
    } else {
      create_mode_ = other.create_mode_;
      index_table_id_ = other.index_table_id_;
    }
    return ret;
  }

  void reset()
  {
    ObIndexArg::reset();
    index_action_type_ = REBUILD_INDEX;
  }
  bool is_valid() const
  {
    return ObIndexArg::is_valid();
  }
  ObCreateTableMode create_mode_;
  uint64_t index_table_id_;

  DECLARE_VIRTUAL_TO_STRING;
};

struct ObAlterIndexParallelArg : public ObIndexArg {
  OB_UNIS_VERSION_V(1);

public:
  ObAlterIndexParallelArg() : ObIndexArg(), new_parallel_(common::OB_DEFAULT_TABLE_DOP)
  {
    index_action_type_ = ALTER_INDEX_PARALLEL;
  }
  virtual ~ObAlterIndexParallelArg()
  {}
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

struct ObRenameIndexArg : public ObIndexArg {
  OB_UNIS_VERSION_V(1);

public:
  ObRenameIndexArg() : ObIndexArg(), origin_index_name_(), new_index_name_()
  {
    index_action_type_ = RENAME_INDEX;
  }
  virtual ~ObRenameIndexArg()
  {}
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

struct ObAlterIndexArg : public ObIndexArg {
  OB_UNIS_VERSION_V(1);

public:
  ObAlterIndexArg() : ObIndexArg(), index_visibility_(common::OB_DEFAULT_INDEX_VISIBILITY)
  {
    index_action_type_ = ALTER_INDEX;
  }
  virtual ~ObAlterIndexArg()
  {}
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

struct ObTruncateTableArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObTruncateTableArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        session_id_(common::OB_INVALID_ID),
        database_name_(),
        table_name_(),
        create_mode_(OB_CREATE_TABLE_MODE_STRICT),
        to_recyclebin_(false)
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  uint64_t session_id_;  // Pass in session id when truncate table
  common::ObString database_name_;
  common::ObString table_name_;
  ObCreateTableMode create_mode_;
  bool to_recyclebin_;
};

struct ObRenameTableItem {
  OB_UNIS_VERSION(1);

public:
  ObRenameTableItem()
      : origin_db_name_(),
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
  uint64_t origin_table_id_;  // only used in work thread, no need add to SERIALIZE now
};

struct ObRenameTableArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObRenameTableArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), rename_table_items_()
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  common::ObSArray<ObRenameTableItem> rename_table_items_;
};

struct ObAlterTableArg : public ObDDLArg {
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
    TABLE_DOP,
    MAX_OPTION = 1000
  };
  enum AlterPartitionType {
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
    NO_OPERATION = 1000
  };
  enum AlterConstraintType {
    ADD_CONSTRAINT = -1,
    DROP_CONSTRAINT,
    ALTER_CONSTRAINT_STATE,
    CONSTRAINT_NO_OPERATION = 1000
  };
  ObAlterTableArg()
      : is_alter_columns_(false),
        is_alter_indexs_(false),
        is_alter_options_(false),
        is_alter_partitions_(false),
        session_id_(common::OB_INVALID_ID),
        alter_part_type_(NO_OPERATION),
        alter_constraint_type_(CONSTRAINT_NO_OPERATION),
        index_arg_list_(),
        foreign_key_arg_list_(),
        alter_table_schema_(),
        // To be precise, this should be called create_partition_mode?
        create_mode_(OB_CREATE_TABLE_MODE_STRICT),
        tz_info_wrap_(),
        is_inner_(false),
        nls_formats_{},
        skip_sys_table_check_(false),
        is_update_global_indexes_(false)
  {}
  ~ObAlterTableArg()
  {
    for (int64_t i = 0; i < index_arg_list_.size(); ++i) {
      ObIndexArg* index_arg = index_arg_list_.at(i);
      if (OB_NOT_NULL(index_arg)) {
        index_arg->~ObIndexArg();
      }
    }
    allocator_.clear();
  }
  bool is_valid() const;
  bool has_rename_action() const
  {
    return alter_table_schema_.alter_option_bitset_.has_member(TABLE_NAME);
  }
  bool need_progressive_merge() const
  {
    return alter_table_schema_.alter_option_bitset_.has_member(BLOCK_SIZE) ||
           alter_table_schema_.alter_option_bitset_.has_member(COMPRESS_METHOD) ||
           alter_table_schema_.alter_option_bitset_.has_member(PCTFREE) ||
           alter_table_schema_.alter_option_bitset_.has_member(STORE_FORMAT) ||
           alter_table_schema_.alter_option_bitset_.has_member(STORAGE_FORMAT_VERSION) ||
           alter_table_schema_.alter_option_bitset_.has_member(PROGRESSIVE_MERGE_ROUND);
  }
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool is_allow_when_upgrade() const;
  bool is_refresh_sess_active_time() const;
  inline void set_tz_info_map(const common::ObTZInfoMap* tz_info_map)
  {
    tz_info_wrap_.set_tz_info_map(tz_info_map);
    tz_info_.set_tz_info_map(tz_info_map);
  }
  int set_nls_formats(const common::ObString* nls_formats);
  TO_STRING_KV(K_(is_alter_columns), K_(is_alter_indexs), K_(is_alter_options), K_(session_id), K_(index_arg_list),
      K_(alter_table_schema), K_(is_inner), "nls_formats",
      common::ObArrayWrap<common::ObString>(nls_formats_, common::ObNLSFormatEnum::NLS_MAX));

  bool is_alter_columns_;
  bool is_alter_indexs_;
  bool is_alter_options_;
  bool is_alter_partitions_;
  uint64_t session_id_;  // Only used to update the last active time of the temporary table. At this time, the session
                         // id used to create the temporary table is passed in
  AlterPartitionType alter_part_type_;
  AlterConstraintType alter_constraint_type_;
  common::ObSArray<ObIndexArg*> index_arg_list_;
  common::ObSArray<ObCreateForeignKeyArg> foreign_key_arg_list_;
  share::schema::AlterTableSchema alter_table_schema_;
  common::ObArenaAllocator allocator_;
  common::ObTimeZoneInfo tz_info_;  // unused now
  ObCreateTableMode create_mode_;
  common::ObTimeZoneInfoWrap tz_info_wrap_;
  bool is_inner_;
  common::ObString nls_formats_[common::ObNLSFormatEnum::NLS_MAX];
  bool skip_sys_table_check_;
  bool is_update_global_indexes_;

  int serialize_index_args(char* buf, const int64_t data_len, int64_t& pos) const;
  int deserialize_index_args(const char* buf, const int64_t data_len, int64_t& pos);
  int64_t get_index_args_serialize_size() const;
};

struct ObTableItem {
  OB_UNIS_VERSION(1);

public:
  ObTableItem()
      : mode_(common::OB_NAME_CASE_INVALID),  // for compare
        database_name_(),
        table_name_()
  {}
  bool operator==(const ObTableItem& table_item) const;
  inline uint64_t hash(uint64_t seed = 0) const;
  void reset()
  {
    mode_ = common::OB_NAME_CASE_INVALID;
    database_name_.reset();
    table_name_.reset();
  }
  DECLARE_TO_STRING;

  common::ObNameCaseMode mode_;
  common::ObString database_name_;
  common::ObString table_name_;
};

inline uint64_t ObTableItem::hash(uint64_t seed) const
{
  uint64_t val = seed;
  if (!database_name_.empty() && !table_name_.empty()) {
    val = common::murmurhash(database_name_.ptr(), database_name_.length(), val);
    val = common::murmurhash(table_name_.ptr(), table_name_.length(), val);
  }
  return val;
}

struct ObDropTableArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropTableArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        session_id_(common::OB_INVALID_ID),
        sess_create_time_(0),
        table_type_(share::schema::MAX_TABLE_TYPE),
        tables_(),
        if_exist_(false),
        to_recyclebin_(false)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  DECLARE_TO_STRING;

  uint64_t tenant_id_;
  uint64_t session_id_;       // Pass in session id when deleting table
  int64_t sess_create_time_;  // When deleting oracle temporary table data, pass in the creation time of sess
  share::schema::ObTableType table_type_;
  common::ObSArray<ObTableItem> tables_;
  bool if_exist_;
  bool to_recyclebin_;
};

struct ObOptimizeTableArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObOptimizeTableArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), tables_()
  {}
  DECLARE_TO_STRING;
  bool is_valid() const;
  uint64_t tenant_id_;
  common::ObSArray<ObTableItem> tables_;
};

struct ObOptimizeTenantArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObOptimizeTenantArg() : ObDDLArg(), tenant_name_()
  {}
  bool is_valid() const;
  DECLARE_TO_STRING;

  common::ObString tenant_name_;
};

struct ObOptimizeAllArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObOptimizeAllArg() : ObDDLArg()
  {}
  bool is_valid() const
  {
    return true;
  }
  DECLARE_TO_STRING;
};

struct ObColumnSortItem {
  OB_UNIS_VERSION(1);

public:
  ObColumnSortItem()
      : column_name_(),
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
  inline uint64_t get_column_id() const
  {
    return column_id_;
  }

  DECLARE_TO_STRING;

  common::ObString column_name_;
  int32_t prefix_len_;
  common::ObOrderType order_type_;
  uint64_t column_id_;
  bool is_func_index_;  // Whether the mark is a function index, the default is false.
};

struct ObTableOption {
  OB_UNIS_VERSION_V(1);

public:
  ObTableOption()
      : block_size_(-1),
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
  common::ObStoreFormatType store_format_;
  int64_t progressive_merge_round_;
  int64_t storage_format_version_;
};

struct ObIndexOption : public ObTableOption {
  OB_UNIS_VERSION(1);

public:
  ObIndexOption()
      : ObTableOption(),
        parser_name_(common::OB_DEFAULT_FULLTEXT_PARSER_NAME),
        index_attributes_set_(common::OB_DEFAULT_INDEX_ATTRIBUTES_SET)
  {}

  bool is_valid() const;
  void reset()
  {
    ObTableOption::reset();
    parser_name_ = common::ObString::make_string(common::OB_DEFAULT_FULLTEXT_PARSER_NAME);
  }
  DECLARE_TO_STRING;

  common::ObString parser_name_;
  uint64_t index_attributes_set_;  // flags, one bit for one attribute
};

struct ObCreateIndexArg : public ObIndexArg {
  OB_UNIS_VERSION_V(1);

public:
  ObCreateIndexArg()
      : index_type_(share::schema::INDEX_TYPE_IS_NOT),
        index_columns_(),
        store_columns_(),
        hidden_store_columns_(),
        fulltext_columns_(),
        index_option_(),
        create_mode_(OB_CREATE_TABLE_MODE_STRICT),
        data_table_id_(common::OB_INVALID_ID),
        index_table_id_(common::OB_INVALID_ID),
        if_not_exist_(false),
        with_rowid_(false),
        index_schema_(),
        is_inner_(false),
        nls_date_format_(),
        nls_timestamp_format_(),
        nls_timestamp_tz_format_(),
        sql_mode_(0)
  {
    index_action_type_ = ADD_INDEX;
    index_using_type_ = share::schema::USING_BTREE;
  }
  virtual ~ObCreateIndexArg()
  {}
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
  }
  bool is_valid() const;
  int assign(const ObCreateIndexArg& other)
  {
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
    } else if (OB_FAIL(index_schema_.assign(other.index_schema_))) {
      SHARE_LOG(WARN, "fail to assign index schema", K(ret));
    } else {
      index_type_ = other.index_type_;
      index_option_ = other.index_option_;
      index_using_type_ = other.index_using_type_;
      create_mode_ = other.create_mode_;
      data_table_id_ = other.data_table_id_;
      index_table_id_ = other.index_table_id_;
      if_not_exist_ = other.if_not_exist_;
      with_rowid_ = other.with_rowid_;
      is_inner_ = other.is_inner_;
      nls_date_format_ = other.nls_date_format_;
      nls_timestamp_format_ = other.nls_timestamp_format_;
      nls_timestamp_tz_format_ = other.nls_timestamp_tz_format_;
      sql_mode_ = other.sql_mode_;
    }
    return ret;
  }
  inline bool is_unique_primary_index() const
  {
    return share::schema::INDEX_TYPE_UNIQUE_LOCAL == index_type_ ||
           share::schema::INDEX_TYPE_UNIQUE_GLOBAL == index_type_ ||
           share::schema::INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type_ ||
           share::schema::INDEX_TYPE_PRIMARY == index_type_;
  }
  DECLARE_VIRTUAL_TO_STRING;

  share::schema::ObIndexType index_type_;
  common::ObSEArray<ObColumnSortItem, common::OB_PREALLOCATED_NUM> index_columns_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> store_columns_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> hidden_store_columns_;
  common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM> fulltext_columns_;
  ObIndexOption index_option_;
  share::schema::ObIndexUsingType index_using_type_;
  ObCreateTableMode create_mode_;
  uint64_t data_table_id_;
  uint64_t index_table_id_;  // Data_table_id and index_table_id will be given in SQL during recovery
  bool if_not_exist_;
  bool with_rowid_;
  share::schema::ObTableSchema index_schema_;  // Index table schema
  bool is_inner_;
  // Nls_xx_format is required when creating a functional index
  common::ObString nls_date_format_;
  common::ObString nls_timestamp_format_;
  common::ObString nls_timestamp_tz_format_;
  ObSQLMode sql_mode_;
};

struct ObCreateForeignKeyArg : public ObIndexArg {
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
        ref_cst_type_(),
        ref_cst_id_(),
        validate_flag_(true),
        is_modify_validate_flag_(false),
        rely_flag_(false),
        is_modify_rely_flag_(false),
        is_modify_fk_state_(false)
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
    validate_flag_ = true;
    is_modify_validate_flag_ = false;
    rely_flag_ = false;
    is_modify_rely_flag_ = false;
    is_modify_fk_state_ = false;
  }
  bool is_valid() const;
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
  bool validate_flag_;
  bool is_modify_validate_flag_;
  bool rely_flag_;
  bool is_modify_rely_flag_;
  bool is_modify_fk_state_;
};

struct ObDropForeignKeyArg : public ObIndexArg {
  OB_UNIS_VERSION_V(1);

public:
  ObDropForeignKeyArg() : ObIndexArg(), foreign_key_name_()
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

struct ObFlashBackTableFromRecyclebinArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObFlashBackTableFromRecyclebinArg()
      : ObDDLArg(),
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
  uint64_t origin_table_id_;  // only used in work thread, no need add to SERIALIZE now

  TO_STRING_KV(K_(tenant_id), K_(origin_db_name), K_(origin_table_name), K_(new_db_name), K_(new_table_name),
      K_(origin_table_id));
};

struct ObFlashBackIndexArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObFlashBackIndexArg()
      : ObDDLArg(),
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
  uint64_t origin_table_id_;  // only used in work thread, no need add to SERIALIZE now

  TO_STRING_KV(K_(tenant_id), K_(origin_table_name), K_(new_db_name), K_(new_table_name), K_(origin_table_id));
};

struct ObFlashBackDatabaseArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObFlashBackDatabaseArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), origin_db_name_(), new_db_name_()
  {}
  bool is_valid() const;
  uint64_t tenant_id_;
  common::ObString origin_db_name_;
  common::ObString new_db_name_;

  TO_STRING_KV(K_(tenant_id), K_(origin_db_name), K_(new_db_name));
};

struct ObFlashBackTenantArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObFlashBackTenantArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), origin_tenant_name_(), new_tenant_name_()
  {}
  bool is_valid() const;
  uint64_t tenant_id_;
  common::ObString origin_tenant_name_;
  common::ObString new_tenant_name_;

  TO_STRING_KV(K_(tenant_id), K_(origin_tenant_name), K_(new_tenant_name));
};

struct ObPurgeTableArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObPurgeTableArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), table_name_()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  uint64_t tenant_id_;
  common::ObString table_name_;
  TO_STRING_KV(K_(tenant_id), K_(table_name));
};

struct ObPurgeIndexArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObPurgeIndexArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), table_name_(), table_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  uint64_t tenant_id_;
  common::ObString table_name_;
  uint64_t table_id_;  // only used in work thread, no need add to SERIALIZE now

  TO_STRING_KV(K_(tenant_id), K_(table_name), K_(table_id));
};

struct ObPurgeDatabaseArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObPurgeDatabaseArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), db_name_()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  uint64_t tenant_id_;
  common::ObString db_name_;
  TO_STRING_KV(K_(tenant_id), K_(db_name));
};

struct ObPurgeTenantArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObPurgeTenantArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), tenant_name_()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  uint64_t tenant_id_;
  common::ObString tenant_name_;
  TO_STRING_KV(K_(tenant_id), K_(tenant_name));
};

struct ObPurgeRecycleBinArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  static const int DEFAULT_PURGE_EACH_TIME = 10;
  ObPurgeRecycleBinArg()
      : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), purge_num_(0), expire_time_(0), auto_purge_(false)
  {}
  virtual ~ObPurgeRecycleBinArg()
  {}
  bool is_valid() const;
  int assign(const ObPurgeRecycleBinArg& other);
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  uint64_t tenant_id_;
  int64_t purge_num_;
  int64_t expire_time_;
  bool auto_purge_;
  TO_STRING_KV(K_(tenant_id), K_(purge_num), K_(expire_time), K_(auto_purge));
};

/*
 * 1. When pg_key_ and partition_key_ are equal, the entity partition needs to be created.
 * 1.1 When both partition_key and pg_key are filled in as the partition key of a table,
 *  the physical partition of the standalone table is created
 * 1.2 When partition_key and pg_key are both filled in as a tg pg_key, a pg physical partition is created
 * 1.3 In both cases 1.1 and 1.2, the entity partition is created, and the is_binding() method is false
 * 2. When pg_key_ and partition_key_ are not equal, there is no need to create an entity partition
 * 2.1 At this time, the partition partition of a table is bound to a physical partition of pg
 * 2.2 In the case of 2.1, the partition of the table is helped by the physical partition of the pg,
 *  and the is_binding() method returns true
 */
struct ObCreatePartitionArg {
  OB_UNIS_VERSION(1);

public:
  ObCreatePartitionArg()
  {
    reset();
  }
  ~ObCreatePartitionArg()
  {}
  inline void reset();
  bool is_valid() const;
  bool is_binding() const
  {
    return pg_key_ != partition_key_;
  }
  int check_need_create_sstable(bool& need_create_sstable) const;
  int set_memstore_percent(const int64_t mp)
  {
    return replica_property_.set_memstore_percent(mp);
  }
  int64_t get_memstore_percent() const
  {
    return replica_property_.get_memstore_percent();
  }
  // Meaning of pg_key_ / partition_key_ value:
  // (1) partition/partition -> craete_partition
  // (2) pg / partition -> add partition to pg
  // (3) pg / pg -> create pg
  bool is_create_pg() const
  {
    return pg_key_.is_pg() && pg_key_.is_valid() && pg_key_ == partition_key_;
  }
  bool is_create_pg_partition() const
  {
    return pg_key_.is_pg() && pg_key_.is_valid() && partition_key_.is_valid() && pg_key_ != partition_key_;
  }
  bool is_standby_restore() const
  {
    return share::REPLICA_RESTORE_STANDBY == restore_;
  }
  int deep_copy(const ObCreatePartitionArg& arg);
  int assign(const ObCreatePartitionArg& other);

  DECLARE_TO_STRING;

  common::ObZone zone_;
  common::ObPartitionKey partition_key_;
  int64_t schema_version_;
  int64_t memstore_version_;
  int64_t replica_num_;  // The number of versatile copies
  common::ObMemberList member_list_;
  common::ObAddr leader_;
  int64_t lease_start_;
  int64_t logonly_replica_num_;         // Number of journal copies
  int64_t backup_replica_num_;          // Number of backup copies
  int64_t readonly_replica_num_;        // Number of read-only replicas
  common::ObReplicaType replica_type_;  // Copy type
  int64_t last_submit_timestamp_;       // next log id timestamp
  int64_t restore_;                     // If it is not 0, it means that it needs to be restored
  common::ObSArray<share::schema::ObTableSchema> table_schemas_;
  common::ObPartitionKey source_partition_key_;  // It makes sense when dismantling tables and splitting to build
                                                 // partiton (obsolete)
  int64_t frozen_timestamp_;  // It is a pair with memstore_version, indicating the snapshot corresponding to the
                              // version;
  int64_t non_paxos_replica_num_;
  uint64_t last_replay_log_id_;  // Only used for force_create_sys_table, the default value is 0
  common::ObPGKey pg_key_;
  common::ObReplicaProperty replica_property_;
  share::ObSplitPartition split_info_;  // It makes sense when dismantling tables and splitting to build partiton
  bool ignore_member_list_;  // Used to mark whether the partition needs to be created repeatedly, currently only used
                             // in the creation of replicas synchronized from the standby database
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreatePartitionArg);
};

struct ObCreatePartitionStorageArg {
  OB_UNIS_VERSION(1);

public:
  ObCreatePartitionStorageArg()
  {
    reset();
  }
  ~ObCreatePartitionStorageArg()
  {}
  inline void reset();
  bool is_valid() const;

  DECLARE_TO_STRING;

  common::ObPGKey rgkey_;
  common::ObPartitionKey partition_key_;
  int64_t schema_version_;
  int64_t memstore_version_;
  int64_t last_submit_timestamp_;  // next log id timestamp
  int64_t restore_;                // If it is not 0, it means that it needs to be restored
  common::ObSArray<share::schema::ObTableSchema> table_schemas_;
};

struct ObCreatePartitionBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObCreatePartitionBatchArg()
  {
    reset();
  }
  ~ObCreatePartitionBatchArg()
  {}
  bool is_valid() const;
  inline void reset();
  int assign(const ObCreatePartitionBatchArg& other);
  void reuse()
  {
    args_.reuse();
  }

  DECLARE_TO_STRING;

  common::ObSArray<ObCreatePartitionArg> args_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreatePartitionBatchArg);
};

struct ObSetMemberListArg {
  OB_UNIS_VERSION(1);

public:
  ObSetMemberListArg() : key_(), member_list_(), quorum_(0), lease_start_(0), leader_()
  {}
  ~ObSetMemberListArg()
  {}
  bool is_valid() const;
  void reset();
  int assign(const ObSetMemberListArg& other);
  int init(const int64_t table_id, const int64_t partition_id, const int64_t partition_cnt,
      const common::ObMemberList& member_list, const int64_t quorum, const int64_t lease_start = 0,
      const common::ObAddr& leader = common::ObAddr());
  DECLARE_TO_STRING;
  common::ObPartitionKey key_;
  common::ObMemberList member_list_;
  int64_t quorum_;
  int64_t lease_start_;
  common::ObAddr leader_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSetMemberListArg);
};

struct ObSetMemberListBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObSetMemberListBatchArg()
  {
    reset();
  }
  ~ObSetMemberListBatchArg()
  {}
  bool is_valid() const;
  void reset();
  int assign(const ObSetMemberListBatchArg& other);
  int add_arg(const common::ObPartitionKey& key, const common::ObMemberList& member_list);
  bool reach_concurrency_limit() const
  {
    return args_.count() >= MAX_COUNT;
  }
  bool has_task() const
  {
    return args_.count() > 0;
  }
  DECLARE_TO_STRING;

  common::ObSArray<ObSetMemberListArg> args_;
  int64_t timestamp_;  // Used to verify asynchronous rpc return packets
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetMemberListBatchArg);
};

struct ObCreatePartitionBatchRes {
  OB_UNIS_VERSION(1);

public:
  ObCreatePartitionBatchRes() : ret_list_(), timestamp_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObCreatePartitionBatchRes()
  {}
  inline void reset();
  int assign(const ObCreatePartitionBatchRes& other);
  inline void reuse();

  DECLARE_TO_STRING;
  // response includes all rets
  common::ObSArray<int> ret_list_;
  int64_t timestamp_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreatePartitionBatchRes);
};

struct ObGetMinSSTableSchemaVersionRes {
  OB_UNIS_VERSION(1);

public:
  ObGetMinSSTableSchemaVersionRes() : ret_list_()
  {}
  ~ObGetMinSSTableSchemaVersionRes()
  {
    reset();
  }
  inline void reset()
  {
    ret_list_.reset();
  }
  inline void reuse()
  {
    ret_list_.reuse();
  }

  TO_STRING_KV(K_(ret_list));
  // response includes all rets
  common::ObSArray<int64_t> ret_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGetMinSSTableSchemaVersionRes);
};

struct ObCheckUniqueIndexRequestArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckUniqueIndexRequestArg()
  {
    reset();
  }
  ~ObCheckUniqueIndexRequestArg() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(pkey), K_(index_id), K_(schema_version));

public:
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
  int64_t schema_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckUniqueIndexRequestArg);
};

struct ObCheckUniqueIndexResponseArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckUniqueIndexResponseArg()
  {
    reset();
  }
  ~ObCheckUniqueIndexResponseArg() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(pkey), K_(index_id), K_(ret_code), K_(is_valid));

public:
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
  int ret_code_;
  bool is_valid_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckUniqueIndexResponseArg);
};

struct ObCalcColumnChecksumRequestArg {
  OB_UNIS_VERSION(1);

public:
  ObCalcColumnChecksumRequestArg()
  {
    reset();
  }
  ~ObCalcColumnChecksumRequestArg() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(pkey), K_(index_id), K_(schema_version), K_(execution_id), K_(snapshot_version));

public:
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
  int64_t schema_version_;
  uint64_t execution_id_;
  int64_t snapshot_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCalcColumnChecksumRequestArg);
};

struct ObCalcColumnChecksumResponseArg {
  OB_UNIS_VERSION(1);

public:
  ObCalcColumnChecksumResponseArg()
  {
    reset();
  }
  ~ObCalcColumnChecksumResponseArg() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(pkey), K_(index_id), K_(ret_code));

public:
  common::ObPartitionKey pkey_;
  uint64_t index_id_;
  int ret_code_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCalcColumnChecksumResponseArg);
};

struct ObCheckSingleReplicaMajorSSTableExistArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckSingleReplicaMajorSSTableExistArg()
  {
    reset();
  }
  ~ObCheckSingleReplicaMajorSSTableExistArg() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(pkey), K_(index_id));
  common::ObPartitionKey pkey_;
  uint64_t index_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckSingleReplicaMajorSSTableExistArg);
};

struct ObCheckSingleReplicaMajorSSTableExistResult {
  OB_UNIS_VERSION(1);

public:
  ObCheckSingleReplicaMajorSSTableExistResult()
  {
    reset();
  }
  ~ObCheckSingleReplicaMajorSSTableExistResult() = default;
  void reset();
  TO_STRING_KV(K_(timestamp));
  int64_t timestamp_;
};

struct ObCheckAllReplicaMajorSSTableExistArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckAllReplicaMajorSSTableExistArg()
  {
    reset();
  }
  ~ObCheckAllReplicaMajorSSTableExistArg() = default;
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(pkey), K_(index_id));
  common::ObPartitionKey pkey_;
  uint64_t index_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCheckAllReplicaMajorSSTableExistArg);
};

struct ObCheckAllReplicaMajorSSTableExistResult {
  OB_UNIS_VERSION(1);

public:
  ObCheckAllReplicaMajorSSTableExistResult()
  {
    reset();
  }
  ~ObCheckAllReplicaMajorSSTableExistResult() = default;
  void reset();
  TO_STRING_KV(K_(max_timestamp));
  int64_t max_timestamp_;
};

// TODO(liuyue):delete these two rpc
struct ObMigrateArg {
  OB_UNIS_VERSION(1);

public:
  ObMigrateArg() : keep_src_(false)
  {}
  bool is_valid() const
  {
    return partition_key_.is_valid() && src_.is_valid() && dst_.is_valid();
  }
  DECLARE_TO_STRING;

  common::ObPartitionKey partition_key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember replace_;
  bool keep_src_;
};

//----Structs for partition online/offline----
struct ObCopySSTableArg {
  OB_UNIS_VERSION(1);

public:
  ObCopySSTableArg()
      : key_(),
        src_(),
        dst_(),
        task_id_(),
        type_(common::OB_COPY_SSTABLE_TYPE_INVALID),
        index_table_id_(common::OB_INVALID_ID),
        priority_(common::ObReplicaOpPriority::PRIO_LOW),
        cluster_id_(common::OB_INVALID_ID),
        skip_change_member_list_(false),
        switch_epoch_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const
  {
    bool bret = false;
    bret = key_.is_valid() && src_.is_valid() && dst_.is_valid() && common::OB_COPY_SSTABLE_TYPE_INVALID != type_ &&
           (common::OB_COPY_SSTABLE_TYPE_LOCAL_INDEX != type_ || common::OB_INVALID_ID != index_table_id_);
    if (bret && !IS_CLUSTER_VERSION_BEFORE_2200) {
      bret = (common::OB_INVALID_VERSION != switch_epoch_);
    }
    return bret;
  }
  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(cluster_id), K_(task_id), K_(type), K_(index_table_id), K_(priority),
      K(skip_change_member_list_), K(switch_epoch_));
  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  share::ObTaskId task_id_;
  common::ObCopySSTableType type_;
  uint64_t index_table_id_;
  common::ObReplicaOpPriority priority_;
  int64_t cluster_id_;
  bool skip_change_member_list_;
  int64_t switch_epoch_;
};

struct ObCopySSTableRes {
  OB_UNIS_VERSION(1);

public:
  ObCopySSTableRes()
      : key_(),
        src_(),
        dst_(),
        data_src_(),
        type_(common::OB_COPY_SSTABLE_TYPE_INVALID),
        index_table_id_(common::OB_INVALID_ID),
        result_(0)
  {}
  bool is_valid() const
  {
    return key_.is_valid() && src_.is_valid() && dst_.is_valid() && common::OB_COPY_SSTABLE_TYPE_INVALID != type_ &&
           (common::OB_COPY_SSTABLE_TYPE_LOCAL_INDEX != type_ || common::OB_INVALID_ID != index_table_id_);
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(data_src), K_(result), K_(type), K_(index_table_id));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  common::ObCopySSTableType type_;
  uint64_t index_table_id_;
  int64_t result_;
};

struct ObAddReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObAddReplicaArg()
      : key_(),
        src_(),
        dst_(),
        quorum_(0),
        reserved_modify_quorum_type_(common::MAX_MODIFY_QUORUM_TYPE),
        task_id_(),
        priority_(common::ObReplicaOpPriority::PRIO_HIGH),
        cluster_id_(common::OB_INVALID_ID),
        skip_change_member_list_(false),
        switch_epoch_(common::OB_INVALID_VERSION),
        pg_file_id_(common::OB_INVALID_DATA_FILE_ID)
  {}
  bool is_valid() const
  {
    bool bret = false;
    bret = key_.is_valid() && src_.is_valid() && dst_.is_valid() && src_.get_server() != dst_.get_server() &&
           is_replica_op_priority_valid(priority_);
    if (bret && !IS_CLUSTER_VERSION_BEFORE_2200) {
      bret = (common::OB_INVALID_VERSION != switch_epoch_);
    }
    return bret;
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(quorum), K_(reserved_modify_quorum_type), K(task_id_), K_(priority),
      K(cluster_id_), K_(skip_change_member_list), K_(switch_epoch), K_(pg_file_id));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  int64_t quorum_;
  common::ObModifyQuorumType reserved_modify_quorum_type_;  // unused
  share::ObTaskId task_id_;
  common::ObReplicaOpPriority priority_;
  int64_t cluster_id_;
  bool skip_change_member_list_;
  int64_t switch_epoch_;
  int64_t pg_file_id_;
};

struct ObAddReplicaRes {
  OB_UNIS_VERSION(2);

public:
  ObAddReplicaRes() : key_(), src_(), dst_(), data_src_(), quorum_(0), result_(0)
  {}
  bool is_valid() const
  {
    return key_.is_valid() && src_.is_valid() && dst_.is_valid();
  }
  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(quorum), K_(data_src), K_(result));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  int64_t quorum_;
  int64_t result_;
};

struct ObRebuildReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObRebuildReplicaArg()
      : key_(),
        src_(),
        dst_(),
        task_id_(),
        priority_(common::ObReplicaOpPriority::PRIO_HIGH),
        skip_change_member_list_(false),
        switch_epoch_(common::OB_INVALID_VERSION)
  {}

  bool is_valid() const
  {
    bool bret = false;
    bret = key_.is_valid() && src_.is_valid() && dst_.is_valid() && is_replica_op_priority_valid(priority_);
    if (bret && !IS_CLUSTER_VERSION_BEFORE_2200) {
      bret = (common::OB_INVALID_VERSION != switch_epoch_);
    }
    return bret;
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(task_id), K_(priority), K_(skip_change_member_list), K_(switch_epoch));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  share::ObTaskId task_id_;
  common::ObReplicaOpPriority priority_;
  bool skip_change_member_list_;
  int64_t switch_epoch_;
};

struct ObRebuildReplicaRes {
  OB_UNIS_VERSION(1);

public:
  ObRebuildReplicaRes() : key_(), src_(), dst_(), data_src_(), result_(0)
  {}
  bool is_valid() const
  {
    return key_.is_valid() && src_.is_valid() && dst_.is_valid();
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(data_src), K_(result));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  int64_t result_;
};

struct ObRemoveNonPaxosReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObRemoveNonPaxosReplicaArg()
      : key_(), dst_(), task_id_(), skip_change_member_list_(false), switch_epoch_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const
  {
    bool bret = false;
    bret = key_.is_valid() && dst_.is_valid();
    if (bret && !IS_CLUSTER_VERSION_BEFORE_2200) {
      bret = (common::OB_INVALID_VERSION != switch_epoch_);
    }
    return bret;
  }

  TO_STRING_KV(K_(key), K_(dst), K_(task_id), K_(skip_change_member_list), K_(switch_epoch));

  common::ObPartitionKey key_;
  common::ObReplicaMember dst_;
  share::ObTaskId task_id_;
  bool skip_change_member_list_;
  int64_t switch_epoch_;
};

struct ObRemoveNonPaxosReplicaBatchResult {
public:
  ObRemoveNonPaxosReplicaBatchResult() : return_array_()
  {}

public:
  TO_STRING_KV(K_(return_array));

public:
  common::ObSArray<int> return_array_;

  OB_UNIS_VERSION(3);
};

struct ObRestoreReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObRestoreReplicaArg()
      : key_(),
        src_(),
        dst_(),
        task_id_(),
        priority_(common::ObReplicaOpPriority::PRIO_LOW),
        skip_change_member_list_(false),
        switch_epoch_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const
  {
    bool bret = false;
    bret = key_.is_valid() && src_.is_valid() && dst_.is_valid() && is_replica_op_priority_valid(priority_);
    if (bret && !IS_CLUSTER_VERSION_BEFORE_2200) {
      bret = (common::OB_INVALID_VERSION != switch_epoch_);
    }
    return bret;
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(priority), K_(skip_change_member_list), K_(switch_epoch));

  common::ObPartitionKey key_;
  share::ObRestoreArgs src_;
  common::ObReplicaMember dst_;
  share::ObTaskId task_id_;
  common::ObReplicaOpPriority priority_;
  bool skip_change_member_list_;
  int64_t switch_epoch_;
};

struct ObRestoreReplicaRes {
  OB_UNIS_VERSION(1);

public:
  ObRestoreReplicaRes() : key_(), src_(), dst_(), result_(0)
  {}
  bool is_valid() const
  {
    return key_.is_valid() && src_.is_valid() && dst_.is_valid();
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(result));

  common::ObPartitionKey key_;
  share::ObRestoreArgs src_;
  common::ObReplicaMember dst_;
  int64_t result_;
};

struct ObPhyRestoreReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObPhyRestoreReplicaArg();
  bool is_valid() const;
  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(task_id), K_(priority));

  common::ObPartitionKey key_;
  share::ObPhysicalRestoreArg src_;
  common::ObReplicaMember dst_;
  share::ObTaskId task_id_;
  common::ObReplicaOpPriority priority_;
};

struct ObPhyRestoreReplicaRes {
  OB_UNIS_VERSION(1);

public:
  ObPhyRestoreReplicaRes();
  bool is_valid() const;

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(result));

  common::ObPartitionKey key_;
  share::ObPhysicalRestoreArg src_;
  common::ObReplicaMember dst_;
  int64_t result_;
};

struct ObMigrateReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObMigrateReplicaArg()
      : key_(),
        src_(),
        dst_(),
        data_source_(),
        quorum_(0),
        task_id_(),
        priority_(common::ObReplicaOpPriority::PRIO_LOW),
        skip_change_member_list_(false),
        switch_epoch_(common::OB_INVALID_VERSION),
        migrate_mode_(MigrateMode::MT_LOCAL_FS_MODE)
  {}
  bool is_valid() const
  {
    bool bret = false;
    bret = key_.is_valid() && src_.is_valid() && dst_.is_valid() && data_source_.is_valid() &&
           src_.get_server() != dst_.get_server() && data_source_.get_server() != dst_.get_server() &&
           is_replica_op_priority_valid(priority_) && migrate_mode_ < obrpc::MigrateMode::MT_MAX;
    if (bret && !IS_CLUSTER_VERSION_BEFORE_2200) {
      bret = (common::OB_INVALID_VERSION != switch_epoch_);
    }
    return bret;
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(data_source), K_(quorum), K_(task_id), K_(priority),
      K_(skip_change_member_list), K_(switch_epoch), K_(migrate_mode));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_source_;
  int64_t quorum_;
  share::ObTaskId task_id_;
  common::ObReplicaOpPriority priority_;
  bool skip_change_member_list_;
  int64_t switch_epoch_;
  MigrateMode migrate_mode_;
};

struct ObMigrateReplicaRes {
  OB_UNIS_VERSION(2);

public:
  ObMigrateReplicaRes() : key_(), src_(), dst_(), data_src_(), result_(0)
  {}
  bool is_valid() const
  {
    return key_.is_valid() && src_.is_valid() && dst_.is_valid();
  }
  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(result), K_(data_src));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  int64_t result_;
};

struct ObChangeReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObChangeReplicaArg()
      : key_(),
        src_(),
        dst_(),
        quorum_(0),
        task_id_(),
        priority_(common::ObReplicaOpPriority::PRIO_LOW),
        skip_change_member_list_(false),
        switch_epoch_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const;

  TO_STRING_KV(
      K_(key), K_(src), K_(dst), K_(quorum), K_(task_id), K_(priority), K_(skip_change_member_list), K_(switch_epoch))

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  int64_t quorum_;
  share::ObTaskId task_id_;
  common::ObReplicaOpPriority priority_;
  bool skip_change_member_list_;
  int64_t switch_epoch_;
};

struct ObChangeReplicaRes {
  OB_UNIS_VERSION(1);

public:
  ObChangeReplicaRes() : key_(), src_(), dst_(), data_src_(), quorum_(0), result_(0)
  {}
  bool is_valid() const
  {
    return key_.is_valid() && src_.is_valid() && dst_.is_valid();
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(quorum), K_(data_src), K_(result));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  int64_t quorum_;
  int64_t result_;
};

struct ObBackupArg {
  OB_UNIS_VERSION(1);

public:
  ObBackupArg()
      : key_(),
        src_(),
        dst_(),
        physical_backup_arg_(),
        task_id_(),
        priority_(common::ObReplicaOpPriority::PRIO_HIGH),
        cluster_id_(common::OB_INVALID_ID),
        skip_change_member_list_(false),
        switch_epoch_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const
  {
    bool bret = false;
    bret = key_.is_valid() && src_.is_valid() && dst_.is_valid() && physical_backup_arg_.is_valid();
    if (bret && !IS_CLUSTER_VERSION_BEFORE_2200) {
      bret = (common::OB_INVALID_VERSION != switch_epoch_);
    }
    return bret;
  }
  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(physical_backup_arg), K_(cluster_id), K_(task_id), K_(priority),
      K(skip_change_member_list_), K(switch_epoch_));
  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  share::ObPhysicalBackupArg physical_backup_arg_;
  share::ObTaskId task_id_;
  common::ObReplicaOpPriority priority_;
  int64_t cluster_id_;
  bool skip_change_member_list_;
  int64_t switch_epoch_;
};

struct ObValidateArg {
  OB_UNIS_VERSION(1);

public:
  ObValidateArg() : trace_id_(), dst_(), physical_validate_arg_(), priority_(common::ObReplicaOpPriority::PRIO_LOW)
  {}
  int assign(const ObValidateArg& arg);
  bool is_valid() const
  {
    return physical_validate_arg_.is_valid();
  }
  TO_STRING_KV(K_(trace_id), K_(dst), K_(physical_validate_arg), K_(priority));

  share::ObTaskId trace_id_;
  common::ObReplicaMember dst_;
  share::ObPhysicalValidateArg physical_validate_arg_;
  common::ObReplicaOpPriority priority_;
};

struct ObStandbyCutDataTaskArg {
  OB_UNIS_VERSION(1);

public:
  ObStandbyCutDataTaskArg() : dst_(), pkey_()
  {}
  bool is_valid() const
  {
    return pkey_.is_valid();
  }
  TO_STRING_KV(K_(dst), K_(pkey));
  common::ObReplicaMember dst_;
  common::ObPartitionKey pkey_;
};

struct ObMigrateBackupsetArg {
  OB_UNIS_VERSION(1);

public:
  ObMigrateBackupsetArg();
  int assign(const ObMigrateBackupsetArg& arg);
  bool is_valid() const;

  TO_STRING_KV(K_(backup_set_id), K_(pg_key), K_(backup_backupset_arg));

  uint64_t backup_set_id_;
  common::ObPartitionKey pg_key_;
  share::ObBackupBackupsetArg backup_backupset_arg_;
};

struct ObBackupRes {
  OB_UNIS_VERSION(1);

public:
  ObBackupRes() : key_(), src_(), dst_(), data_src_(), physical_backup_arg_(), result_(0)
  {}
  bool is_valid() const
  {
    return key_.is_valid() && src_.is_valid() && dst_.is_valid() && physical_backup_arg_.is_valid();
  }

  TO_STRING_KV(K_(key), K_(src), K_(dst), K_(data_src), K_(physical_backup_arg), K_(result));

  common::ObPartitionKey key_;
  common::ObReplicaMember src_;
  common::ObReplicaMember dst_;
  common::ObReplicaMember data_src_;
  share::ObPhysicalBackupArg physical_backup_arg_;
  int64_t result_;
};

struct ObValidateRes {
  OB_UNIS_VERSION(1);

public:
  ObValidateRes() : key_(), dst_(), validate_arg_(), result_(0)
  {}
  int assign(const ObValidateRes& res);
  bool is_valid() const
  {
    return key_.is_valid() && dst_.is_valid() && validate_arg_.is_valid();
  }

  TO_STRING_KV(K_(key), K_(dst), K_(validate_arg), K_(result));
  common::ObPartitionKey key_;
  common::ObReplicaMember dst_;
  share::ObPhysicalValidateArg validate_arg_;
  int64_t result_;
};

//----End structs for partition online/offline----

struct ObAddReplicaBatchRes {
  OB_UNIS_VERSION(2);

public:
  ObAddReplicaBatchRes() : res_array_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(res_array));

public:
  common::ObSArray<ObAddReplicaRes> res_array_;
};

struct ObRebuildReplicaBatchRes {
  OB_UNIS_VERSION(1);

public:
  ObRebuildReplicaBatchRes() : res_array_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(res_array));

public:
  common::ObSArray<ObRebuildReplicaRes> res_array_;
};

struct ObCopySSTableBatchRes {
  OB_UNIS_VERSION(1);

public:
  ObCopySSTableBatchRes() : res_array_(), type_(common::OB_COPY_SSTABLE_TYPE_INVALID)
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(res_array), K_(type));

public:
  common::ObSArray<ObCopySSTableRes> res_array_;
  common::ObCopySSTableType type_;
};

struct ObMigrateReplicaBatchRes {
  OB_UNIS_VERSION(2);

public:
  ObMigrateReplicaBatchRes() : res_array_()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(res_array));

public:
  common::ObSArray<ObMigrateReplicaRes> res_array_;
};

struct ObChangeReplicaBatchRes {
  OB_UNIS_VERSION(1);

public:
  ObChangeReplicaBatchRes() : res_array_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(res_array));

public:
  common::ObSArray<ObChangeReplicaRes> res_array_;
};

struct ObStandbyCutDataTaskRes {
  OB_UNIS_VERSION(1);

public:
  ObStandbyCutDataTaskRes() : key_(), dst_(), result_(0)
  {}
  bool is_valid() const
  {
    return key_.is_valid() && dst_.is_valid();
  }

  TO_STRING_KV(K_(key), K_(dst), K_(result));
  common::ObPartitionKey key_;
  common::ObReplicaMember dst_;
  int64_t result_;
};

struct ObStandbyCutDataBatchTaskRes {
  OB_UNIS_VERSION(1);

public:
  ObStandbyCutDataBatchTaskRes() : res_array_()
  {}

public:
  int assign(const struct ObStandbyCutDataBatchTaskRes& res);
  bool is_valid() const;
  TO_STRING_KV(K_(res_array));

public:
  common::ObSArray<ObStandbyCutDataTaskRes> res_array_;
};

struct ObBackupBatchRes {
  OB_UNIS_VERSION(1);

public:
  ObBackupBatchRes() : res_array_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(res_array));

public:
  common::ObSArray<ObBackupRes> res_array_;
};

struct ObValidateBatchRes {
  OB_UNIS_VERSION(1);

public:
  ObValidateBatchRes() : res_array_()
  {}

public:
  int assign(const ObValidateBatchRes& res);
  bool is_valid() const;
  TO_STRING_KV(K_(res_array));

public:
  common::ObSArray<ObValidateRes> res_array_;
};

struct ObPGBackupArchiveLogArg {
  OB_UNIS_VERSION(1);

public:
  ObPGBackupArchiveLogArg();

public:
  bool is_valid() const;
  int assign(const ObPGBackupArchiveLogArg& arg);
  TO_STRING_KV(K_(archive_round), K_(pg_key));

public:
  int64_t archive_round_;
  common::ObPGKey pg_key_;
};

struct ObPGBackupArchiveLogRes {
  OB_UNIS_VERSION(1);

public:
  ObPGBackupArchiveLogRes();

public:
  bool is_valid() const;
  int assign(const ObPGBackupArchiveLogRes& res);
  TO_STRING_KV(K_(result), K_(finished), K_(checkpoint_ts), K_(pg_key));

public:
  int result_;
  bool finished_;
  int64_t checkpoint_ts_;
  common::ObPGKey pg_key_;
};

struct ObBackupArchiveLogBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObBackupArchiveLogBatchArg();

public:
  bool is_valid() const;
  int assign(const ObBackupArchiveLogBatchArg& arg);
  TO_STRING_KV(K_(tenant_id), K_(archive_round), K_(checkpoint_ts), K_(task_id), K_(src_root_path),
      K_(src_storage_info), K_(dst_root_path), K_(dst_storage_info), K_(arg_array));

public:
  uint64_t tenant_id_;
  int64_t archive_round_;
  int64_t piece_id_;
  int64_t create_date_;
  int64_t job_id_;
  int64_t checkpoint_ts_;  // rs_checkpoint_ts
  share::ObTaskId task_id_;
  char src_root_path_[share::OB_MAX_BACKUP_PATH_LENGTH];
  char src_storage_info_[share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  char dst_root_path_[share::OB_MAX_BACKUP_PATH_LENGTH];
  char dst_storage_info_[share::OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
  common::ObSArray<ObPGBackupArchiveLogArg> arg_array_;
};

struct ObBackupArchiveLogBatchRes {
  OB_UNIS_VERSION(1);

public:
  ObBackupArchiveLogBatchRes();

public:
  bool is_valid() const;
  bool is_interrupted() const;
  int assign(const ObBackupArchiveLogBatchRes& arg);
  int get_min_checkpoint_ts(int64_t& checkpoint_ts) const;
  int get_finished_pg_list(common::ObIArray<common::ObPGKey>& pg_list) const;
  int get_failed_pg_list(common::ObIArray<common::ObPGKey>& pg_list) const;
  TO_STRING_KV(
      K_(server), K_(tenant_id), K_(archive_round), K_(piece_id), K_(checkpoint_ts), K_(job_id), K_(res_array));

public:
  common::ObAddr server_;
  uint64_t tenant_id_;
  int64_t archive_round_;
  int64_t piece_id_;
  int64_t job_id_;
  int64_t checkpoint_ts_;  // rs checkpoint ts
  common::ObSArray<ObPGBackupArchiveLogRes> res_array_;
};

struct ObBackupBackupsetReplicaRes {
  OB_UNIS_VERSION(1);

public:
  ObBackupBackupsetReplicaRes();

public:
  int assign(const ObBackupBackupsetReplicaRes& res);
  bool is_valid() const;
  TO_STRING_KV(K_(key), K_(dst), K_(result));

public:
  common::ObPartitionKey key_;
  common::ObReplicaMember dst_;
  share::ObBackupBackupsetArg arg_;
  int result_;
};

struct ObBackupBackupsetBatchRes {
  OB_UNIS_VERSION(1);

public:
  int assign(const ObBackupBackupsetBatchRes& res);
  bool is_valid() const;
  TO_STRING_KV(K_(res_array));

public:
  common::ObSArray<ObBackupBackupsetReplicaRes> res_array_;
};

// ---Structs for partition batch online/offline---
struct ObAddReplicaBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObAddReplicaBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  bool is_valid() const;

  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObAddReplicaArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
};

struct ObRemoveNonPaxosReplicaBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObRemoveNonPaxosReplicaBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObRemoveNonPaxosReplicaArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
};

struct ObMigrateReplicaBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObMigrateReplicaBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObMigrateReplicaArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
};

struct ObChangeReplicaBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObChangeReplicaBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObChangeReplicaArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
};

struct ObCopySSTableBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObCopySSTableBatchArg() : arg_array_(), timeout_ts_(0), task_id_(), type_(common::OB_COPY_SSTABLE_TYPE_INVALID)
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id), K_(type));

public:
  common::ObSArray<ObCopySSTableArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
  common::ObCopySSTableType type_;
};

struct ObRebuildReplicaBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObRebuildReplicaBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObRebuildReplicaArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
};

struct ObServerCopyLocalIndexSSTableArg {
  OB_UNIS_VERSION(1);

public:
  ObServerCopyLocalIndexSSTableArg()
      : data_src_(), dst_(), pkey_(), index_table_id_(common::OB_INVALID_ID), cluster_id_(common::OB_INVALID_ID), data_size_(0)
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(data_src), K_(dst), K_(pkey), K_(index_table_id), K_(cluster_id), K_(data_size));

public:
  common::ObAddr data_src_;
  common::ObAddr dst_;
  common::ObPartitionKey pkey_;
  uint64_t index_table_id_;
  int64_t cluster_id_;
  int64_t data_size_;
};

struct ObBackupBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObBackupBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObBackupArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
};

enum ObReplicaMovingType : int8_t {
  REPLICA_MOVING_TYPE_INVALID = 0,
  REPLICA_MOVING_TYPE_ADD_REPLICA,
  REPLICA_MOVING_TYPE_DROP_REPLICA,
  REPLICA_MOVING_TYPE_MAX
};

struct ObAuthReplicaMovingkArg {
  OB_UNIS_VERSION(1);

public:
  ObAuthReplicaMovingkArg() : file_id_(0), type_(REPLICA_MOVING_TYPE_INVALID)
  {}
  bool is_valid() const;
  TO_STRING_KV(K(pg_key_), K(addr_), K(file_id_), K(type_));

public:
  common::ObPGKey pg_key_;
  common::ObAddr addr_;
  int64_t file_id_;
  ObReplicaMovingType type_;
};

struct ObValidateBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObValidateBatchArg() : arg_array_(), timeout_ts_(0), task_id_()
  {}

public:
  int assign(const ObValidateBatchArg& arg);
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(task_id));

public:
  common::ObSArray<ObValidateArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
};

struct ObStandbyCutDataBatchTaskArg {
  OB_UNIS_VERSION(1);

public:
  ObStandbyCutDataBatchTaskArg()
      : arg_array_(), timeout_ts_(0), trace_id_(), fo_trace_id_(), flashback_ts_(0), switchover_epoch_(0)
  {}

public:
  int init(const int64_t timeout, const share::ObTaskId& task_id, const common::ObCurTraceId::TraceId& fo_trace_id,
      const int64_t flashback_ts, const int64_t switchover_epoch);
  bool is_valid() const;
  TO_STRING_KV(K_(arg_array), K_(timeout_ts), K_(trace_id), K_(flashback_ts), K_(switchover_epoch), K_(fo_trace_id));

public:
  common::ObSArray<ObStandbyCutDataTaskArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId trace_id_;
  common::ObCurTraceId::TraceId fo_trace_id_;
  int64_t flashback_ts_;
  int64_t switchover_epoch_;
};

struct ObBackupBackupsetBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObBackupBackupsetBatchArg() : arg_array_(), timeout_ts_(), task_id_(), tenant_dropped_(false)
  {}

public:
  int assign(const ObBackupBackupsetBatchArg& res);
  bool is_valid() const;
  TO_STRING_KV(K_(timeout_ts), K_(task_id), K_(tenant_dropped));

public:
  common::ObSArray<ObMigrateBackupsetArg> arg_array_;
  int64_t timeout_ts_;
  share::ObTaskId task_id_;
  bool tenant_dropped_;
};

//----Structs for managing privileges----

struct ObMajorFreezeArg {
  OB_UNIS_VERSION(1);

public:
  ObMajorFreezeArg() : frozen_version_(0), schema_version_(0), frozen_timestamp_(0)
  {}
  inline void reset();
  inline void reuse();
  bool is_valid() const;
  DECLARE_TO_STRING;

  int64_t frozen_version_;
  int64_t schema_version_;
  int64_t frozen_timestamp_;

private:
  // DISALLOW_COPY_AND_ASSIGN(ObMajorFreezeArg);
};

struct ObSetReplicaNumArg {
  OB_UNIS_VERSION(1);

public:
  ObSetReplicaNumArg() : partition_key_(), replica_num_(0)
  {}
  ~ObSetReplicaNumArg()
  {}
  inline void reset();
  bool is_valid() const
  {
    return partition_key_.is_valid() && replica_num_ > 0;
  }

  DECLARE_TO_STRING;

  common::ObPartitionKey partition_key_;
  int64_t replica_num_;
};

struct ObSetParentArg {
  OB_UNIS_VERSION(1);

public:
  ObSetParentArg() : partition_key_(), parent_addr_()
  {}
  ~ObSetParentArg()
  {}
  inline void reset();
  bool is_valid() const
  {
    return partition_key_.is_valid() && parent_addr_.is_valid();
  }

  DECLARE_TO_STRING;

  common::ObPartitionKey partition_key_;
  common::ObAddr parent_addr_;
};

struct ObSwitchLeaderArg {
  OB_UNIS_VERSION(1);

public:
  ObSwitchLeaderArg() : partition_key_(), leader_addr_()
  {}
  ~ObSwitchLeaderArg()
  {}
  inline void reset();
  bool is_valid() const
  {
    return partition_key_.is_valid() && leader_addr_.is_valid();
  }

  DECLARE_TO_STRING;

  common::ObPartitionKey partition_key_;
  common::ObAddr leader_addr_;
};

struct ObSwitchSchemaArg {
  OB_UNIS_VERSION(1);

public:
  explicit ObSwitchSchemaArg() : schema_info_(), force_refresh_(false)
  {}
  explicit ObSwitchSchemaArg(const share::schema::ObRefreshSchemaInfo& schema_info, bool force_refresh)
      : schema_info_(schema_info), force_refresh_(force_refresh)
  {}
  ~ObSwitchSchemaArg()
  {}
  void reset();
  bool is_valid() const
  {
    return schema_info_.get_schema_version() > 0;
  }

  DECLARE_TO_STRING;

  share::schema::ObRefreshSchemaInfo schema_info_;
  bool force_refresh_;
};

struct ObCheckSchemaVersionElapsedArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckSchemaVersionElapsedArg() : pkey_(), schema_version_(0)
  {}
  bool is_valid() const
  {
    return pkey_.is_valid() && schema_version_ > 0;
  }
  void reuse()
  {
    pkey_.reset();
    schema_version_ = 0;
  }
  int build(rootserver::ObGlobalIndexTask* task, common::ObPartitionKey& pkey);
  TO_STRING_KV(K_(pkey), K_(schema_version));
  common::ObPartitionKey pkey_;
  int64_t schema_version_;
};

struct ObCheckCtxCreateTimestampElapsedArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckCtxCreateTimestampElapsedArg() : pkey_(), sstable_exist_ts_(0)
  {}
  bool is_valid() const
  {
    return pkey_.is_valid() && sstable_exist_ts_ > 0;
  }
  void reuse()
  {
    pkey_.reset();
    sstable_exist_ts_ = 0;
  }
  int build(rootserver::ObGlobalIndexTask* task, common::ObPartitionKey& pkey);
  TO_STRING_KV(K_(pkey), K_(sstable_exist_ts));
  common::ObPartitionKey pkey_;
  int64_t sstable_exist_ts_;
};

struct ObCheckSchemaVersionElapsedResult {
  OB_UNIS_VERSION(1);

public:
  ObCheckSchemaVersionElapsedResult() : snapshot_(common::OB_INVALID_TIMESTAMP)
  {}
  bool is_valid() const
  {
    return snapshot_ != common::OB_INVALID_TIMESTAMP;
  }
  void reuse()
  {
    snapshot_ = common::OB_INVALID_TIMESTAMP;
  }
  TO_STRING_KV(K_(snapshot));
  int64_t snapshot_;
};

struct ObCheckCtxCreateTimestampElapsedResult {
  OB_UNIS_VERSION(1);

public:
  ObCheckCtxCreateTimestampElapsedResult() : snapshot_(common::OB_INVALID_TIMESTAMP)
  {}
  bool is_valid() const
  {
    return snapshot_ != common::OB_INVALID_TIMESTAMP;
  }
  void reuse()
  {
    snapshot_ = common::OB_INVALID_TIMESTAMP;
  }
  TO_STRING_KV(K_(snapshot));
  int64_t snapshot_;
};

struct ObGetLeaderCandidatesArg {
  OB_UNIS_VERSION(1);

public:
  ObGetLeaderCandidatesArg() : partitions_()
  {}
  void reuse()
  {
    partitions_.reuse();
  }
  bool is_valid() const
  {
    return partitions_.count() > 0;
  }

  TO_STRING_KV(K_(partitions));

  ObPartitionList partitions_;
};

struct ObGetLeaderCandidatesV2Arg {
  OB_UNIS_VERSION(1);

public:
  ObGetLeaderCandidatesV2Arg() : partitions_(), prep_candidates_()
  {}
  void reuse()
  {
    partitions_.reuse();
    prep_candidates_.reuse();
  }
  bool is_valid() const
  {
    return ((partitions_.count() > 0) && (prep_candidates_.count() > 0));
  }

  TO_STRING_KV(K_(partitions));

  ObPartitionList partitions_;
  ObServerList prep_candidates_;
};

class CandidateStatus {
  OB_UNIS_VERSION(1);

public:
  CandidateStatus() : candidate_status_(0)
  {}
  virtual ~CandidateStatus()
  {}

public:
  void set_in_black_list(const bool in_black_list)
  {
    if (in_black_list) {
      in_black_list_ = 1;
    } else {
      in_black_list_ = 0;
    }
  }
  bool get_in_black_list() const
  {
    bool ret_in_black_list = false;
    if (0 == in_black_list_) {  // false, do nothing
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
      uint64_t in_black_list_ : 1;  // Boolean
      uint64_t reserved_ : 63;
    };
  };
};

typedef common::ObSArray<CandidateStatus> CandidateStatusList;

struct ObGetLeaderCandidatesResult {
  OB_UNIS_VERSION(1);

public:
  ObGetLeaderCandidatesResult() : candidates_()
  {}
  void reuse()
  {
    candidates_.reuse();
    candidate_status_array_.reuse();
  }
  TO_STRING_KV(K_(candidates));

  common::ObSArray<ObServerList> candidates_;
  common::ObSArray<CandidateStatusList> candidate_status_array_;
};

//----Structs for managing privileges----

struct ObAccountArg {
  OB_UNIS_VERSION(1);

public:
  ObAccountArg() : user_name_(), host_name_(), is_role_(false)
  {}
  ObAccountArg(const common::ObString& user_name, const common::ObString& host_name)
      : user_name_(user_name), host_name_(host_name), is_role_(false)
  {}
  ObAccountArg(const char* user_name, const char* host_name)
      : user_name_(user_name), host_name_(host_name), is_role_(false)
  {}
  ObAccountArg(const common::ObString& user_name, const common::ObString& host_name, const bool is_role)
      : user_name_(user_name), host_name_(host_name), is_role_(is_role)
  {}
  ObAccountArg(const char* user_name, const char* host_name, const bool is_role)
      : user_name_(user_name), host_name_(host_name), is_role_(is_role)
  {}
  bool is_valid() const
  {
    return !user_name_.empty();
  }
  bool is_default_host_name() const
  {
    return 0 == host_name_.compare(common::OB_DEFAULT_HOST_NAME);
  }
  TO_STRING_KV(K_(user_name), K_(host_name), K_(is_role));

  common::ObString user_name_;
  common::ObString host_name_;
  bool is_role_;
};

struct ObCreateUserArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateUserArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        if_not_exist_(false),
        creator_id_(common::OB_INVALID_ID),
        primary_zone_()
  {}
  virtual ~ObCreateUserArg()
  {}
  bool is_valid() const;
  int assign(const ObCreateUserArg& other);
  TO_STRING_KV(K_(tenant_id), K_(user_infos));

  uint64_t tenant_id_;
  bool if_not_exist_;
  common::ObSArray<share::schema::ObUserInfo> user_infos_;
  uint64_t creator_id_;
  common::ObString primary_zone_;  // only used in oracle mode
};

struct ObDropUserArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropUserArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), is_role_(false)
  {}
  virtual ~ObDropUserArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  TO_STRING_KV(K_(tenant_id), K_(users), K_(hosts), K_(is_role));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> users_;
  common::ObSArray<common::ObString> hosts_;  // can not use ObAccountArg for compatibility
  bool is_role_;
};

struct ObRenameUserArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObRenameUserArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObRenameUserArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(old_users), K_(old_hosts), K_(new_users), K_(new_hosts));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> old_users_;
  common::ObSArray<common::ObString> new_users_;
  common::ObSArray<common::ObString> old_hosts_;
  common::ObSArray<common::ObString> new_hosts_;
};

struct ObSetPasswdArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObSetPasswdArg()
      : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), ssl_type_(share::schema::ObSSLType::SSL_TYPE_NOT_SPECIFIED),
        modify_max_connections_(false), max_connections_per_hour_(OB_INVALID_ID), max_user_connections_(OB_INVALID_ID)
  {}
  virtual ~ObSetPasswdArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(
      K_(tenant_id), K_(user), K_(host), K_(passwd), K_(ssl_type), K_(ssl_cipher), K_(x509_issuer), K_(x509_subject),
      K_(modify_max_connections), K_(max_connections_per_hour), K_(max_user_connections));

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

struct ObLockUserArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObLockUserArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), locked_(false)
  {}
  virtual ~ObLockUserArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(users), K_(hosts), K_(locked));

  uint64_t tenant_id_;
  common::ObSArray<common::ObString> users_;
  common::ObSArray<common::ObString> hosts_;
  bool locked_;
};

struct ObAlterUserProfileArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObAlterUserProfileArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        user_name_(),
        host_name_(),
        profile_name_(),
        user_id_(common::OB_INVALID_TENANT_ID),
        default_role_flag_(common::OB_INVALID_TENANT_ID),
        role_id_array_()
  {}
  virtual ~ObAlterUserProfileArg()
  {}
  int assign(const ObAlterUserProfileArg& other);
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_name), K_(host_name), K_(profile_name));

  uint64_t tenant_id_;
  common::ObString user_name_;
  common::ObString host_name_;
  common::ObString profile_name_;
  uint64_t user_id_;
  uint64_t default_role_flag_;
  common::ObSEArray<uint64_t, 4> role_id_array_;
};

struct ObGrantArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObGrantArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        priv_level_(share::schema::OB_PRIV_INVALID_LEVEL),
        priv_set_(0),
        users_passwd_(),
        hosts_(),
        need_create_user_(false),
        has_create_user_priv_(false),
        roles_(),
        option_(0),
        sys_priv_array_(),
        obj_priv_array_(),
        object_type_(share::schema::ObObjectType::INVALID),
        object_id_(common::OB_INVALID_ID),
        ins_col_ids_(),
        upd_col_ids_(),
        ref_col_ids_(),
        grantor_id_(common::OB_INVALID_ID),
        remain_roles_(),
        is_inner_(false)
  {}
  virtual ~ObGrantArg()
  {}
  bool is_valid() const;
  int assign(const ObGrantArg& other);
  virtual bool is_allow_when_disable_ddl() const;

  TO_STRING_KV(K_(tenant_id), K_(priv_level), K_(db), K_(table), K_(priv_set), K_(users_passwd), K_(hosts),
      K_(need_create_user), K_(has_create_user_priv), K_(option), K_(object_type), K_(object_id), K_(grantor_id),
      K_(ins_col_ids), K_(upd_col_ids), K_(ref_col_ids), K_(grantor_id));

  uint64_t tenant_id_;
  share::schema::ObPrivLevel priv_level_;
  common::ObString db_;
  common::ObString table_;
  ObPrivSet priv_set_;
  common::ObSArray<common::ObString> users_passwd_;  
  common::ObSArray<common::ObString> hosts_;        
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
};

struct ObStandbyGrantArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObStandbyGrantArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        user_id_(0),
        db_(),
        table_(),
        priv_level_(share::schema::OB_PRIV_INVALID_LEVEL),
        priv_set_()
  {}
  virtual ~ObStandbyGrantArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(priv_level), K_(priv_set), K_(db), K_(table), K_(priv_level));
  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  common::ObString table_;
  share::schema::ObPrivLevel priv_level_;
  ObPrivSet priv_set_;
  share::ObRawObjPrivArray obj_priv_array_;
};

struct ObRevokeUserArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObRevokeUserArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        user_id_(common::OB_INVALID_ID),
        priv_set_(0),
        revoke_all_(false),
        role_ids_()
  {}
  bool is_valid() const;
  TO_STRING_KV(
      K_(tenant_id), K_(user_id), "priv_set", share::schema::ObPrintPrivSet(priv_set_), K_(revoke_all), K_(role_ids));

  uint64_t tenant_id_;
  uint64_t user_id_;
  ObPrivSet priv_set_;
  bool revoke_all_;
  common::ObSArray<uint64_t> role_ids_;
};

struct ObRevokeDBArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObRevokeDBArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), user_id_(common::OB_INVALID_ID), priv_set_(0)
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), "priv_set", share::schema::ObPrintPrivSet(priv_set_));

  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  ObPrivSet priv_set_;
};

struct ObRevokeTableArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObRevokeTableArg()
      : ObDDLArg(),
        tenant_id_(common::OB_INVALID_ID),
        user_id_(common::OB_INVALID_ID),
        priv_set_(0),
        grant_(true),
        obj_id_(common::OB_INVALID_ID),
        obj_type_(common::OB_INVALID_ID),
        grantor_id_(common::OB_INVALID_ID),
        obj_priv_array_(),
        revoke_all_ora_(false)
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(db), K_(table), "priv_set", share::schema::ObPrintPrivSet(priv_set_),
      K_(grant), K_(obj_id), K_(obj_type), K_(grantor_id), K_(obj_priv_array));

  uint64_t tenant_id_;
  uint64_t user_id_;
  common::ObString db_;
  common::ObString table_;
  ObPrivSet priv_set_;
  bool grant_;
  uint64_t obj_id_;
  uint64_t obj_type_;
  uint64_t grantor_id_;
  share::ObRawObjPrivArray obj_priv_array_;
  bool revoke_all_ora_;
};

struct ObRevokeSysPrivArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObRevokeSysPrivArg()
      : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), grantee_id_(common::OB_INVALID_ID), sys_priv_array_()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(grantee_id), K_(sys_priv_array));

  uint64_t tenant_id_;
  uint64_t grantee_id_;
  share::ObRawPrivArray sys_priv_array_;
};

struct ObCreateRoleArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateRoleArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(user_infos));

  uint64_t tenant_id_;
  // role and user share the same user schema structure
  common::ObSArray<share::schema::ObUserInfo> user_infos_;
};

//----End of structs for managing privileges----

// system admin (alter system ...) rpc argument define
struct ObAdminServerArg {
  OB_UNIS_VERSION(1);

public:
  enum AdminServerOp {
    INVALID_OP = 0,
    ADD = 1,
    DELETE = 2,
    CANCEL_DELETE = 3,
    START = 4,
    STOP = 5,
    FORCE_STOP = 6,
    ISOLATE = 7,
  };

  ObAdminServerArg() : servers_(), zone_(), force_stop_(false), op_(INVALID_OP)
  {}
  ~ObAdminServerArg()
  {}
  // zone can be empty, so don't check it
  bool is_valid() const
  {
    return servers_.count() > 0;
  }
  TO_STRING_KV(K_(servers), K_(zone), K_(force_stop), K_(op));

  ObServerList servers_;
  common::ObZone zone_;
  bool force_stop_;
  AdminServerOp op_;
};

struct ObAdminZoneArg {
  OB_UNIS_VERSION(1);

public:
  enum AdminZoneOp {
    ADD = 1,
    DELETE = 2,
    START = 3,
    STOP = 4,
    MODIFY = 5,
    FORCE_STOP = 6,
    ISOLATE = 7,
  };
  enum ALTER_ZONE_OPTION {
    ALTER_ZONE_REGION = 0,
    ALTER_ZONE_IDC = 1,
    ALTER_ZONE_TYPE = 2,
    ALTER_ZONE_MAX = 128,
  };

public:
  ObAdminZoneArg()
      : zone_(), region_(), idc_(), zone_type_(common::ObZoneType::ZONE_TYPE_INVALID), force_stop_(false), op_(ADD)
  {}
  ~ObAdminZoneArg()
  {}

  bool is_valid() const
  {
    return !zone_.is_empty();
  }
  TO_STRING_KV(K_(zone), K_(region), K_(idc), K_(zone_type), K_(sql_stmt_str), K_(force_stop), K_(op));

  common::ObZone zone_;
  common::ObRegion region_;
  common::ObIDC idc_;
  common::ObZoneType zone_type_;
  common::ObString sql_stmt_str_;

  common::ObBitSet<ALTER_ZONE_MAX> alter_zone_options_;
  bool force_stop_;
  AdminZoneOp op_;
};

struct ObAdminSwitchReplicaRoleArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminSwitchReplicaRoleArg() : role_(common::FOLLOWER), partition_key_(), server_(), zone_(), tenant_name_()
  {}
  ~ObAdminSwitchReplicaRoleArg()
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(role), K_(partition_key), K_(server), K_(zone), K_(tenant_name));

  common::ObRole role_;
  common::ObPartitionKey partition_key_;
  common::ObAddr server_;
  common::ObZone zone_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
};

struct ObAdminSwitchRSRoleArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminSwitchRSRoleArg() : role_(common::FOLLOWER), server_(), zone_()
  {}
  ~ObAdminSwitchRSRoleArg()
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(role), K_(server), K_(zone));

  common::ObRole role_;
  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminChangeReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminChangeReplicaArg() : partition_key_(), member_(), force_cmd_(false)
  {}
  ~ObAdminChangeReplicaArg()
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(partition_key), K_(member), K_(force_cmd));

  common::ObPartitionKey partition_key_;
  common::ObReplicaMember member_;
  bool force_cmd_;
};

struct ObCheckGtsReplicaStopServer {
  OB_UNIS_VERSION(1);

public:
  ObCheckGtsReplicaStopServer() : servers_()
  {}
  TO_STRING_KV(K_(servers));
  int init(const common::ObIArray<common::ObAddr>& servers)
  {
    int ret = common::OB_SUCCESS;
    servers_.reset();
    if (OB_FAIL(servers_.assign(servers))) {
      SHARE_LOG(WARN, "fail to assign servers", K(ret));
    }
    return ret;
  }
  bool is_valid() const
  {
    return servers_.count() > 0;
  }

public:
  common::ObSArray<common::ObAddr> servers_;
};

struct ObCheckGtsReplicaStopZone {
  OB_UNIS_VERSION(1);

public:
  ObCheckGtsReplicaStopZone() : zone_()
  {}
  ObCheckGtsReplicaStopZone(const common::ObZone& zone) : zone_(zone)
  {}
  TO_STRING_KV(K_(zone));
  bool is_valid() const
  {
    return !zone_.is_empty();
  }

public:
  common::ObZone zone_;
};

struct ObAdminDropReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminDropReplicaArg() : partition_key_(), server_(), zone_(), create_timestamp_(0), force_cmd_(false)
  {}
  ~ObAdminDropReplicaArg()
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(partition_key), K_(server), K_(zone), K_(create_timestamp), K_(force_cmd));

  common::ObPartitionKey partition_key_;
  common::ObAddr server_;
  common::ObZone zone_;
  int64_t create_timestamp_;
  bool force_cmd_;
};

struct ObAdminAddDiskArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminAddDiskArg() : diskgroup_name_(), disk_path_(), alias_name_(), server_(), zone_()
  {}
  ~ObAdminAddDiskArg()
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(diskgroup_name), K_(disk_path), K_(alias_name), K_(server), K_(zone));

  common::ObString diskgroup_name_;
  common::ObString disk_path_;
  common::ObString alias_name_;
  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminDropDiskArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminDropDiskArg() : diskgroup_name_(), alias_name_(), server_(), zone_()
  {}
  ~ObAdminDropDiskArg()
  {}

  TO_STRING_KV(K_(diskgroup_name), K_(alias_name), K_(server), K_(zone));

  common::ObString diskgroup_name_;
  common::ObString alias_name_;
  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminMigrateReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminMigrateReplicaArg() : is_copy_(false), partition_key_(), src_(), dest_(), force_cmd_(false)
  {}
  ~ObAdminMigrateReplicaArg()
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(is_copy), K_(partition_key), K_(src), K_(dest), K_(force_cmd));

  bool is_copy_;
  common::ObPartitionKey partition_key_;
  common::ObAddr src_;
  common::ObAddr dest_;
  bool force_cmd_;
};

struct ObPhysicalRestoreTenantArg : public ObCmdArg {
  OB_UNIS_VERSION(1);

public:
  ObPhysicalRestoreTenantArg();
  virtual ~ObPhysicalRestoreTenantArg()
  {}
  bool is_valid() const;
  int assign(const ObPhysicalRestoreTenantArg& other);
  int add_table_item(const ObTableItem& item);
  TO_STRING_KV(K_(tenant_name), K_(uri), K_(restore_option), K_(restore_timestamp), K_(backup_tenant_name),
      K_(passwd_array), K_(table_items), K_(multi_uri));

  common::ObString tenant_name_;
  common::ObString uri_;
  common::ObString restore_option_;
  int64_t restore_timestamp_;
  common::ObString backup_tenant_name_;
  common::ObString passwd_array_;  // Password verification
  common::ObSArray<ObTableItem> table_items_;
  common::ObString multi_uri_;  
};

struct ObRestoreTenantArg : public ObCmdArg {
  OB_UNIS_VERSION(1);

public:
  ObRestoreTenantArg() : ObCmdArg(), tenant_name_(), oss_uri_()
  {}
  bool is_valid() const
  {
    return !tenant_name_.is_empty() && !oss_uri_.empty();
  }
  TO_STRING_KV(K_(tenant_name), K_(oss_uri));

  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
  common::ObString oss_uri_;
};

struct ObRestorePartitionsArg {
  OB_UNIS_VERSION(1);

public:
  ObRestorePartitionsArg()
      : schema_id_(common::OB_INVALID_ID),
        mode_(OB_CREATE_TABLE_MODE_RESTORE),
        partition_ids_(),
        schema_version_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(schema_id), K_(mode), K_(schema_version), "partition_cnt", partition_ids_.count());

  uint64_t schema_id_;
  obrpc::ObCreateTableMode mode_;
  common::ObSArray<int64_t> partition_ids_;  // Physical recovery, the list of partition_id existing in the baseline
                                             // SSTABLE
  int64_t schema_version_;                   // Physical recovery, the schema_version corresponding to the baseline
};

struct ObServerZoneArg {
  OB_UNIS_VERSION(1);

public:
  ObServerZoneArg() : server_(), zone_()
  {}

  // server can be invalid, zone can be empty
  bool is_valid() const
  {
    return true;
  }
  TO_STRING_KV(K_(server), K_(zone));

  common::ObAddr server_;
  common::ObZone zone_;
};

struct ObAdminReportReplicaArg : public ObServerZoneArg {};

struct ObAdminRecycleReplicaArg : public ObServerZoneArg {};

struct ObAdminRefreshSchemaArg : public ObServerZoneArg {};

struct ObAdminRefreshMemStatArg : public ObServerZoneArg {};

struct ObAdminClearLocationCacheArg : public ObServerZoneArg {};

struct ObRunJobArg : public ObServerZoneArg {
  OB_UNIS_VERSION(1);

public:
  ObRunJobArg() : ObServerZoneArg(), job_()
  {}

  bool is_valid() const
  {
    return ObServerZoneArg::is_valid() && !job_.empty();
  }
  TO_STRING_KV(K_(server), K_(zone), K_(job));

  common::ObString job_;
};

struct ObUpgradeJobArg {
  OB_UNIS_VERSION(1);

public:
  enum Action { INVALID_ACTION, RUN_UPGRADE_JOB, STOP_UPGRADE_JOB };

public:
  ObUpgradeJobArg();
  bool is_valid() const;
  int assign(const ObUpgradeJobArg& other);
  TO_STRING_KV(K_(action), K_(version));

public:
  Action action_;
  int64_t version_;
};

struct ObAdminMergeArg {
  OB_UNIS_VERSION(1);

public:
  enum Type {
    START_MERGE = 1,
    SUSPEND_MERGE = 2,
    RESUME_MERGE = 3,
  };

  ObAdminMergeArg() : type_(START_MERGE), zone_()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(type), K_(zone));

  Type type_;
  common::ObZone zone_;
};

struct ObAdminClearRoottableArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminClearRoottableArg() : tenant_name_()
  {}

  // tenant_name be empty means all tenant
  bool is_valid() const
  {
    return true;
  }
  TO_STRING_KV(K_(tenant_name));

  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
};

struct ObAdminSetConfigItem {
  OB_UNIS_VERSION(1);

public:
  ObAdminSetConfigItem()
      : name_(),
        value_(),
        comment_(),
        zone_(),
        server_(),
        tenant_name_(),
        exec_tenant_id_(common::OB_SYS_TENANT_ID),
        tenant_ids_()
  {}
  TO_STRING_KV(
      K_(name), K_(value), K_(comment), K_(zone), K_(server), K_(tenant_name), K_(exec_tenant_id), K_(tenant_ids));

  common::ObFixedLengthString<common::OB_MAX_CONFIG_NAME_LEN> name_;
  common::ObFixedLengthString<common::OB_MAX_CONFIG_VALUE_LEN> value_;
  common::ObFixedLengthString<common::OB_MAX_CONFIG_INFO_LEN> comment_;
  common::ObZone zone_;
  common::ObAddr server_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH + 1> tenant_name_;
  uint64_t exec_tenant_id_;
  common::ObSArray<uint64_t> tenant_ids_;
};

struct ObAdminSetConfigArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminSetConfigArg() : items_(), is_inner_(false)
  {}
  ~ObAdminSetConfigArg()
  {}

  bool is_valid() const
  {
    return items_.count() > 0;
  }
  TO_STRING_KV(K_(items), K_(is_inner));

  common::ObSArray<ObAdminSetConfigItem> items_;
  bool is_inner_;
};

struct ObAdminLoadBaselineArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminLoadBaselineArg()
      : tenant_ids_(), sql_id_(), plan_hash_value_(common::OB_INVALID_ID), fixed_(false), enabled_(true)
  {}
  virtual ~ObAdminLoadBaselineArg()
  {}
  int push_tenant(uint64_t tenant_id)
  {
    return tenant_ids_.push_back(tenant_id);
  }
  TO_STRING_KV(K_(tenant_ids), K_(sql_id), K_(plan_hash_value), K_(fixed), K_(enabled));

  common::ObSEArray<uint64_t, 8> tenant_ids_;
  common::ObString sql_id_;
  uint64_t plan_hash_value_;
  bool fixed_;
  bool enabled_;
};

struct ObAdminFlushCacheArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminFlushCacheArg() :
    cache_type_(CACHE_TYPE_INVALID),
    is_fine_grained_(false)
  {
  }
  virtual ~ObAdminFlushCacheArg() {}
  bool is_valid() const
  {
    return cache_type_ > CACHE_TYPE_INVALID && cache_type_ < CACHE_TYPE_MAX;
  }
  int push_tenant(uint64_t tenant_id) { return tenant_ids_.push_back(tenant_id); }
  int push_database(uint64_t db_id) { return db_ids_.push_back(db_id); }
  TO_STRING_KV(K_(tenant_ids), K_(cache_type), K_(db_ids), K_(sql_id), K_(is_fine_grained));

  common::ObSEArray<uint64_t, 8> tenant_ids_;
  ObCacheType cache_type_;
  common::ObSEArray<uint64_t, 8> db_ids_;
  common::ObString sql_id_;
  bool is_fine_grained_;
};

struct ObAdminMigrateUnitArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminMigrateUnitArg() : unit_id_(0), is_cancel_(false), destination_()
  {}
  ~ObAdminMigrateUnitArg()
  {}

  bool is_valid() const
  {
    return common::OB_INVALID_ID != unit_id_ && (destination_.is_valid() || is_cancel_);
  }
  TO_STRING_KV(K_(unit_id), K_(is_cancel), K_(destination));

  uint64_t unit_id_;
  bool is_cancel_;
  common::ObAddr destination_;
};

struct ObAutoincSyncArg {
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
  // TODO may need add first_part_num. As table_part_num not see usefull.
  // Not add first_part_num now.
  uint64_t table_part_num_;
  uint64_t auto_increment_;  // only for sync table option auto_increment
};

struct ObDropReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObDropReplicaArg() : partition_key_(), member_()
  {}
  bool is_valid() const
  {
    return partition_key_.is_valid() && member_.is_valid();
  }

  TO_STRING_KV(K_(partition_key), K_(member));

  common::ObPartitionKey partition_key_;
  common::ObReplicaMember member_;
};

struct ObUpdateIndexStatusArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObUpdateIndexStatusArg()
      : ObDDLArg(),
        index_table_id_(common::OB_INVALID_ID),
        status_(share::schema::INDEX_STATUS_MAX),
        create_mem_version_(0),
        convert_status_(true)
  {}
  bool is_valid() const;
  virtual bool is_allow_when_disable_ddl() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  TO_STRING_KV(K_(index_table_id), K_(status), K_(create_mem_version), K_(convert_status));

  uint64_t index_table_id_;
  share::schema::ObIndexStatus status_;
  int64_t create_mem_version_;
  bool convert_status_;
};

struct ObMergeFinishArg {
  OB_UNIS_VERSION(1);

public:
  ObMergeFinishArg() : frozen_version_(0)
  {}

  bool is_valid() const
  {
    return server_.is_valid() && frozen_version_ > 0;
  }
  TO_STRING_KV(K_(server), K_(frozen_version));

  common::ObAddr server_;
  int64_t frozen_version_;
};

struct ObMergeErrorArg {
  OB_UNIS_VERSION(1);

public:
  ObMergeErrorArg() : error_code_(0)
  {}

  bool is_valid() const;
  TO_STRING_KV(K_(partition_key), K_(server), K_(error_code));

  common::ObPartitionKey partition_key_;
  common::ObAddr server_;
  int error_code_;
};

struct ObAdminRebuildReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminRebuildReplicaArg() : key_(), server_()
  {}

  bool is_valid() const
  {
    return key_.is_valid() && server_.is_valid();
  }
  TO_STRING_KV(K_(key), K_(server));

  common::ObPartitionKey key_;
  common::ObAddr server_;
};

struct ObDebugSyncActionArg {
  OB_UNIS_VERSION(1);

public:
  ObDebugSyncActionArg() : reset_(false), clear_(false)
  {}

  bool is_valid() const
  {
    return reset_ || clear_ || action_.is_valid();
  }
  TO_STRING_KV(K_(reset), K_(clear), K_(action));

  bool reset_;
  bool clear_;
  common::ObDebugSyncAction action_;
};

struct ObRootMajorFreezeArg {
  OB_UNIS_VERSION(1);

public:
  ObRootMajorFreezeArg()
      : try_frozen_version_(0),
        launch_new_round_(false),
        ignore_server_list_(),
        svr_(),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        force_launch_(false)
  {}
  inline void reset();
  inline bool is_valid() const
  {
    return try_frozen_version_ >= 0;
  }

  TO_STRING_KV(
      K_(try_frozen_version), K_(launch_new_round), K(ignore_server_list_), K(svr_), K(tenant_id_), K_(force_launch));

  int64_t try_frozen_version_;
  bool launch_new_round_;
  common::ObSArray<common::ObAddr> ignore_server_list_;
  common::ObAddr svr_;
  int64_t tenant_id_;
  bool force_launch_;
};

struct ObMinorFreezeArg {
  OB_UNIS_VERSION(1);

public:
  ObMinorFreezeArg()
  {}

  void reset()
  {
    tenant_ids_.reset();
    partition_key_.reset();
  }

  bool is_valid() const
  {
    return true;
  }

  TO_STRING_KV(K_(tenant_ids), K_(partition_key));

  common::ObSArray<uint64_t> tenant_ids_;
  common::ObPartitionKey partition_key_;
};

struct ObRootMinorFreezeArg {
  OB_UNIS_VERSION(2);

public:
  ObRootMinorFreezeArg()
  {}

  void reset()
  {
    tenant_ids_.reset();
    partition_key_.reset();
    server_list_.reset();
    zone_.reset();
  }

  bool is_valid() const
  {
    return true;
  }

  TO_STRING_KV(K_(tenant_ids), K_(partition_key), K_(server_list), K_(zone));

  common::ObSArray<uint64_t> tenant_ids_;
  common::ObPartitionKey partition_key_;
  common::ObSArray<common::ObAddr> server_list_;
  common::ObZone zone_;
};

struct ObSyncPartitionTableFinishArg {
  OB_UNIS_VERSION(1);

public:
  ObSyncPartitionTableFinishArg() : server_(), version_(0)
  {}

  inline bool is_valid() const
  {
    return server_.is_valid() && version_ > 0;
  }
  TO_STRING_KV(K_(server), K_(version));

  common::ObAddr server_;
  int64_t version_;
};

struct ObSyncPGPartitionMTFinishArg {
  OB_UNIS_VERSION(1);

public:
  ObSyncPGPartitionMTFinishArg() : server_(), version_(0)
  {}

  inline bool is_valid() const
  {
    return server_.is_valid() && version_ > 0;
  }
  TO_STRING_KV(K_(server), K_(version));

  common::ObAddr server_;
  int64_t version_;
};

struct ObCheckDanglingReplicaFinishArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckDanglingReplicaFinishArg() : server_(), version_(0), dangling_count_(common::OB_INVALID_ID)
  {}

  inline bool is_valid() const
  {
    return server_.is_valid() && version_ > 0;
  }
  TO_STRING_KV(K_(server), K_(version), K_(dangling_count));

  common::ObAddr server_;
  int64_t version_;
  int64_t dangling_count_;
};

struct ObGetMemberListAndLeaderResult final {
  OB_UNIS_VERSION(1);

public:
  ObGetMemberListAndLeaderResult()
      : member_list_(), leader_(), self_(), lower_list_(), replica_type_(common::REPLICA_TYPE_MAX), property_()
  {}
  void reset();
  inline bool is_valid() const
  {
    return member_list_.count() > 0 && self_.is_valid() && common::REPLICA_TYPE_MAX != replica_type_ &&
           property_.is_valid();
  }

  int assign(const ObGetMemberListAndLeaderResult& other);
  TO_STRING_KV(K_(member_list), K_(leader), K_(self), K_(lower_list), K_(replica_type), K_(property));

  common::ObSEArray<common::ObMember, common::OB_MAX_MEMBER_NUMBER, common::ObNullAllocator, false>
      member_list_;  // copy won't fail
  common::ObAddr leader_;
  common::ObAddr self_;
  common::ObSEArray<common::ObReplicaMember, common::OB_MAX_CHILD_MEMBER_NUMBER> lower_list_;  // Cascaded downstream
                                                                                               // information
  common::ObReplicaType replica_type_;  // The type of copy actually stored in the local copy
  common::ObReplicaProperty property_;
};

struct ObMemberListAndLeaderArg {
  OB_UNIS_VERSION(1);

public:
  ObMemberListAndLeaderArg()
      : member_list_(),
        leader_(),
        self_(),
        lower_list_(),
        replica_type_(common::REPLICA_TYPE_MAX),
        property_(),
        role_(common::INVALID_ROLE)
  {}
  void reset();
  bool is_valid() const;
  bool check_leader_is_valid() const;
  int assign(const ObMemberListAndLeaderArg& other);
  TO_STRING_KV(K_(member_list), K_(leader), K_(self), K_(lower_list), K_(replica_type), K_(property), K_(role));

  common::ObSEArray<common::ObAddr, common::OB_MAX_MEMBER_NUMBER, common::ObNullAllocator, false>
      member_list_;  // copy won't fail
  common::ObAddr leader_;
  common::ObAddr self_;
  common::ObSEArray<common::ObReplicaMember, common::OB_MAX_CHILD_MEMBER_NUMBER> lower_list_;  // Cascaded downstream
                                                                                               // information
  common::ObReplicaType replica_type_;  // The type of copy actually stored in the local copy
  common::ObReplicaProperty property_;
  common::ObRole role_;
};

struct ObBatchGetRoleArg {
  OB_UNIS_VERSION(1);

public:
  ObBatchGetRoleArg() : keys_()
  {}
  virtual ~ObBatchGetRoleArg()
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(keys));
  common::ObSArray<common::ObPartitionKey> keys_;
};

struct ObBatchGetRoleResult {
  OB_UNIS_VERSION(1);

public:
  ObBatchGetRoleResult() : results_()
  {}
  virtual ~ObBatchGetRoleResult()
  {}
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(results));
  common::ObSArray<int> results_;
};

struct ObSwitchLeaderListArg {
  OB_UNIS_VERSION(1);

public:
  ObSwitchLeaderListArg() : partition_key_list_(), leader_addr_()
  {}
  ~ObSwitchLeaderListArg()
  {}
  inline void reset()
  {
    partition_key_list_.reset();
    leader_addr_.reset();
  }
  bool is_valid() const
  {
    return partition_key_list_.count() > 0 && leader_addr_.is_valid();
  }

  TO_STRING_KV(K_(leader_addr), K_(partition_key_list));
  ;

  common::ObSArray<common::ObPartitionKey> partition_key_list_;
  common::ObAddr leader_addr_;
};

struct ObCheckFlashbackInfoArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckFlashbackInfoArg() : min_weak_read_timestamp_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObCheckFlashbackInfoArg()
  {}
  int assign(const ObCheckFlashbackInfoArg& other);
  bool is_valid() const
  {
    return min_weak_read_timestamp_ != common::OB_INVALID_TIMESTAMP;
  }
  TO_STRING_KV(K_(min_weak_read_timestamp))
  int64_t min_weak_read_timestamp_;
};

struct ObCheckFlashbackInfoResult {
  OB_UNIS_VERSION(1);

public:
  ObCheckFlashbackInfoResult()
      : addr_(),
        pkey_(),
        result_(false),
        switchover_timestamp_(common::OB_INVALID_TIMESTAMP),
        ret_code_(common::OB_SUCCESS)
  {}
  ~ObCheckFlashbackInfoResult()
  {}
  int assign(const ObCheckFlashbackInfoResult& other);
  TO_STRING_KV(K_(addr), K_(pkey), K_(result), K_(switchover_timestamp), K_(ret_code))
  common::ObAddr addr_;           // Addr of current obs
  common::ObPartitionKey pkey_;   // Unfinished partition_key
  bool result_;                   // Current obs query result
  int64_t switchover_timestamp_;  // The current switchover_timestap of the bottom layer to prevent the wrong return
                                  // packet
  int ret_code_;
};
struct ObGetPartitionCountResult {
  OB_UNIS_VERSION(1);

public:
  ObGetPartitionCountResult() : partition_count_(0)
  {}
  void reset()
  {
    partition_count_ = 0;
  }
  TO_STRING_KV(K_(partition_count));

  int64_t partition_count_;
};

inline void Int64::reset()
{
  v_ = common::OB_INVALID_ID;
}

inline void ObCreatePartitionArg::reset()
{
  zone_.reset();
  partition_key_.reset();
  schema_version_ = 0;
  memstore_version_ = 0;
  replica_num_ = 0;
  member_list_.reset();
  leader_.reset();
  lease_start_ = 0;
  logonly_replica_num_ = 0;
  backup_replica_num_ = 0;
  readonly_replica_num_ = 0;
  replica_type_ = common::REPLICA_TYPE_MAX;
  last_submit_timestamp_ = 0;
  restore_ = share::REPLICA_NOT_RESTORE;
  table_schemas_.reset();
  source_partition_key_.reset();
  frozen_timestamp_ = 0;
  non_paxos_replica_num_ = 0;
  last_replay_log_id_ = 0;
  pg_key_.reset();
  replica_property_.reset();
  split_info_.reset();
  ignore_member_list_ = false;
}

inline void ObCreatePartitionStorageArg::reset()
{
  rgkey_.reset();
  partition_key_.reset();
  schema_version_ = 0;
  memstore_version_ = 0;
  last_submit_timestamp_ = 0;
  restore_ = share::REPLICA_NOT_RESTORE;
  table_schemas_.reset();
}

inline void ObCreatePartitionBatchArg::reset()
{
  args_.reset();
}
inline void ObMajorFreezeArg::reset()
{
  frozen_version_ = 0;
  schema_version_ = 0;
  frozen_timestamp_ = 0;
}

inline void ObMajorFreezeArg::reuse()
{
  frozen_version_ = 0;
  schema_version_ = 0;
  frozen_timestamp_ = 0;
}

inline void ObSetParentArg::reset()
{
  partition_key_.reset();
  parent_addr_.reset();
}

inline void ObSwitchLeaderArg::reset()
{
  partition_key_.reset();
  leader_addr_.reset();
}

inline void ObRootMajorFreezeArg::reset()
{
  try_frozen_version_ = 0;
  launch_new_round_ = false;
  force_launch_ = false;
}

struct ObCreateUserDefinedFunctionArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateUserDefinedFunctionArg() : ObDDLArg(), udf_()
  {}
  virtual ~ObCreateUserDefinedFunctionArg()
  {}
  bool is_valid() const
  {
    return !udf_.get_name_str().empty();
  }
  TO_STRING_KV(K_(udf));

  share::schema::ObUDF udf_;
};

struct ObDropUserDefinedFunctionArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropUserDefinedFunctionArg() : tenant_id_(common::OB_INVALID_ID), name_(), if_exist_(false)
  {}
  virtual ~ObDropUserDefinedFunctionArg()
  {}
  bool is_valid() const
  {
    return !name_.empty();
  }
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  TO_STRING_KV(K_(tenant_id), K_(name));

  uint64_t tenant_id_;
  common::ObString name_;
  bool if_exist_;
};

struct ObCreateOutlineArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateOutlineArg() : ObDDLArg(), or_replace_(false), outline_info_(), db_name_()
  {}
  virtual ~ObCreateOutlineArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(or_replace), K_(outline_info), K_(db_name));

  bool or_replace_;
  share::schema::ObOutlineInfo outline_info_;
  common::ObString db_name_;
};

struct ObAlterOutlineArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  enum AlterOutlineOptions { ADD_OUTLINE_CONTENT = 1, ADD_CONCURRENT_LIMIT, MAX_OPTION };
  ObAlterOutlineArg() : ObDDLArg(), alter_outline_info_(), db_name_()
  {}
  virtual ~ObAlterOutlineArg()
  {}
  bool is_valid() const
  {
    return (!db_name_.empty() && !alter_outline_info_.get_signature_str().empty() &&
            (!alter_outline_info_.get_outline_content_str().empty() || alter_outline_info_.has_outline_params()));
  }
  TO_STRING_KV(K_(alter_outline_info), K_(db_name));

  share::schema::ObAlterOutlineInfo alter_outline_info_;
  common::ObString db_name_;
};

struct ObDropOutlineArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropOutlineArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), db_name_(), outline_name_()
  {}
  virtual ~ObDropOutlineArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  TO_STRING_KV(K_(tenant_id), K_(db_name), K_(outline_name));

  uint64_t tenant_id_;
  common::ObString db_name_;
  common::ObString outline_name_;
};

struct ObCreateDbLinkArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateDbLinkArg() : ObDDLArg(), dblink_info_()
  {}
  virtual ~ObCreateDbLinkArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(dblink_info));
  share::schema::ObDbLinkInfo dblink_info_;
};

struct ObDropDbLinkArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDropDbLinkArg() : ObDDLArg(), tenant_id_(common::OB_INVALID_ID), dblink_name_()
  {}
  virtual ~ObDropDbLinkArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(dblink_name));
  uint64_t tenant_id_;
  common::ObString dblink_name_;
};

struct ObUseDatabaseArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObUseDatabaseArg() : ObDDLArg()
  {}
};

struct ObFetchAliveServerArg {
  OB_UNIS_VERSION(1);

public:
  ObFetchAliveServerArg() : cluster_id_(0)
  {}
  TO_STRING_KV(K_(cluster_id));
  bool is_valid() const
  {
    return cluster_id_ >= 0;
  }

  int64_t cluster_id_;
};

struct ObFetchAliveServerResult {
  OB_UNIS_VERSION(1);

public:
  TO_STRING_KV(K_(active_server_list));
  bool is_valid() const
  {
    return !active_server_list_.empty();
  }

  ObServerList active_server_list_;
  ObServerList inactive_server_list_;
};

struct ObLoadBaselineArg {
  OB_UNIS_VERSION(1);

public:
  ObLoadBaselineArg()
      : is_all_tenant_(false),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        sql_id_(),
        plan_hash_value_(common::OB_INVALID_ID),
        fixed_(false),
        enabled_(false)
  {}
  virtual ~ObLoadBaselineArg()
  {}
  TO_STRING_KV(K(is_all_tenant_), K_(tenant_id), K_(sql_id), K_(plan_hash_value), K_(fixed), K_(enabled));

  bool is_all_tenant_;
  uint64_t tenant_id_;
  common::ObString sql_id_;
  uint64_t plan_hash_value_;
  bool fixed_;
  bool enabled_;
};

struct ObFlushCacheArg {
  OB_UNIS_VERSION(1);

public:
   ObFlushCacheArg() :
    is_all_tenant_(false),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    cache_type_(CACHE_TYPE_INVALID),
    is_fine_grained_(false){};
  virtual ~ObFlushCacheArg()
  {}
  bool is_valid() const
  {
    return cache_type_ > CACHE_TYPE_INVALID && cache_type_ < CACHE_TYPE_MAX;
  }
  int push_database(uint64_t db_id) { return db_ids_.push_back(db_id); }
  TO_STRING_KV(K(is_all_tenant_), K_(tenant_id), K_(cache_type), K_(db_ids), K_(sql_id), K_(is_fine_grained));

  bool is_all_tenant_;
  uint64_t tenant_id_;
  ObCacheType cache_type_;
  common::ObSEArray<uint64_t, 8> db_ids_;
  common::ObString sql_id_;
  bool is_fine_grained_;
};

struct ObGetAllSchemaArg {
  OB_UNIS_VERSION(1);

public:
  ObGetAllSchemaArg() : schema_version_(common::OB_INVALID_VERSION)
  {}
  TO_STRING_KV(K_(schema_version), K_(tenant_name));

  int64_t schema_version_;
  common::ObString tenant_name_;
};

struct ObAdminSetTPArg : public ObServerZoneArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminSetTPArg() : event_no_(0), occur_(0), trigger_freq_(1), error_code_(0)
  {}

  inline bool is_valid() const
  {
    return (error_code_ <= 0 && (trigger_freq_ >= 0));
  }

  TO_STRING_KV(K_(event_no), K_(event_name), K_(occur), K_(trigger_freq), K_(error_code), K_(server), K_(zone));

  int64_t event_no_;             // tracepoint no
  common::ObString event_name_;  // tracepoint name
  int64_t occur_;                // number of occurrences
  int64_t trigger_freq_;         // trigger frequency
  int64_t error_code_;           // error code to return
};

struct ObCancelTaskArg : public ObServerZoneArg {
  OB_UNIS_VERSION(2);

public:
  ObCancelTaskArg() : task_id_()
  {}
  TO_STRING_KV(K_(task_id));
  share::ObTaskId task_id_;
};

struct ObReportSingleReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObReportSingleReplicaArg() : partition_key_()
  {}
  bool is_valid() const
  {
    return partition_key_.is_valid();
  }

  TO_STRING_KV(K_(partition_key));

  common::ObPartitionKey partition_key_;
};

struct ObSetDiskValidArg {
  OB_UNIS_VERSION(1);

public:
  ObSetDiskValidArg()
  {}
  bool is_valid() const
  {
    return true;
  }
};

struct ObAdminClearBalanceTaskArg {
  OB_UNIS_VERSION(1);

public:
  enum TaskType { AUTO = 0, MANUAL, ALL, MAX_TYPE };

  ObAdminClearBalanceTaskArg() : tenant_ids_(), type_(ALL), zone_names_()
  {}
  ~ObAdminClearBalanceTaskArg()
  {}

  TO_STRING_KV(K_(tenant_ids), K_(type), K_(zone_names));
  common::ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM> tenant_ids_;
  TaskType type_;
  common::ObSEArray<common::ObZone, common::OB_PREALLOCATED_NUM> zone_names_;
};

class ObMCLogInfo {
  OB_UNIS_VERSION(1);

public:
  ObMCLogInfo() : log_id_(common::OB_INVALID_ID), timestamp_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObMCLogInfo(){};
  bool is_valid() const
  {
    return common::OB_INVALID_ID != log_id_ && common::OB_INVALID_TIMESTAMP != timestamp_;
  }

public:
  uint64_t log_id_;
  int64_t timestamp_;
  TO_STRING_KV(K_(log_id), K_(timestamp));
};

struct ObChangeMemberArg {
  OB_UNIS_VERSION(1);

public:
  ObChangeMemberArg() : partition_key_(), member_(), quorum_(), switch_epoch_(common::OB_INVALID_VERSION)
  {}
  common::ObPartitionKey partition_key_;
  common::ObMember member_;
  int64_t quorum_;
  int64_t switch_epoch_;
  TO_STRING_KV(K_(partition_key), K_(member), K_(quorum), K_(switch_epoch));
};

struct ObChangeMemberCtx {
  OB_UNIS_VERSION(1);

public:
  ObChangeMemberCtx() : partition_key_(), ret_value_(common::OB_SUCCESS), log_info_()
  {}
  common::ObPartitionKey partition_key_;
  int32_t ret_value_;
  ObMCLogInfo log_info_;
  TO_STRING_KV(K_(partition_key), K_(ret_value), K_(log_info));
};

typedef common::ObSArray<ObChangeMemberArg> ObChangeMemberArgs;
typedef common::ObSArray<ObChangeMemberCtx> ObChangeMemberCtxs;

struct ObChangeMemberCtxsWrapper {
  OB_UNIS_VERSION(1);

public:
  ObChangeMemberCtxsWrapper() : result_code_(common::OB_SUCCESS), ctxs_()
  {}
  int32_t result_code_;
  ObChangeMemberCtxs ctxs_;
  TO_STRING_KV(K_(result_code), K_(ctxs));
};

struct ObMemberMajorSSTableCheckArg {
  OB_UNIS_VERSION(1);

public:
  ObMemberMajorSSTableCheckArg() : pkey_(), table_ids_()
  {}
  TO_STRING_KV(K_(pkey), K_(table_ids));
  common::ObPartitionKey pkey_;
  common::ObSArray<uint64_t> table_ids_;
};

struct ObForceSwitchILogFileArg {
  OB_UNIS_VERSION(1);

public:
  ObForceSwitchILogFileArg() : force_(true)
  {}
  ~ObForceSwitchILogFileArg()
  {}

  DECLARE_TO_STRING;
  bool force_;
};

struct ObForceSetAllAsSingleReplicaArg {
  OB_UNIS_VERSION(1);

public:
  ObForceSetAllAsSingleReplicaArg() : force_(true)
  {}
  ~ObForceSetAllAsSingleReplicaArg()
  {}

  DECLARE_TO_STRING;
  bool force_;
};

struct ObForceSetServerListArg {
  OB_UNIS_VERSION(1);

public:
  ObForceSetServerListArg() : server_list_(), replica_num_(0)
  {}
  ~ObForceSetServerListArg()
  {}

  DECLARE_TO_STRING;
  obrpc::ObServerList server_list_;
  int64_t replica_num_;
};

struct ObForceCreateSysTableArg {
  OB_UNIS_VERSION(1);

public:
  ObForceCreateSysTableArg()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        table_id_(common::OB_INVALID_ID),
        last_replay_log_id_(common::OB_INVALID_ID)
  {}
  ~ObForceCreateSysTableArg()
  {}
  bool is_valid() const;

  DECLARE_TO_STRING;
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t last_replay_log_id_;
};

struct ObForceSetLocalityArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObForceSetLocalityArg() : ObDDLArg(), locality_()
  {}
  virtual ~ObForceSetLocalityArg()
  {}
  bool is_valid() const;
  int assign(const ObForceSetLocalityArg& other);
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  common::ObString locality_;
  TO_STRING_KV(K_(exec_tenant_id), K_(locality));
};

struct ObRootSplitPartitionArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObRootSplitPartitionArg() : ObDDLArg(), table_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObRootSplitPartitionArg()
  {}
  bool is_valid() const
  {
    return table_id_ != common::OB_INVALID_ID;
  }
  int assign(const ObRootSplitPartitionArg& other);
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }

  DECLARE_TO_STRING;
  uint64_t table_id_;
};

struct ObSplitPartitionArg {
  OB_UNIS_VERSION(1);

public:
  ObSplitPartitionArg() : split_info_()
  {}
  ~ObSplitPartitionArg()
  {}
  TO_STRING_KV(K_(split_info));
  bool is_valid() const
  {
    return split_info_.is_valid();
  }
  void reset()
  {
    split_info_.reset();
  }

public:
  share::ObSplitPartition split_info_;
};

struct ObSplitPartitionResult {
  OB_UNIS_VERSION(1);

public:
  ObSplitPartitionResult() : results_()
  {}
  ~ObSplitPartitionResult()
  {}
  void reset()
  {
    results_.reuse();
  }
  common::ObSArray<share::ObPartitionSplitProgress>& get_result()
  {
    return results_;
  }
  const common::ObSArray<share::ObPartitionSplitProgress>& get_result() const
  {
    return results_;
  }
  TO_STRING_KV(K_(results));

private:
  common::ObSArray<share::ObPartitionSplitProgress> results_;
};

struct ObSplitPartitionBatchArg {
  OB_UNIS_VERSION(1);

public:
  ObSplitPartitionBatchArg()
  {}
  ~ObSplitPartitionBatchArg()
  {}
  bool is_valid() const;
  int assign(const ObSplitPartitionBatchArg& other);
  void reset()
  {
    return split_info_.reset();
  }
  DECLARE_TO_STRING;
  share::ObSplitPartition split_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSplitPartitionBatchArg);
};

struct ObSplitPartitionBatchRes {
  OB_UNIS_VERSION(1);

public:
  ObSplitPartitionBatchRes() : ret_list_()
  {}
  ~ObSplitPartitionBatchRes()
  {}
  inline void reset()
  {
    ret_list_.reset();
  }
  int assign(const ObSplitPartitionBatchRes& other);
  inline void reuse();

  DECLARE_TO_STRING;
  // response includes all rets
  common::ObSArray<common::ObPartitionKey> ret_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSplitPartitionBatchRes);
};

struct ObQueryMaxDecidedTransVersionRequest {
  OB_UNIS_VERSION(1);

public:
  ObQueryMaxDecidedTransVersionRequest() : partition_array_(), last_max_decided_trans_version_(0)
  {}
  ~ObQueryMaxDecidedTransVersionRequest()
  {}
  void reset()
  {
    partition_array_.reset();
    last_max_decided_trans_version_ = 0;
  }

  common::ObPartitionArray partition_array_;
  int64_t last_max_decided_trans_version_;
  TO_STRING_KV(K(partition_array_), K_(last_max_decided_trans_version));
};

struct ObQueryMaxDecidedTransVersionResponse {
  OB_UNIS_VERSION(1);

public:
  ObQueryMaxDecidedTransVersionResponse() : ret_value_(common::OB_SUCCESS), trans_version_(0)
  {}
  ~ObQueryMaxDecidedTransVersionResponse()
  {}
  void reset()
  {
    ret_value_ = common::OB_SUCCESS;
    trans_version_ = 0;
  }

  int ret_value_;
  int64_t trans_version_;
  common::ObPartitionKey pkey_;
  TO_STRING_KV(K(ret_value_), K(trans_version_), K_(pkey));
};

struct ObQueryIsValidMemberRequest {
  OB_UNIS_VERSION(1);

public:
  ObQueryIsValidMemberRequest() : self_addr_(), partition_array_()
  {}
  ~ObQueryIsValidMemberRequest()
  {}
  void reset()
  {
    self_addr_.reset();
    partition_array_.reset();
  }

  common::ObAddr self_addr_;
  common::ObPartitionArray partition_array_;
  TO_STRING_KV(K(self_addr_), K(partition_array_));
};

struct ObQueryIsValidMemberResponse {
  OB_UNIS_VERSION(1);

public:
  ObQueryIsValidMemberResponse()
      : ret_value_(common::OB_SUCCESS), partition_array_(), candidates_status_(), ret_array_()
  {}
  ~ObQueryIsValidMemberResponse()
  {}
  void reset()
  {
    ret_value_ = common::OB_SUCCESS;
    partition_array_.reset();
    candidates_status_.reset();
    ret_array_.reset();
  }

  int ret_value_;
  common::ObPartitionArray partition_array_;
  common::ObSEArray<bool, 16> candidates_status_;
  common::ObSEArray<int, 16> ret_array_;
  TO_STRING_KV(K(ret_value_), K(partition_array_), K(candidates_status_), K(ret_array_));
};

struct ObQueryMaxFlushedILogIdRequest {
  OB_UNIS_VERSION(1);

public:
  ObQueryMaxFlushedILogIdRequest() : partition_array_()
  {}
  ~ObQueryMaxFlushedILogIdRequest()
  {}
  void reset()
  {
    partition_array_.reset();
  }

  common::ObPartitionArray partition_array_;
  TO_STRING_KV(K(partition_array_));
};

struct ObQueryMaxFlushedILogIdResponse {
  OB_UNIS_VERSION(1);

public:
  ObQueryMaxFlushedILogIdResponse() : err_code_(common::OB_SUCCESS), partition_array_(), max_flushed_ilog_ids_()
  {}
  ~ObQueryMaxFlushedILogIdResponse()
  {}
  void reset()
  {
    err_code_ = common::OB_SUCCESS;
    partition_array_.reset();
    max_flushed_ilog_ids_.reset();
  }

  int err_code_;
  common::ObPartitionArray partition_array_;
  common::ObSEArray<uint64_t, 16> max_flushed_ilog_ids_;
  TO_STRING_KV(K_(err_code), K_(partition_array), K_(max_flushed_ilog_ids));
};

struct ObBootstrapArg {
  OB_UNIS_VERSION(1);

public:
  ObBootstrapArg()
      : server_list_(),
        cluster_type_(common::PRIMARY_CLUSTER),
        initial_frozen_version_(0),
        initial_schema_version_(0),
        primary_cluster_id_(common::OB_INVALID_ID),
        primary_rs_list_(),
        freeze_schemas_(),
        frozen_status_()
  {}
  ~ObBootstrapArg()
  {}
  TO_STRING_KV(K_(server_list), K_(cluster_type), K_(initial_frozen_version), K_(initial_schema_version),
      K_(primary_cluster_id), K_(primary_rs_list), K_(freeze_schemas), K_(frozen_status));
  int assign(const ObBootstrapArg& arg);
  ObServerInfoList server_list_;
  common::ObClusterType cluster_type_;
  int64_t initial_frozen_version_;
  int64_t initial_schema_version_;
  int64_t primary_cluster_id_;
  common::ObSArray<common::ObAddr> primary_rs_list_;
  common::ObSArray<share::TenantIdAndSchemaVersion> freeze_schemas_;
  common::ObSArray<storage::ObFrozenStatus> frozen_status_;
};

struct ObBatchStartElectionArg {
  OB_UNIS_VERSION(1);

public:
  enum SwitchType {
    INVALID_TYPE = -1,
    SWITCHOVER,
    FAILOVER,
  };
  ObBatchStartElectionArg()
      : pkeys_(),
        lease_start_time_(0),
        leader_(),
        member_list_(),
        switch_timestamp_(0),
        switch_type_(INVALID_TYPE),
        quorum_(-1),
        trace_id_()
  {}
  ~ObBatchStartElectionArg()
  {}
  int add_partition_key(const common::ObPartitionKey& pkey);
  const ObPkeyArray& get_pkey_array() const
  {
    return pkeys_;
  }
  int set_leader(const common::ObAddr& leader)
  {
    leader_ = leader;
    return common::OB_SUCCESS;
  }
  const common::ObAddr get_leader() const
  {
    return leader_;
  }
  void set_switch_timestamp(const int64_t switch_timestamp)
  {
    switch_timestamp_ = switch_timestamp;
  }
  int64_t get_switch_timestamp() const
  {
    return switch_timestamp_;
  }
  int set_member_list(const common::ObMemberList& member_list)
  {
    return member_list_.deep_copy(member_list);
  }
  const common::ObMemberList& get_member_list() const
  {
    return member_list_;
  }
  void set_quorum(const int64_t quorum)
  {
    quorum_ = quorum;
  }
  int64_t get_quorum() const
  {
    return quorum_;
  }
  int assign(const ObBatchStartElectionArg& other);
  int64_t count() const
  {
    return pkeys_.count();
  }
  bool reach_concurrency_limit() const
  {
    return pkeys_.count() >= MAX_COUNT;
  }
  bool has_task() const
  {
    return pkeys_.count() > 0;
  }
  void set_lease_timestamp()
  {
    lease_start_time_ = common::ObTimeUtility::current_time();
  }
  int64_t get_lease_start() const
  {
    return lease_start_time_;
  }
  void set_trace_id(const common::ObCurTraceId::TraceId& trace_id)
  {
    trace_id_ = trace_id;
  }
  void reset();
  bool is_valid() const;
  void set_switch_type(const SwitchType& switch_type)
  {
    switch_type_ = switch_type;
  }
  SwitchType get_switch_type() const
  {
    return switch_type_;
  }
  TO_STRING_KV(K_(trace_id), "count", pkeys_.count(), K_(member_list), K_(lease_start_time), K_(leader),
      K_(switch_timestamp), K_(switch_type), K_(pkeys), K_(quorum));

public:
  static bool is_valid_switch_type(const int32_t switch_type)
  {
    return (switch_type >= SWITCHOVER && switch_type <= FAILOVER);
  }
  ObPkeyArray pkeys_;

private:
  int64_t lease_start_time_;
  common::ObAddr leader_;
  common::ObMemberList member_list_;
  int64_t switch_timestamp_;
  SwitchType switch_type_;
  int64_t quorum_;  // In the failover process, there may be a single copy, so quorum needs to be specified by rs
  common::ObCurTraceId::TraceId trace_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBatchStartElectionArg);
};

struct ObBatchFlashbackArg {
  OB_UNIS_VERSION(1);

public:
  static const int64_t MAX_COUNT = 128;
  ObBatchFlashbackArg()
      : switchover_timestamp_(0),
        flashback_to_ts_(0),
        leader_(),
        pkeys_(),
        flashback_from_ts_(0),
        frozen_timestamp_(0),
        is_logical_flashback_(false),
        query_end_time_(0),
        schema_version_(0)
  {}
  ~ObBatchFlashbackArg()
  {}
  void set_leader(const common::ObAddr& addr)
  {
    leader_ = addr;
  }
  void reset();
  void set_switchover_timestamp(const int64_t switchover_timestamp)
  {
    switchover_timestamp_ = switchover_timestamp;
  }
  void set_flashback_ts(const int64_t flashback_from_ts, const int64_t flashback_to_ts)
  {
    flashback_from_ts_ = flashback_from_ts;
    flashback_to_ts_ = flashback_to_ts;
  }
  void set_frozen_timestamp(const int64_t frozen_timestamp)
  {
    frozen_timestamp_ = frozen_timestamp;
  }
  int64_t get_frozen_timestamp() const
  {
    return frozen_timestamp_;
  }
  void set_is_logical_flashback(const bool is_logical_flashback)
  {
    is_logical_flashback_ = is_logical_flashback;
  }
  bool is_logical_flashback() const
  {
    return is_logical_flashback_;
  }
  void set_query_end_time(const int64_t query_end_time)
  {
    query_end_time_ = query_end_time;
  }
  int64_t get_query_end_time() const
  {
    return query_end_time_;
  }
  void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  bool reach_concurrency_limit() const
  {
    return pkeys_.count() >= MAX_COUNT;
  }
  int add_pkey(const common::ObPartitionKey& pkey);
  const ObPkeyArray& get_pkey_array() const
  {
    return pkeys_;
  }
  common::ObAddr get_server() const
  {
    return leader_;
  }
  int64_t get_flashback_to_ts() const
  {
    return flashback_to_ts_;
  }
  int64_t get_flashback_from_ts() const
  {
    return flashback_from_ts_;
  }
  int64_t get_switchover_timestamp() const
  {
    return switchover_timestamp_;
  }
  bool is_valid() const;
  bool has_task() const
  {
    return pkeys_.count() > 0;
  }
  TO_STRING_KV(K_(switchover_timestamp), K_(flashback_to_ts), K_(flashback_from_ts), K_(leader),
      K_(is_logical_flashback), K_(pkeys), K_(frozen_timestamp), K_(query_end_time), K_(schema_version));

public:
  int64_t switchover_timestamp_;
  int64_t flashback_to_ts_;
  common::ObAddr leader_;
  ObPkeyArray pkeys_;
  int64_t flashback_from_ts_;
  int64_t frozen_timestamp_;
  bool is_logical_flashback_;  // failover use
  int64_t query_end_time_;
  int64_t schema_version_;
};

struct ObAlterClusterInfoArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  enum OpType {
    INVALID_TYPE = -1,
    SWITCH_TO_STANDBY = 0,
    SWITCH_TO_PRIMARY = 1,
    FAILOVER_TO_PRIMARY = 2,
    CONVERT_TO_STANDBY = 3,
    INTACT_FAILOVER = 4,
    SET_PROTECTION_MODE = 5,
    UPDATE_PROTECTION_LEVEL = 6,
    UPDATE_SYNC_STANDBY_PROTECTION_LEVEL = 7,
  };
  ObAlterClusterInfoArg()
      : op_type_(INVALID_TYPE),
        mode_(common::MAXIMUM_PERFORMANCE_MODE),
        is_force_(false),
        level_(common::MAXIMUM_PERFORMANCE_LEVEL)
  {}
  TO_STRING_KV(K_(op_type), K_(ddl_stmt_str), K_(mode), K_(level), K_(is_force));
  int assign(ObAlterClusterInfoArg& arg);
  ~ObAlterClusterInfoArg()
  {}

  const char* get_alter_type_str() const
  {
    const char* cstr = "invalid";
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
      case CONVERT_TO_STANDBY:
        cstr = "convert to physical standby";
        break;
      case INTACT_FAILOVER:
        cstr = "intact failover to primary";
        break;
      case SET_PROTECTION_MODE:
        cstr = "set cluster protection mode";
        break;
      case UPDATE_PROTECTION_LEVEL:
        cstr = "update protection level in maximum availability mode";
        break;
      case UPDATE_SYNC_STANDBY_PROTECTION_LEVEL:
        cstr = "update sync standby protection level";
        break;
      default:
        cstr = "invalid";
        break;
    }
    return cstr;
  }

public:
  OpType op_type_;
  common::ObProtectionMode mode_;
  bool is_force_;
  common::ObProtectionLevel level_;
};

struct ObAdminClusterArg {
  OB_UNIS_VERSION(1);

public:
  enum AlterClusterType {
    INVALID_TYPE = -1,
    ADD_CLUSTER = 0,
    REMOVE_CLUSTER,
    ENABLE_CLUSTER,
    DISABLE_CLUSTER,
    ALTER_CLUSTER,
  };

public:
  ObAdminClusterArg()
      : cluster_name_(),
        cluster_id_(common::OB_INVALID_ID),
        alter_type_(INVALID_TYPE),
        is_force_(false),
        ddl_stmt_str_(),
        rootservice_list_(),
        redo_transport_options_()
  {}
  ~ObAdminClusterArg()
  {}
  void set_cluster_name(const common::ObString& cluster_name)
  {
    cluster_name_ = cluster_name;
  }
  void set_cluster_id(const int64_t cluster_id)
  {
    cluster_id_ = cluster_id;
  }
  void set_alter_cluster_type(const AlterClusterType type)
  {
    alter_type_ = type;
  }
  void set_is_force(const bool is_force)
  {
    is_force_ = is_force;
  }
  bool is_valid() const
  {
    return !cluster_name_.empty() && common::OB_INVALID_ID != cluster_id_ && INVALID_TYPE != alter_type_;
  }
  void set_ddl_stmt_str(const common::ObString& ddl_stmt_str)
  {
    ddl_stmt_str_ = ddl_stmt_str;
  }
  int set_rootservice_list(const common::ObIArray<common::ObAddr>& rs_list)
  {
    return rootservice_list_.assign(rs_list);
  }
  void set_redo_transport_options_str(const common::ObString& redo_transport_options_str)
  {
    redo_transport_options_ = redo_transport_options_str;
  }
  const char* get_cluster_admin_type_cstr() const
  {
    const char* cstr = "invalid";
    switch (alter_type_) {
      case ADD_CLUSTER:
        cstr = "add cluster";
        break;
      case REMOVE_CLUSTER:
        cstr = "remove cluster";
        break;
      case ENABLE_CLUSTER:
        cstr = "enable cluster synchronization";
        break;
      case DISABLE_CLUSTER:
        cstr = "disable cluster synchronization";
        break;
      case ALTER_CLUSTER:
        cstr = "alter cluster";
        break;
      default:
        cstr = "invalid";
        break;
    }
    return cstr;
  }
  TO_STRING_KV(K_(cluster_name), K_(cluster_id), K_(alter_type), K_(is_force), K_(ddl_stmt_str), K_(rootservice_list),
      K_(redo_transport_options));
  common::ObString cluster_name_;
  int64_t cluster_id_;
  AlterClusterType alter_type_;
  bool is_force_;
  common::ObString ddl_stmt_str_;
  common::ObSArray<common::ObAddr> rootservice_list_;
  common::ObString redo_transport_options_;
};

struct ObClusterActionVerifyArg {
  OB_UNIS_VERSION(1);

public:
  enum VerifyType {
    INVALID_TYPE = -1,
    ADD_CLUSTER_VERIFY = 0,
  };

public:
  ObClusterActionVerifyArg() : verify_type_(INVALID_TYPE)
  {}
  ~ObClusterActionVerifyArg()
  {}
  void set_verify_type(const VerifyType type)
  {
    verify_type_ = type;
  }
  bool is_valid() const
  {
    return INVALID_TYPE != verify_type_;
  }
  TO_STRING_KV(K_(verify_type));
  VerifyType verify_type_;
};

struct ObDDLNopOpreatorArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObDDLNopOpreatorArg() : schema_operation_()
  {}
  ~ObDDLNopOpreatorArg()
  {}

public:
  share::schema::ObSchemaOperation schema_operation_;
  bool is_valid() const
  {
    return schema_operation_.is_valid();
  }
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  void reset()
  {
    schema_operation_.reset();
  }
  TO_STRING_KV(K_(schema_operation));

private:
  DISALLOW_COPY_AND_ASSIGN(ObDDLNopOpreatorArg);
};

// end for ddl arg
//////////////////////////////////////////////////

struct ObEstPartArgElement {
  ObEstPartArgElement() : index_id_(common::OB_INVALID_ID), range_columns_count_(0)
  {}
  // Essentially, we can use ObIArray<ObNewRange> here
  // For compatibility reason, we still use ObSimpleBatch
  common::ObSimpleBatch batch_;
  common::ObQueryFlag scan_flag_;
  int64_t index_id_;
  int64_t range_columns_count_;

  TO_STRING_KV(K(scan_flag_), K(index_id_), K(batch_), K(range_columns_count_));
  int64_t get_serialize_size(void) const;
  int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(common::ObIAllocator& allocator, const char* buf, const int64_t data_len, int64_t& pos);
};

struct ObEstPartArg {
  // Deserialization use
  common::ObArenaAllocator allocator_;
  //***** Parameters involved in obtaining master table information
  common::ObPartitionKey pkey_;
  int64_t schema_version_;
  common::ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> column_ids_;
  // Estimated table row length parameter
  common::ObSEArray<common::ObPartitionKey, 4, common::ModulePageAllocator, true> partition_keys_;
  // Estimate the number of rows in the entire table parameter
  ObEstPartArgElement scan_param_;
  //***** end
  // Get statistics specific variables for each index
  common::ObSEArray<ObEstPartArgElement, 4, common::ModulePageAllocator, true> index_params_;
  // best estimation partition key for each index
  common::ObSEArray<common::ObPartitionKey, 4, common::ModulePageAllocator, true> index_pkeys_;

  ObEstPartArg()
      : allocator_(common::ObModIds::OB_SQL_QUERY_RANGE),
        pkey_(),
        schema_version_(0),
        column_ids_(),
        partition_keys_(),
        scan_param_(),
        index_params_(),
        index_pkeys_()
  {}
  ~ObEstPartArg()
  {}

  bool is_valid_table_pkey() const
  {
    return pkey_.is_valid();
  }

  TO_STRING_KV(K_(column_ids), K_(pkey), K_(schema_version), K_(partition_keys), K_(scan_param), K_(index_params),
      K_(index_pkeys));

  OB_UNIS_VERSION(1);
};

struct ObEstPartResElement {
  int64_t logical_row_count_;
  int64_t physical_row_count_;
  /**
   * @brief reliable_
   * storage estimation is not successfully called,
   * we use ndv to estimate row count in the following
   */
  bool reliable_;
  common::ObSEArray<common::ObEstRowCountRecord, common::MAX_SSTABLE_CNT_IN_STORAGE, common::ModulePageAllocator, true>
      est_records_;

  ObEstPartResElement()
  {
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

struct ObEstPartRowCountSizeRes {
  int64_t row_count_;
  int64_t part_size_;
  double avg_row_size_;
  /**
   * @brief reliable_
   * reliable = true if the table is empty,
   * we will use default row count in the following
   */
  bool reliable_;

  ObEstPartRowCountSizeRes()
  {
    reset();
  }

  void reset()
  {
    row_count_ = common::OB_INVALID_COUNT;
    part_size_ = common::OB_INVALID_COUNT;
    avg_row_size_ = -1;
    reliable_ = false;
  }

  TO_STRING_KV(K(row_count_), K(part_size_), K(avg_row_size_), K(reliable_));

  OB_UNIS_VERSION(1);
};

struct ObEstPartRes {
  ObEstPartRowCountSizeRes part_rowcount_size_res_;
  common::ObSEArray<ObEstPartResElement, 4, common::ModulePageAllocator, true> index_param_res_;

  ObEstPartRes() : part_rowcount_size_res_(), index_param_res_()
  {}

  TO_STRING_KV(K(part_rowcount_size_res_), K(index_param_res_));

  OB_UNIS_VERSION(1);
};

struct TenantServerUnitConfig {
public:
  TenantServerUnitConfig()
      : tenant_id_(common::OB_INVALID_ID),
        compat_mode_(share::ObWorker::CompatMode::INVALID),
        unit_config_(),
        replica_type_(common::ObReplicaType::REPLICA_TYPE_MAX),
        if_not_grant_(false),
        is_delete_(false)
  {}
  int init(const uint64_t tenant_id, const share::ObWorker::CompatMode compat_mode,
      const share::ObUnitConfig& unit_config, const common::ObReplicaType replica_type, const bool if_not_grant,
      const bool is_delete);
  uint64_t tenant_id_;
  share::ObWorker::CompatMode compat_mode_;
  share::ObUnitConfig unit_config_;
  common::ObReplicaType replica_type_;
  bool if_not_grant_;
  bool is_delete_;
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(unit_config), K_(compat_mode), K_(replica_type), K_(if_not_grant), K_(is_delete));

  OB_UNIS_VERSION(1);
};

struct ObGetWRSArg {
  OB_UNIS_VERSION(1);

public:
  enum Scope { INVALID_RANGE = 0, INNER_TABLE, USER_TABLE, ALL_TABLE };
  TO_STRING_KV(K_(tenant_id), K_(scope), K_(need_filter));
  bool is_valid() const;
  int64_t tenant_id_;
  Scope scope_;  // The machine-readable timestamp can be calculated separately for the timestamp of the system table or
                 // user table, or collectively calculated together
  bool need_filter_;

  ObGetWRSArg()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        scope_(ALL_TABLE),   // Statistics of all types of tables by default
        need_filter_(false)  // Unreadable partitions are not filtered by default
  {}

  explicit ObGetWRSArg(const int64_t tenant_id)
      : tenant_id_(tenant_id),
        scope_(ALL_TABLE),   // Statistics of all types of tables by default
        need_filter_(false)  // Unreadable partitions are not filtered by default
  {}
};

struct ObGetWRSResult {
  OB_UNIS_VERSION(1);

public:
  ObGetWRSResult() : self_addr_(), err_code_(0), replica_wrs_info_list_()
  {}

  void reset()
  {
    self_addr_.reset();
    err_code_ = 0;
    replica_wrs_info_list_.reset();
  }

public:
  common::ObAddr self_addr_;
  int err_code_;  // error code

  // According to the ObGetWRSArg::need_filter_ field to determine whether to filter the copies with invalid
  // machine-readable timestamps
  share::ObReplicaWrsInfoList replica_wrs_info_list_;  // Standby machine-readable timestamp of each copy of this server

  TO_STRING_KV(K_(err_code), K_(self_addr), "replica_count", replica_wrs_info_list_.count(), K_(replica_wrs_info_list));
};

struct ObTenantSchemaVersions {
  OB_UNIS_VERSION(1);

public:
  ObTenantSchemaVersions() : tenant_schema_versions_()
  {}
  common::ObSArray<share::TenantIdAndSchemaVersion> tenant_schema_versions_;
  int add(const int64_t tenant_id, const int64_t schema_version);
  void reset()
  {
    return tenant_schema_versions_.reset();
  }
  bool is_valid() const
  {
    return 0 < tenant_schema_versions_.count();
  }
  int assign(const ObTenantSchemaVersions& arg)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(tenant_schema_versions_.assign(arg.tenant_schema_versions_))) {
      SHARE_LOG(WARN, "failed to assign tenant schema version", KR(ret), K(arg));
    }
    return ret;
  }
  TO_STRING_KV(K_(tenant_schema_versions));
};

struct ObGetSchemaArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObGetSchemaArg() : reserve_(0), ignore_fail_(false)
  {}
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  int64_t reserve_;
  bool ignore_fail_;
};

struct TenantIdAndStats {
  OB_UNIS_VERSION(1);

public:
  TenantIdAndStats()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        refreshed_schema_version_(0),
        ddl_lag_(0),
        min_sys_table_scn_(0),
        min_user_table_scn_(0)
  {}

  TenantIdAndStats(const uint64_t tenant_id, const int64_t refreshed_schema_version, const int64_t ddl_lag,
      const int64_t min_sys_table_scn, const int64_t min_user_table_scn)
      : tenant_id_(tenant_id),
        refreshed_schema_version_(refreshed_schema_version),
        ddl_lag_(ddl_lag),
        min_sys_table_scn_(min_sys_table_scn),
        min_user_table_scn_(min_user_table_scn)
  {}

  TO_STRING_KV(K_(tenant_id), K_(refreshed_schema_version), K_(ddl_lag), K_(min_sys_table_scn), K_(min_user_table_scn));

  void reset()
  {
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

struct ObClusterTenantStats {
  OB_UNIS_VERSION(1);

public:
  void reset()
  {
    return tenant_stats_array_.reset();
  }
  int assign(const ObClusterTenantStats& other)
  {
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(tenant_stats_array_.assign(other.tenant_stats_array_))) {
      SHARE_LOG(WARN, "fail to assign partition ids", K(ret));
    }
    return ret;
  }
  TO_STRING_KV(K_(tenant_stats_array));
  common::ObSArray<obrpc::TenantIdAndStats> tenant_stats_array_;
};

struct ObFinishSchemaSplitArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObFinishSchemaSplitArg()
      : tenant_id_(common::OB_INVALID_TENANT_ID), type_(rootserver::ObRsJobType::JOB_TYPE_SCHEMA_SPLIT_V2)
  {}
  virtual ~ObFinishSchemaSplitArg()
  {}
  bool is_valid() const;
  int assign(const ObFinishSchemaSplitArg& other);
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  TO_STRING_KV(K_(tenant_id), K_(type));

public:
  uint64_t tenant_id_;
  rootserver::ObRsJobType type_;
};

struct ObBroadcastSchemaArg {
  OB_UNIS_VERSION(1);

public:
  ObBroadcastSchemaArg() : tenant_id_(common::OB_INVALID_TENANT_ID), schema_version_(common::OB_INVALID_VERSION)
  {}
  void reset();

public:
  uint64_t tenant_id_;
  int64_t schema_version_;
  TO_STRING_KV(K_(tenant_id), K_(schema_version));
};

struct ObCheckMergeFinishArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckMergeFinishArg() : frozen_version_(0)
  {}
  bool is_valid() const;

public:
  int64_t frozen_version_;
  TO_STRING_KV(K_(frozen_version));
};

struct ObGetRecycleSchemaVersionsArg {
  OB_UNIS_VERSION(1);

public:
  ObGetRecycleSchemaVersionsArg() : tenant_ids_()
  {}
  virtual ~ObGetRecycleSchemaVersionsArg()
  {}
  bool is_valid() const;
  void reset();
  int assign(const ObGetRecycleSchemaVersionsArg& other);

public:
  common::ObSArray<uint64_t> tenant_ids_;
  TO_STRING_KV(K_(tenant_ids));
};

struct ObGetRecycleSchemaVersionsResult {
  OB_UNIS_VERSION(1);

public:
  ObGetRecycleSchemaVersionsResult() : recycle_schema_versions_()
  {}
  virtual ~ObGetRecycleSchemaVersionsResult()
  {}
  bool is_valid() const;
  void reset();
  int assign(const ObGetRecycleSchemaVersionsResult& other);

public:
  common::ObSArray<share::TenantIdAndSchemaVersion> recycle_schema_versions_;
  TO_STRING_KV(K_(recycle_schema_versions));
};

struct ObGetClusterInfoArg {
  OB_UNIS_VERSION(1);

public:
  ObGetClusterInfoArg()
      : need_check_sync_(false),
        max_primary_schema_version_(common::OB_INVALID_VERSION),
        primary_schema_versions_(),
        cluster_version_(common::OB_INVALID_ID),
        standby_became_primary_scn_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(need_check_sync), K_(max_primary_schema_version), K_(primary_schema_versions), K_(cluster_version),
      K_(standby_became_primary_scn));

public:
  bool need_check_sync_;
  int64_t max_primary_schema_version_;              // The largest schema version of the main library system tenant
  ObTenantSchemaVersions primary_schema_versions_;  // The schema version of all tenants of the main library, used to
                                                    // check user_tenant_schema_sync
  uint64_t cluster_version_;
  int64_t standby_became_primary_scn_;
};

struct ObCheckAddStandbyArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckAddStandbyArg() : cluster_version_(common::OB_INVALID_ID)
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != cluster_version_;
  }
  void reset()
  {
    cluster_version_ = common::OB_INVALID_ID;
  }
  TO_STRING_KV(K_(cluster_version));

public:
  uint64_t cluster_version_;
};

class ObHaGtsPingRequest {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsPingRequest()
      : gts_id_(common::OB_INVALID_ID),
        req_id_(common::OB_INVALID_ID),
        epoch_id_(common::OB_INVALID_TIMESTAMP),
        request_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObHaGtsPingRequest()
  {}

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
    return common::is_valid_gts_id(gts_id_) && common::OB_INVALID_ID != req_id_ && epoch_id_ > 0 && request_ts_ > 0;
  }
  void set(const uint64_t gts_id, const uint64_t req_id, const int64_t epoch_id, const int64_t request_ts)
  {
    gts_id_ = gts_id;
    req_id_ = req_id;
    epoch_id_ = epoch_id;
    request_ts_ = request_ts;
  }
  uint64_t get_gts_id() const
  {
    return gts_id_;
  }
  uint64_t get_req_id() const
  {
    return req_id_;
  }
  int64_t get_epoch_id() const
  {
    return epoch_id_;
  }
  int64_t get_request_ts() const
  {
    return request_ts_;
  }
  TO_STRING_KV(K(gts_id_), K(req_id_), K(epoch_id_), K(request_ts_));

private:
  uint64_t gts_id_;
  uint64_t req_id_;
  int64_t epoch_id_;
  int64_t request_ts_;
};

class ObHaGtsPingResponse {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsPingResponse()
      : gts_id_(common::OB_INVALID_ID),
        req_id_(common::OB_INVALID_ID),
        epoch_id_(common::OB_INVALID_TIMESTAMP),
        response_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObHaGtsPingResponse()
  {}

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
    return common::is_valid_gts_id(gts_id_) && common::OB_INVALID_ID != req_id_ && epoch_id_ > 0 && response_ts_ > 0;
  }
  void set(const uint64_t gts_id, const uint64_t req_id, const int64_t epoch_id, const int64_t response_ts)
  {
    gts_id_ = gts_id;
    req_id_ = req_id;
    epoch_id_ = epoch_id;
    response_ts_ = response_ts;
  }
  uint64_t get_gts_id() const
  {
    return gts_id_;
  }
  uint64_t get_req_id() const
  {
    return req_id_;
  }
  int64_t get_epoch_id() const
  {
    return epoch_id_;
  }
  int64_t get_response_ts() const
  {
    return response_ts_;
  }
  TO_STRING_KV(K(gts_id_), K(req_id_), K(epoch_id_), K(response_ts_));

private:
  uint64_t gts_id_;
  uint64_t req_id_;
  int64_t epoch_id_;
  int64_t response_ts_;
};

class ObHaGtsGetRequest {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsGetRequest()
      : gts_id_(common::OB_INVALID_ID),
        self_addr_(),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        srr_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObHaGtsGetRequest()
  {}

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
    return common::is_valid_gts_id(gts_id_) && self_addr_.is_valid() && srr_.is_valid();
  }
  void set(const uint64_t gts_id, const common::ObAddr& self_addr, const uint64_t tenant_id,
      const transaction::MonotonicTs srr)
  {
    gts_id_ = gts_id;
    self_addr_ = self_addr;
    tenant_id_ = tenant_id;
    srr_ = srr;
  }
  uint64_t get_gts_id() const
  {
    return gts_id_;
  }
  const common::ObAddr& get_self_addr() const
  {
    return self_addr_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  transaction::MonotonicTs get_srr() const
  {
    return srr_;
  }
  TO_STRING_KV(K(gts_id_), K(self_addr_), K(tenant_id_), K(srr_));

private:
  uint64_t gts_id_;
  common::ObAddr self_addr_;
  // TODO: To be deleted
  uint64_t tenant_id_;
  transaction::MonotonicTs srr_;
};

class ObHaGtsGetResponse {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsGetResponse()
      : gts_id_(common::OB_INVALID_ID),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        srr_(common::OB_INVALID_TIMESTAMP),
        gts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObHaGtsGetResponse()
  {}

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
    return common::is_valid_gts_id(gts_id_) && srr_.is_valid() && gts_ > 0;
  }
  void set(const uint64_t gts_id, const uint64_t tenant_id, const transaction::MonotonicTs srr, const int64_t gts)
  {
    gts_id_ = gts_id;
    tenant_id_ = tenant_id;
    srr_ = srr;
    gts_ = gts;
  }
  int64_t get_gts_id() const
  {
    return gts_id_;
  }
  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  transaction::MonotonicTs get_srr() const
  {
    return srr_;
  }
  int64_t get_gts() const
  {
    return gts_;
  }
  TO_STRING_KV(K(gts_id_), K(tenant_id_), K(srr_), K(gts_));

private:
  uint64_t gts_id_;
  // TODO: To be deleted
  uint64_t tenant_id_;
  transaction::MonotonicTs srr_;
  int64_t gts_;
};

class ObHaGtsHeartbeat {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsHeartbeat() : gts_id_(common::OB_INVALID_ID), addr_()
  {}
  ~ObHaGtsHeartbeat()
  {}

public:
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && addr_.is_valid();
  }
  void set(const uint64_t gts_id, const common::ObAddr& addr)
  {
    gts_id_ = gts_id;
    addr_ = addr;
  }
  uint64_t get_gts_id() const
  {
    return gts_id_;
  }
  const common::ObAddr& get_addr() const
  {
    return addr_;
  }
  TO_STRING_KV(K(gts_id_), K(addr_));

private:
  uint64_t gts_id_;
  common::ObAddr addr_;
};

class ObHaGtsUpdateMetaRequest {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsUpdateMetaRequest()
      : gts_id_(common::OB_INVALID_ID),
        epoch_id_(common::OB_INVALID_TIMESTAMP),
        member_list_(),
        local_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObHaGtsUpdateMetaRequest()
  {}

public:
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && epoch_id_ > 0 && member_list_.get_member_number() > 0 &&
           member_list_.get_member_number() <= 3 && local_ts_ > 0;
  }
  void set(
      const uint64_t gts_id, const int64_t epoch_id, const common::ObMemberList& member_list, const int64_t local_ts)
  {
    gts_id_ = gts_id;
    epoch_id_ = epoch_id;
    member_list_ = member_list;
    local_ts_ = local_ts;
  }
  uint64_t get_gts_id() const
  {
    return gts_id_;
  }
  int64_t get_epoch_id() const
  {
    return epoch_id_;
  }
  const common::ObMemberList& get_member_list() const
  {
    return member_list_;
  }
  int64_t get_local_ts() const
  {
    return local_ts_;
  }
  TO_STRING_KV(K(gts_id_), K(epoch_id_), K(member_list_), K(local_ts_));

private:
  uint64_t gts_id_;
  int64_t epoch_id_;
  common::ObMemberList member_list_;
  int64_t local_ts_;
};

class ObHaGtsUpdateMetaResponse {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsUpdateMetaResponse() : local_ts_(common::OB_INVALID_TIMESTAMP)
  {}
  ~ObHaGtsUpdateMetaResponse()
  {}

public:
  bool is_valid() const
  {
    return local_ts_ > 0;
  }
  void set(const int64_t local_ts)
  {
    local_ts_ = local_ts;
  }
  int64_t get_local_ts() const
  {
    return local_ts_;
  }
  TO_STRING_KV(K(local_ts_));

private:
  int64_t local_ts_;
};

class ObHaGtsChangeMemberRequest {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsChangeMemberRequest() : gts_id_(common::OB_INVALID_ID), offline_replica_()
  {}
  ~ObHaGtsChangeMemberRequest()
  {}

public:
  bool is_valid() const
  {
    return common::is_valid_gts_id(gts_id_) && offline_replica_.is_valid();
  }
  void set(const uint64_t gts_id, const common::ObAddr& offline_replica)
  {
    gts_id_ = gts_id;
    offline_replica_ = offline_replica;
  }
  uint64_t get_gts_id() const
  {
    return gts_id_;
  }
  const common::ObAddr& get_offline_replica() const
  {
    return offline_replica_;
  }
  TO_STRING_KV(K(gts_id_), K(offline_replica_));

private:
  uint64_t gts_id_;
  common::ObAddr offline_replica_;
};

class ObHaGtsChangeMemberResponse {
  OB_UNIS_VERSION(1);

public:
  ObHaGtsChangeMemberResponse() : ret_value_(common::OB_SUCCESS)
  {}
  ~ObHaGtsChangeMemberResponse()
  {}

public:
  void set(const int ret_value)
  {
    ret_value_ = ret_value;
  }
  int get_ret_value() const
  {
    return ret_value_;
  }
  TO_STRING_KV(K(ret_value_));

private:
  int ret_value_;
};

struct ObClusterInfoArg {
  OB_UNIS_VERSION(1);

public:
  ObClusterInfoArg()
      : cluster_info_(), server_status_(share::OBSERVER_INVALID_STATUS), sync_cluster_ids_(), redo_options_()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(cluster_info), K_(server_status), K_(sync_cluster_ids), K_(redo_options));
  int assign(const ObClusterInfoArg& other);

public:
  share::ObClusterInfo cluster_info_;
  share::ServerServiceStatus server_status_;
  common::ObSEArray<int64_t, 1> sync_cluster_ids_;
  share::ObRedoTransportOption redo_options_;
};

struct ObSchemaSnapshotArg {
  OB_UNIS_VERSION(1);

public:
  ObSchemaSnapshotArg() : schema_version_(0), frozen_version_(0)
  {}
  bool is_valid() const
  {
    return schema_version_ >= 0 && frozen_version_ >= 0;
  }
  void reset()
  {
    schema_version_ = 0;
  }
  TO_STRING_KV(K_(schema_version), K_(frozen_version));

public:
  int64_t schema_version_;
  int64_t frozen_version_;
};

// The maximum size of the RPC packet is 64M, and it can store about 2W schemas. The streaming interface is not used for
// the time being
struct ObSchemaSnapshotRes {
  OB_UNIS_VERSION(1);

public:
  ObSchemaSnapshotRes();
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(schema_version), K_(frozen_version), "table_count", table_schemas_.count(), "tenant_count",
      tenant_schemas_.count(), "user_count", user_infos_.count(), "freeze_schema", freeze_schemas_,
      K_(tenant_flashback_scn), K_(failover_timestamp), K_(frozen_status), K_(cluster_name));
  int assign(const ObSchemaSnapshotRes& res);

public:
  int64_t schema_version_;
  int64_t frozen_version_;
  common::ObSArray<share::schema::ObTableSchema> table_schemas_;
  common::ObSArray<share::schema::ObTenantSchema> tenant_schemas_;
  common::ObSArray<share::schema::ObUserInfo> user_infos_;
  common::ObSArray<share::schema::ObDBPriv> db_privs_;
  common::ObSArray<share::schema::ObTablePriv> table_privs_;
  common::ObSArray<share::TenantIdAndSchemaVersion> freeze_schemas_;  // Record the schema_version corresponding to the
                                                                      // frozen_version_ + 2 version;
  int64_t failover_timestamp_;
  common::ObSArray<share::ObTenantFlashbackSCN> tenant_flashback_scn_;
  common::ObSArray<storage::ObFrozenStatus> frozen_status_;
  bool sys_schema_changed_;
  // Used to verify that the primary and standby cluster names are consistent
  common::ObFixedLengthString<common::OB_MAX_CONFIG_VALUE_LEN> cluster_name_;
};

struct ObRegistClusterArg {
  OB_UNIS_VERSION(1);

public:
  ObRegistClusterArg() : cluster_name_(), cluster_id_(common::OB_INVALID_ID), pre_regist_(false), cluster_addr_()
  {}
  bool is_valid() const
  {
    return cluster_id_ > 0 && !cluster_name_.empty();
  }

public:
  common::ObString cluster_name_;
  uint64_t cluster_id_;
  bool pre_regist_;
  share::ObClusterAddr cluster_addr_;
  TO_STRING_KV(K_(cluster_id), K_(cluster_name), K_(pre_regist), K_(cluster_addr));
};

struct ObAlterTableResArg {
  OB_UNIS_VERSION(1);

public:
  ObAlterTableResArg()
      : schema_type_(share::schema::OB_MAX_SCHEMA),
        schema_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION)
  {}
  ObAlterTableResArg(
      const share::schema::ObSchemaType schema_type, const uint64_t schema_id, const int64_t schema_version)
      : schema_type_(schema_type), schema_id_(schema_id), schema_version_(schema_version)
  {}
  void reset();

public:
  TO_STRING_KV(K_(schema_type), K_(schema_id), K_(schema_version));
  share::schema::ObSchemaType schema_type_;
  uint64_t schema_id_;
  int64_t schema_version_;
};

struct ObAlterTableRes {
  OB_UNIS_VERSION(1);

public:
  ObAlterTableRes()
      : index_table_id_(common::OB_INVALID_ID),
        constriant_id_(common::OB_INVALID_ID),
        schema_version_(common::OB_INVALID_VERSION),
        res_arg_array_()
  {}
  void reset();

public:
  TO_STRING_KV(K_(index_table_id), K_(constriant_id), K_(schema_version), K_(res_arg_array));
  uint64_t index_table_id_;
  uint64_t constriant_id_;
  int64_t schema_version_;
  common::ObSArray<ObAlterTableResArg> res_arg_array_;
};

struct ObRegistClusterRes {
  OB_UNIS_VERSION(1);

public:
  ObRegistClusterRes() : cluster_idx_(common::OB_INVALID_INDEX)
  {}
  bool is_valid() const
  {
    return cluster_idx_ >= 0;
  }
  int set_login_name(const char* login_name)
  {
    return login_name_.assign(login_name);
  }
  int set_login_passwd(const char* login_passwd)
  {
    return login_passwd_.assign(login_passwd);
  }
  int set_primary_cluster(const share::ObClusterAddr& primary_cluster);
  void reset();

public:
  TO_STRING_KV(K_(cluster_idx), K_(login_name));
  int64_t cluster_idx_;
  share::ObClusterInfo::UserNameString login_name_;
  share::ObClusterInfo::PassWdString login_passwd_;
  share::ObClusterAddr primary_cluster_;
};

struct ObStandbyHeartBeatRes {
  OB_UNIS_VERSION(1);

public:
  ObStandbyHeartBeatRes()
      : primary_addr_(),
        standby_addr_(),
        primary_schema_info_(),
        protection_mode_(common::MAXIMUM_PROTECTION_MODE),
        tenant_schema_vers_()
  {}
  ~ObStandbyHeartBeatRes()
  {}
  void set_refreshed_schema_version(const int64_t refreshed_schema_version)
  {
    primary_schema_info_.primary_broadcasted_schema_version_ = refreshed_schema_version;
  }
  void set_next_schema_version(const int64_t next_schema_version)
  {
    primary_schema_info_.primary_next_schema_version_ = next_schema_version;
  }
  int set_primary_addr(const share::ObClusterAddr& primary_addr);
  int set_standby_addr(const share::ObClusterAddr& standby_addr);
  void set_protection_mode(common::ObProtectionMode mode)
  {
    protection_mode_ = mode;
  }

  void reset()
  {
    primary_addr_.reset();
    standby_addr_.reset();
    primary_schema_info_.reset();
    tenant_schema_vers_.reset();
    protection_mode_ = common::MAXIMUM_PROTECTION_MODE;
  }

public:
  TO_STRING_KV(K_(primary_addr), K_(standby_addr), K_(primary_schema_info), K_(protection_mode));
  share::ObClusterAddr primary_addr_;
  share::ObClusterAddr standby_addr_;
  share::schema::ObPrimarySchemaInfo primary_schema_info_;
  common::ObProtectionMode protection_mode_;
  ObTenantSchemaVersions tenant_schema_vers_;
};

struct ObGetSwitchoverStatusRes {
  OB_UNIS_VERSION(1);

public:
  ObGetSwitchoverStatusRes() : switchover_status_(share::ObClusterInfo::I_INVALID), switchover_info_()
  {}
  ~ObGetSwitchoverStatusRes()
  {}

public:
  TO_STRING_KV(K_(switchover_status), K_(switchover_info));
  share::ObClusterInfo::InMemorySwitchOverStatus switchover_status_;
  share::ObClusterSwitchoverInfoWrap switchover_info_;
};

struct ObGetTenantSchemaVersionArg {
  OB_UNIS_VERSION(1);

public:
  ObGetTenantSchemaVersionArg() : tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_;
  }

  TO_STRING_KV(K_(tenant_id));
  uint64_t tenant_id_;
};

struct ObGetTenantSchemaVersionResult {
  OB_UNIS_VERSION(1);

public:
  ObGetTenantSchemaVersionResult() : schema_version_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const
  {
    return schema_version_ > 0;
  }

  TO_STRING_KV(K_(schema_version));
  int64_t schema_version_;
};

struct ObFinishReplayArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObFinishReplayArg() : schema_version_(common::OB_INVALID_VERSION)
  {}
  bool is_valid() const
  {
    return schema_version_ > 0;
  }
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }

  TO_STRING_KV(K_(schema_version));
  int64_t schema_version_;
};

struct ObTenantMemoryArg {
  OB_UNIS_VERSION(1);

public:
  ObTenantMemoryArg() : tenant_id_(0), memory_size_(0), refresh_interval_(0)
  {}
  bool is_valid() const
  {
    return tenant_id_ > 0 && memory_size_ > 0 && refresh_interval_ >= 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(memory_size));
  int64_t tenant_id_;
  int64_t memory_size_;
  int64_t refresh_interval_;
};

struct ObProfileDDLArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObProfileDDLArg() : ObDDLArg(), schema_(), ddl_type_(), is_cascade_(false)
  {}
  virtual bool is_allow_when_upgrade() const
  {
    return share::schema::OB_DDL_DROP_PROFILE == ddl_type_;
  }

  share::schema::ObProfileSchema schema_;
  share::schema::ObSchemaOperationType ddl_type_;
  bool is_cascade_;
  TO_STRING_KV(K_(schema), K_(ddl_type), K_(is_cascade));
};

struct ObCheckServerEmptyArg {
  OB_UNIS_VERSION(1);

public:
  enum Mode { BOOTSTRAP, ADD_SERVER };

  ObCheckServerEmptyArg() : mode_(BOOTSTRAP)
  {}
  TO_STRING_KV(K_(mode));
  Mode mode_;
};

struct ObForceDropSchemaArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObForceDropSchemaArg()
      : ObDDLArg(),
        recycle_schema_version_(common::OB_INVALID_VERSION),
        schema_id_(common::OB_INVALID_ID),
        partition_ids_(),
        type_(share::schema::OB_MAX_SCHEMA),
        subpartition_ids_()
  {}
  virtual ~ObForceDropSchemaArg()
  {}

  bool is_valid() const;
  int assign(const ObForceDropSchemaArg& other);
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  TO_STRING_KV(K_(exec_tenant_id), K_(schema_id), K_(type), "partition_cnt", partition_ids_.count(), "subpartition_cnt",
      subpartition_ids_.count());
  int64_t recycle_schema_version_;           // The backed up schema_version is used for verification
  uint64_t schema_id_;                       // table_id/tablegroup_id
  common::ObSArray<int64_t> partition_ids_;  // Non-empty means to delete the partition (including the delayed deletion
                                             // object of the second-level partition table and the first-level
                                             // partition)
  share::schema::ObSchemaType type_;
  common::ObSArray<int64_t> subpartition_ids_;  // Non-empty means delete subpartition (only non-templated secondary
                                                // partition table)
};

struct ObArchiveLogArg {
  OB_UNIS_VERSION(1);

public:
  ObArchiveLogArg() : enable_(true)
  {}
  TO_STRING_KV(K_(enable));
  bool enable_;
};

struct ObBackupDatabaseArg {
  OB_UNIS_VERSION(1);

public:
  ObBackupDatabaseArg();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(is_incremental), K_(passwd), K_(encryption_mode));
  uint64_t tenant_id_;
  bool is_incremental_;
  share::ObBackupEncryptionMode::EncryptionMode encryption_mode_;
  common::ObFixedLengthString<common::OB_MAX_PASSWORD_LENGTH> passwd_;
};

struct ObTableTTLArg {
  OB_UNIS_VERSION(1);
public:
  ObTableTTLArg();
  int assign(const ObTableTTLArg& other);
  TO_STRING_KV(K_(cmd_code));
  int64_t cmd_code_;
};

struct ObBackupManageArg {
  OB_UNIS_VERSION(1);

public:
  enum Type {
    CANCEL_BACKUP = 0,
    SUSPEND_BACKUP = 1,
    RESUME_BACKUP = 2,
    // DELETE_EXPIRED_BACKUP = 3,
    DELETE_BACKUP = 4,
    VALIDATE_DATABASE = 5,
    VALIDATE_BACKUPSET = 6,
    CANCEL_VALIDATE = 7,
    DELETE_OBSOLETE_BACKUP = 8,
    CANCEL_BACKUP_BACKUPSET = 9,
    CANCEL_DELETE_BACKUP = 10,
    DELETE_BACKUPPIECE = 11,
    DELETE_OBSOLETE_BACKUP_BACKUP = 12,
    CANCEL_BACKUP_BACKUPPIECE = 13,
    DELETE_BACKUPROUND = 14,
    CANCEL_ALL_BACKUP_FORCE = 15,
    MAX_TYPE
  };
  ObBackupManageArg() : tenant_id_(OB_INVALID_TENANT_ID), type_(MAX_TYPE), value_(0), copy_id_(0)
  {}
  TO_STRING_KV(K_(type), K_(value), K_(copy_id));
  uint64_t tenant_id_;
  Type type_;
  int64_t value_;
  int64_t copy_id_;
};

struct ObBackupBackupsetArg {
  OB_UNIS_VERSION(1);

public:
  ObBackupBackupsetArg();
  bool is_valid() const;
  int assign(const ObBackupBackupsetArg& o);
  TO_STRING_KV(K_(tenant_id), K_(backup_set_id), K_(tenant_name), K_(backup_backup_dest), K_(max_backup_times));
  uint64_t tenant_id_;
  int64_t backup_set_id_;
  common::ObString tenant_name_;
  char backup_backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH];
  int64_t max_backup_times_;
};

struct ObBackupArchiveLogArg {
  OB_UNIS_VERSION(1);

public:
  ObBackupArchiveLogArg() : enable_(false)
  {}
  int assign(const ObBackupArchiveLogArg& o);
  TO_STRING_KV(K_(enable));
  bool enable_;
};

struct ObBackupBackupPieceArg {
  OB_UNIS_VERSION(1);

public:
  ObBackupBackupPieceArg();
  ~ObBackupBackupPieceArg()
  {}
  bool is_valid() const
  {
    return OB_INVALID_ID != tenant_id_ && piece_id_ != -1;
  }
  int assign(const ObBackupBackupPieceArg& o);
  TO_STRING_KV(K_(tenant_id), K_(piece_id), K_(max_backup_times), K_(backup_all), K_(backup_backup_dest));

  uint64_t tenant_id_;
  int64_t piece_id_;
  common::ObString tenant_name_;
  char backup_backup_dest_[share::OB_MAX_BACKUP_DEST_LENGTH];
  int64_t max_backup_times_;
  bool backup_all_;
  bool with_active_piece_;
};

struct ObCheckStandbyCanAccessArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckStandbyCanAccessArg()
      : failover_epoch_(common::OB_INVALID_VERSION),
        last_merged_version_(common::OB_INVALID_VERSION),
        cluster_status_(),
        tenant_flashback_scn_()
  {}
  ~ObCheckStandbyCanAccessArg()
  {}
  TO_STRING_KV(K_(failover_epoch), K_(last_merged_version), K_(cluster_status), K_(tenant_flashback_scn));
  bool is_valid() const
  {
    return common::OB_INVALID_VERSION != failover_epoch_ && common::OB_INVALID_VERSION != last_merged_version_ &&
           0 < cluster_status_.count() && 0 < tenant_flashback_scn_.count();
  }
  int64_t failover_epoch_;
  int64_t last_merged_version_;
  common::ObSArray<share::ObClusterAddr> cluster_status_;
  common::ObSArray<share::ObTenantFlashbackSCN> tenant_flashback_scn_;
};

struct ObPhysicalFlashbackResultArg {
  OB_UNIS_VERSION(1);

public:
  ObPhysicalFlashbackResultArg()
      : min_version_(common::OB_INVALID_VERSION), max_version_(common::OB_INVALID_VERSION), enable_result_(false)
  {}
  ~ObPhysicalFlashbackResultArg()
  {}
  TO_STRING_KV(K_(min_version), K_(max_version), K_(enable_result));
  int64_t min_version_;
  int64_t max_version_;
  bool enable_result_;
};

struct ObCheckPhysicalFlashbackArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckPhysicalFlashbackArg()
      : merged_version_(common::OB_INVALID_VERSION), flashback_scn_(common::OB_INVALID_VERSION)
  {}
  ~ObCheckPhysicalFlashbackArg()
  {}
  TO_STRING_KV(K_(merged_version), K_(flashback_scn));

public:
  int64_t merged_version_;
  int64_t flashback_scn_;
};

struct CheckLeaderRpcIndex {
  OB_UNIS_VERSION(1);

public:
  int64_t switchover_timestamp_;  // Switch logo
  int64_t epoch_;  //(switchover_timestamp, epoch) uniquely identifies a statistical information during a switching
                   // process
  int64_t tenant_id_;
  int64_t ml_pk_index_;            // Position the coordinates of pkey
  int64_t pkey_info_start_index_;  // Position the coordinates of pkey
  CheckLeaderRpcIndex()
      : switchover_timestamp_(0),
        epoch_(0),
        tenant_id_(common::OB_INVALID_TENANT_ID),
        ml_pk_index_(0),
        pkey_info_start_index_(0){};
  ~CheckLeaderRpcIndex()
  {
    reset();
  }
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(switchover_timestamp), K_(epoch), K_(tenant_id), K_(ml_pk_index), K_(pkey_info_start_index));
};

struct ObBatchWriteCutdataClogArg {
  OB_UNIS_VERSION(1);

public:
  ObBatchWriteCutdataClogArg()
  {
    reset();
  }
  ~ObBatchWriteCutdataClogArg()
  {}

  void build_arg(const int64_t schema_version, const int64_t switchover_timestamp, const int64_t flashback_ts,
      const int64_t query_end_time)
  {
    schema_version_ = schema_version;
    switchover_timestamp_ = switchover_timestamp;
    flashback_ts_ = flashback_ts;
    query_end_time_ = query_end_time;
  }

  void set_switchover_timestamp(const int64_t switchover_timestamp)
  {
    switchover_timestamp_ = switchover_timestamp;
  }
  void set_flashback_ts(const int64_t flashback_ts)
  {
    flashback_ts_ = flashback_ts;
  }
  void set_schema_version(const int64_t schema_version)
  {
    schema_version_ = schema_version;
  }
  int64_t get_schema_version() const
  {
    return schema_version_;
  }
  void set_query_end_time(const int64_t query_end_time)
  {
    query_end_time_ = query_end_time;
  }
  int64_t get_query_end_time() const
  {
    return query_end_time_;
  }
  bool reach_concurrency_limit() const
  {
    return pkeys_.count() >= MAX_COUNT;
  }
  int add_pkey(const common::ObPartitionKey& pkey);
  const ObPkeyArray& get_pkey_array() const
  {
    return pkeys_;
  }
  int64_t get_flashback_ts() const
  {
    return flashback_ts_;
  }
  int64_t get_switchover_timestamp() const
  {
    return switchover_timestamp_;
  }
  bool is_valid() const;
  bool has_task() const
  {
    return pkeys_.count() > 0;
  }
  void reset();
  void set_trace_id(const common::ObCurTraceId::TraceId& trace_id)
  {
    trace_id_ = trace_id;
  }
  TO_STRING_KV(
      K_(switchover_timestamp), K_(pkeys), K_(flashback_ts), K_(schema_version), K_(trace_id), K_(query_end_time));

public:
  static const int64_t MAX_COUNT = 1024;
  int64_t switchover_timestamp_;
  ObPkeyArray pkeys_;
  int64_t flashback_ts_;
  int64_t schema_version_;
  CheckLeaderRpcIndex index_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t query_end_time_;
};

struct ObBatchCheckLeaderArg {
  OB_UNIS_VERSION(1);

public:
  ObPkeyArray pkeys_;
  CheckLeaderRpcIndex index_;
  common::ObCurTraceId::TraceId trace_id_;  // TODO: set trace to facilitate tracking
  ObBatchCheckLeaderArg()
  {
    reset();
  }
  ~ObBatchCheckLeaderArg()
  {}
  bool is_valid() const;
  void reset();
  bool reach_concurrency_limit() const
  {
    return pkeys_.count() >= MAX_COUNT;
  }
  bool has_task() const
  {
    return pkeys_.count() > 0;
  }
  void set_trace_id(const common::ObCurTraceId::TraceId& trace_id)
  {
    trace_id_ = trace_id;
  }
  TO_STRING_KV(K_(index), K_(pkeys));
};

struct ObBatchCheckRes {
  OB_UNIS_VERSION(1);

public:
  common::ObSArray<bool> results_;  // Corresponding to the above pkeys--, true means there is a master, false means no
                                    // master
  CheckLeaderRpcIndex index_;       // Same value as ObBatchCheckLeaderArg
  ObBatchCheckRes()
  {
    reset();
  }
  ~ObBatchCheckRes()
  {}
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(index), K_(results));
};

struct ObRebuildIndexInRestoreArg {
  OB_UNIS_VERSION(1);

public:
  ObRebuildIndexInRestoreArg() : tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  ~ObRebuildIndexInRestoreArg()
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_;
  }
  void reset()
  {
    tenant_id_ = common::OB_INVALID_TENANT_ID;
  }

public:
  uint64_t tenant_id_;
  TO_STRING_KV(K_(tenant_id));
};

struct ObUpdateTableSchemaVersionArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  enum Action {
    INVALID = 0,
    UPDATE_SYS_ALL_INNER_TABLE,
    UPDATE_SYS_INNER_TABLE,
    UPDATE_SYS_TABLE_IN_TENANT_SPACE,
  };
  ObUpdateTableSchemaVersionArg()
      : tenant_id_(common::OB_INVALID_TENANT_ID),
        table_id_(OB_INVALID_ID),
        schema_version_(share::OB_INVALID_SCHEMA_VERSION),
        action_(Action::INVALID)
  {}
  ~ObUpdateTableSchemaVersionArg()
  {
    reset();
  }
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const;
  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(schema_version));
  void reset();
  void init(const int64_t tenant_id, const int64_t table_id, const int64_t schema_version, const bool is_replay_schema,
      const Action action);
  int assign(const ObUpdateTableSchemaVersionArg &other);

public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t schema_version_;
  Action action_;
};

struct ObRestoreModifySchemaArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  enum TYPE {
    INVALID_TYPE,
    RESET_DATABASE_PRIMARY_ZONE,
    RESET_TABLEGROUP_PRIMARY_ZONE,
    RESET_TABLEGROUP_LOCALITY,
    RESET_TABLEGROUP_PREVIOUS_LOCALITY,
    RESET_TABLE_PRIMARY_ZONE,
    RESET_TABLE_LOCALITY,
    RESET_TABLE_PREVIOUS_LOCALITY,
    MAX_TYPE,
  };

public:
  ObRestoreModifySchemaArg() : type_(INVALID_TYPE), schema_id_(common::OB_INVALID_ID)
  {}
  ~ObRestoreModifySchemaArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(type), K_(schema_id));

public:
  TYPE type_;
  uint64_t schema_id_;
};

struct ObCheckDeploymentModeArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckDeploymentModeArg() : single_zone_deployment_on_(false)
  {}
  TO_STRING_KV(K_(single_zone_deployment_on));
  bool single_zone_deployment_on_;
};

struct ObPreProcessServerArg {
  OB_UNIS_VERSION(1);

public:
  ObPreProcessServerArg() : server_(), rescue_server_()
  {}
  TO_STRING_KV(K_(server));
  bool is_valid() const
  {
    return server_.is_valid() && rescue_server_.is_valid();
  }
  int init(const common::ObAddr& server, const common::ObAddr& rescue_server);

public:
  common::ObAddr server_;
  common::ObAddr rescue_server_;
};

struct ObAdminRollingUpgradeArg {
  OB_UNIS_VERSION(1);

public:
  ObAdminRollingUpgradeArg() : stage_(OB_UPGRADE_STAGE_MAX)
  {}
  ~ObAdminRollingUpgradeArg()
  {}
  bool is_valid() const;
  TO_STRING_KV(K_(stage));

  ObUpgradeStage stage_;
};

struct ObPreProcessServerReplyArg {
  OB_UNIS_VERSION(1);

public:
  ObPreProcessServerReplyArg() : server_(), rescue_server_(), ret_code_(common::OB_SUCCESS)
  {}
  TO_STRING_KV(K_(server), K_(rescue_server), K_(ret_code));
  bool is_valid() const
  {
    return server_.is_valid() && rescue_server_.is_valid();
  }
  int init(const common::ObAddr& server, const common::ObAddr& rescue_server, const int ret_code);

public:
  common::ObAddr server_;
  common::ObAddr rescue_server_;
  int ret_code_;
};

struct ObRsListArg {
  OB_UNIS_VERSION(1);

public:
  ObRsListArg() : rs_list_(), master_rs_()
  {}
  ~ObRsListArg()
  {}
  bool is_valid() const
  {
    return rs_list_.count() > 0 && master_rs_.is_valid();
  }
  TO_STRING_KV(K_(rs_list), K_(master_rs));
  ObServerList rs_list_;
  common::ObAddr master_rs_;
};

struct ObLocationRpcRenewArg {
  OB_UNIS_VERSION(1);

public:
  ObLocationRpcRenewArg() : keys_()
  {}
  ObLocationRpcRenewArg(common::ObIAllocator& allocator);
  ~ObLocationRpcRenewArg()
  {}
  bool is_valid() const
  {
    return keys_.count() > 0;
  }
  TO_STRING_KV(K_(keys));

  common::ObSArray<common::ObPartitionKey> keys_;
};

struct ObLocationRpcRenewResult {
  OB_UNIS_VERSION(1);

public:
  ObLocationRpcRenewResult() : results_()
  {}
  ~ObLocationRpcRenewResult()
  {}
  bool is_valid() const
  {
    return results_.count() > 0;
  }
  TO_STRING_KV(K_(results));

  common::ObSArray<ObMemberListAndLeaderArg> results_;
};

struct ObGetMasterKeyResultArg {
  OB_UNIS_VERSION(1);

public:
  ObGetMasterKeyResultArg() : str_(share::OB_CLOG_ENCRYPT_MASTER_KEY_LEN, 0, buf_)
  {}
  ~ObGetMasterKeyResultArg()
  {}
  bool is_valid() const
  {
    return true;
  }
  TO_STRING_KV(K_(str));
  char buf_[share::OB_CLOG_ENCRYPT_MASTER_KEY_LEN];
  common::ObString str_;
};

struct ObKillPartTransCtxArg {
  OB_UNIS_VERSION(1);

public:
  ObKillPartTransCtxArg()
  {}
  ~ObKillPartTransCtxArg()
  {}
  bool is_valid() const
  {
    return partition_key_.is_valid();
  }
  TO_STRING_KV(K_(partition_key), K_(trans_id));
  common::ObPartitionKey partition_key_;
  transaction::ObTransID trans_id_;
};

struct ObPhysicalRestoreResult {
  OB_UNIS_VERSION(1);

public:
  ObPhysicalRestoreResult();
  virtual ~ObPhysicalRestoreResult()
  {}
  bool is_valid() const;
  int assign(const ObPhysicalRestoreResult& other);
  TO_STRING_KV(K_(job_id), K_(return_ret), K_(mod), K_(tenant_id), K_(trace_id), K_(addr));

  int64_t job_id_;
  int32_t return_ret_;
  share::PhysicalRestoreMod mod_;
  uint64_t tenant_id_;
  common::ObCurTraceId::TraceId trace_id_;
  common::ObAddr addr_;
};

struct ObExecuteRangePartSplitArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObExecuteRangePartSplitArg() : ObDDLArg(), partition_key_(), rowkey_()
  {}
  virtual ~ObExecuteRangePartSplitArg()
  {}
  bool is_valid() const;
  virtual bool is_allow_when_upgrade() const
  {
    return true;
  }
  virtual int assign(const ObExecuteRangePartSplitArg& other);

  common::ObPartitionKey partition_key_;
  common::ObRowkey rowkey_;

  TO_STRING_KV(K(partition_key_), K(rowkey_));
};

struct ObRefreshTimezoneArg {
  OB_UNIS_VERSION(1);

public:
  ObRefreshTimezoneArg() : tenant_id_(common::OB_INVALID_TENANT_ID)
  {}
  ObRefreshTimezoneArg(uint64_t tenant_id) : tenant_id_(tenant_id)
  {}
  ~ObRefreshTimezoneArg()
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_;
  }
  TO_STRING_KV(K_(tenant_id));
  uint64_t tenant_id_;
};

struct ObCreateRestorePointArg {
  OB_UNIS_VERSION(1);

public:
  ObCreateRestorePointArg() : tenant_id_(0), name_()
  {}
  int assign(const ObCreateRestorePointArg& arg)
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

struct ObDropRestorePointArg {
  OB_UNIS_VERSION(1);

public:
  ObDropRestorePointArg() : tenant_id_(0), name_()
  {}
  int assign(const ObDropRestorePointArg& arg)
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

struct ObCheckBuildIndexTaskExistArg {
  OB_UNIS_VERSION(1);

public:
  ObCheckBuildIndexTaskExistArg() : tenant_id_(0), task_id_(), scheduler_id_(0)
  {}

  int assign(const ObCheckBuildIndexTaskExistArg& arg)
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

struct ObPartitionBroadcastArg {
  OB_UNIS_VERSION(1);

public:
  ObPartitionBroadcastArg() : keys_()
  {}
  ~ObPartitionBroadcastArg()
  {}
  bool is_valid() const;
  int assign(const ObPartitionBroadcastArg& other);
  TO_STRING_KV(K_(keys));

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBroadcastArg);

public:
  common::ObSEArray<share::ObPartitionBroadcastTask, common::UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM> keys_;
};

struct ObPartitionBroadcastResult {
  OB_UNIS_VERSION(1);

public:
  ObPartitionBroadcastResult() : ret_(common::OB_SUCCESS)
  {}
  ~ObPartitionBroadcastResult()
  {}
  bool is_valid() const;
  int assign(const ObPartitionBroadcastResult& other);
  TO_STRING_KV(K_(ret));

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionBroadcastResult);

public:
  int ret_;
};

struct ObSubmitBuildIndexTaskArg : public ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObSubmitBuildIndexTaskArg() : ObDDLArg(), index_tid_(0)
  {}
  ~ObSubmitBuildIndexTaskArg()
  {}
  bool is_valid() const
  {
    return index_tid_ > 0;
  }
  virtual int assign(const ObSubmitBuildIndexTaskArg &other);
  TO_STRING_KV(K_(index_tid));

private:
  DISALLOW_COPY_AND_ASSIGN(ObSubmitBuildIndexTaskArg);

public:
  uint64_t index_tid_;
};

struct ObFetchSstableSizeArg final {
  OB_UNIS_VERSION(1);

public:
  ObFetchSstableSizeArg() : pkey_(), index_id_(-1)
  {}
  ~ObFetchSstableSizeArg()
  {}
  bool is_valid() const
  {
    return pkey_.is_valid() && index_id_ > 0;
  }
  int assign(const ObFetchSstableSizeArg &other);
  TO_STRING_KV(K_(pkey), K_(index_id));

public:
  common::ObPartitionKey pkey_;
  int64_t index_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFetchSstableSizeArg);
};

struct ObFetchSstableSizeRes final {
  OB_UNIS_VERSION(1);

public:
  ObFetchSstableSizeRes() : size_(0)
  {}
  ~ObFetchSstableSizeRes()
  {}
  int assign(const ObFetchSstableSizeRes &other);
  TO_STRING_KV(K_(size));

public:
  int64_t size_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFetchSstableSizeRes);
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
    return cmd_code_ != -1 && OB_INVALID_ID != task_id_ && trigger_type_ != -1 && tenant_id_ != OB_INVALID_ID; 
  }
  int assign(const ObTTLRequestArg &other);
  TO_STRING_KV(K_(cmd_code), K_(trigger_type), K_(task_id), K_(tenant_id));
public:
  int32_t cmd_code_; // enum TTLCmdType
  int32_t trigger_type_; // system or user 
  int64_t task_id_;  // task id 
  uint64_t tenant_id_; // tenand_id array
};

struct ObTTLResult final
{
  OB_UNIS_VERSION(1);
public:
  ObTTLResult()
    : ret_code_(-1)
  {}
  ~ObTTLResult() = default;
  bool is_valid() const { return OB_INVALID_ID != ret_code_; }
  int assign(const ObTTLResult &other) { 
    ret_code_ = other.ret_code_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(ret_code));
public:
  int32_t ret_code_; // SUCCESS or error code
};

struct ObTTLResponseArg {
  OB_UNIS_VERSION(1);

public:
  ObTTLResponseArg();
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(server_addr), K_(task_status));
public:
  uint64_t tenant_id_;
  int64_t task_id_;
  ObAddr server_addr_;
  uint8_t task_status_;
};


}  // end namespace obrpc
}  // end namespace oceanbase
#endif
