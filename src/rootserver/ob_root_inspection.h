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

#ifndef OCEANBASE_ROOTSERVER_OB_ROOT_INSPECTION_H_
#define OCEANBASE_ROOTSERVER_OB_ROOT_INSPECTION_H_

#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/thread/ob_work_queue.h"
#include "share/backup/ob_backup_info_mgr.h"
#include "share/ob_virtual_table_projector.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}

namespace share {
class ObPartitionTableOperator;
class ObRemoteSqlProxy;
namespace schema {
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
}  // namespace schema
}  // namespace share

namespace rootserver {
class ObZoneManager;
class ObRootService;
class ObStandbyClusterSchemaProcessor;
class ObFetchPrimaryDDLOperator;
class ObServerManager;

// Interface of all the inspection task
class ObInspectionTask {
public:
  ObInspectionTask()
  {}
  virtual ~ObInspectionTask()
  {}
  // do the inspection task
  virtual int inspect(bool& passed, const char*& warning_info) = 0;
  virtual const char* get_task_name() const = 0;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObInspectionTask);
};

class ObTenantChecker : public ObInspectionTask {
public:
  ObTenantChecker(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy,
      obrpc::ObCommonRpcProxy& rpc_proxy)
      : schema_service_(&schema_service), sql_proxy_(&sql_proxy), rpc_proxy_(rpc_proxy)
  {}
  virtual ~ObTenantChecker()
  {}

  virtual int inspect(bool& passed, const char*& warning_info) override;
  virtual const char* get_task_name() const
  {
    return "tenant_checker";
  };

private:
  int alter_tenant_primary_zone();
  int check_create_tenant_end();

private:
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
  obrpc::ObCommonRpcProxy& rpc_proxy_;
};

class ObTableGroupChecker : public ObInspectionTask {
public:
  explicit ObTableGroupChecker(share::schema::ObMultiVersionSchemaService& schema_service);
  virtual ~ObTableGroupChecker();
  int init();
  virtual int inspect(bool& passed, const char*& warning_info) override;
  virtual const char* get_task_name() const
  {
    return "tablegroup_checker";
  };

private:
  int check_primary_zone(const share::schema::ObTableSchema& table, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_locality(const share::schema::ObTableSchema& table, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_part_option(const share::schema::ObTableSchema& table, share::schema::ObSchemaGetterGuard& schema_guard);
  int check_if_part_option_equal(
      const share::schema::ObTableSchema& t1, const share::schema::ObTableSchema& t2, bool& is_matched);
  template <typename SCHEMA>
  int check_if_part_option_equal_v2(const share::schema::ObTableSchema& table, const SCHEMA& schema, bool& is_matched);

private:
  static const int TABLEGROUP_BUCKET_NUM = 1024;
  typedef common::hash::ObHashMap<uint64_t, const share::schema::ObTableSchema*, common::hash::NoPthreadDefendMode>
      ObTableGroupCheckInfoMap;
  share::schema::ObMultiVersionSchemaService& schema_service_;
  ObTableGroupCheckInfoMap check_primary_zone_map_;
  ObTableGroupCheckInfoMap check_locality_map_;
  ObTableGroupCheckInfoMap check_part_option_map_;
  common::hash::ObHashSet<uint64_t> primary_zone_not_match_set_;
  common::hash::ObHashSet<uint64_t> locality_not_match_set_;
  common::hash::ObHashSet<uint64_t> part_option_not_match_set_;
  bool is_inited_;
};

class ObDropTenantChecker : public ObInspectionTask {
public:
  explicit ObDropTenantChecker(
      share::schema::ObMultiVersionSchemaService& schema_service, obrpc::ObCommonRpcProxy& rpc_proxy)
      : schema_service_(schema_service), rpc_proxy_(rpc_proxy), is_inited_(false)
  {}
  virtual ~ObDropTenantChecker()
  {}
  virtual int inspect(bool& passed, const char*& warning_info) override;
  virtual const char* get_task_name() const
  {
    return "drop_tenant_checker";
  }

private:
  int drop_tenant_force(const common::ObString& tenant_name);
  int record_log_archive_history_(const uint64_t tenant_id, const int64_t drop_schema_version,
      const int64_t drop_tenant_time, bool& is_delay_delete);

private:
  static const int64_t CHECK_DROP_TENANT_INTERVAL = 600 * 1000 * 1000L;  // 10min
  share::schema::ObMultiVersionSchemaService& schema_service_;
  obrpc::ObCommonRpcProxy& rpc_proxy_;
  bool is_inited_;
};

class ObForceDropSchemaChecker : public ObInspectionTask {
public:
  enum DropSchemaMode { DROP_SCHEMA_ONLY = 0, DROP_PARTITION_ONLY = 1, DROP_SCHEMA_AND_PARTITION = 2 };
  enum DropSchemaTaskMode {
    // Normal inspection mode
    NORMAL_MODE = 0,
    // compelete physical recovery, need to clear schemas in delayed deleted
    RESTORE_MODE = 1,
    // Failover need clear delayed deleted schema objects. But can not clear by RPC as DDL thread has been occupied.
    FAILOVER_MODE = 2,
  };

public:
  explicit ObForceDropSchemaChecker(ObRootService& root_service,
      share::schema::ObMultiVersionSchemaService& schema_service, obrpc::ObCommonRpcProxy& rpc_proxy,
      common::ObMySQLProxy& sql_proxy, const DropSchemaTaskMode task_mode)
      : root_service_(root_service),
        schema_service_(schema_service),
        rpc_proxy_(rpc_proxy),
        sql_proxy_(sql_proxy),
        task_mode_(task_mode)
  {}
  virtual ~ObForceDropSchemaChecker()
  {}
  virtual int inspect(bool& passed, const char*& warning_info) override;
  virtual const char* get_task_name() const
  {
    return "drop_schema_checker";
  }
  int force_drop_schema(const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt);
  int check_dropped_schema_exist(const uint64_t tenant_id, bool& exist);

private:
  bool schema_need_drop(const int64_t drop_schema_version, const int64_t recycle_schema_version);
  int get_dropped_partition_ids(const int64_t recycle_schema_version, const share::schema::ObPartitionSchema& schema,
      common::ObArray<int64_t>& dropped_partition_ids, common::ObArray<int64_t>& dropped_subpartition_ids);
  int get_existed_partition_ids(
      const uint64_t tenant_id, const uint64_t table_id, common::ObArray<int64_t>& partition_ids);
  int filter_partition_ids(const share::schema::ObPartitionSchema& partition_schema,
      const common::ObArray<int64_t>& dropped_partition_ids, const common::ObArray<int64_t>& dropped_subpartition_ids,
      const common::ObArray<int64_t>& existed_partition_ids, obrpc::ObForceDropSchemaArg& arg);
  int recycle_gc_partition_info(const obrpc::ObForceDropSchemaArg& arg);
  int get_recycle_schema_version(const uint64_t tenant_id, int64_t& recycle_schema_version);

  int force_drop_index(const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt);
  int force_drop_tablegroup_partitions(
      const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt);
  int force_drop_table_and_partitions(
      const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt);
  int force_drop_tablegroup(const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt);
  int force_drop_database(const uint64_t tenant_id, const int64_t recycle_schema_version, int64_t& task_cnt);

  int check_dropped_database_exist(const uint64_t tenant_id, bool& exist);
  int check_dropped_tablegroup_exist(const uint64_t tenant_id, bool& exist);
  int check_dropped_table_exist(const uint64_t tenant_id, bool& exist);

  int try_drop_schema(const share::schema::ObPartitionSchema& partition_schema, const int64_t recycle_schema_version,
      const DropSchemaMode mode, int64_t& task_cnt);

private:
  static const int BATCH_DROP_SCHEMA_NUM = 100;
  static const int64_t DROP_SCHEMA_IDLE_TIME = 100 * 1000;  // 100ms
private:
  ObRootService& root_service_;
  share::schema::ObMultiVersionSchemaService& schema_service_;
  obrpc::ObCommonRpcProxy& rpc_proxy_;
  common::ObMySQLProxy& sql_proxy_;
  DropSchemaTaskMode task_mode_;
};

////////////////////////////////////////////////////////////////
// Class I: regular inspection in the background
class ObInspector : public common::ObAsyncTimerTask {
public:
  const static int64_t INSPECT_INTERVAL = 600L * 1000L * 1000L;  // 600s
  explicit ObInspector(ObRootService& rs);
  virtual ~ObInspector()
  {}

  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(*this);
  }
  virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

private:
  ObRootService& rs_;
};

// Class I: purge recyclebin in the background
class ObPurgeRecyclebinTask : public common::ObAsyncTimerTask {
public:
  explicit ObPurgeRecyclebinTask(ObRootService& rs);
  virtual ~ObPurgeRecyclebinTask()
  {}

  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(*this);
  }
  virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

private:
  ObRootService& root_service_;
};

// Class I: purge recylebin in backgroud
class ObForceDropSchemaTask : public common::ObAsyncTimerTask {
public:
  explicit ObForceDropSchemaTask(ObRootService& rs);
  virtual ~ObForceDropSchemaTask()
  {}

  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override
  {
    return sizeof(*this);
  }
  virtual ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const override;

private:
  ObRootService& root_service_;
};

// class 2: trigger inspection by ALTER SYSTEM RUN JOB 'ROOT_INSPECTION'
class ObRootInspection : public ObInspectionTask {
public:
  ObRootInspection();
  virtual ~ObRootInspection();

  virtual int inspect(bool& passed, const char*& warning_info) override;
  virtual const char* get_task_name() const
  {
    return "sys_schema_checker";
  };

  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObZoneManager& zone_mgr,
      common::ObMySQLProxy& sql_proxy, obrpc::ObCommonRpcProxy* rpc_proxy = NULL);
  inline bool is_inited() const
  {
    return inited_;
  }
  void start()
  {
    stopped_ = false;
  }
  void stop()
  {
    stopped_ = true;
  }
  virtual int check_all();

  inline bool is_zone_passed() const
  {
    return zone_passed_;
  }
  inline bool is_sys_param_passed() const
  {
    return sys_param_passed_;
  }
  inline bool is_sys_stat_passed() const
  {
    return sys_stat_passed_;
  }
  inline bool is_sys_table_schema_passed() const
  {
    return sys_table_schema_passed_;
  }
  inline bool is_all_checked() const
  {
    return all_checked_;
  }
  inline bool is_all_passed() const
  {
    return all_passed_;
  }
  inline bool can_retry() const
  {
    return can_retry_ && !stopped_;
  }

  // return OB_SCHEMA_ERROR for table schema mismatch
  virtual int check_table_schema(const share::schema::ObTableSchema& hard_code_table);
  static int check_table_schema(
      const share::schema::ObTableSchema& hard_code_table, const share::schema::ObTableSchema& inner_table);
  int check_sys_table_schemas();

private:
  static const int64_t NAME_BUF_LEN = 64;
  typedef common::ObFixedLengthString<NAME_BUF_LEN> Name;
  int check_zone();
  int check_sys_stat();
  int check_sys_param();

  template <typename Item>
  int get_names(const common::ObDList<Item>& list, common::ObIArray<const char*>& names);
  int get_sys_param_names(common::ObIArray<const char*>& names);
  int check_and_insert_sys_params(uint64_t tenant_id, const char* table_name,
      const common::ObIArray<const char*>& names, const common::ObSqlString& extra_cond);
  int check_names(const uint64_t tenant_id, const char* table_name, const common::ObIArray<const char*>& names,
      const common::ObSqlString& extra_cond);
  int calc_diff_names(const uint64_t tenant_id, const char* table_name, const common::ObIArray<const char*>& names,
      const common::ObSqlString& extra_cond, common::ObIArray<Name>& fetch_names, /* data from inner table*/
      common::ObIArray<Name>& extra_names,                                        /* inner table more than hard code*/
      common::ObIArray<Name>& miss_names /* inner table less than hard code*/);
  static int check_table_options(
      const share::schema::ObTableSchema& table, const share::schema::ObTableSchema& hard_code_table);
  static int check_column_schema(const common::ObString& table_name, const share::schema::ObColumnSchemaV2& column,
      const share::schema::ObColumnSchemaV2& hard_code_column, const bool ignore_column_id);
  int get_schema_status(
      const uint64_t tenant_id, const char* table_name, share::schema::ObRefreshSchemaStatus& schema_status);
  int check_cancel();

private:
  bool inited_;
  volatile bool stopped_;

  bool zone_passed_;
  bool sys_param_passed_;
  bool sys_stat_passed_;
  bool sys_table_schema_passed_;

  bool all_checked_;
  bool all_passed_;
  bool can_retry_;  // only execute sql fail can retry
  common::ObMySQLProxy* sql_proxy_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObZoneManager* zone_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootInspection);
};

// virtual table __all_virtual_upgrade_inspection
// the result of ALTER SYSTEM RUN JOB 'ROOT_INSPECTION'
class ObUpgradeInspection : public common::ObVirtualTableProjector {
public:
  ObUpgradeInspection();
  virtual ~ObUpgradeInspection();

  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObRootInspection& root_inspection);
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  int get_full_row(
      const share::schema::ObTableSchema* table, const char* name, const char* info, common::ObIArray<Column>& columns);

  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObRootInspection* root_inspection_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObUpgradeInspection);
};

class ObPrimaryClusterInspection : public ObInspectionTask {
public:
  ObPrimaryClusterInspection()
      : inited_(false),
        schema_service_(nullptr),
        pt_operator_(nullptr),
        zone_mgr_(nullptr),
        sql_proxy_(nullptr),
        server_mgr_(nullptr)
  {}
  virtual ~ObPrimaryClusterInspection()
  {}

  virtual int inspect(bool& passed, const char*& warning_info) override;
  virtual const char* get_task_name() const
  {
    return "primary_checker";
  };

  int init(share::schema::ObMultiVersionSchemaService& schema_service, share::ObPartitionTableOperator& pt_operator,
      ObZoneManager& zone_mgr, common::ObMySQLProxy& sql_proxy, ObServerManager& server_mgr);
  inline bool is_inited() const
  {
    return inited_;
  }

private:
  int check_all_ddl_replay(bool& all_ddl_valid);
  int check_all_partition_created(bool& all_partition_created);
  int check_has_replica_in_standby_restore(bool& has_standby_restore_replica);

private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObPartitionTableOperator* pt_operator_;
  ObZoneManager* zone_mgr_;
  common::ObMySQLProxy* sql_proxy_;
  ObServerManager* server_mgr_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ROOT_INSPECTION_H_
