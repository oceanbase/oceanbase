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

#ifndef OCEANBASE_SHARE_UPGRADE_UTILS_H_
#define OCEANBASE_SHARE_UPGRADE_UTILS_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_check_stop_provider.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/ob_rs_job_table_operator.h"
#include "rootserver/ob_ddl_operator.h"

namespace oceanbase {
namespace share {

static const int64_t UPGRADE_JOB_TYPE_COUNT = 4;
static const rootserver::ObRsJobType upgrade_job_type_array[UPGRADE_JOB_TYPE_COUNT] = {
    rootserver::JOB_TYPE_INVALID,
    rootserver::JOB_TYPE_STATISTIC_PRIMARY_ZONE_ENTITY_COUNT,
    rootserver::JOB_TYPE_SCHEMA_SPLIT_V2,
    rootserver::JOB_TYPE_SCHEMA_REVISE,
};

class ObUpgradeUtils {
public:
  ObUpgradeUtils()
  {}
  virtual ~ObUpgradeUtils()
  {}
  static int create_tenant_tables(share::schema::ObMultiVersionSchemaService& schema_service, const uint64_t table_id);
  static int can_run_upgrade_job(rootserver::ObRsJobType job_type, bool& can);
  static int check_upgrade_job_passed(rootserver::ObRsJobType job_type);
  static int force_create_tenant_table(
      const uint64_t tenant_id, const uint64_t table_id, const uint64_t last_replay_log_id);
  static int create_tenant_table(
      const uint64_t tenant_id, share::schema::ObTableSchema& table_schema, bool in_sync, bool& allow_sys_create_table);
  static int check_schema_sync(bool& is_sync);
  /* physical restore related */
  // Can't call upgrade_sys_variable()/upgrade_sys_stat() concurrenly.
  static int upgrade_sys_variable(common::ObISQLClient& sql_client, const uint64_t tenant_id);
  static int upgrade_sys_stat(common::ObISQLClient& sql_client, const uint64_t tenant_id);
  /* ----------------------- */
private:
  static int check_table_exist(uint64_t table_id, bool& exist);
  static int check_table_partition_exist(uint64_t table_id, bool& exist);
  static int check_rs_job_exist(rootserver::ObRsJobType job_type, bool& exist);
  static int check_rs_job_success(rootserver::ObRsJobType job_type, bool& success);

  /* upgrade sys variable */
  static int calc_diff_sys_var(common::ObISQLClient& sql_client, const uint64_t tenant_id,
      common::ObArray<int64_t>& update_list, common::ObArray<int64_t>& add_list);
  static int update_sys_var(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, common::ObArray<int64_t>& update_list);
  static int add_sys_var(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, common::ObArray<int64_t>& add_list);
  static int execute_update_sys_var_sql(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, const share::schema::ObSysParam& sys_param);
  static int execute_update_sys_var_history_sql(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, const share::schema::ObSysParam& sys_param);
  static int execute_add_sys_var_sql(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, const share::schema::ObSysParam& sys_param);
  static int execute_add_sys_var_history_sql(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, const share::schema::ObSysParam& sys_param);
  static int convert_sys_variable_value(
      const int64_t var_store_idx, common::ObIAllocator& allocator, common::ObString& value);
  static int gen_basic_sys_variable_dml(
      const uint64_t tenant_id, const share::schema::ObSysParam& sys_param, share::ObDMLSqlSplicer& dml);
  /* upgrade sys variable end */
  static int filter_sys_stat(
      common::ObISQLClient& sql_client, const uint64_t tenant_id, rootserver::ObSysStat& sys_stat);

private:
  typedef common::ObFixedLengthString<OB_MAX_CONFIG_NAME_LEN> Name;
};

/* =========== upgrade processor ============= */

// Special upgrade actions for specific cluster version,
// which should be stateless and reentrant.
class ObBaseUpgradeProcessor {
public:
  enum UpgradeMode { UPGRADE_MODE_INVALID, UPGRADE_MODE_OB, UPGRADE_MODE_PHYSICAL_RESTORE };

public:
  ObBaseUpgradeProcessor();
  virtual ~ObBaseUpgradeProcessor(){};

public:
  int init(int64_t cluster_version, UpgradeMode mode, common::ObMySQLProxy& sql_proxy, obrpc::ObSrvRpcProxy& rpc_proxy,
      share::schema::ObMultiVersionSchemaService& schema_service, share::ObCheckStopProvider& check_server_provider);
  int64_t get_version() const
  {
    return cluster_version_;
  }
  void set_tenant_id(const uint64_t tenant_id)
  {
    tenant_id_ = tenant_id;
  }
  int64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  virtual int pre_upgrade() = 0;
  virtual int post_upgrade() = 0;
  TO_STRING_KV(K_(inited), K_(cluster_version), K_(tenant_id), K_(mode));

protected:
  virtual int check_inner_stat() const;

protected:
  bool inited_;
  int64_t cluster_version_;
  uint64_t tenant_id_;
  UpgradeMode mode_;
  common::ObMySQLProxy* sql_proxy_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObCheckStopProvider* check_stop_provider_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseUpgradeProcessor);
};

class ObUpgradeProcesserSet {
public:
  ObUpgradeProcesserSet();
  virtual ~ObUpgradeProcesserSet();
  int init(ObBaseUpgradeProcessor::UpgradeMode mode, common::ObMySQLProxy& sql_proxy, obrpc::ObSrvRpcProxy& rpc_proxy,
      share::schema::ObMultiVersionSchemaService& schema_service, share::ObCheckStopProvider& check_server_provider);
  int get_processor_by_idx(const int64_t idx, ObBaseUpgradeProcessor*& processor) const;
  int get_processor_by_version(const int64_t version, ObBaseUpgradeProcessor*& processor) const;
  int get_processor_idx_by_range(
      const int64_t start_version, const int64_t end_version, int64_t& start_idx, int64_t& end_idx);

private:
  virtual int check_inner_stat() const;
  int get_processor_idx_by_version(const int64_t version, int64_t& idx) const;

private:
  bool inited_;
  common::ObArenaAllocator allocator_;
  common::ObArray<ObBaseUpgradeProcessor*> processor_list_;
  DISALLOW_COPY_AND_ASSIGN(ObUpgradeProcesserSet);
};

#define DEF_SIMPLE_UPGRARD_PROCESSER(MAJOR, MINOR, PATCH)                              \
  class ObUpgradeFor##MAJOR##MINOR##PATCH##Processor : public ObBaseUpgradeProcessor { \
  public:                                                                              \
    ObUpgradeFor##MAJOR##MINOR##PATCH##Processor() : ObBaseUpgradeProcessor()          \
    {}                                                                                 \
    virtual ~ObUpgradeFor##MAJOR##MINOR##PATCH##Processor()                            \
    {}                                                                                 \
    virtual int pre_upgrade() override                                                 \
    {                                                                                  \
      return common::OB_SUCCESS;                                                       \
    }                                                                                  \
    virtual int post_upgrade() override                                                \
    {                                                                                  \
      return common::OB_SUCCESS;                                                       \
    }                                                                                  \
  };

/*
 * NOTE: The Following code should be modified when CLUSTER_CURRENT_VERSION changed.
 * 1. ObUpgradeChecker: CLUTER_VERSION_NUM, UPGRADE_PATH
 * 2. Implement new ObUpgradeProcessor by cluster version.
 * 3. Modify int ObUpgradeProcesserSet::init().
 */
class ObUpgradeChecker {
public:
  static bool check_cluster_version_exist(const uint64_t version);

public:
  static const int64_t CLUTER_VERSION_NUM = 32;
  static const uint64_t UPGRADE_PATH[CLUTER_VERSION_NUM];
};

// 2.2.60
DEF_SIMPLE_UPGRARD_PROCESSER(2, 2, 60);
// 2.2.70
class ObUpgradeFor2270Processor : public ObBaseUpgradeProcessor {
public:
  ObUpgradeFor2270Processor() : ObBaseUpgradeProcessor()
  {}
  virtual ~ObUpgradeFor2270Processor()
  {}
  virtual int pre_upgrade() override;
  virtual int post_upgrade() override;

private:
  int modify_trigger_package_source_body();
  int modify_oracle_public_database_name();

  int rebuild_subpart_partition_gc_info();
  int batch_replace_partition_gc_info(const common::ObIArray<common::ObPartitionKey>& keys);
};
// 2.2.71
DEF_SIMPLE_UPGRARD_PROCESSER(2, 2, 71);
// 2.2.72
DEF_SIMPLE_UPGRARD_PROCESSER(2, 2, 72);
// 2.2.73
DEF_SIMPLE_UPGRARD_PROCESSER(2, 2, 73);
// 2.2.74
DEF_SIMPLE_UPGRARD_PROCESSER(2, 2, 74);

class ObUpgradeFor2275Processor : public ObBaseUpgradeProcessor {
public:
  ObUpgradeFor2275Processor() : ObBaseUpgradeProcessor()
  {}
  virtual ~ObUpgradeFor2275Processor()
  {}
  virtual int pre_upgrade() override
  {
    return common::OB_SUCCESS;
  };
  virtual int post_upgrade() override;
};

// 2.2.76
DEF_SIMPLE_UPGRARD_PROCESSER(2, 2, 76);
// 3.1.0
DEF_SIMPLE_UPGRARD_PROCESSER(3, 1, 0);

/* =========== upgrade processor end ============= */

}  // end namespace share
}  // end namespace oceanbase
#endif /* _OB_UPGRADE_UTILS_H */
