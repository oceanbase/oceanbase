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

namespace oceanbase
{
namespace share
{

static const int64_t UPGRADE_JOB_TYPE_COUNT = 1;
static const rootserver::ObRsJobType upgrade_job_type_array[UPGRADE_JOB_TYPE_COUNT] = {
  rootserver::JOB_TYPE_INVALID,
};

class ObUpgradeUtils
{
public:
  ObUpgradeUtils() {}
  virtual ~ObUpgradeUtils() {}
  static int can_run_upgrade_job(rootserver::ObRsJobType job_type, bool &can);
  static int check_upgrade_job_passed(rootserver::ObRsJobType job_type);
  static int check_schema_sync(const uint64_t tenant_id, bool &is_sync);
  // upgrade_sys_variable()/upgrade_sys_stat() can be called when enable_ddl = false.
  static int upgrade_sys_variable(
             obrpc::ObCommonRpcProxy &rpc_proxy,
             common::ObISQLClient &sql_client,
             const uint64_t tenant_id);
  static int upgrade_sys_stat(common::ObISQLClient &sql_client, const uint64_t tenant_id);
  /* ----------------------- */
private:
  static int check_rs_job_exist(rootserver::ObRsJobType job_type, bool &exist);
  static int check_rs_job_success(rootserver::ObRsJobType job_type, bool &success);

  /* upgrade sys variable */
  static int calc_diff_sys_var_(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      common::ObArray<int64_t> &update_list,
      common::ObArray<int64_t> &add_list);
  static int update_sys_var_(
             obrpc::ObCommonRpcProxy &rpc_proxy,
             const uint64_t tenant_id,
             const bool is_update,
             common::ObArray<int64_t> &update_list);
  /* upgrade sys variable end */
  static int filter_sys_stat(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      rootserver::ObSysStat &sys_stat);
private:
  typedef common::ObFixedLengthString<OB_MAX_CONFIG_NAME_LEN> Name;
};

/* =========== upgrade processor ============= */

// Special upgrade actions for specific data version,
// which should be stateless and reentrant.
class ObBaseUpgradeProcessor
{
public:
  enum UpgradeMode {
    UPGRADE_MODE_INVALID,
    UPGRADE_MODE_OB,
    UPGRADE_MODE_PHYSICAL_RESTORE
  };
public:
  ObBaseUpgradeProcessor();
  virtual ~ObBaseUpgradeProcessor() {};
public:
  int init(int64_t data_version,
           UpgradeMode mode,
           common::ObMySQLProxy &sql_proxy,
           common::ObOracleSqlProxy &oracle_sql_proxy,
           obrpc::ObSrvRpcProxy &rpc_proxy,
           obrpc::ObCommonRpcProxy &common_proxy,
           share::schema::ObMultiVersionSchemaService &schema_service,
           share::ObCheckStopProvider &check_server_provider);
  int64_t get_version() const { return data_version_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  int64_t get_tenant_id() const { return tenant_id_; }
  virtual int pre_upgrade() = 0;
  virtual int post_upgrade() = 0;
  TO_STRING_KV(K_(inited), K_(data_version), K_(tenant_id), K_(mode));
protected:
  virtual int check_inner_stat() const;
protected:
  bool inited_;
  int64_t data_version_;
  uint64_t tenant_id_;
  UpgradeMode mode_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObOracleSqlProxy *oracle_sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  obrpc::ObCommonRpcProxy *common_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObCheckStopProvider *check_stop_provider_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseUpgradeProcessor);
};

class ObUpgradeProcesserSet
{
public:
  ObUpgradeProcesserSet();
  virtual ~ObUpgradeProcesserSet();
  int init(ObBaseUpgradeProcessor::UpgradeMode mode,
           common::ObMySQLProxy &sql_proxy,
           common::ObOracleSqlProxy &oracle_sql_proxy,
           obrpc::ObSrvRpcProxy &rpc_proxy,
           obrpc::ObCommonRpcProxy &common_proxy,
           share::schema::ObMultiVersionSchemaService &schema_service,
           share::ObCheckStopProvider &check_server_provider);
  int get_processor_by_idx(const int64_t idx,
                           ObBaseUpgradeProcessor *&processor) const;
  int get_processor_by_version(const int64_t version,
                               ObBaseUpgradeProcessor *&processor) const;
  int get_processor_idx_by_range(const int64_t start_version,
                                 const int64_t end_version,
                                 int64_t &start_idx,
                                 int64_t &end_idx);
private:
  virtual int check_inner_stat() const;
  int get_processor_idx_by_version(
      const int64_t version,
      int64_t &idx) const;
private:
  bool inited_;
  common::ObArenaAllocator allocator_;
  common::ObArray<ObBaseUpgradeProcessor *> processor_list_;
  DISALLOW_COPY_AND_ASSIGN(ObUpgradeProcesserSet);
};

#define DEF_SIMPLE_UPGRARD_PROCESSER(MAJOR, MINOR, MAJOR_PATCH, MINOR_PATCH) \
class ObUpgradeFor##MAJOR##MINOR##MAJOR_PATCH##MINOR_PATCH##Processor : public ObBaseUpgradeProcessor \
{ \
public: \
  ObUpgradeFor##MAJOR##MINOR##MAJOR_PATCH##MINOR_PATCH##Processor() : ObBaseUpgradeProcessor() {} \
  virtual ~ObUpgradeFor##MAJOR##MINOR##MAJOR_PATCH##MINOR_PATCH##Processor() {} \
  virtual int pre_upgrade() override { return common::OB_SUCCESS; } \
  virtual int post_upgrade() override { return common::OB_SUCCESS; } \
};

/*
 * NOTE: The Following code should be modified when DATA_CURRENT_VERSION changed.
 * 1. ObUpgradeChecker: DATA_VERSION_NUM, UPGRADE_PATH
 * 2. Implement new ObUpgradeProcessor by data_version.
 * 3. Modify int ObUpgradeProcesserSet::init().
 */
class ObUpgradeChecker
{
public:
  static bool check_data_version_exist(const uint64_t version);
  static bool check_data_version_valid_for_backup(const uint64_t data_version);
  static bool check_cluster_version_exist(const uint64_t version);
  static int get_data_version_by_cluster_version(
             const uint64_t cluster_version,
             uint64_t &data_version);
public:
  static const int64_t DATA_VERSION_NUM = 19;
  static const uint64_t UPGRADE_PATH[];
};

/* =========== special upgrade processor start ============= */
DEF_SIMPLE_UPGRARD_PROCESSER(4, 0, 0, 0)

class ObUpgradeFor4100Processor : public ObBaseUpgradeProcessor
{
public:
  ObUpgradeFor4100Processor() : ObBaseUpgradeProcessor() {}
  virtual ~ObUpgradeFor4100Processor() {}
  virtual int pre_upgrade() override { return common::OB_SUCCESS; }
  virtual int post_upgrade() override;
private:
  int post_upgrade_for_srs();
  int post_upgrade_for_backup();
  int init_rewrite_rule_version(const uint64_t tenant_id);
  static int recompile_all_views_and_synonyms(const uint64_t tenant_id);
};

DEF_SIMPLE_UPGRARD_PROCESSER(4, 1, 0, 1)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 1, 0, 2)

class ObUpgradeFor4200Processor : public ObBaseUpgradeProcessor
{
public:
  ObUpgradeFor4200Processor() : ObBaseUpgradeProcessor() {}
  virtual ~ObUpgradeFor4200Processor() {}
  virtual int pre_upgrade() override { return common::OB_SUCCESS; }
  virtual int post_upgrade() override;
private:
  int post_upgrade_for_grant_create_database_link_priv();
  int post_upgrade_for_grant_drop_database_link_priv();
  int post_upgrade_for_heartbeat_and_server_zone_op_service();
  int post_upgrade_for_max_ls_id_();

};

DEF_SIMPLE_UPGRARD_PROCESSER(4, 2, 1, 0)
class ObUpgradeFor4211Processor : public ObBaseUpgradeProcessor
{
public:
  ObUpgradeFor4211Processor() : ObBaseUpgradeProcessor() {}
  virtual ~ObUpgradeFor4211Processor() {}
  virtual int pre_upgrade() override { return common::OB_SUCCESS; }
  virtual int post_upgrade() override;
private:
  int post_upgrade_for_dbms_scheduler();

};
DEF_SIMPLE_UPGRARD_PROCESSER(4, 2, 1, 2)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 2, 2, 0)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 2, 2, 1)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 2, 3, 0)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 2, 3, 1)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 2, 4, 0)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 2, 5, 0)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 3, 0, 0)
DEF_SIMPLE_UPGRARD_PROCESSER(4, 3, 0, 1)

class ObUpgradeFor4310Processor : public ObBaseUpgradeProcessor
{
public:
  ObUpgradeFor4310Processor() : ObBaseUpgradeProcessor() {}
  virtual ~ObUpgradeFor4310Processor() {}
  virtual int pre_upgrade() override { return common::OB_SUCCESS; }
  virtual int post_upgrade() override;
private:
  int post_upgrade_for_create_replication_role_in_oracle();
};

class ObUpgradeFor4320Processor : public ObBaseUpgradeProcessor
{
public:
  ObUpgradeFor4320Processor() : ObBaseUpgradeProcessor() {}
  virtual ~ObUpgradeFor4320Processor() {}
  virtual int pre_upgrade() override { return common::OB_SUCCESS; }
  virtual int post_upgrade() override;
private:
  int post_upgrade_for_reset_compat_version();
  int try_reset_version(const uint64_t tenant_id, const char *var_name);
  int post_upgrade_for_spm();
};

DEF_SIMPLE_UPGRARD_PROCESSER(4, 3, 3, 0)
/* =========== special upgrade processor end   ============= */

/* =========== upgrade processor end ============= */

} // end namespace share
} // end namespace oceanbase
#endif /* _OB_UPGRADE_UTILS_H */
