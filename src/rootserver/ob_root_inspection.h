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
#include "share/ob_virtual_table_projector.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"
#include "src/rootserver/ob_rs_async_rpc_proxy.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}

namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
}
}

namespace rootserver
{
class ObZoneManager;
class ObRootService;

// Interface of all the inspection task
class ObInspectionTask
{
public:
  ObInspectionTask() {}
  virtual ~ObInspectionTask() {}
  // do the inspection task
  virtual int inspect(bool &passed, const char* &warning_info) = 0;
  virtual const char* get_task_name() const = 0;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObInspectionTask);
};

class ObTenantChecker: public ObInspectionTask
{
  public:
    ObTenantChecker(share::schema::ObMultiVersionSchemaService &schema_service,
                    common::ObMySQLProxy &sql_proxy,
                    obrpc::ObCommonRpcProxy &rpc_proxy)
                   : schema_service_(&schema_service),
                     sql_proxy_(&sql_proxy),
                     rpc_proxy_(rpc_proxy) {}
    virtual ~ObTenantChecker() {}

    virtual int inspect(bool &passed, const char* &warning_info) override;
    virtual const char* get_task_name() const { return "tenant_checker"; };
  private:
    int alter_tenant_primary_zone_();
    int check_garbage_tenant_(bool &passed);
  private:
    share::schema::ObMultiVersionSchemaService *schema_service_;
    common::ObMySQLProxy *sql_proxy_;
    obrpc::ObCommonRpcProxy &rpc_proxy_;
};

class ObTableGroupChecker: public ObInspectionTask
{
public:
  explicit ObTableGroupChecker(share::schema::ObMultiVersionSchemaService &schema_service);
  virtual ~ObTableGroupChecker();
  int init();
  virtual int inspect(bool &passed, const char* &warning_info) override;
  virtual const char* get_task_name() const { return "tablegroup_checker"; };
private:
  int inspect_(const uint64_t tenant_id, bool &passed);
  int check_part_option(const share::schema::ObSimpleTableSchemaV2 &table,
                        share::schema::ObSchemaGetterGuard &schema_guard);
private:
  static const int TABLEGROUP_BUCKET_NUM = 1024;
  typedef common::hash::ObHashMap<uint64_t, const share::schema::ObSimpleTableSchemaV2*, common::hash::NoPthreadDefendMode> ObTableGroupCheckInfoMap;
  share::schema::ObMultiVersionSchemaService &schema_service_;
  ObTableGroupCheckInfoMap check_part_option_map_;
  common::hash::ObHashSet<uint64_t> part_option_not_match_set_;
  common::ObArenaAllocator allocator_;
  bool is_inited_;
};

////////////////////////////////////////////////////////////////
// Class I: regular inspection in the background
class ObInspector: public common::ObAsyncTimerTask
{
public:
  const static int64_t INSPECT_INTERVAL = 1500L * 1000L * 1000L;  //1500s
  explicit ObInspector(ObRootService &rs);
  virtual ~ObInspector() {}

  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  ObRootService &rs_;
};

// Class I: purge recyclebin in the background
class ObPurgeRecyclebinTask: public common::ObAsyncTimerTask
{
public:
  explicit ObPurgeRecyclebinTask(ObRootService &rs);
  virtual ~ObPurgeRecyclebinTask() {}

  virtual int process() override;
  virtual int64_t get_deep_copy_size() const override { return sizeof(*this); }
  virtual ObAsyncTask *deep_copy(char *buf, const int64_t buf_size) const override;
private:
  ObRootService &root_service_;
};

class ObInspectionCancelChecker
{
public:
  virtual int check_cancel() = 0;
  int operator() () { return check_cancel(); }
};

class ObInspectionCheckSelfRS : public ObInspectionCancelChecker
{
public:
  virtual int check_cancel() override;
};

class ObInspectionCheckRemoteRS : public ObInspectionCancelChecker
{
public:
  ObInspectionCheckRemoteRS(const obrpc::ObCheckSysTableSchemaArg &arg);
  virtual int check_cancel() override;
private:
  const obrpc::ObCheckSysTableSchemaArg &arg_;
  // to make hotfix can check epoch_id, we record the epoch_id when receiving the RPC
  int64_t origin_rs_epoch_id_;
};

class ObSysTableInspection
{
public:
  ObSysTableInspection(ObInspectionCancelChecker &checker) : check_cancel(checker) {}
  // return OB_SCHEMA_ERROR for table schema mismatch
  static int check_table_schema(const uint64_t tenant_id,
      const share::schema::ObTableSchema &hard_code_table);

  static int check_table_schema(const share::schema::ObTableSchema &hard_code_table,
                                const share::schema::ObTableSchema &inner_table);

  // For system tables, check and get column schemas' difference
  // between table schema in memory and hard code table schema.
  // 1. Drop column: Not supported.
  // 2. Add column: Can only add columns at last.
  // 3. Alter column: Can only alter columns online.
  static int check_and_get_system_table_column_diff(
             const share::schema::ObTableSchema &table_schema,
             const share::schema::ObTableSchema &hard_code_schema,
             common::ObIArray<uint64_t> &add_column_ids,
             common::ObIArray<uint64_t> &alter_column_ids);
  // !!! ATTENTION !!!
  // this function will return SUCCESS if inner table schema has error
  // remember to check result.error_table_ids to get all error tables
  static int check_sys_table_schema(const obrpc::ObCheckSysTableSchemaArg &arg,
      obrpc::ObCheckSysTableSchemaResult &result);
  // if OB_SCHEMA_ERROR, will add table_id in error_table_ids and ignore errcode
  int check_sys_table_schemas(const uint64_t tenant_id, ObIArray<uint64_t> &error_table_ids);
private:

  int check_single_table(const uint64_t tenant_id, const ObTableSchema &hard_code_table,
      ObIArray<uint64_t> &error_table_ids);

  static int check_table_options_(const share::schema::ObTableSchema &table,
                                  const share::schema::ObTableSchema &hard_code_table);
  static int check_column_schema_(const common::ObString &table_name,
                                  const share::schema::ObColumnSchemaV2 &column,
                                  const share::schema::ObColumnSchemaV2 &hard_code_column);
  int check_sys_view_(const uint64_t tenant_id,
                      const share::schema::ObTableSchema &hard_code_table);
  int check_tenant_status_(const uint64_t tenant_id);
  bool check_str_with_lower_case_(const ObString &str);
  void check_add_to_error_table_ids_(const int tmp_ret, const uint64_t &table_id,
      ObIArray<uint64_t> &error_table_ids, int &back_ret);
private:
  ObInspectionCancelChecker &check_cancel;
};

// class 2: trigger inspection by ALTER SYSTEM RUN JOB 'ROOT_INSPECTION'
class ObRootInspection: public ObInspectionTask
{
public:
  ObRootInspection();
  virtual ~ObRootInspection();

  virtual int inspect(bool &passed, const char* &warning_info) override;
  virtual const char* get_task_name() const { return "sys_schema_checker"; };

  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           ObZoneManager &zone_mgr, common::ObMySQLProxy &sql_proxy, obrpc::ObCommonRpcProxy *rpc_proxy = NULL);
  inline bool is_inited() const { return inited_; }
  void start() { stopped_ = false; }
  void stop() { stopped_ = true; }
  virtual int check_all();
  // only called in upgrade status.
  // due to performance issues, not check sys table schema
  // the caller should call check sys table schema manually
  int check_tenant_in_upgrade(const uint64_t tenant_id);

  static int check_tenant_status(const uint64_t tenant_id, ObMultiVersionSchemaService *schema_service);

  static bool need_ignore_error_message(const int64_t &tenant_id);

  inline bool is_zone_passed() const { return zone_passed_; }
  inline bool is_sys_param_passed() const { return sys_param_passed_; }
  inline bool is_sys_stat_passed() const { return sys_stat_passed_; }
  inline bool is_sys_table_schema_passed() const { return sys_table_schema_passed_; }
  inline bool is_data_version_passed() const { return data_version_passed_; }
  inline bool is_all_checked() const { return all_checked_; }
  inline bool is_all_passed() const { return all_passed_; }
  int check_sys_table_schemas(const ObIArray<uint64_t> &tenant_ids);
private:
  static const int64_t NAME_BUF_LEN = 64;
  typedef common::ObFixedLengthString<NAME_BUF_LEN> Name;
  int construct_tenant_ids_(common::ObIArray<uint64_t> &tenant_ids);
  int check_zone();
  int check_sys_stat_();
  int check_sys_stat_(const uint64_t tenant_id);
  int check_sys_param_();
  int check_sys_param_(const uint64_t tenant_id);

  template<typename Item>
  int get_names(const common::ObDList<Item> &list, common::ObIArray<const char*> &names);
  int get_sys_param_names(common::ObIArray<const char *> &names);
  int check_names(const uint64_t tenant_id,
                  const char *table_name,
                  const common::ObIArray<const char *> &names,
                  const common::ObSqlString &extra_cond);
  int calc_diff_names(const uint64_t tenant_id,
                      const char *table_name,
                      const common::ObIArray<const char *> &names,
                      const common::ObSqlString &extra_cond,
                      common::ObIArray<Name> &fetch_names, /* data from inner table*/
                      common::ObIArray<Name> &extra_names, /* inner table more than hard code*/
                      common::ObIArray<Name> &miss_names /* inner table less than hard code*/);

  int check_sys_table_schemas_();
  int check_sys_table_schemas_(const uint64_t tenant_ids);
  int check_sys_table_schema_(const uint64_t tenant_id,
      rootserver::ObCheckSysTableSchemaProxy &proxy);

  int wait_and_check_rpc_response(rootserver::ObCheckSysTableSchemaProxy &proxy);

  int check_error_table_ids_(const uint64_t tenant_id, const ObIArray<uint64_t> &table_ids);

  int check_data_version_();
  int check_data_version_(const uint64_t tenant_id);

  int check_cancel();
  int check_tenant_status_(const uint64_t tenant_id);
  static int check_in_compatibility_mode_(const int64_t &tenant_id, bool &in_compatibility_mode);
private:
  bool inited_;
  volatile bool stopped_;

  bool zone_passed_;
  bool sys_param_passed_;
  bool sys_stat_passed_;
  bool sys_table_schema_passed_;
  bool data_version_passed_;

  bool all_checked_;
  bool all_passed_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObCommonRpcProxy *rpc_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObZoneManager *zone_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootInspection);
};

// virtual table __all_virtual_upgrade_inspection
// the result of ALTER SYSTEM RUN JOB 'ROOT_INSPECTION'
class ObUpgradeInspection : public common::ObVirtualTableProjector
{
public:
  ObUpgradeInspection();
  virtual ~ObUpgradeInspection();

  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           ObRootInspection &root_inspection);
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int get_full_row(const share::schema::ObTableSchema *table,
                   const char *name, const char *info,
                   common::ObIArray<Column> &columns);

  bool inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObRootInspection *root_inspection_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUpgradeInspection);
};
}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_ROOT_INSPECTION_H_
