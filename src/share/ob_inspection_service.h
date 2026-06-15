/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_INSPECTION_SERVICE_H
#define OCEANBASE_SHARE_OB_INSPECTION_SERVICE_H

#include "lib/list/ob_dlist.h"
#include "lib/container/ob_iarray.h"
#include "lib/lock/ob_mutex.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObColumnSchemaV2;
}

class ObInspectionCancelChecker
{
public:
  virtual int check_cancel() = 0;
  int operator() () { return check_cancel(); }
};

class ObInspectionCancelCheckerMTL : public ObInspectionCancelChecker
{
public:
  virtual int check_cancel() override;
};

class ObInspectionCancelCheckerSelfRS : public ObInspectionCancelChecker
{
public:
  virtual int check_cancel() override;
};

class ObInspectionCancelCheckerRemoteRS : public ObInspectionCancelChecker
{
public:
  ObInspectionCancelCheckerRemoteRS(const obrpc::ObCheckSysTableSchemaArg &arg);
  virtual int check_cancel() override;
private:
  const obrpc::ObCheckSysTableSchemaArg &arg_;
  // to make hotfix can check epoch_id, we record the epoch_id when receiving the RPC
  int64_t origin_rs_epoch_id_;
};

class ObInspector
{
private:
  static const int64_t NAME_BUF_LEN = 64;
  typedef common::ObFixedLengthString<NAME_BUF_LEN> Name;
public:
  ObInspector(const uint64_t tenant_id, ObInspectionCancelChecker &check_cancel)
    : tenant_id_(tenant_id), check_cancel(check_cancel) {}
  virtual ~ObInspector() {}
  int check_sys_stat();
  int check_sys_param();
  int check_sys_table_schemas();
  int check_sys_table_schemas(common::ObIArray<uint64_t> &error_table_ids);
  int check_data_version();
  static int check_tenant_status(const uint64_t tenant_id);
  template<typename Item>
  static int get_names(
    const common::ObDList<Item> &list,
    common::ObIArray<const char*> &names);
  static int check_names(
    const uint64_t tenant_id,
    const char *table_name,
    const common::ObIArray<const char *> &names,
    const common::ObSqlString &extra_cond);
  static int check_column_schema(
    const common::ObString &table_name,
    const schema::ObColumnSchemaV2 &column,
    const schema::ObColumnSchemaV2 &hard_code_column);
  static int check_error_table_ids(
    const uint64_t tenant_id,
    const common::ObIArray<uint64_t> &error_table_ids);
  static bool need_ignore_error_message(const uint64_t tenant_id);
  static int check_single_table(
    const uint64_t tenant_id,
    const schema::ObTableSchema &hard_code_table,
    common::ObIArray<uint64_t> &error_table_ids);
  static int check_table_schema(
    const uint64_t tenant_id,
    const schema::ObTableSchema &hard_code_table);
  static int check_table_schema(
    const share::schema::ObTableSchema &hard_code_table,
    const share::schema::ObTableSchema &inner_table);
private:
  static int get_sys_param_names(common::ObIArray<const char *> &names);
  static int calc_diff_names(
    const uint64_t tenant_id,
    const char *table_name,
    const common::ObIArray<const char *> &names,
    const common::ObSqlString &extra_cond,
    common::ObIArray<Name> &fetch_names, /* data from inner table*/
    common::ObIArray<Name> &extra_names, /* inner table more than hard code*/
    common::ObIArray<Name> &miss_names /* inner table less than hard code*/);
  static int check_in_compatibility_mode(
    const uint64_t tenant_id,
    bool &in_compatibility_mode);
  static int check_table_options(
    const share::schema::ObTableSchema &table,
    const share::schema::ObTableSchema &hard_code_table);
  static int check_sys_view(
    const uint64_t tenant_id,
    const schema::ObTableSchema &hard_code_table);
  static bool check_str_with_lower_case(const ObString &str);
private:
  const uint64_t tenant_id_;
  ObInspectionCancelChecker &check_cancel;
};

// Tenant MTL service for inspection results/execution.
class ObInspectionService
{
public:
  static int mtl_init(ObInspectionService *&inspection_service);

  ObInspectionService();
  ~ObInspectionService() {}

  int run_inspection();
  bool is_sys_stat_passed() const;
  bool is_sys_param_passed() const;
  bool is_sys_table_schema_passed() const;
  bool is_data_version_passed() const;
  bool is_all_checked() const;
  int init();
  void destroy();
private:
  bool is_inited_;
  lib::ObMutex mutex_; // guard against concurrent run_inspection
  bool sys_stat_passed_;
  bool sys_param_passed_;
  bool sys_table_schema_passed_;
  bool data_version_passed_;
  bool all_checked_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_INSPECTION_SERVICE_H
