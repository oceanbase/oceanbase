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

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_OPERATOR_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_OPERATOR_H_

#include "lib/net/ob_addr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/schema/ob_table_schema.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "ob_backup_struct.h"
#include "ob_backup_manager.h"

namespace oceanbase
{
namespace share
{

class ObITenantBackupTaskOperator
{
public:
  ObITenantBackupTaskOperator() = default;
  virtual ~ObITenantBackupTaskOperator() = default;
  static int get_tenant_ids(
      const uint64_t tenant_id,
      const common::ObSqlString &sql,
      common::ObIArray<uint64_t> &tenant_ids,
      common::ObISQLClient &sql_proxy);
};


class ObTenantBackupInfoOperation
{
public:
  static int update_info_item(common::ObISQLClient &sql_client,
      const uint64_t tenant_id, const ObBackupInfoItem &item);
  static int get_tenant_list(
      common::ObISQLClient &sql_client, common::ObIArray<uint64_t> &tenant_id_list);
  static int load_base_backup_info(common::ObISQLClient &sql_client, ObBaseBackupInfo &info);
  static int remove_base_backup_info(common::ObISQLClient &sql_client, const uint64_t tenant_id);
  static int load_info_item(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id, ObBackupInfoItem &item, const bool need_lock = true);
  static int get_backup_snapshot_version(common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id, int64_t &backup_snapshot_version);
  static int get_backup_schema_version(common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id, int64_t &backup_schema_version);
  static int get_tenant_name_backup_schema_version(common::ObISQLClient &sql_proxy,
      int64_t &backup_schema_version);
  static int update_tenant_name_backup_schema_version(common::ObISQLClient &sql_proxy,
      const int64_t backup_schema_version);
  static int insert_info_item(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObBackupInfoItem &item);
  static int clean_backup_scheduler_leader(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const common::ObAddr &scheduler_leader);
  static int remove_info_item(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const char *name);
private:
  template <typename T>
      static int set_info_item(const char *name, const char *info_str,
          T &info);
  template <typename T>
      static int load_info(common::ObISQLClient &sql_client, T &info);
  template <typename T>
      static int insert_info(common::ObISQLClient &sql_client, T &info);
  static int get_backup_info_item_count(int64_t &cnt);

};

  // TODO: merge ObBackupInfoOperator into ObTenantBackupInfoOperation.
class ObBackupInfoOperator final
{
public:
  static int get_inner_table_version(common::ObISQLClient &sql_proxy,
      ObBackupInnerTableVersion &version);
  static int set_inner_table_version(common::ObISQLClient &sql_proxy,
      const ObBackupInnerTableVersion &version);
  static int set_max_piece_id(common::ObISQLClient &sql_proxy,
      const int64_t max_piece_id);
  static int set_max_piece_create_date(common::ObISQLClient &sql_proxy,
      const int64_t max_piece_create_date);
  static int get_tenant_name_backup_schema_version(common::ObISQLClient &sql_proxy,
      int64_t &backup_schema_version);
  static int get_backup_leader(common::ObISQLClient &sql_proxy, const bool for_update,
      common::ObAddr &leader, bool &has_leader);
  static int set_backup_leader(common::ObISQLClient &sql_proxy,
      const common::ObAddr &leader);
  static int clean_backup_leader(common::ObISQLClient &sql_proxy,
      const common::ObAddr &leader);
  static int get_backup_leader_epoch(common::ObISQLClient &sql_proxy, const bool for_update, int64_t &epoch);
  static int set_backup_leader_epoch(common::ObISQLClient &sql_proxy, const int64_t epoch);
private:
  static int get_int_value_(common::ObISQLClient &sql_proxy, const bool for_update,
      const char *name, int64_t &value);
  static int set_int_value_(common::ObISQLClient &sql_proxy, const char *name, const int64_t &value);
  static int get_string_value_(common::ObISQLClient &sql_proxy, const bool for_update,
      const char *name, char *buf, int64_t buf_len);
  static int set_string_value_(common::ObISQLClient &sql_proxy, const char *name, const char *value);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_BACKUP_OB_BACKUP_OPERATOR_H_
