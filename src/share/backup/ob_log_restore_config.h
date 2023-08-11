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

#ifndef OCEANBASE_SHARE_BACKUP_OB_LOG_RESTORE_CONFIG_H_
#define OCEANBASE_SHARE_BACKUP_OB_LOG_RESTORE_CONFIG_H_

#include <utility>
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "ob_backup_struct.h"
#include "ob_backup_config.h"
#include "ob_log_restore_struct.h"

namespace oceanbase
{
namespace share
{
class ObLogArchiveDestConfigParser;
class ObBackupConfigType;
class ObIBackupConfigItemParser;
class ObLogRestoreProxyUtil;

class ObLogRestoreSourceLocationConfigParser : public ObLogArchiveDestConfigParser
{
public:
  ObLogRestoreSourceLocationConfigParser(const ObBackupConfigType::Type &type, const uint64_t tenant_id, const int64_t dest_no)
    : ObLogArchiveDestConfigParser(type, tenant_id, dest_no) {}
  virtual ~ObLogRestoreSourceLocationConfigParser() {}
  virtual int update_inner_config_table(common::ObISQLClient &trans) override;
  virtual int check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans) override;

protected:
  virtual int do_parse_sub_config_(const common::ObString &config_str) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreSourceLocationConfigParser);
};

class ObLogRestoreSourceServiceConfigParser : public ObIBackupConfigItemParser
{
public:
  ObLogRestoreSourceServiceConfigParser(const ObBackupConfigType::Type &type, const uint64_t tenant_id)
    : ObIBackupConfigItemParser(type, tenant_id) {}
  virtual ~ObLogRestoreSourceServiceConfigParser() {}
  virtual int parse_from(const common::ObSqlString &value) override;
  virtual int update_inner_config_table(common::ObISQLClient &trans) override;
  virtual int check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans) override;
  int check_before_update_inner_config(
      const bool for_verify,
      ObCompatibilityMode &compat_mode);
  virtual int get_compatibility_mode(common::ObCompatibilityMode &compatibility_mode);
  int get_primary_server_addr(const common::ObSqlString &value,
  uint64_t &primary_tenant_id, uint64_t &primary_cluster_id,
  ObIArray<common::ObAddr> &addr_list);
private:
  int do_parse_sub_config_(const common::ObString &config_str);
  int do_parse_restore_service_host_(const common::ObString &name, const common::ObString &value);
  int do_parse_restore_service_user_(const common::ObString &name, const common::ObString &value);
  int do_parse_restore_service_passwd_(const common::ObString &name, const common::ObString &value);
  int check_doing_service_restore_(common::ObISQLClient &trans, bool &is_doing);
  int update_data_backup_dest_config_(common::ObISQLClient &trans);
  int construct_restore_sql_proxy_(ObLogRestoreProxyUtil &log_restore_proxy);
private:
  ObRestoreSourceServiceAttr service_attr_;
  bool is_empty_;
  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreSourceServiceConfigParser);
};

}//share
}//oceanbase

#endif /* OCEANBASE_SHARE_BACKUP_OB_LOG_RESTORE_CONFIG_H_ */
