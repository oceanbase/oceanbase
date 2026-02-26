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

#ifndef OCEANBASE_ROOTSERVER_OB_BACKUP_PARAM_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_BACKUP_PARAM_OPERATOR_H_

#include "lib/mysqlclient/ob_isql_client.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_data_store.h"

namespace oceanbase {
namespace backup {
class ObBackupParamOperator final
{
public:
  //sys tenant get cluster parameters, user tenant get tenant parameters.
  static int get_backup_parameters_info(const uint64_t tenant_id, ObExternParamInfoDesc &param_info,
    common::ObISQLClient &sql_client);
  // observer call to backup cluster param
  static int backup_cluster_parameters(const share::ObBackupPathString &backup_dest_str);
private:
  const static char *TENANT_BLACK_PARAMETER_LIST[];
  const static char *CLUSTER_BLACK_PARAMETER_LIST[];
  static int construct_tenant_param_sql_(const uint64_t tenant_id, common::ObSqlString &sql);
  static int construct_cluster_param_sql_(common::ObSqlString &sql);
  static int construct_query_sql_(const uint64_t tenant_id, common::ObSqlString &sql);
  static int handle_one_result_(ObExternParamInfoDesc &param_info,
    common::sqlclient::ObMySQLResult &result);
};
}  // namespace backup
}  // namespace oceanbase

#endif
