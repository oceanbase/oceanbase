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

#ifndef OB_SERVER_INNER_TABLE_MONITOR_H_
#define OB_SERVER_INNER_TABLE_MONITOR_H_

#include "lib/net/ob_addr.h"                       // tbsys
#include "lib/mysqlclient/ob_mysql_transaction.h"  // common::ObMySQLTransaction

namespace oceanbase {
namespace common {
class ObServerConfig;
class ObMySQLProxy;
}  // namespace common
namespace obrpc {
class ObCommonRpcProxy;
}

namespace rootserver {
class ObRootService;
class ObInnerTableMonitor {
public:
  ObInnerTableMonitor();
  ~ObInnerTableMonitor() = default;
  int init(common::ObMySQLProxy& sql_proxy, obrpc::ObCommonRpcProxy& rpc_proxy, ObRootService& root_service);
  int purge_inner_table_history();

private:
  const static int N_PARTITION_FOR_COLUMN_HISTORY = 16;
  const static int N_PARTITION_FOR_TABLE_HISTORY = 16;
  const static int N_PARTITION_FOR_TABLEGROUP_HISTORY = 16;
  const static int N_PARTITION_FOR_DATABASE_HISTORY = 16;
  const static int N_PARTITION_FOR_OUTLINE_HISTORY = 16;
  const static int N_PARTITION_FOR_TENANT_HISTORY = 1;
  const static int N_PARTITION_FOR_USER_HISTORY = 16;
  const static int N_PARTITION_FOR_TABLE_PRIVILEGE_HISTORY = 16;
  const static int N_PARTITION_FOR_DATABASE_PRIVILEGE_HISTORY = 16;
  const static int N_PARTITION_FOR_DDL_OPERATION = 1;

private:
  int check_inner_stat() const;

  int get_all_tenants_from_stats(common::ObIArray<uint64_t>& tenant_ids);

  int get_last_forzen_time(int64_t& last_frozen_time);

  int purge_inner_table_history(const uint64_t tenant_id, const int64_t last_frozen_time, int64_t& purged_rows);

  int purge_history(const char* tname, const int np, const uint64_t tenant_id, const int64_t schema_version,
      const char* prefix_pk, int64_t& purged_rows);

  int delete_history_for_partitions(const char* tname, const int np, const uint64_t tenant_id,
      const int64_t schema_version, const char* prefix_pk, int64_t& purged_rows, common::ObMySQLTransaction& trans);
  int purge_table_column_history(const uint64_t tid, const int64_t schema_version, int64_t& purged_rows);

  int purge_recyclebin_objects(common::ObIArray<uint64_t>& tenant_id);

  int check_cancel() const;

private:
  bool inited_;
  obrpc::ObCommonRpcProxy* rs_proxy_;
  common::ObMySQLProxy* sql_proxy_;
  ObRootService* root_service_;
};

}  // namespace rootserver
}  // namespace oceanbase
#endif
