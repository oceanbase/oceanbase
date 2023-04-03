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

#include "lib/net/ob_addr.h" // tbsys
#include "lib/mysqlclient/ob_mysql_transaction.h" // common::ObMySQLTransaction

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}
namespace obrpc
{
class ObCommonRpcProxy;
}

namespace rootserver
{
class ObRootService;
class ObInnerTableMonitor
{
public:
  ObInnerTableMonitor();
  ~ObInnerTableMonitor() = default;
  int init(common::ObMySQLProxy &sql_proxy,
           obrpc::ObCommonRpcProxy &rpc_proxy,
           ObRootService &root_service);
  int purge_inner_table_history();
private:
  int check_inner_stat() const;

  int get_all_tenants_from_stats(
      common::ObIArray<uint64_t> &tenant_ids);

  int purge_recyclebin_objects(common::ObIArray<uint64_t> &tenant_id);

  int check_cancel() const;
private:
  bool inited_;
  obrpc::ObCommonRpcProxy *rs_proxy_;
  common::ObMySQLProxy *sql_proxy_;
  ObRootService *root_service_;
};

}
}
#endif
